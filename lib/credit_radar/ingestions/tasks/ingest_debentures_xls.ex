defmodule CreditRadar.Ingestions.Tasks.IngestDebenturesXls do
  @moduledoc """
  Ingestion task for Debentures from XLS file.

  This module provides an alternative ingestion method to the Anbima API,
  reading data from an Excel file exported from Anbima's website.

  ## XLS File Structure

  Expected columns (based on Anbima's Debentures export):
  - A: Data de referência (reference_date)
  - B: Código (code)
  - C: Emissor (issuer)
  - D: Tipo Remuneração (correction_rate_type)
  - E: Remuneração (correction_rate)
  - F: Data de vencimento (maturity_date)
  - G: Taxa de compra (buy_rate)
  - H: Taxa de venda (sell_rate)
  - I: Taxa indicativa (coupon_rate)
  - J: PU Indicativo
  - K: Desvio padrão
  - L: Intervalo indicativo mínimo
  - M: Intervalo indicativo máximo
  - N: % PU par
  - O: % VNE
  - P: Duration (dias úteis) (duration)
  - Q: % Reúne
  - R: Referência NTN-B (ntnb_reference)
  - S: Z-Spread
  - T: VNA
  - U: PU Par

  ## Usage

      # With execution tracking
      IngestDebenturesXls.run(%Execution{id: 123}, "priv/debentures_export.xls")

      # Without execution tracking
      IngestDebenturesXls.run(nil, "priv/debentures_export.xls")

      # Process file and return operations without persisting
      {:ok, operations} = IngestDebenturesXls.parse_file("priv/debentures_export.xls")
  """

  use Task, restart: :transient

  require Logger

  alias CreditRadar.FixedIncome
  alias CreditRadar.FixedIncome.Security
  alias CreditRadar.Ingestions
  alias CreditRadar.Ingestions.Execution
  alias CreditRadar.Repo
  alias Ecto.Changeset
  alias Decimal

  import SweetXml

  # Define CSV parser using NimbleCSV
  NimbleCSV.define(CreditRadar.Ingestions.Tasks.IngestDebenturesXls.CSVParser, separator: ",", escape: "\"")

  def start_link({execution, file_path}) do
    Task.start_link(__MODULE__, :run, [execution, file_path])
  end

  # Size of each batch to process (to avoid OOM issues with large files)
  @batch_size 250

  @doc """
  Executes the Debentures XLS ingestion pipeline.

  Converts XLSX to CSV first (handles inline strings properly), then processes in batches.
  """
  def run(execution \\ nil, file_path) do
    Logger.info(
      "Starting Debentures XLS ingestion from #{file_path} for execution #{execution_id(execution)}"
    )

    result =
      with {:ok, csv_path} <- convert_xlsx_to_csv(file_path),
           {:ok, stats} <- parse_and_persist_csv_in_batches(csv_path, execution) do
        # Clean up CSV file
        File.rm(csv_path)

        Logger.info("✅ Debentures XLS ingestion completed successfully: #{inspect(stats)}")
        _ = report_intermediate_progress(execution, 100)
        {:ok, stats}
      else
        {:error, reason} = error ->
          Logger.error("❌ Debentures XLS ingestion failed: #{inspect(reason)}")
          error
      end

    result
  end

  @doc """
  Converts XLSX to CSV using Python script.

  This is far more memory-efficient than parsing XML inline strings.
  """
  defp convert_xlsx_to_csv(xlsx_path) do
    csv_path = xlsx_path <> ".csv"
    script_path = Path.join([:code.priv_dir(:credit_radar), "scripts", "xlsx_to_csv.py"])

    Logger.info("Converting XLSX to CSV: #{xlsx_path} -> #{csv_path}")
    Logger.info("Using script: #{script_path}")

    case System.cmd("python3", [script_path, xlsx_path, csv_path], stderr_to_stdout: true) do
      {output, 0} ->
        Logger.info("✅ XLSX converted to CSV successfully: #{output}")
        {:ok, csv_path}

      {error_output, exit_code} ->
        Logger.error("❌ Failed to convert XLSX to CSV (exit code: #{exit_code})")
        Logger.error("Error output: #{error_output}")
        {:error, {:conversion_failed, error_output}}
    end
  rescue
    error ->
      Logger.error("❌ Exception converting XLSX to CSV: #{inspect(error)}")
      {:error, {:conversion_exception, error}}
  end

  @doc """
  Parses and persists CSV file in batches to avoid OOM issues.

  CSV is much more memory-efficient than XLSX XML parsing.
  """
  defp parse_and_persist_csv_in_batches(csv_path, execution) do
    alias CreditRadar.Ingestions.Tasks.IngestDebenturesXls.CSVParser

    unless File.exists?(csv_path) do
      {:error, :file_not_found}
    else
      try do
        # Count total rows for progress tracking (first pass)
        total_rows =
          csv_path
          |> File.stream!()
          |> Enum.count()

        Logger.info("Found #{total_rows} rows in CSV (including header)")

        # Stream and process in batches (second pass)
        stats =
          csv_path
          |> File.stream!()
          |> CSVParser.parse_stream(skip_headers: false)
          |> Stream.drop(1)  # Skip header
          |> Stream.with_index(2)  # Start at row 2 (1 is header)
          |> Stream.chunk_every(@batch_size)
          |> Stream.with_index(1)
          |> Enum.reduce(%{created: 0, updated: 0, skipped: 0, errors: []}, fn {batch, batch_num}, acc ->
            batch_start = (batch_num - 1) * @batch_size + 2
            batch_end = batch_start + length(batch) - 1

            Logger.info(
              "Processing batch #{batch_num}: rows #{batch_start}-#{batch_end} of #{total_rows}"
            )

            # Parse batch
            operations =
              batch
              |> Enum.map(fn {row, row_index} -> parse_csv_row(row, row_index) end)
              |> Enum.reject(&is_nil/1)

            Logger.info("Batch #{batch_num}: parsed #{length(operations)} operations")

            # Persist batch
            batch_stats = persist_batch(operations)

            # Update progress
            progress = min(100, div(batch_end * 100, total_rows))
            _ = report_intermediate_progress(execution, progress)

            # Merge stats
            merged = %{
              created: acc.created + batch_stats.created,
              updated: acc.updated + batch_stats.updated,
              skipped: acc.skipped + batch_stats.skipped,
              errors: acc.errors ++ batch_stats.errors
            }

            # Force garbage collection after each batch to free memory
            :erlang.garbage_collect()

            Logger.info(
              "Batch #{batch_num} complete - created: #{batch_stats.created}, updated: #{batch_stats.updated}, skipped: #{batch_stats.skipped}, errors: #{length(batch_stats.errors)}"
            )

            merged
          end)

        Logger.info(
          "Debentures CSV ingestion completed - Total: created=#{stats.created}, updated=#{stats.updated}, skipped=#{stats.skipped}, errors=#{length(stats.errors)}"
        )

        case stats.errors do
          [] -> {:ok, stats}
          errors -> {:error, {:persistence_failed, errors}}
        end
      rescue
        error ->
          Logger.error("Failed to parse CSV file: #{inspect(error)}")
          {:error, {:parse_error, error}}
      end
    end
  end

  @doc """
  Parses and persists the XLS/XLSX file in batches to avoid OOM issues.

  NOTE: This function is deprecated in favor of CSV-based parsing.
  Kept for backward compatibility.
  """
  defp parse_and_persist_file_in_batches(file_path, execution) do
    unless File.exists?(file_path) do
      {:error, :file_not_found}
    else
      try do
        # Read inlineStr cells using custom XML parser (this is memory-efficient as it's just metadata)
        Logger.info("Extracting inline string cells from XLSX...")
        inline_str_data = extract_inline_str_cells(file_path)

        # Read file with xlsxir
        Logger.info("Opening XLSX file for batch processing...")
        {:ok, pid} = Xlsxir.multi_extract(file_path, 0)

        # Get total row count for progress tracking
        rows = Xlsxir.get_list(pid)
        total_rows = length(rows) - 1  # Exclude header
        Logger.info("Total rows to process: #{total_rows}")

        # Process in batches to avoid memory issues
        stats =
          rows
          # Skip header row
          |> Enum.drop(1)
          # Start from row 2 (after header)
          |> Stream.with_index(2)
          # Process in chunks
          |> Stream.chunk_every(@batch_size)
          |> Stream.with_index(1)
          |> Enum.reduce(%{created: 0, updated: 0, skipped: 0, errors: []}, fn {batch, batch_num}, acc ->
            batch_start = (batch_num - 1) * @batch_size + 1
            batch_end = min(batch_num * @batch_size, total_rows)

            Logger.info("Processing batch #{batch_num}: rows #{batch_start}-#{batch_end} of #{total_rows}")

            # Parse batch
            operations =
              batch
              |> Enum.map(fn {row, row_index} ->
                parse_row_with_inline_str(row, row_index, inline_str_data)
              end)
              |> Enum.reject(&is_nil/1)

            # Persist batch
            batch_stats = persist_batch(operations)

            # Update progress
            progress = min(100, div(batch_end * 100, total_rows))
            _ = report_intermediate_progress(execution, progress)

            # Merge stats
            merged = %{
              created: acc.created + batch_stats.created,
              updated: acc.updated + batch_stats.updated,
              skipped: acc.skipped + batch_stats.skipped,
              errors: acc.errors ++ batch_stats.errors
            }

            # Force garbage collection after each batch to free memory
            :erlang.garbage_collect()

            Logger.info("Batch #{batch_num} complete - created: #{batch_stats.created}, updated: #{batch_stats.updated}, skipped: #{batch_stats.skipped}, errors: #{length(batch_stats.errors)}")

            merged
          end)

        Xlsxir.close(pid)

        Logger.info(
          "Debentures XLS ingestion completed - Total: created=#{stats.created}, updated=#{stats.updated}, skipped=#{stats.skipped}, errors=#{length(stats.errors)}"
        )

        case stats.errors do
          [] -> {:ok, stats}
          errors -> {:error, {:persistence_failed, errors}}
        end
      rescue
        error ->
          Logger.error("Failed to parse XLS file: #{inspect(error)}")
          {:error, {:parse_error, error}}
      end
    end
  end

  defp persist_batch(operations) when is_list(operations) do
    Enum.reduce(operations, %{created: 0, updated: 0, skipped: 0, errors: []}, fn operation, acc ->
      case persist_operation(operation) do
        {:ok, :created} ->
          %{acc | created: acc.created + 1}

        {:ok, :updated} ->
          %{acc | updated: acc.updated + 1}

        {:skip, reason} ->
          Logger.debug(
            "Skipping Debentures security persistence because #{inspect(reason)}: #{inspect(operation)}"
          )
          %{acc | skipped: acc.skipped + 1}

        {:error, reason} ->
          %{acc | errors: acc.errors ++ [{:error, reason, operation}]}
      end
    end)
  end

  @doc """
  Parses an XLS/XLSX file and returns a list of operations ready to be persisted.

  NOTE: This function loads the entire file in memory and is kept for backward compatibility.
  For large files, use run/2 instead which processes in batches.
  """
  def parse_file(file_path) do
    unless File.exists?(file_path) do
      {:error, :file_not_found}
    else
      try do
        # Read inlineStr cells using custom XML parser
        inline_str_data = extract_inline_str_cells(file_path)

        # Read numeric data using xlsxir
        {:ok, pid} = Xlsxir.multi_extract(file_path, 0)
        rows = Xlsxir.get_list(pid)
        Xlsxir.close(pid)

        operations =
          rows
          # Skip header row
          |> Enum.drop(1)
          # Start from row 2 (after header)
          |> Enum.with_index(2)
          |> Enum.map(fn {row, row_index} ->
            parse_row_with_inline_str(row, row_index, inline_str_data)
          end)
          |> Enum.reject(&is_nil/1)

        Logger.info("Parsed #{length(operations)} operations from XLS file")

        if length(operations) > 0 do
          IO.puts("\n=== Exemplo de Mapeamento XLS (primeiro item) ===")

          IO.inspect(List.first(operations),
            label: "Item mapeado",
            limit: :infinity,
            pretty: true
          )

          IO.puts("Total de items mapeados: #{length(operations)}\n")
        end

        {:ok, operations}
      rescue
        error ->
          Logger.error("Failed to parse XLS file: #{inspect(error)}")
          {:error, {:parse_error, error}}
      end
    end
  end

  defp extract_inline_str_cells(file_path) do
    # TEMPORARILY DISABLED: Inline string extraction causes OOM on large files
    # TODO: Investigate alternative approaches or streaming XML parsers
    Logger.warning("⚠️  Inline string extraction disabled to prevent OOM - some fields may be missing")
    %{}
  end

  defp extract_inline_str_from_xml(sheet_xml) do
    # Use xpath to extract only rows that have inlineStr cells (./c/is)
    # This is much more efficient than parsing all rows
    sheet_xml
    |> xpath(~x"//row[c/is]"l,
      r: ~x"./@r"s,
      cells: [
        ~x"./c[is]"l,  # Only cells with inlineStr
        ref: ~x"./@r"s,
        value: ~x"./is/t/text()"s
      ]
    )
    |> Enum.reduce(%{}, fn row, acc ->
      row_num = String.to_integer(row.r)

      cells_map =
        row.cells
        |> Enum.reduce(%{}, fn cell, cell_acc ->
          if cell.value != "" do
            col = cell.ref |> String.replace(~r/\d+/, "")
            Map.put(cell_acc, col, cell.value)
          else
            cell_acc
          end
        end)

      if map_size(cells_map) > 0 do
        Map.put(acc, row_num, cells_map)
      else
        acc
      end
    end)
  end

  defp parse_inline_str_xml(sheet_xml) do
    result =
      sheet_xml
      |> xpath(~x"//row"l,
        r: ~x"./@r"s,
        cells: [
          ~x"./c"l,
          ref: ~x"./@r"s,
          value: ~x"./is/t/text()"s
        ]
      )
      |> Enum.reduce(%{}, fn row, acc ->
        row_num = String.to_integer(row.r)

        cells_map =
          row.cells
          |> Enum.reduce(%{}, fn cell, cell_acc ->
            if cell.value != "" do
              # Extract column letter from cell reference (e.g., "A2" -> "A")
              col = cell.ref |> String.replace(~r/\d+/, "")
              Map.put(cell_acc, col, cell.value)
            else
              cell_acc
            end
          end)

        if map_size(cells_map) > 0 do
          Map.put(acc, row_num, cells_map)
        else
          acc
        end
      end)

    # Force garbage collection to free XML parsing memory
    :erlang.garbage_collect()

    Logger.info("Extracted inline strings from #{map_size(result)} rows")
    result
  end

  @doc """
  Parses a row from CSV file.

  CSV columns correspond to the XLSX columns:
  - Column A (index 0): reference_date
  - Column B (index 1): code
  - Column C (index 2): issuer
  - Column D (index 3): correction_rate_type
  - Column E (index 4): correction_rate
  - Column F (index 5): maturity_date
  - Column I (index 8): coupon_rate (Taxa indicativa)
  - Column P (index 15): duration
  - Column R (index 17): ntnb_reference
  """
  defp parse_csv_row(row, row_index) when is_list(row) and length(row) > 15 do
    # Skip rows that are all empty
    if Enum.all?(row, &(&1 == "" or is_nil(&1))) do
      nil
    else
      # Extract data directly from CSV columns
      reference_date = row |> Enum.at(0) |> parse_brazilian_date()
      code = row |> Enum.at(1) |> to_string_safe()
      issuer = row |> Enum.at(2) |> to_string_safe()
      correction_rate_type = row |> Enum.at(3) |> to_string_safe()
      correction_rate_str = row |> Enum.at(4) |> to_string_safe()
      maturity_date = row |> Enum.at(5) |> parse_brazilian_date()
      coupon_rate = row |> Enum.at(8) |> to_decimal()
      duration = row |> Enum.at(15) |> to_decimal()
      ntnb_reference_str = row |> Enum.at(17) |> to_string_safe()

      # Parse ntnb_reference as date if it's not empty
      ntnb_reference_date = parse_brazilian_date(ntnb_reference_str)

      benchmark_index = determine_benchmark_index(ntnb_reference_date, correction_rate_type)

      # Build the operation map with all data
      attrs = %{
        reference_date: reference_date,
        security_type: :debenture,
        code: code,
        issuer: issuer,
        credit_risk: issuer,
        correction_rate_type: correction_rate_type,
        correction_rate: correction_rate_str,
        series: "ÚNICA",
        issuing: "N/A",
        maturity_date: maturity_date,
        coupon_rate: coupon_rate,
        duration: duration,
        ntnb_reference_date: ntnb_reference_date,
        benchmark_index: benchmark_index,
        # Legacy field
        ntnb_reference: ntnb_reference_str
      }

      attrs
    end
  rescue
    error ->
      Logger.warning("Failed to parse CSV row #{row_index}: #{inspect(error)}")
      nil
  end

  defp parse_csv_row(row, row_index) do
    Logger.warning("Skipping row #{row_index}: insufficient columns (#{length(row)})")
    nil
  end

  defp parse_row_with_inline_str(row, row_index, inline_str_data)
       when is_list(row) and length(row) > 15 do
    # Skip rows that are all nil
    if Enum.all?(row, &is_nil/1) do
      nil
    else
      # Get inlineStr data for this row
      row_inline_data = Map.get(inline_str_data, row_index, %{})

      # Extract data from inlineStr cells (columns A-F, R)
      reference_date = row_inline_data |> Map.get("A") |> parse_brazilian_date()
      code = row_inline_data |> Map.get("B") |> to_string_safe()
      issuer = row_inline_data |> Map.get("C") |> to_string_safe()
      correction_rate_type = row_inline_data |> Map.get("D") |> to_string_safe()
      correction_rate_str = row_inline_data |> Map.get("E") |> to_string_safe()
      maturity_date = row_inline_data |> Map.get("F") |> parse_brazilian_date()
      ntnb_reference_str = row_inline_data |> Map.get("R") |> to_string_safe()

      # Extract numeric data from xlsxir
      # Column I (index 8): Taxa indicativa (coupon_rate)
      # Column P (index 15): Duration
      coupon_rate = row |> Enum.at(8) |> to_decimal()
      duration = row |> Enum.at(15) |> to_decimal()

      # Parse ntnb_reference as date if it's not empty
      ntnb_reference_date = parse_brazilian_date(ntnb_reference_str)

      benchmark_index = determine_benchmark_index(ntnb_reference_date, correction_rate_type)

      # Build the operation map with all data
      attrs = %{
        reference_date: reference_date,
        security_type: :debenture,
        code: code,
        issuer: issuer,
        credit_risk: issuer,
        correction_rate_type: correction_rate_type,
        correction_rate: correction_rate_str,
        series: "ÚNICA",
        issuing: "N/A",
        maturity_date: maturity_date,
        coupon_rate: coupon_rate,
        duration: duration,
        ntnb_reference_date: ntnb_reference_date,
        benchmark_index: benchmark_index,
        # Legacy field
        ntnb_reference: ntnb_reference_str
      }

      attrs
    end
  rescue
    error ->
      Logger.warning("Failed to parse row #{row_index}: #{inspect(error)}")
      nil
  end

  defp parse_row_with_inline_str(_row, _row_index, _inline_str_data), do: nil

  defp to_string_safe(nil), do: nil
  defp to_string_safe(""), do: nil

  defp to_string_safe(value) when is_binary(value) do
    trimmed = String.trim(value)
    if trimmed == "", do: nil, else: trimmed
  end

  defp to_string_safe(value) when is_number(value), do: to_string(value)
  defp to_string_safe(value), do: to_string(value)

  defp to_decimal(nil), do: nil
  defp to_decimal(""), do: nil
  defp to_decimal(value) when is_integer(value), do: Decimal.new(value)
  defp to_decimal(value) when is_float(value), do: Decimal.from_float(value)
  defp to_decimal(%Decimal{} = value), do: value

  defp to_decimal(value) when is_binary(value) do
    case Decimal.parse(value) do
      {decimal, ""} -> decimal
      _ -> nil
    end
  end

  defp to_decimal(_), do: nil

  defp parse_brazilian_date(nil), do: nil
  defp parse_brazilian_date(""), do: nil

  defp parse_brazilian_date(value) when is_binary(value) do
    # Try parsing DD/MM/YYYY format
    case String.split(value, "/") do
      [day, month, year] ->
        with {day_int, ""} <- Integer.parse(day),
             {month_int, ""} <- Integer.parse(month),
             {year_int, ""} <- Integer.parse(year),
             {:ok, date} <- Date.new(year_int, month_int, day_int) do
          date
        else
          _ -> nil
        end

      _ ->
        # Try ISO format as fallback
        case Date.from_iso8601(value) do
          {:ok, date} -> date
          _ -> nil
        end
    end
  end

  defp parse_brazilian_date(_), do: nil

  defp determine_benchmark_index(ntnb_reference_date, correction_rate_type) do
    if ntnb_reference_date do
      "ipca"
    else
      correction_rate_type
      |> normalize_remuneration()
      |> case do
        # CRI/CRA patterns
        "di aditivo" -> "di_plus"
        "di multiplicativo" -> "di_multiple"
        type when type in ["cdi", "di"] -> "cdi"

        # Debentures patterns
        "di spread" -> "di_plus"
        "di percentual" -> "di_multiple"
        "ipca spread" -> "ipca"
        "igp-m" -> "igp_m"

        # Generic fallback
        "ipca" -> "ipca"

        _ -> nil
      end
    end
  end

  defp normalize_remuneration(nil), do: nil

  defp normalize_remuneration(value) when is_binary(value) do
    value
    |> String.trim()
    |> String.downcase()
  end

  defp normalize_remuneration(_), do: nil

  # Reuse persistence logic from the API-based ingestion
  def persist_operations(operations) when is_list(operations) do
    stats =
      Enum.reduce(operations, %{created: 0, updated: 0, skipped: 0, errors: []}, fn operation,
                                                                                    acc ->
        case persist_operation(operation) do
          {:ok, :created} ->
            %{acc | created: acc.created + 1}

          {:ok, :updated} ->
            %{acc | updated: acc.updated + 1}

          {:skip, reason} ->
            Logger.debug(
              "Skipping Debentures security persistence because #{inspect(reason)}: #{inspect(operation)}"
            )

            %{acc | skipped: acc.skipped + 1}

          {:error, reason} ->
            %{acc | errors: acc.errors ++ [{:error, reason, operation}]}
        end
      end)

    Logger.info(
      "Debentures XLS ingestion persisted #{stats.created + stats.updated} securities " <>
        "(created: #{stats.created}, updated: #{stats.updated}, skipped: #{stats.skipped}, errors: #{length(stats.errors)})"
    )

    case stats.errors do
      [] -> {:ok, stats}
      errors -> {:error, {:persistence_failed, errors}}
    end
  end

  def persist_operations(_operations), do: {:error, :invalid_operations}

  defp persist_operation(%{} = operation) do
    case normalize_security_attrs(operation) do
      {:ok, attrs} -> upsert_security(attrs)
      other -> other
    end
  end

  defp persist_operation(_operation), do: {:skip, :invalid_operation}

  defp normalize_security_attrs(operation) do
    code = operation |> Map.get(:code) |> normalize_string()
    security_type = Map.get(operation, :security_type)
    issuer = operation |> Map.get(:issuer) |> normalize_string()
    series = operation |> Map.get(:series) |> normalize_string()
    issuing = operation |> Map.get(:issuing) |> normalize_string()
    credit_risk = operation |> Map.get(:credit_risk) |> normalize_string()
    duration = operation |> Map.get(:duration) |> decimal_to_integer()
    reference_date = Map.get(operation, :reference_date)
    benchmark_index = operation |> Map.get(:benchmark_index) |> normalize_string()
    ntnb_reference = operation |> Map.get(:ntnb_reference) |> normalize_string()
    ntnb_reference_date = Map.get(operation, :ntnb_reference_date)
    coupon_rate = Map.get(operation, :coupon_rate)
    correction_rate = Map.get(operation, :correction_rate)

    cond do
      is_nil(code) ->
        {:skip, :missing_code}

      is_nil(security_type) ->
        {:skip, :missing_security_type}

      is_nil(issuer) ->
        {:skip, :missing_issuer}

      is_nil(duration) ->
        {:skip, :missing_duration}

      true ->
        attrs =
          %{
            code: code,
            security_type: security_type,
            issuer: issuer,
            series: series || "ÚNICA",
            issuing: issuing || "N/A",
            credit_risk: credit_risk,
            duration: duration,
            reference_date: reference_date,
            benchmark_index: benchmark_index,
            ntnb_reference: ntnb_reference,
            ntnb_reference_date: ntnb_reference_date,
            coupon_rate: coupon_rate,
            correction_rate: correction_rate,
            sync_source: :xls
          }

        {:ok, attrs}
    end
  end

  defp upsert_security(attrs) do
    lookup =
      [:code, :series]
      |> Enum.reduce(%{}, fn key, acc ->
        case Map.get(attrs, key) do
          nil -> acc
          value -> Map.put(acc, key, value)
        end
      end)

    if map_size(lookup) == 0 do
      {:error, :missing_lookup_keys}
    else
      case Repo.get_by(Security, lookup) do
        nil -> create_security(attrs)
        %Security{} = security -> update_security(security, attrs)
      end
    end
  end

  defp create_security(attrs) do
    attrs = Map.put_new(attrs, :sync_source, :xls)

    %Security{}
    |> FixedIncome.security_create_changeset(attrs)
    |> Repo.insert()
    |> case do
      {:ok, _security} ->
        {:ok, :created}

      {:error, %Changeset{} = changeset} ->
        {:error, {:changeset_error, changeset_errors(changeset)}}
    end
  end

  defp update_security(%Security{} = security, attrs) do
    attrs = Map.put_new(attrs, :sync_source, security.sync_source || :xls)

    security
    |> FixedIncome.security_update_changeset(attrs)
    |> Repo.update()
    |> case do
      {:ok, _security} ->
        {:ok, :updated}

      {:error, %Changeset{} = changeset} ->
        {:error, {:changeset_error, changeset_errors(changeset)}}
    end
  end

  defp normalize_string(value) when is_binary(value) do
    value
    |> String.trim()
    |> case do
      "" -> nil
      trimmed -> trimmed
    end
  end

  defp normalize_string(value), do: value

  defp decimal_to_integer(nil), do: nil
  defp decimal_to_integer(value) when is_integer(value), do: value

  defp decimal_to_integer(%Decimal{} = value) do
    value
    |> Decimal.round(0, :floor)
    |> Decimal.to_integer()
  rescue
    _ -> nil
  end

  defp decimal_to_integer(value) when is_float(value), do: trunc(value)

  defp decimal_to_integer(value) when is_binary(value) do
    case Integer.parse(value) do
      {integer, _} -> integer
      :error -> nil
    end
  end

  defp decimal_to_integer(_), do: nil

  defp changeset_errors(%Changeset{} = changeset) do
    Changeset.traverse_errors(changeset, fn {message, opts} ->
      Enum.reduce(opts, message, fn {key, value}, acc ->
        # Safely convert value to string, handling tuples and other types
        string_value = safe_to_string(value)
        String.replace(acc, "%{#{key}}", string_value)
      end)
    end)
  end

  defp safe_to_string(value) when is_binary(value), do: value
  defp safe_to_string(value) when is_atom(value), do: to_string(value)
  defp safe_to_string(value) when is_integer(value), do: Integer.to_string(value)
  defp safe_to_string(value) when is_float(value), do: Float.to_string(value)
  defp safe_to_string(value) when is_tuple(value), do: inspect(value)
  defp safe_to_string(value), do: inspect(value)

  defp execution_id(%{id: id}) when not is_nil(id), do: id
  defp execution_id(_), do: "n/a"

  defp report_intermediate_progress(%{id: id}, progress) when is_integer(id) do
    Ingestions.report_progress(id, progress)
  rescue
    _ -> :ok
  end

  defp report_intermediate_progress(%Execution{} = execution, progress) do
    Ingestions.report_progress(execution, progress)
  rescue
    _ -> :ok
  end

  defp report_intermediate_progress(_execution, _progress), do: :ok
end
