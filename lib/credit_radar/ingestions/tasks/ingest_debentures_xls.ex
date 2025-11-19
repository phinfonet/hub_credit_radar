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
  - D: Tipo de remuneração (correction_rate_type)
  - E: Taxa de correção (correction_rate)
  - F: Data de vencimento (maturity_date)
  - G: Taxa indicativa (coupon_rate)
  - H: Duration (dias úteis) (duration)
  - I: Referência NTN-B (ntnb_reference)

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

  def start_link({execution, file_path}) do
    Task.start_link(__MODULE__, :run, [execution, file_path])
  end

  @doc """
  Executes the Debentures XLS ingestion pipeline.
  """
  def run(execution \\ nil, file_path) do
    Logger.info(
      "Starting Debentures XLS ingestion from #{file_path} for execution #{execution_id(execution)}"
    )

    result =
      with {:ok, operations} <- parse_file(file_path),
           {:ok, stats} <- persist_operations(operations) do
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
  Parses an XLS/XLSX file and returns a list of operations ready to be persisted.
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
    # Unzip the XLSX file and read the sheet XML
    {:ok, files} = :zip.unzip(String.to_charlist(file_path), [:memory])

    # Find the sheet1.xml file
    {_, sheet_xml} =
      Enum.find(files, fn {name, _} ->
        List.to_string(name) =~ ~r/xl\/worksheets\/sheet1\.xml$/
      end)

    # Parse all rows and extract inlineStr values
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
  rescue
    error ->
      Logger.warning("Failed to extract inlineStr cells: #{inspect(error)}")
      %{}
  end

  defp parse_row_with_inline_str(row, row_index, inline_str_data)
       when is_list(row) and length(row) > 5 do
    # Skip rows that are all nil
    if Enum.all?(row, &is_nil/1) do
      nil
    else
      # Get inlineStr data for this row
      row_inline_data = Map.get(inline_str_data, row_index, %{})

      # Extract data from inlineStr cells (columns A-I)
      reference_date = row_inline_data |> Map.get("A") |> parse_brazilian_date()
      code = row_inline_data |> Map.get("B") |> to_string_safe()
      issuer = row_inline_data |> Map.get("C") |> to_string_safe()
      correction_rate_type = row_inline_data |> Map.get("D") |> to_string_safe()

      # Extract numeric data from xlsxir (columns E onwards)
      correction_rate = row |> Enum.at(4) |> to_decimal()
      maturity_date = row |> Enum.at(5) |> parse_brazilian_date()
      coupon_rate = row |> Enum.at(6) |> to_decimal()
      duration = row |> Enum.at(7) |> to_decimal()
      ntnb_reference_date = row |> Enum.at(8) |> parse_brazilian_date()

      benchmark_index = determine_benchmark_index(ntnb_reference_date, correction_rate_type)

      # Build the operation map with all data
      attrs = %{
        reference_date: reference_date,
        security_type: :debenture,
        code: code,
        issuer: issuer,
        credit_risk: issuer,
        correction_rate_type: correction_rate_type,
        correction_rate: correction_rate,
        series: "ÚNICA",
        issuing: "N/A",
        maturity_date: maturity_date,
        coupon_rate: coupon_rate,
        duration: duration,
        ntnb_reference_date: ntnb_reference_date,
        benchmark_index: benchmark_index,
        # Legacy field
        ntnb_reference: benchmark_index
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
        "di aditivo" -> "di_plus"
        "di multiplicativo" -> "di_multiple"
        type when type in ["cdi", "di"] -> "cdi"
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
