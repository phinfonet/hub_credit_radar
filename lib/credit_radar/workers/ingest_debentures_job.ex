defmodule CreditRadar.Workers.IngestDebenturesJob do
  @moduledoc """
  Oban worker for processing debenture XLS file uploads.

  This job reads the XLSX file and processes it synchronously using streams
  and batching to avoid OOM issues. No ETS required.
  """
  use Oban.Worker, queue: :debentures, max_attempts: 3

  alias CreditRadar.Ingestions
  alias CreditRadar.FixedIncome
  alias CreditRadar.FixedIncome.Security
  alias CreditRadar.Repo
  alias Ecto.Changeset
  alias Decimal

  import SweetXml

  require Logger

  # Size of each batch to process
  @batch_size 250

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"execution_id" => execution_id, "file_path" => file_path}}) do
    Logger.info("🟢 Starting IngestDebenturesJob for execution ##{execution_id}")
    Logger.info("🟢 File path: #{file_path}")

    # Load the execution record
    execution = Repo.get!(Ingestions.Execution, execution_id)

    # Mark execution as running
    {:ok, execution} =
      execution
      |> Ingestions.execution_update_changeset(%{"status" => "running"})
      |> Repo.update()

    Logger.info("🟢 Execution ##{execution_id} marked as running")

    # Process file synchronously using streams
    try do
      case parse_and_persist_file_in_batches(file_path, execution) do
        {:ok, stats} ->
          Logger.info("🟢 IngestDebenturesJob completed successfully")
          Logger.info("   Created: #{stats.created}, Updated: #{stats.updated}, Skipped: #{stats.skipped}, Errors: #{length(stats.errors)}")

          # Mark execution as completed
          {:ok, _execution} =
            execution
            |> Ingestions.execution_update_changeset(%{
              "status" => "completed",
              "progress" => 100,
              "finished_at" => DateTime.utc_now()
            })
            |> Repo.update()

          :ok

        {:error, reason} = error ->
          Logger.error("🔴 IngestDebenturesJob failed for execution ##{execution_id}")
          Logger.error("🔴 Error: #{inspect(reason)}")

          # Mark execution as failed
          {:ok, _execution} =
            execution
            |> Ingestions.execution_update_changeset(%{
              "status" => "failed",
              "finished_at" => DateTime.utc_now()
            })
            |> Repo.update()

          error
      end
    after
      # Clean up the uploaded file
      Logger.info("🟢 Cleaning up file: #{file_path}")
      File.rm(file_path)
    end
  end

  @doc """
  Parses and persists the XLS/XLSX file in batches to avoid OOM issues.
  Uses synchronous stream processing with no ETS required.
  """
  defp parse_and_persist_file_in_batches(file_path, execution) do
    unless File.exists?(file_path) do
      {:error, :file_not_found}
    else
      try do
        # Read inlineStr cells using custom XML parser (this is memory-efficient)
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
          "Debentures ingestion completed - Total: created=#{stats.created}, updated=#{stats.updated}, skipped=#{stats.skipped}, errors=#{length(stats.errors)}"
        )

        case stats.errors do
          [] -> {:ok, stats}
          errors -> {:error, {:persistence_failed, errors}}
        end
      rescue
        error ->
          Logger.error("Failed to parse XLSX file: #{inspect(error)}")
          {:error, {:parse_error, error}}
      end
    end
  end

  defp extract_inline_str_cells(file_path) do
    # Extract only sheet1.xml without loading other files
    charlist_path = String.to_charlist(file_path)

    {:ok, file_list} = :zip.list_dir(charlist_path)

    sheet_file =
      Enum.find(file_list, fn
        {:zip_file, name, _info, _comment, _offset, _comp_size} ->
          List.to_string(name) =~ ~r/xl\/worksheets\/sheet1\.xml$/

        _ ->
          false
      end)

    case sheet_file do
      {:zip_file, sheet_name, _info, _comment, _offset, _comp_size} ->
        # Extract only this file
        {:ok, [{^sheet_name, sheet_xml}]} =
          :zip.extract(charlist_path, [
            {:file_list, [sheet_name]},
            :memory
          ])

        # Parse with xpath filter (only rows with inline strings)
        result = extract_inline_str_from_xml(sheet_xml)

        # Force GC immediately
        :erlang.garbage_collect()

        Logger.info("Extracted inline strings from #{map_size(result)} rows")
        result

      nil ->
        Logger.warning("Could not find sheet1.xml in XLSX file")
        %{}
    end
  rescue
    error ->
      Logger.error("Failed to extract inlineStr cells: #{inspect(error)}")
      Logger.error("Stacktrace: #{inspect(__STACKTRACE__)}")
      # Return empty map to allow processing to continue without inline strings
      %{}
  end

  defp extract_inline_str_from_xml(sheet_xml) do
    # Use xpath to extract only rows that have inlineStr cells (./c/is)
    sheet_xml
    |> xpath(~x"//row[c/is]"l,
      r: ~x"./@r"s,
      cells: [
        ~x"./c[is]"l,
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

  defp persist_batch(operations) when is_list(operations) do
    Enum.reduce(operations, %{created: 0, updated: 0, skipped: 0, errors: []}, fn operation, acc ->
      case persist_operation(operation) do
        {:ok, :created} ->
          %{acc | created: acc.created + 1}

        {:ok, :updated} ->
          %{acc | updated: acc.updated + 1}

        {:skip, reason} ->
          Logger.debug(
            "Skipping debenture security persistence because #{inspect(reason)}: #{inspect(operation)}"
          )
          %{acc | skipped: acc.skipped + 1}

        {:error, reason} ->
          %{acc | errors: acc.errors ++ [{:error, reason, operation}]}
      end
    end)
  end

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

  # Helper functions for parsing and normalization
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

  defp report_intermediate_progress(%{id: id}, progress) when is_integer(id) do
    Ingestions.report_progress(id, progress)
  rescue
    _ -> :ok
  end

  defp report_intermediate_progress(%Ingestions.Execution{} = execution, progress) do
    Ingestions.report_progress(execution, progress)
  rescue
    _ -> :ok
  end

  defp report_intermediate_progress(_execution, _progress), do: :ok

  @impl Oban.Worker
  def timeout(_job), do: :timer.minutes(30)
end
