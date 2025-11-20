defmodule CreditRadar.Workers.IngestDebenturesJob do
  @moduledoc """
  Oban worker for processing debenture XLS file uploads.

  This job reads the XLSX file and enqueues ProcessDebentureRowJob for each row.
  Each row job receives all data it needs (no ETS dependency).
  Multiple workers can process rows in parallel safely.
  """
  use Oban.Worker, queue: :debentures, max_attempts: 3

  alias CreditRadar.Ingestions
  alias CreditRadar.Workers.ProcessDebentureRowJob
  alias CreditRadar.Repo
  alias Decimal

  import SweetXml

  require Logger

  # Size of each batch when enqueuing jobs
  @batch_size 50

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

    # Parse file and enqueue row jobs (no ETS needed)
    try do
      case parse_and_enqueue_rows(file_path, execution_id) do
        {:ok, row_count} ->
          Logger.info("🟢 IngestDebenturesJob completed: enqueued #{row_count} row jobs")
          Logger.info("   Jobs will be processed by #{row_count} workers in parallel")

          # Mark execution as completed (row jobs will process in background)
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
  Parses XLSX file and enqueues ProcessDebentureRowJob for each row.
  All data is passed in job args - no ETS needed.
  """
  defp parse_and_enqueue_rows(file_path, execution_id) do
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

        # Get all rows
        rows = Xlsxir.get_list(pid)
        total_rows = length(rows) - 1  # Exclude header
        Logger.info("Total rows to enqueue: #{total_rows}")

        # Enqueue jobs in batches
        row_count =
          rows
          # Skip header row
          |> Enum.drop(1)
          # Start from row 2 (after header)
          |> Stream.with_index(2)
          # Process in chunks to batch inserts
          |> Stream.chunk_every(@batch_size)
          |> Enum.reduce(0, fn batch, acc ->
            # Parse rows and create jobs
            jobs =
              batch
              |> Enum.map(fn {row, row_index} ->
                case parse_row_with_inline_str(row, row_index, inline_str_data) do
                  nil ->
                    nil

                  attrs ->
                    # Serialize attrs to job-compatible format (no ETS needed!)
                    ProcessDebentureRowJob.new(%{
                      row_index: row_index,
                      execution_id: execution_id,
                      attrs: serialize_attrs(attrs)
                    })
                end
              end)
              |> Enum.reject(&is_nil/1)

            # Insert batch of jobs
            Oban.insert_all(jobs)

            # Aggressive GC
            :erlang.garbage_collect()

            new_count = acc + length(jobs)

            if rem(new_count, 200) == 0 do
              Logger.info("Enqueued #{new_count} jobs so far...")
            end

            new_count
          end)

        Xlsxir.close(pid)

        Logger.info("Enqueued #{row_count} jobs total")

        {:ok, row_count}
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

  @doc """
  Serializes attrs to a JSON-compatible format.
  Converts Decimal and Date to strings, converts atom keys to strings.
  """
  defp serialize_attrs(attrs) do
    %{
      "reference_date" => date_to_string(attrs[:reference_date]),
      "security_type" => to_string(attrs[:security_type]),
      "code" => attrs[:code],
      "issuer" => attrs[:issuer],
      "credit_risk" => attrs[:credit_risk],
      "correction_rate_type" => attrs[:correction_rate_type],
      "correction_rate" => attrs[:correction_rate],
      "series" => attrs[:series],
      "issuing" => attrs[:issuing],
      "maturity_date" => date_to_string(attrs[:maturity_date]),
      "coupon_rate" => decimal_to_string(attrs[:coupon_rate]),
      "duration" => decimal_to_integer(attrs[:duration]),
      "ntnb_reference_date" => date_to_string(attrs[:ntnb_reference_date]),
      "benchmark_index" => attrs[:benchmark_index],
      "ntnb_reference" => attrs[:ntnb_reference]
    }
  end

  defp date_to_string(nil), do: nil
  defp date_to_string(%Date{} = date), do: Date.to_iso8601(date)
  defp date_to_string(_), do: nil

  defp decimal_to_string(nil), do: nil
  defp decimal_to_string(%Decimal{} = decimal), do: Decimal.to_string(decimal)
  defp decimal_to_string(_), do: nil

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

  @impl Oban.Worker
  def timeout(_job), do: :timer.minutes(30)
end
