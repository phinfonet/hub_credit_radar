defmodule CreditRadar.Workers.IngestDebenturesJob do
  @moduledoc """
  Oban worker for processing debenture XLS file uploads.

  This job reads the XLSX file and enqueues individual ProcessDebentureRowJob
  for each row. This approach avoids OOM by distributing work across many small jobs.
  """
  use Oban.Worker, queue: :debentures, max_attempts: 3

  alias CreditRadar.Ingestions
  alias CreditRadar.Workers.ProcessDebentureRowJob
  alias CreditRadar.Repo
  alias Decimal

  require Logger

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

    # Parse file and enqueue row jobs
    try do
      case parse_and_enqueue_rows(file_path, execution_id) do
        {:ok, row_count} ->
          Logger.info("🟢 IngestDebenturesJob completed: enqueued #{row_count} row jobs for execution ##{execution_id}")

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
  Parses XLSX file and enqueues a job for each row.

  Reads file incrementally to avoid loading entire file in memory.
  """
  defp parse_and_enqueue_rows(file_path, execution_id) do
    unless File.exists?(file_path) do
      {:error, :file_not_found}
    else
      try do
        # Extract inline strings (needed for columns A-F, R)
        Logger.info("Extracting inline string cells...")
        inline_str_data = extract_inline_str_cells(file_path)

        # Read numeric data with xlsxir
        {:ok, pid} = Xlsxir.multi_extract(file_path, 0)
        rows = Xlsxir.get_list(pid)

        Logger.info("Found #{length(rows)} total rows (including header)")

        # Skip header and process rows
        row_count =
          rows
          |> Enum.drop(1)
          |> Enum.with_index(2)
          |> Enum.reduce(0, fn {row, row_index}, count ->
            case parse_row(row, row_index, inline_str_data) do
              nil ->
                count

              row_data ->
                # Enqueue job for this row
                %{row_data: row_data, execution_id: execution_id}
                |> ProcessDebentureRowJob.new()
                |> Oban.insert!()

                count + 1
            end
          end)

        Xlsxir.close(pid)

        Logger.info("Enqueued #{row_count} row processing jobs")

        {:ok, row_count}
      rescue
        error ->
          Logger.error("Failed to parse XLSX file: #{inspect(error)}")
          {:error, {:parse_error, error}}
      end
    end
  end

  defp extract_inline_str_cells(_file_path) do
    # DISABLED: Inline string extraction causes OOM even with optimizations
    # The sheet1.xml file (~5-10MB uncompressed) + xpath parsing exceeds available memory
    Logger.warning("⚠️  Inline string extraction disabled - server memory insufficient")
    %{}
  end

  defp parse_row(row, row_index, inline_str_data) when is_list(row) and length(row) > 15 do
    # Skip rows that are all nil
    if Enum.all?(row, &is_nil/1) do
      nil
    else
      # Get inline str data for this row
      row_inline_data = Map.get(inline_str_data, row_index, %{})

      # Extract data from columns
      reference_date = row_inline_data |> Map.get("A") |> parse_brazilian_date()
      code = row_inline_data |> Map.get("B") |> to_string_safe()
      issuer = row_inline_data |> Map.get("C") |> to_string_safe()
      correction_rate_type = row_inline_data |> Map.get("D") |> to_string_safe()
      correction_rate_str = row_inline_data |> Map.get("E") |> to_string_safe()
      maturity_date = row_inline_data |> Map.get("F") |> parse_brazilian_date()
      ntnb_reference_str = row_inline_data |> Map.get("R") |> to_string_safe()

      # Extract numeric data from xlsxir
      coupon_rate = row |> Enum.at(8) |> to_decimal()
      duration = row |> Enum.at(15) |> to_decimal()

      # Parse ntnb_reference as date
      ntnb_reference_date = parse_brazilian_date(ntnb_reference_str)

      benchmark_index = determine_benchmark_index(ntnb_reference_date, correction_rate_type)

      # Build row data map (convert to JSON-serializable types)
      %{
        "reference_date" => date_to_string(reference_date),
        "security_type" => "debenture",
        "code" => code,
        "issuer" => issuer,
        "credit_risk" => issuer,
        "correction_rate_type" => correction_rate_type,
        "correction_rate" => correction_rate_str,
        "series" => "ÚNICA",
        "issuing" => "N/A",
        "maturity_date" => date_to_string(maturity_date),
        "coupon_rate" => coupon_rate,
        "duration" => duration_to_int(duration),
        "ntnb_reference_date" => date_to_string(ntnb_reference_date),
        "benchmark_index" => benchmark_index,
        "ntnb_reference" => ntnb_reference_str
      }
    end
  rescue
    error ->
      Logger.warning("Failed to parse row #{row_index}: #{inspect(error)}")
      nil
  end

  defp parse_row(_row, _row_index, _inline_str_data), do: nil

  defp to_string_safe(nil), do: ""
  defp to_string_safe(value) when is_binary(value), do: String.trim(value)
  defp to_string_safe(value), do: to_string(value)

  defp to_decimal(nil), do: nil
  defp to_decimal(value) when is_number(value), do: Decimal.new(to_string(value))
  defp to_decimal(value) when is_binary(value), do: Decimal.new(value)
  defp to_decimal(_), do: nil

  defp date_to_string(nil), do: nil
  defp date_to_string(%Date{} = date), do: Date.to_iso8601(date)

  defp duration_to_int(nil), do: nil

  defp duration_to_int(%Decimal{} = d) do
    d
    |> Decimal.round(0, :down)
    |> Decimal.to_integer()
  end

  defp duration_to_int(_), do: nil

  defp parse_brazilian_date(nil), do: nil
  defp parse_brazilian_date(""), do: nil

  defp parse_brazilian_date(date_string) when is_binary(date_string) do
    case Regex.run(~r/(\d{2})\/(\d{2})\/(\d{4})/, date_string) do
      [_, day, month, year] ->
        case Date.new(String.to_integer(year), String.to_integer(month), String.to_integer(day)) do
          {:ok, date} -> date
          {:error, _} -> nil
        end

      _ ->
        nil
    end
  end

  defp parse_brazilian_date(_), do: nil

  defp determine_benchmark_index(nil, _correction_rate_type), do: nil

  defp determine_benchmark_index(_ntnb_reference_date, correction_rate_type)
       when is_binary(correction_rate_type) do
    cond do
      String.contains?(correction_rate_type, "IPCA") -> "IPCA"
      String.contains?(correction_rate_type, "CDI") -> "CDI"
      String.contains?(correction_rate_type, "IGPM") || String.contains?(correction_rate_type, "IGP-M") -> "IGPM"
      String.contains?(correction_rate_type, "Pré") || String.contains?(correction_rate_type, "PRÉ") -> "PRE"
      true -> nil
    end
  end

  defp determine_benchmark_index(_ntnb_reference_date, _correction_rate_type), do: nil

  @impl Oban.Worker
  def timeout(_job), do: :timer.minutes(30)
end
