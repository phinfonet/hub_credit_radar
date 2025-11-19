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
  Parses XLSX file and processes all rows directly in this job.

  Instead of creating 5850+ individual jobs (which causes OOM),
  processes all rows in a single job to minimize memory overhead.
  """
  defp parse_and_enqueue_rows(file_path, execution_id) do
    unless File.exists?(file_path) do
      {:error, :file_not_found}
    else
      try do
        # Create temp directory
        tmp_dir = "/tmp/debentures-#{execution_id}"
        File.mkdir_p!(tmp_dir)
        Logger.info("Created temporary directory: #{tmp_dir}")

        # Extract sheet1.xml to temporary file (for inline strings)
        Logger.info("Extracting sheet1.xml to temporary file...")
        xml_file_path = extract_sheet_xml_to_file(file_path, tmp_dir)
        Logger.info("Extracted sheet1.xml to: #{xml_file_path}")

        # Open XLSX with XlsxReader
        Logger.info("Opening XLSX with XlsxReader...")
        {:ok, package} = XlsxReader.open(file_path)

        # Get first sheet
        sheet_names = XlsxReader.sheet_names(package)
        Logger.info("Found sheets: #{inspect(sheet_names)}")

        {:ok, rows} = XlsxReader.sheet(package, Enum.at(sheet_names, 0))
        Logger.info("Read #{length(rows)} rows total")

        # Process all rows directly (skip header)
        {created, updated, skipped, errors} =
          rows
          |> Enum.drop(1)  # Skip header
          |> Enum.with_index(2)  # Start from row 2
          |> Enum.reduce({0, 0, 0, 0}, fn {row_data, row_index}, {c, u, s, e} ->
            # Extract inline strings for this row
            inline_str_data = ProcessDebentureRowJob.extract_inline_str_for_row(xml_file_path, row_index)

            # Parse and persist
            parsed_data = ProcessDebentureRowJob.parse_row(row_data, row_index, inline_str_data)

            result = ProcessDebentureRowJob.persist_debenture(parsed_data)

            # Update counters based on result
            {new_c, new_u, new_s, new_e} = case result do
              {:ok, :created} -> {c + 1, u, s, e}
              {:ok, :updated} -> {c, u + 1, s, e}
              {:skip, _} -> {c, u, s + 1, e}
              {:error, _} -> {c, u, s, e + 1}
            end

            # Periodic GC and logging
            total = new_c + new_u + new_s + new_e
            if rem(total, 50) == 0 do
              :erlang.garbage_collect()

              if rem(total, 200) == 0 do
                Logger.info("Processed #{total} rows so far (created: #{new_c}, updated: #{new_u}, skipped: #{new_s}, errors: #{new_e})")
              end
            end

            {new_c, new_u, new_s, new_e}
          end)

        # Cleanup temp files
        File.rm_rf!(tmp_dir)
        Logger.info("Cleaned up temp directory: #{tmp_dir}")

        total_processed = created + updated + skipped + errors
        Logger.info("Completed processing #{total_processed} rows: created=#{created}, updated=#{updated}, skipped=#{skipped}, errors=#{errors}")

        {:ok, total_processed}
      rescue
        error ->
          Logger.error("Failed to parse XLSX file: #{inspect(error)}")
          {:error, {:parse_error, error}}
      end
    end
  end

  # Extract sheet1.xml to a temporary file to avoid loading entire XML in memory
  defp extract_sheet_xml_to_file(file_path, tmp_dir) do
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
        Logger.info("Extracting #{List.to_string(sheet_name)} to memory first...")

        # Extract to memory first (only sheet1.xml, ~5-10MB)
        {:ok, [{^sheet_name, sheet_xml_content}]} =
          :zip.extract(charlist_path, [
            {:file_list, [sheet_name]},
            :memory
          ])

        Logger.info("Extracted #{byte_size(sheet_xml_content)} bytes to memory")

        # Save to file for jobs to read
        xml_path = Path.join(tmp_dir, "sheet1.xml")
        File.write!(xml_path, sheet_xml_content)

        Logger.info("Saved sheet XML to: #{xml_path}, size: #{File.stat!(xml_path).size} bytes")

        # Force GC to free the XML content from memory
        :erlang.garbage_collect()

        xml_path

      nil ->
        raise "Could not find sheet1.xml in XLSX file"
    end
  end

  @impl Oban.Worker
  def timeout(_job), do: :timer.minutes(30)
end
