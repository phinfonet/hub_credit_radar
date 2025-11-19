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
        # Create temp directory
        tmp_dir = "/tmp/debentures-#{execution_id}"
        File.mkdir_p!(tmp_dir)
        Logger.info("Created temporary directory: #{tmp_dir}")

        # Extract sheet1.xml to temporary file (avoids loading entire XML in memory)
        Logger.info("Extracting sheet1.xml to temporary file...")
        xml_file_path = extract_sheet_xml_to_file(file_path, tmp_dir)
        Logger.info("Extracted sheet1.xml to: #{xml_file_path}")

        # Create ETS table to share file paths with row jobs
        table_name = :"debentures_xml_#{execution_id}"
        :ets.new(table_name, [:named_table, :public, :set])
        :ets.insert(table_name, {:xml_file_path, xml_file_path})
        :ets.insert(table_name, {:total_jobs, 0})
        Logger.info("Created ETS table: #{table_name}")

        # Copy XLSX to temp dir for jobs to access
        xlsx_copy_path = Path.join(tmp_dir, "source.xlsx")
        File.cp!(file_path, xlsx_copy_path)
        :ets.insert(table_name, {:xlsx_file_path, xlsx_copy_path})
        Logger.info("Copied XLSX to: #{xlsx_copy_path}")

        # Get row count WITHOUT loading all rows into memory
        Logger.info("Counting rows in XLSX...")
        {:ok, pid} = Xlsxir.multi_extract(file_path, 0)

        # Get table info to count rows (Xlsxir uses ETS internally)
        row_count = :ets.info(pid, :size) - 1  # Subtract header

        Xlsxir.close(pid)
        Logger.info("Found #{row_count} data rows (excluding header)")

        # Enqueue jobs with ONLY row_index (no row_data to avoid OOM)
        # Each job will open the XLSX and read only its row
        Logger.info("Enqueuing #{row_count} jobs...")

        for row_index <- 2..(row_count + 1) do
          %{
            row_index: row_index,
            ets_table: Atom.to_string(table_name),
            execution_id: execution_id
          }
          |> ProcessDebentureRowJob.new()
          |> Oban.insert!()

          # GC every 100 jobs
          if rem(row_index, 100) == 0 do
            :erlang.garbage_collect()
            Logger.info("Enqueued #{row_index - 1} jobs so far...")
          end
        end

        # Store total job count in ETS for cleanup tracking
        :ets.insert(table_name, {:total_jobs, row_count})
        :ets.insert(table_name, {:completed_jobs, 0})

        Logger.info("Enqueued #{row_count} row processing jobs (streamed)")

        {:ok, row_count}
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
