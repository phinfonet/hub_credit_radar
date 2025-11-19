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
        # Extract sheet1.xml to temporary file (avoids loading entire XML in memory)
        Logger.info("Extracting sheet1.xml to temporary file...")
        xml_file_path = extract_sheet_xml_to_file(file_path, execution_id)
        Logger.info("Extracted sheet1.xml to: #{xml_file_path}")

        # Create ETS table to share XML file path with row jobs
        table_name = :"debentures_xml_#{execution_id}"
        :ets.new(table_name, [:named_table, :public, :set])
        :ets.insert(table_name, {:xml_file_path, xml_file_path})
        :ets.insert(table_name, {:total_jobs, 0})
        Logger.info("Created ETS table: #{table_name}")

        # Stream the file row by row to avoid loading all rows in memory
        Logger.info("Opening XLSX file for streaming...")
        {:ok, pid} = Xlsxir.multi_extract(file_path, 0)

        # Use stream_list to process row by row without loading everything in memory
        row_count =
          pid
          |> Xlsxir.stream_list()
          |> Stream.drop(1)  # Skip header row
          |> Stream.with_index(2)  # Start counting from row 2 (Excel row numbers)
          |> Enum.reduce(0, fn {row, row_index}, count ->
            # Skip empty rows
            if Enum.all?(row, &is_nil/1) do
              count
            else
              # Enqueue job with numeric data + reference to ETS table
              %{
                row_index: row_index,
                row_data: row,
                ets_table: Atom.to_string(table_name),
                execution_id: execution_id
              }
              |> ProcessDebentureRowJob.new()
              |> Oban.insert!()

              count + 1
            end
          end)

        Xlsxir.close(pid)

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
  defp extract_sheet_xml_to_file(file_path, execution_id) do
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
        # Extract to temporary directory (not to memory)
        tmp_dir = "/tmp/debentures-#{execution_id}"
        File.mkdir_p!(tmp_dir)

        Logger.info("Extracting #{List.to_string(sheet_name)} to #{tmp_dir}")

        # Extract using :zip.extract with proper options
        case :zip.extract(charlist_path, [
          {:file_list, [sheet_name]},
          {:cwd, String.to_charlist(tmp_dir)}
        ]) do
          {:ok, extracted_files} ->
            Logger.info("Successfully extracted files: #{inspect(extracted_files)}")

            # Return path to extracted XML file
            xml_path = Path.join(tmp_dir, List.to_string(sheet_name))
            Logger.info("Sheet XML extracted to: #{xml_path}, size: #{File.stat!(xml_path).size} bytes")
            xml_path

          {:error, reason} ->
            Logger.error("Failed to extract XML: #{inspect(reason)}")
            raise "Failed to extract sheet1.xml: #{inspect(reason)}"
        end

      nil ->
        raise "Could not find sheet1.xml in XLSX file"
    end
  end


  @impl Oban.Worker
  def timeout(_job), do: :timer.minutes(30)
end
