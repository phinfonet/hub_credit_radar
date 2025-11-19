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
  Parses XLSX file and enqueues a job for each row using TRUE streaming.

  Uses ElixirXlsx for memory-efficient streaming instead of loading entire file.
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

        # Create ETS table to share XML file path with row jobs
        table_name = :"debentures_xml_#{execution_id}"
        :ets.new(table_name, [:named_table, :public, :set])
        :ets.insert(table_name, {:xml_file_path, xml_file_path})
        :ets.insert(table_name, {:total_jobs, 0})
        Logger.info("Created ETS table: #{table_name}")

        # Stream XLSX with ElixirXlsx - TRUE streaming without loading all data
        Logger.info("Opening XLSX with ElixirXlsx for streaming...")

        {:ok, xlsx} = Elixir.Xlsx.open(file_path)

        # Get first sheet
        [sheet | _] = Elixir.Xlsx.sheets(xlsx)

        Logger.info("Streaming rows and enqueuing jobs one by one...")

        # Stream rows, skip header, enqueue one job at a time
        # Since we're streaming, we can safely pass row_data in args without OOM
        row_count =
          sheet
          |> Elixir.Xlsx.stream()
          |> Stream.drop(1)  # Skip header
          |> Stream.with_index(2)  # Start from row 2 (after header)
          |> Enum.reduce(0, fn {row_data, row_index}, count ->
            # Enqueue single job with row data
            # Safe to pass row_data because we're creating jobs one at a time via streaming
            ProcessDebentureRowJob.new(%{
              row_index: row_index,
              row_data: row_data,
              ets_table: Atom.to_string(table_name),
              execution_id: execution_id
            })
            |> Oban.insert!()

            # Periodic GC and logging
            if rem(count, 50) == 0 do
              :erlang.garbage_collect()
              Process.sleep(10)

              if rem(count, 200) == 0 do
                Logger.info("Enqueued #{count} jobs so far...")
              end
            end

            count + 1
          end)

        Elixir.Xlsx.close(xlsx)

        Logger.info("Enqueued #{row_count} jobs total")

        # Store total job count in ETS for cleanup tracking
        :ets.insert(table_name, {:total_jobs, row_count})
        :ets.insert(table_name, {:completed_jobs, 0})

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
