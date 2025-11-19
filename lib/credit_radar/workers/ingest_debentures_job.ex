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
        # Extract inline strings ONCE for all rows (reusing existing logic from ingest_debentures_xls.ex)
        Logger.info("Extracting inline strings from XLSX...")
        inline_str_data = extract_inline_str_cells(file_path)
        Logger.info("Extracted inline strings from #{map_size(inline_str_data)} rows")

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
              # Get inline strings for this specific row
              row_inline_data = Map.get(inline_str_data, row_index, %{})

              # Enqueue job with numeric data + inline strings for this row
              %{
                row_index: row_index,
                row_data: row,
                inline_str_data: row_inline_data,
                execution_id: execution_id
              }
              |> ProcessDebentureRowJob.new()
              |> Oban.insert!()

              count + 1
            end
          end)

        Xlsxir.close(pid)

        Logger.info("Enqueued #{row_count} row processing jobs (streamed)")

        {:ok, row_count}
      rescue
        error ->
          Logger.error("Failed to parse XLSX file: #{inspect(error)}")
          {:error, {:parse_error, error}}
      end
    end
  end

  # Reuse the inline string extraction logic from ingest_debentures_xls.ex
  defp extract_inline_str_cells(file_path) do
    import SweetXml

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
        # Extract only sheet1.xml
        {:ok, [{^sheet_name, sheet_xml}]} =
          :zip.extract(charlist_path, [
            {:file_list, [sheet_name]},
            :memory
          ])

        # Parse with xpath filter (only rows with inline strings)
        result =
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

        # Force GC immediately after extraction
        :erlang.garbage_collect()

        result

      nil ->
        Logger.warning("Could not find sheet1.xml in XLSX file")
        %{}
    end
  rescue
    error ->
      Logger.error("Failed to extract inline strings: #{inspect(error)}")
      %{}
  end


  @impl Oban.Worker
  def timeout(_job), do: :timer.minutes(30)
end
