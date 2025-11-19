defmodule CreditRadar.Workers.IngestDebenturesJob do
  @moduledoc """
  Oban worker for processing debenture XLS file uploads.

  Configured to run in the :debentures queue with max_concurrency: 1
  to prevent OOM issues when processing large files.
  """
  use Oban.Worker, queue: :debentures, max_attempts: 3

  alias CreditRadar.Ingestions
  alias CreditRadar.Ingestions.Tasks.IngestDebenturesXls
  alias CreditRadar.Repo

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

    # Run the ingestion task with the execution for progress tracking
    try do
      case IngestDebenturesXls.run(execution, file_path) do
        {:ok, stats} ->
          Logger.info("🟢 IngestDebenturesJob completed successfully for execution ##{execution_id}")
          Logger.info("🟢 Stats: #{inspect(stats)}")

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

  @impl Oban.Worker
  def timeout(_job), do: :timer.minutes(30)
end
