defmodule CreditRadar.Workers.ProcessDebentureRowJob do
  @moduledoc """
  Oban worker for processing a single debenture row.

  This job is enqueued by IngestDebenturesJob for each row in the XLSX file.
  Processing rows individually avoids OOM issues with large files.
  """
  use Oban.Worker, queue: :debenture_rows, max_attempts: 3

  alias CreditRadar.FixedIncome
  alias CreditRadar.FixedIncome.Security
  alias CreditRadar.Repo
  alias Decimal

  require Logger

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"row_data" => row_data, "execution_id" => execution_id}}) do
    Logger.debug("Processing debenture row for execution ##{execution_id}")

    case persist_debenture(row_data) do
      {:ok, :created} ->
        Logger.debug("Created debenture: #{row_data["code"]}")
        :ok

      {:ok, :updated} ->
        Logger.debug("Updated debenture: #{row_data["code"]}")
        :ok

      {:skip, reason} ->
        Logger.debug("Skipped debenture #{row_data["code"]}: #{inspect(reason)}")
        :ok

      {:error, reason} ->
        Logger.error("Failed to persist debenture #{row_data["code"]}: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp persist_debenture(attrs) do
    # Validate required fields
    unless attrs["code"] && attrs["issuer"] do
      {:skip, :missing_required_fields}
    else
      # Convert string dates to Date
      attrs_with_dates =
        attrs
        |> Map.update("reference_date", nil, &parse_date/1)
        |> Map.update("maturity_date", nil, &parse_date/1)
        |> Map.update("ntnb_reference_date", nil, &parse_date/1)

      # Try to find existing security by code
      case Repo.get_by(Security, code: attrs_with_dates["code"]) do
        nil ->
          # Create new security
          %Security{}
          |> FixedIncome.security_create_changeset(attrs_with_dates, %{})
          |> Repo.insert()
          |> case do
            {:ok, _security} -> {:ok, :created}
            {:error, changeset} -> {:error, changeset}
          end

        existing ->
          # Update existing security
          existing
          |> FixedIncome.security_update_changeset(attrs_with_dates, %{})
          |> Repo.update()
          |> case do
            {:ok, _security} -> {:ok, :updated}
            {:error, changeset} -> {:error, changeset}
          end
      end
    end
  end

  defp parse_date(nil), do: nil
  defp parse_date(%Date{} = date), do: date

  defp parse_date(date_string) when is_binary(date_string) do
    case Date.from_iso8601(date_string) do
      {:ok, date} -> date
      {:error, _} -> nil
    end
  end

  defp parse_date(_), do: nil
end
