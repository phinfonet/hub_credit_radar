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
  def perform(%Oban.Job{
        args: %{
          "row_index" => row_index,
          "row_data" => row_data,
          "file_path" => _file_path,
          "execution_id" => execution_id
        }
      }) do
    Logger.debug("Processing debenture row ##{row_index} for execution ##{execution_id}")

    # Parse row data (list of cell values from xlsxir)
    parsed_data = parse_row_data(row_data, row_index)

    case persist_debenture(parsed_data) do
      {:ok, :created} ->
        Logger.debug("Created debenture from row ##{row_index}")
        :ok

      {:ok, :updated} ->
        Logger.debug("Updated debenture from row ##{row_index}")
        :ok

      {:skip, reason} ->
        Logger.debug("Skipped row ##{row_index}: #{inspect(reason)}")
        :ok

      {:error, reason} ->
        Logger.error("Failed to persist row ##{row_index}: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp parse_row_data(row, _row_index) when is_list(row) and length(row) > 15 do
    # Extract only numeric data from xlsxir (inline strings not available)
    # Column I (index 8): coupon_rate
    # Column P (index 15): duration
    coupon_rate = row |> Enum.at(8) |> to_decimal()
    duration = row |> Enum.at(15) |> to_decimal()

    %{
      "security_type" => "debenture",
      "series" => "ÚNICA",
      "issuing" => "N/A",
      "coupon_rate" => coupon_rate,
      "duration" => to_integer(duration),
      # Fields below are empty (come from inline strings)
      "code" => "",
      "issuer" => "",
      "correction_rate_type" => "",
      "correction_rate" => "",
      "reference_date" => nil,
      "maturity_date" => nil,
      "ntnb_reference_date" => nil,
      "benchmark_index" => nil,
      "ntnb_reference" => "",
      "credit_risk" => ""
    }
  end

  defp parse_row_data(_row, _row_index), do: nil

  defp to_decimal(nil), do: nil
  defp to_decimal(value) when is_number(value), do: Decimal.from_float(value)
  defp to_decimal(_), do: nil

  defp to_integer(nil), do: nil

  defp to_integer(%Decimal{} = d) do
    d
    |> Decimal.round(0, :down)
    |> Decimal.to_integer()
  end

  defp to_integer(value) when is_integer(value), do: value
  defp to_integer(_), do: nil

  defp persist_debenture(attrs) do
    # Validate required fields
    unless attrs["code"] && attrs["issuer"] && attrs["code"] != "" && attrs["issuer"] != "" do
      {:skip, :missing_required_fields}
    else
      # Convert string dates to Date and decimals to proper types
      attrs_with_dates =
        attrs
        |> Map.update("reference_date", nil, &parse_date/1)
        |> Map.update("maturity_date", nil, &parse_date/1)
        |> Map.update("ntnb_reference_date", nil, &parse_date/1)
        |> Map.update("coupon_rate", nil, &parse_decimal/1)
        |> Map.update("duration", nil, &parse_duration/1)

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

  defp parse_decimal(nil), do: nil
  defp parse_decimal(%Decimal{} = decimal), do: decimal
  defp parse_decimal(number) when is_number(number), do: Decimal.from_float(number)

  defp parse_decimal(decimal_string) when is_binary(decimal_string) do
    case Decimal.parse(decimal_string) do
      {decimal, ""} -> decimal
      _ -> nil
    end
  end

  defp parse_decimal(_), do: nil

  defp parse_duration(nil), do: nil
  defp parse_duration(duration) when is_integer(duration), do: duration

  defp parse_duration(duration_string) when is_binary(duration_string) do
    case Integer.parse(duration_string) do
      {int, ""} -> int
      _ -> nil
    end
  end

  defp parse_duration(_), do: nil
end
