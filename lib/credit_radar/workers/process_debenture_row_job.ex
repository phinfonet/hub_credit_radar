defmodule CreditRadar.Workers.ProcessDebentureRowJob do
  @moduledoc """
  Oban worker for processing a single debenture row.

  This job is enqueued by IngestDebenturesJob for each row in the XLSX file.
  Receives row data from XlsxReader and inline strings from XML file.
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
          "ets_table" => ets_table_str,
          "execution_id" => execution_id
        }
      }) do
    Logger.debug("Processing debenture row ##{row_index} for execution ##{execution_id}")

    # Get file paths from ETS table
    ets_table = String.to_atom(ets_table_str)
    [{:xml_file_path, xml_file_path}] = :ets.lookup(ets_table, :xml_file_path)

    # Extract inline strings for THIS row only from XML file
    inline_str_data = extract_inline_str_for_row_from_file(xml_file_path, row_index)

    # Parse row data combining numeric data + inline strings
    parsed_data = parse_row_data(row_data, row_index, inline_str_data)

    result = persist_debenture(parsed_data)

    # Track job completion and cleanup if all jobs are done
    cleanup_if_all_jobs_completed(ets_table, xml_file_path)

    case result do
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

  # Extract inline strings for a specific row from the XML file (memory efficient)
  defp extract_inline_str_for_row_from_file(xml_file_path, row_index) do
    import SweetXml

    # Read XML file and extract only this specific row
    xml_content = File.read!(xml_file_path)

    result =
      xml_content
      |> xpath(~x"//row[@r='#{row_index}']"o,
        cells: [
          ~x"./c[is]"l,
          ref: ~x"./@r"s,
          value: ~x"./is/t/text()"s
        ]
      )

    cells_map =
      case result do
        nil ->
          %{}

        row ->
          row.cells
          |> Enum.reduce(%{}, fn cell, acc ->
            if cell.value != "" do
              col = cell.ref |> String.replace(~r/\d+/, "")
              Map.put(acc, col, cell.value)
            else
              acc
            end
          end)
      end

    # Force GC after processing
    :erlang.garbage_collect()

    cells_map
  rescue
    error ->
      Logger.error("Failed to extract inline strings for row #{row_index}: #{inspect(error)}")
      %{}
  end

  # Track completed jobs and cleanup when all jobs are done
  defp cleanup_if_all_jobs_completed(ets_table, xml_file_path) do
    try do
      # Atomically increment completed counter
      completed = :ets.update_counter(ets_table, :completed_jobs, {2, 1})
      [{:total_jobs, total}] = :ets.lookup(ets_table, :total_jobs)

      if completed >= total do
        Logger.info("All #{total} jobs completed, cleaning up XML file and ETS table")

        # Delete temporary XML file and directory
        xml_dir = Path.dirname(xml_file_path)
        File.rm_rf!(xml_dir)
        Logger.info("Deleted temporary directory: #{xml_dir}")

        # Delete ETS table
        :ets.delete(ets_table)
        Logger.info("Deleted ETS table: #{ets_table}")
      end
    rescue
      error ->
        Logger.warning("Failed to cleanup (this is normal if table was already deleted): #{inspect(error)}")
    end
  end

  defp parse_row_data(row, _row_index, inline_str_data) when is_list(row) and length(row) > 15 do
    # Extract data from inline strings
    reference_date = inline_str_data |> Map.get("A") |> parse_brazilian_date()
    code = inline_str_data |> Map.get("B") |> to_string_safe()
    issuer = inline_str_data |> Map.get("C") |> to_string_safe()
    correction_rate_type = inline_str_data |> Map.get("D") |> to_string_safe()
    correction_rate_str = inline_str_data |> Map.get("E") |> to_string_safe()
    maturity_date = inline_str_data |> Map.get("F") |> parse_brazilian_date()
    ntnb_reference_str = inline_str_data |> Map.get("R") |> to_string_safe()

    # Extract numeric data from xlsxir
    coupon_rate = row |> Enum.at(8) |> to_decimal()
    duration = row |> Enum.at(15) |> to_decimal()

    # Parse ntnb_reference as date
    ntnb_reference_date = parse_brazilian_date(ntnb_reference_str)

    benchmark_index = determine_benchmark_index(ntnb_reference_date, correction_rate_type)

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
      "duration" => to_integer(duration),
      "ntnb_reference_date" => date_to_string(ntnb_reference_date),
      "benchmark_index" => benchmark_index,
      "ntnb_reference" => ntnb_reference_str
    }
  end

  defp parse_row_data(_row, _row_index, _inline_str_data), do: nil

  defp to_string_safe(nil), do: ""
  defp to_string_safe(value) when is_binary(value), do: String.trim(value)
  defp to_string_safe(value), do: to_string(value)

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

  defp date_to_string(nil), do: nil
  defp date_to_string(%Date{} = date), do: Date.to_iso8601(date)

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
