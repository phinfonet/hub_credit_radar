defmodule CreditRadar.Workers.ProcessDebentureRowJob do
  @moduledoc """
  Oban worker for processing a single debenture row.

  This job receives ALL data needed in its args (no ETS dependency).
  Can run in parallel with other row jobs safely.
  """
  use Oban.Worker, queue: :debenture_rows, max_attempts: 3

  alias CreditRadar.FixedIncome
  alias CreditRadar.FixedIncome.Security
  alias CreditRadar.Repo
  alias Ecto.Changeset
  alias Decimal

  require Logger

  @impl Oban.Worker
  def perform(%Oban.Job{
        args: %{
          "row_index" => row_index,
          "execution_id" => execution_id,
          "attrs" => attrs
        }
      }) do
    Logger.debug("Processing debenture row ##{row_index} for execution ##{execution_id}")

    # Convert string keys to atoms and parse special types
    attrs_parsed = parse_attrs(attrs)

    result = persist_debenture(attrs_parsed)

    case result do
      {:ok, :created} ->
        Logger.debug("✓ Created debenture from row ##{row_index}")
        :ok

      {:ok, :updated} ->
        Logger.debug("✓ Updated debenture from row ##{row_index}")
        :ok

      {:skip, reason} ->
        Logger.debug("⊘ Skipped row ##{row_index}: #{inspect(reason)}")
        :ok

      {:error, reason} ->
        Logger.error("✗ Failed to persist row ##{row_index}: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp parse_attrs(attrs) do
    %{
      reference_date: parse_date(attrs["reference_date"]),
      security_type: String.to_existing_atom(attrs["security_type"]),
      code: attrs["code"],
      issuer: attrs["issuer"],
      credit_risk: attrs["credit_risk"],
      correction_rate_type: attrs["correction_rate_type"],
      correction_rate: attrs["correction_rate"],
      series: attrs["series"],
      issuing: attrs["issuing"],
      maturity_date: parse_date(attrs["maturity_date"]),
      coupon_rate: parse_decimal(attrs["coupon_rate"]),
      duration: attrs["duration"],
      ntnb_reference_date: parse_date(attrs["ntnb_reference_date"]),
      benchmark_index: attrs["benchmark_index"],
      ntnb_reference: attrs["ntnb_reference"]
    }
  end

  defp persist_debenture(attrs) do
    case normalize_security_attrs(attrs) do
      {:ok, normalized_attrs} -> upsert_security(normalized_attrs)
      other -> other
    end
  end

  defp normalize_security_attrs(attrs) do
    code = normalize_string(attrs[:code])
    security_type = attrs[:security_type]
    issuer = normalize_string(attrs[:issuer])
    series = normalize_string(attrs[:series])
    issuing = normalize_string(attrs[:issuing])
    credit_risk = normalize_string(attrs[:credit_risk])
    duration = attrs[:duration]
    reference_date = attrs[:reference_date]
    benchmark_index = normalize_string(attrs[:benchmark_index])
    ntnb_reference = normalize_string(attrs[:ntnb_reference])
    ntnb_reference_date = attrs[:ntnb_reference_date]
    coupon_rate = attrs[:coupon_rate]
    correction_rate = attrs[:correction_rate]

    cond do
      is_nil(code) ->
        {:skip, :missing_code}

      is_nil(security_type) ->
        {:skip, :missing_security_type}

      is_nil(issuer) ->
        {:skip, :missing_issuer}

      is_nil(duration) ->
        {:skip, :missing_duration}

      true ->
        normalized =
          %{
            code: code,
            security_type: security_type,
            issuer: issuer,
            series: series || "ÚNICA",
            issuing: issuing || "N/A",
            credit_risk: credit_risk,
            duration: duration,
            reference_date: reference_date,
            benchmark_index: benchmark_index,
            ntnb_reference: ntnb_reference,
            ntnb_reference_date: ntnb_reference_date,
            coupon_rate: coupon_rate,
            correction_rate: correction_rate,
            sync_source: :xls
          }

        {:ok, normalized}
    end
  end

  defp upsert_security(attrs) do
    lookup =
      [:code, :series]
      |> Enum.reduce(%{}, fn key, acc ->
        case Map.get(attrs, key) do
          nil -> acc
          value -> Map.put(acc, key, value)
        end
      end)

    if map_size(lookup) == 0 do
      {:error, :missing_lookup_keys}
    else
      case Repo.get_by(Security, lookup) do
        nil -> create_security(attrs)
        %Security{} = security -> update_security(security, attrs)
      end
    end
  end

  defp create_security(attrs) do
    attrs = Map.put_new(attrs, :sync_source, :xls)

    %Security{}
    |> FixedIncome.security_create_changeset(attrs)
    |> Repo.insert()
    |> case do
      {:ok, _security} ->
        {:ok, :created}

      {:error, %Changeset{} = changeset} ->
        {:error, {:changeset_error, changeset_errors(changeset)}}
    end
  end

  defp update_security(%Security{} = security, attrs) do
    attrs = Map.put_new(attrs, :sync_source, security.sync_source || :xls)

    security
    |> FixedIncome.security_update_changeset(attrs)
    |> Repo.update()
    |> case do
      {:ok, _security} ->
        {:ok, :updated}

      {:error, %Changeset{} = changeset} ->
        {:error, {:changeset_error, changeset_errors(changeset)}}
    end
  end

  # Helper functions
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

  defp parse_decimal(number) when is_number(number) do
    Decimal.from_float(number * 1.0)
  end

  defp parse_decimal(decimal_string) when is_binary(decimal_string) do
    case Decimal.parse(decimal_string) do
      {decimal, ""} -> decimal
      _ -> nil
    end
  end

  defp parse_decimal(%{"coef" => coef, "exp" => exp, "sign" => sign}) do
    # Reconstruct Decimal from serialized map
    Decimal.new(sign, coef, exp)
  rescue
    _ -> nil
  end

  defp parse_decimal(_), do: nil

  defp normalize_string(value) when is_binary(value) do
    value
    |> String.trim()
    |> case do
      "" -> nil
      trimmed -> trimmed
    end
  end

  defp normalize_string(value), do: value

  defp changeset_errors(%Changeset{} = changeset) do
    Changeset.traverse_errors(changeset, fn {message, opts} ->
      Enum.reduce(opts, message, fn {key, value}, acc ->
        string_value = safe_to_string(value)
        String.replace(acc, "%{#{key}}", string_value)
      end)
    end)
  end

  defp safe_to_string(value) when is_binary(value), do: value
  defp safe_to_string(value) when is_atom(value), do: to_string(value)
  defp safe_to_string(value) when is_integer(value), do: Integer.to_string(value)
  defp safe_to_string(value) when is_float(value), do: Float.to_string(value)
  defp safe_to_string(value) when is_tuple(value), do: inspect(value)
  defp safe_to_string(value), do: inspect(value)
end
