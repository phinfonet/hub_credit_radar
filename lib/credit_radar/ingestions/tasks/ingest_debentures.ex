defmodule CreditRadar.Ingestions.Tasks.IngestDebentures do
  @moduledoc false

  use Task, restart: :transient

  require Logger

  alias CreditRadar.FixedIncome
  alias CreditRadar.FixedIncome.Security
  alias CreditRadar.Ingestions
  alias CreditRadar.Ingestions.Execution
  alias CreditRadar.Integrations.Anbima.Client
  alias CreditRadar.Repo
  alias Ecto.Changeset
  alias Decimal

  @impl true
  def start_link(execution) do
    Task.start_link(__MODULE__, :run, [execution])
  end

  @doc """
  Executes the Debêntures ingestion pipeline.
  """
  def run(execution \\ %{}) do
    Logger.info("Starting Debêntures ingestion for execution #{execution_id(execution)}")

    with {:ok, payload} <- fetch_remote_data(),
         {:ok, operations} <- describe_operations(payload),
         {:ok, _stats} <- persist_operations(operations) do
      _ = report_intermediate_progress(execution, 100)
      :ok
    else
      {:error, reason} = error ->
        Logger.error("Debêntures ingestion failed: #{inspect(reason)}")
        error
    end
  end

  defp execution_id(%{id: id}) when not is_nil(id), do: id
  defp execution_id(_), do: "n/a"

  defp report_intermediate_progress(%{id: id}, progress) when is_integer(id) do
    Ingestions.report_progress(id, progress)
  end

  defp report_intermediate_progress(%Execution{} = execution, progress) do
    Ingestions.report_progress(execution, progress)
  end

  defp report_intermediate_progress(_execution, _progress), do: :ok

  defp fetch_remote_data do
    Client.fetch_debentures_secondary()
    |> normalize_response()
  end

  defp normalize_response({:ok, %Req.Response{} = response}), do: normalize_response(response)

  defp normalize_response(%Req.Response{status: 200, body: %{"data" => data}}) when is_list(data),
    do: {:ok, data}

  defp normalize_response(%Req.Response{status: 200, body: body}) when is_list(body),
    do: {:ok, body}

  defp normalize_response(%Req.Response{status: status, body: body}),
    do: {:error, {:unexpected_status, status, body}}

  defp normalize_response({:error, reason}), do: {:error, reason}
  defp normalize_response(other), do: {:error, {:unexpected_response, other}}

  defp describe_operations(payload) when is_list(payload) do
    operations =
      payload
      |> Enum.map(&map_entry/1)
      |> Enum.reject(&is_nil/1)

    {:ok, operations}
  end

  defp describe_operations(_payload), do: {:error, :invalid_payload}

  defp map_entry(entry) when is_map(entry) do
    ntnb_reference = fetch(entry, "referencia_ntnb")

    %{
      code: fetch(entry, "codigo_ativo"),
      reference_date: parse_date(fetch(entry, "data_referencia")),
      duration: decimal(fetch(entry, "duration")),
      issuer: fetch(entry, "emissor"),
      credit_risk: fetch(entry, "emissor"),
      benchmark_index: normalize_benchmark_index(fetch(entry, "grupo")),
      ntnb_reference: ntnb_reference,
      ntnb_reference_date: parse_date(ntnb_reference),
      coupon_rate: decimal(fetch(entry, "taxa_indicativa")),
      security_type: :debenture
    }
  rescue
    _ -> nil
  end

  defp map_entry(_), do: nil

  defp normalize_benchmark_index(nil), do: nil
  defp normalize_benchmark_index(value) when is_binary(value), do: String.downcase(value)
  defp normalize_benchmark_index(_), do: nil

  def persist_operations(operations) when is_list(operations) do
    stats =
      Enum.reduce(operations, %{created: 0, updated: 0, skipped: 0, errors: []}, fn operation, acc ->
        case persist_operation(operation) do
          {:ok, :created} ->
            %{acc | created: acc.created + 1}

          {:ok, :updated} ->
            %{acc | updated: acc.updated + 1}

          {:skip, reason} ->
            Logger.debug("Skipping Debêntures security persistence because #{inspect(reason)}: #{inspect(operation)}")
            %{acc | skipped: acc.skipped + 1}

          {:error, reason} ->
            %{acc | errors: acc.errors ++ [{:error, reason, operation}]}
        end
      end)

    Logger.info(
      "Debêntures ingestion persisted #{stats.created + stats.updated} securities " <>
        "(created: #{stats.created}, updated: #{stats.updated}, skipped: #{stats.skipped}, errors: #{length(stats.errors)})"
    )

    case stats.errors do
      [] -> {:ok, stats}
      errors -> {:error, {:persistence_failed, errors}}
    end
  end

  def persist_operations(_operations), do: {:error, :invalid_operations}

  defp persist_operation(%{} = operation) do
    case normalize_security_attrs(operation) do
      {:ok, attrs} -> upsert_security(attrs)
      other -> other
    end
  end

  defp persist_operation(_operation), do: {:skip, :invalid_operation}

  defp normalize_security_attrs(operation) do
    code = operation |> Map.get(:code) |> normalize_string()
    security_type = Map.get(operation, :security_type)
    issuer = operation |> Map.get(:issuer) |> normalize_string()
    credit_risk = operation |> Map.get(:credit_risk) |> normalize_string()
    duration = operation |> Map.get(:duration) |> decimal_to_integer()
    reference_date = Map.get(operation, :reference_date)
    benchmark_index = operation |> Map.get(:benchmark_index) |> normalize_string()
    ntnb_reference = operation |> Map.get(:ntnb_reference) |> normalize_string()
    ntnb_reference_date = Map.get(operation, :ntnb_reference_date)
    coupon_rate = Map.get(operation, :coupon_rate)

    cond do
      is_nil(code) ->
        {:skip, :missing_code}

      is_nil(security_type) ->
        {:skip, :missing_security_type}

      is_nil(issuer) ->
        {:skip, :missing_issuer}

      is_nil(credit_risk) ->
        {:skip, :missing_credit_risk}

      is_nil(duration) ->
        {:skip, :missing_duration}

      true ->
        attrs =
          %{
            code: code,
            security_type: security_type,
            issuer: issuer,
            series: "ÚNICA",
            issuing: "N/A",
            credit_risk: credit_risk,
            duration: duration,
            reference_date: reference_date,
            benchmark_index: benchmark_index,
            ntnb_reference: ntnb_reference,
            ntnb_reference_date: ntnb_reference_date,
            coupon_rate: coupon_rate,
            sync_source: :api
          }

        {:ok, attrs}
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
    attrs = Map.put_new(attrs, :sync_source, :api)

    %Security{}
    |> FixedIncome.security_create_changeset(attrs)
    |> Repo.insert()
    |> case do
      {:ok, _security} -> {:ok, :created}
      {:error, %Changeset{} = changeset} -> {:error, {:changeset_error, changeset_errors(changeset)}}
    end
  end

  defp update_security(%Security{} = security, attrs) do
    attrs = Map.put_new(attrs, :sync_source, security.sync_source || :api)

    security
    |> FixedIncome.security_update_changeset(attrs)
    |> Repo.update()
    |> case do
      {:ok, _security} -> {:ok, :updated}
      {:error, %Changeset{} = changeset} -> {:error, {:changeset_error, changeset_errors(changeset)}}
    end
  end

  defp normalize_string(value) when is_binary(value) do
    value
    |> String.trim()
    |> case do
      "" -> nil
      trimmed -> trimmed
    end
  end

  defp normalize_string(value), do: value

  defp decimal_to_integer(nil), do: nil
  defp decimal_to_integer(value) when is_integer(value), do: value

  defp decimal_to_integer(%Decimal{} = value) do
    value
    |> Decimal.round(0, :floor)
    |> Decimal.to_integer()
  rescue
    _ -> nil
  end

  defp decimal_to_integer(value) when is_float(value), do: trunc(value)

  defp decimal_to_integer(value) when is_binary(value) do
    case Integer.parse(value) do
      {integer, _} -> integer
      :error -> nil
    end
  end

  defp decimal_to_integer(_), do: nil

  defp changeset_errors(%Changeset{} = changeset) do
    Changeset.traverse_errors(changeset, fn {message, opts} ->
      Enum.reduce(opts, message, fn {key, value}, acc ->
        String.replace(acc, "%{#{key}}", to_string(value))
      end)
    end)
  end

  defp fetch(entry, key) when is_binary(key) do
    Map.get(entry, key) || Map.get(entry, String.to_existing_atom(key))
  rescue
    ArgumentError -> Map.get(entry, key)
  end

  defp fetch(entry, key) when is_atom(key) do
    Map.get(entry, key) || Map.get(entry, Atom.to_string(key))
  end

  defp fetch(_entry, _key), do: nil

  defp parse_date(nil), do: nil

  defp parse_date(value) when is_binary(value) do
    case Date.from_iso8601(value) do
      {:ok, date} -> date
      _ -> nil
    end
  end

  defp parse_date(_), do: nil

  defp decimal(nil), do: nil
  defp decimal(%Decimal{} = decimal), do: decimal

  defp decimal(value) when is_binary(value) do
    case Decimal.parse(value) do
      {decimal, ""} -> decimal
      _ -> nil
    end
  end

  defp decimal(value) when is_integer(value), do: Decimal.new(value)
  defp decimal(value) when is_float(value), do: Decimal.from_float(value)
  defp decimal(_), do: nil
end
