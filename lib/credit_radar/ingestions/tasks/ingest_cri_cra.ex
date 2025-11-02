defmodule CreditRadar.Ingestions.Tasks.IngestCriCra do
  @moduledoc """
  Ingestion task for CRI/CRA securities from Anbima API.

  ## Campos retornados pela API Anbima (Mercado Secundário CRI/CRA)

  ### Campos Utilizados no Sistema
  - `codigo_ativo` → `code` (string) - Código do ativo
  - `data_referencia` → `reference_date` (date) - Data de referência
  - `duration` → `duration` (decimal→integer) - Duration em dias
  - `emissao` → `issuing` (string) - Número da emissão
  - `emissor` → `issuer` (string) - Nome do emissor (securitizadora)
  - `originador` → `credit_risk` (string) - Empresa originadora do crédito (risco de crédito real)
  - `referencia_ntnb` → `ntnb_reference` (string) - Referência NTN-B
  - `data_referencia_ntnb` → `ntnb_reference_date` (date) - Data da referência NTN-B
  - `serie` → `series` (string) - Série do ativo
  - `taxa_correcao` → `correction_rate` (decimal) - Taxa de correção
  - `taxa_indicativa` → `coupon_rate` (decimal) - Taxa indicativa
  - `tipo_contrato` → `security_type` (enum) - Tipo de contrato (CRI/CRA)

  ### Campos Disponíveis mas Não Utilizados
  - `data_vencimento` - Data de vencimento
  - `desvio_padrao` - Desvio padrão
  - `originador_credito` - Crédito do originador (parece duplicar `originador`)
  - `pu_indicativo` - PU indicativo
  - `quantidade_disponivel` - Quantidade disponível
  - `vl_pu` - Valor PU
  - `taxa_compra` - Taxa de compra
  - `taxa_venda` - Taxa de venda
  - `tipo_remuneracao` - Tipo de remuneração
  - `data_finalizado` - Data de finalização
  - `percent_vne` - Percentual VNE
  - `percent_pu_par` - Percentual PU par
  - `percent_reune` - Percentual REUNE
  - `pu` - Preço unitário

  ## Campos Calculados
  - `benchmark_index` - Determinado com base em `referencia_ntnb` e `data_referencia_ntnb`
  - `expected_return` - Calculado automaticamente como `coupon_rate * correction_rate`
  """

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

  def start_link(execution) do
    Task.start_link(__MODULE__, :run, [execution])
  end

  @doc """
  Executes the CRI/CRA ingestion pipeline.
  """
  def run(execution \\ %{}) do
    Logger.info("Starting CRI & CRA ingestion for execution #{execution_id(execution)}")

    result =
      with {:ok, payload} <- fetch_remote_data(),
           {:ok, operations} <- describe_operations(payload),
           {:ok, stats} <- persist_operations(operations) do
        Logger.info("✅ CRI/CRA ingestion completed successfully: #{inspect(stats)}")
        _ = report_intermediate_progress(execution, 100)
        {:ok, stats}
      else
        {:error, reason} = error ->
          Logger.error("❌ CRI/CRA ingestion failed: #{inspect(reason)}")
          error
      end

    case result do
      {:ok, _} -> :ok
      error -> error
    end
  end

  defp execution_id(%{id: id}) when not is_nil(id), do: id
  defp execution_id(_), do: "n/a"

  defp report_intermediate_progress(%{id: id}, progress) when is_integer(id) do
    Ingestions.report_progress(id, progress)
  rescue
    _ -> :ok
  end

  defp report_intermediate_progress(%Execution{} = execution, progress) do
    Ingestions.report_progress(execution, progress)
  rescue
    _ -> :ok
  end

  defp report_intermediate_progress(_execution, _progress), do: :ok

  defp fetch_remote_data do
    Client.fetch_cri_cra_secondary()
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

    # Debug: mostra apenas o primeiro item mapeado como exemplo
    if length(operations) > 0 do
      IO.puts("\n=== Exemplo de Mapeamento (primeiro item) ===")
      IO.inspect(List.first(operations), label: "Item mapeado", limit: :infinity, pretty: true)
      IO.puts("Total de items mapeados: #{length(operations)}\n")
    end

    {:ok, operations}
  end

  defp describe_operations(_payload), do: {:error, :invalid_payload}

  defp map_entry(entry) when is_map(entry) do
    ntnb_reference = fetch(entry, "referencia_ntnb")
    ntnb_reference_date = parse_date(fetch(entry, "data_referencia_ntnb"))

    %{
      code: fetch(entry, "codigo_ativo"),
      reference_date: parse_date(fetch(entry, "data_referencia")),
      duration: decimal(fetch(entry, "duration")),
      issuing: fetch(entry, "emissao"),
      issuer: fetch(entry, "emissor"),
      credit_risk: fetch(entry, "originador"),
      ntnb_reference: ntnb_reference,
      ntnb_reference_date: ntnb_reference_date,
      benchmark_index: determine_benchmark_index(ntnb_reference, ntnb_reference_date),
      series: fetch(entry, "serie"),
      correction_rate: decimal(fetch(entry, "taxa_correcao")),
      coupon_rate: decimal(fetch(entry, "taxa_indicativa")),
      security_type: entry |> fetch("tipo_contrato") |> format_contract_type()
    }
  rescue
    _ -> nil
  end

  defp map_entry(_), do: nil

  def persist_operations(operations) when is_list(operations) do
    stats =
      Enum.reduce(operations, %{created: 0, updated: 0, skipped: 0, errors: []}, fn operation, acc ->
        case persist_operation(operation) do
          {:ok, :created} ->
            %{acc | created: acc.created + 1}

          {:ok, :updated} ->
            %{acc | updated: acc.updated + 1}

          {:skip, reason} ->
            Logger.debug("Skipping CRI/CRA security persistence because #{inspect(reason)}: #{inspect(operation)}")
            %{acc | skipped: acc.skipped + 1}

          {:error, reason} ->
            %{acc | errors: acc.errors ++ [{:error, reason, operation}]}
        end
      end)
      
    Logger.info(
      "CRI/CRA ingestion persisted #{stats.created + stats.updated} securities " <>
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
    series = operation |> Map.get(:series) |> normalize_string()
    issuing = operation |> Map.get(:issuing) |> normalize_string()
    credit_risk = operation |> Map.get(:credit_risk) |> normalize_string()
    duration = operation |> Map.get(:duration) |> decimal_to_integer()
    reference_date = Map.get(operation, :reference_date)
    benchmark_index = operation |> Map.get(:benchmark_index) |> normalize_string()
    ntnb_reference = operation |> Map.get(:ntnb_reference) |> normalize_string()
    ntnb_reference_date = Map.get(operation, :ntnb_reference_date)
    coupon_rate = Map.get(operation, :coupon_rate)
    correction_rate = Map.get(operation, :correction_rate)

    cond do
      is_nil(code) ->
        {:skip, :missing_code}

      is_nil(security_type) ->
        {:skip, :missing_security_type}

      is_nil(issuer) ->
        {:skip, :missing_issuer}

      is_nil(series) ->
        {:skip, :missing_series}

      is_nil(issuing) ->
        {:skip, :missing_issuing}

      is_nil(duration) ->
        {:skip, :missing_duration}

      true ->
        attrs =
          %{
            code: code,
            security_type: security_type,
            issuer: issuer,
            series: series,
            issuing: issuing,
            credit_risk: credit_risk,
            duration: duration,
            reference_date: reference_date,
            benchmark_index: benchmark_index,
            ntnb_reference: ntnb_reference,
            ntnb_reference_date: ntnb_reference_date,
            coupon_rate: coupon_rate,
            correction_rate: correction_rate,
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

  # Simplesmente pega o valor do map, sem tentar conversões que podem perder dados
  defp fetch(entry, key) when is_binary(key) do
    Map.get(entry, key)
  end

  defp fetch(entry, key) when is_atom(key) do
    Map.get(entry, Atom.to_string(key))
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

  defp parse_datetime(nil), do: nil

  defp parse_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _} -> datetime
      _ -> nil
    end
  end

  defp parse_datetime(_), do: nil

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

  defp format_contract_type("CRI"), do: :cri
  defp format_contract_type("CRA"), do: :cra
  defp format_contract_type("debenture"), do: :debenture
  defp format_contract_type("debenture+"), do: :debenture_plus

  defp determine_benchmark_index(ntnb_reference, ntnb_reference_date)
       when not is_nil(ntnb_reference) and not is_nil(ntnb_reference_date) do
    "ipca"
  end

  defp determine_benchmark_index(_, _), do: nil
end
