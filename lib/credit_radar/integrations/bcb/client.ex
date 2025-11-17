defmodule CreditRadar.Integrations.BCB.Client do
  @moduledoc """
  Lightweight wrapper around the Banco Central do Brasil (BCB) open data API.

  Currently exposes helpers to read the CDI time series (SGS 4391), which will
  be used downstream for projections and portfolio calculations.
  """

  @default_base_url "https://api.bcb.gov.br"
  @cdi_series_path "/dados/serie/bcdata.sgs.4391/dados"
  @selic_series_path "/dados/serie/bcdata.sgs.4390/dados"

  @doc """
  Fetches CDI daily values (series 4391) from the BCB open data API.

  Accepts optional query params (string or atom keys) understood by the service,
  such as `"dataInicial"` and `"dataFinal"`. The response body is returned as-is
  from the API (a list of maps containing `"data"` and `"valor"`).
  """
  @spec fetch_cdi_series(map(), keyword()) ::
          {:ok, list()} | {:error, {:unexpected_status, non_neg_integer(), term()} | term()}
  def fetch_cdi_series(params \\ %{}, opts \\ [])

  def fetch_cdi_series(params, opts) when is_map(params) and is_list(opts) do
    fetch_series(@cdi_series_path, params, opts)
  end

  def fetch_cdi_series(_params, _opts) do
    raise ArgumentError, "fetch_cdi_series/2 expects a map of query params"
  end

  @doc """
  Fetches SELIC daily values (series 4390) from the BCB open data API.
  """
  @spec fetch_selic_series(map(), keyword()) ::
          {:ok, list()} | {:error, {:unexpected_status, non_neg_integer(), term()} | term()}
  def fetch_selic_series(params \\ %{}, opts \\ [])

  def fetch_selic_series(params, opts) when is_map(params) and is_list(opts) do
    fetch_series(@selic_series_path, params, opts)
  end

  def fetch_selic_series(_params, _opts) do
    raise ArgumentError, "fetch_selic_series/2 expects a map of query params"
  end

  defp fetch_series(path, params, opts) do
    request_fun = Keyword.get(opts, :request_fun, &Req.get/2)

    params
    |> build_params()
    |> request_series(path, client(opts), request_fun)
  end

  defp request_series(params, path, client, request_fun) do
    client
    |> request_fun.(url: path, params: params)
    |> normalize_response()
  end

  defp build_params(params) do
    params
    |> Map.new(fn
      {key, value} when is_atom(key) -> {Atom.to_string(key), value}
      pair -> pair
    end)
    |> Map.put_new("formato", "json")
  end

  defp client(opts) do
    base_url =
      Keyword.get(opts, :base_url) ||
        Application.get_env(:credit_radar, :bcb_api, [])
        |> Keyword.get(:base_url, @default_base_url)

    Req.new(
      base_url: base_url,
      headers: [
        {"accept", "application/json"}
      ]
    )
  end

  defp normalize_response({:ok, %Req.Response{status: 200, body: body}}) when is_list(body),
    do: {:ok, body}

  defp normalize_response({:ok, %Req.Response{status: status, body: body}}),
    do: {:error, {:unexpected_status, status, body}}

  defp normalize_response({:error, reason}), do: {:error, reason}
end
