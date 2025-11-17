defmodule CreditRadar.GraphQL.Client do
  @moduledoc """
  Minimal GraphQL client backed by `Req`. It exposes `query/3` and `mutate/3`
  helpers that return either `{:ok, data}` or `{:error, reason}` to save callers
  from dealing with raw HTTP responses.
  """

  require Logger

  @default_headers [{"content-type", "application/json"}]

  @spec query(String.t(), map(), keyword()) :: {:ok, map()} | {:error, term()}
  def query(document, variables \\ %{}, opts \\ []) do
    run(document, variables, opts)
  end

  @spec mutate(String.t(), map(), keyword()) :: {:ok, map()} | {:error, term()}
  def mutate(document, variables \\ %{}, opts \\ []) do
    run(document, variables, opts)
  end

  defp run(document, variables, opts) do
    opts = build_opts(opts)

    req_opts =
      opts.req_opts
      |> Keyword.put(:url, opts.url)
      |> Keyword.put(:json, %{query: document, variables: variables})
      |> Keyword.update(:headers, opts.headers, fn headers -> opts.headers ++ headers end)

    case Req.request(req_opts) do
      {:ok, %Req.Response{status: status, body: %{"errors" => errors}}}
      when status in 200..299 and is_list(errors) and errors != [] ->
        {:error, errors}

      {:ok, %Req.Response{status: status, body: %{"data" => data}}} when status in 200..299 ->
        {:ok, data}

      {:ok, %Req.Response{status: status, body: body}} ->
        {:error, %{status: status, body: body}}

      {:error, reason} ->
        Logger.error("GraphQL request failed: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp build_opts(opts) do
    config = Application.get_env(:credit_radar, __MODULE__, [])
    url = Keyword.get(opts, :url, Keyword.get(config, :url))

    if is_nil(url) do
      raise """
      Missing GraphQL endpoint configuration for #{inspect(__MODULE__)}.
      Set :url for :credit_radar, #{inspect(__MODULE__)} (e.g. via GRAPHQL_API_URL)
      or pass the :url option when calling the client.
      """
    end

    headers =
      config
      |> Keyword.get(:headers, @default_headers)
      |> Kernel.++(Keyword.get(opts, :headers, []))
      |> maybe_add_token(Keyword.get(opts, :token, Keyword.get(config, :token)))

    req_opts = Keyword.get(config, :req_opts, []) |> Keyword.merge(Keyword.get(opts, :req_opts, []))

    %{url: url, headers: headers, req_opts: req_opts}
  end

  defp maybe_add_token(headers, nil), do: headers
  defp maybe_add_token(headers, token), do: [{"authorization", "Bearer #{token}"} | headers]
end
