defmodule CreditRadar.Integrations.Anbima.Auth do
  @moduledoc """
  Authentication module for Anbima API integration.
  Handles OAuth token retrieval and caching.
  """

  require Logger

  @token_store_key :anbima_token

  def fetch_token! do
    # Garante que o Cache está inicializado antes de tentar usar
    ensure_cache_started!()

    case CreditRadar.Cache.get(@token_store_key) do
      nil ->
        Logger.debug("Anbima token not in cache, fetching new token")

        client()
        |> Req.post(url: "/oauth/access-token", json: %{grant_type: "client_credentials"})
        |> save_token()

      token ->
        Logger.debug("Using cached Anbima token")
        token
    end
  rescue
    e ->
      Logger.error("Failed to fetch Anbima token: #{inspect(e)}")
      Logger.error(Exception.format(:error, e, __STACKTRACE__))
      reraise e, __STACKTRACE__
  end

  defp ensure_cache_started!(retries \\ 50) do
    case Process.whereis(CreditRadar.Cache) do
      nil when retries > 0 ->
        Logger.warning("CreditRadar.Cache not started, waiting... (#{retries} retries left)")
        Process.sleep(100)
        ensure_cache_started!(retries - 1)

      nil ->
        raise "CreditRadar.Cache failed to start after 5 seconds"

      _pid ->
        :ok
    end
  end

  defp client do
    Req.new(
      base_url: "https://api.anbima.com.br",
      auth: {:basic, format_basic_auth()}
    )
  end

  defp save_token({:ok, response}) do
    %{"access_token" => token, "expires_in" => expires_in} = response.body
    CreditRadar.Cache.put(@token_store_key, token, ttl: :timer.seconds(expires_in))
    token
  end

  defp format_basic_auth do
    credentials = Application.fetch_env!(:credit_radar, :anbima_credentials)
    client_id = Keyword.fetch!(credentials, :client_id)
    client_secret = Keyword.fetch!(credentials, :client_secret)

    "#{client_id}:#{client_secret}"
  end
end
