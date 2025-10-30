defmodule CreditRadar.Integrations.Anbima.Auth do
  @token_store_key :anbima_token
  require IEx

  def fetch_token! do
    case CreditRadar.Cache.get(@token_store_key) do
      nil ->  
        client()
        |> Req.post(url: "/oauth/access-token", json: %{grant_type: "client_credentials"})
        |> save_token()
      token -> token
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
