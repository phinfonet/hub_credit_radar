defmodule CreditRadar.Auth do
  @moduledoc """
  Handles authentication against the external GraphQL API.
  """

  alias CreditRadar.GraphQL.Client

  @token_path "/oauth/token"
  @me_query """
  query Me {
    me {
      id
      name
      email
      confirmed
      activeSubscription {
        id
        actived
      }
    }
  }
  """

  @doc """
  Performs the password grant against the OAuth endpoint and retrieves the user profile.
  Returns `{:ok, %{token: token, user: map}}` or `{:error, reason}`.
  """
  def authenticate(email, password) when is_binary(email) and is_binary(password) do
    with {:ok, token} <- fetch_token(email, password),
         {:ok, %{"me" => user}} <- fetch_me(token),
         :ok <- ensure_active_user(user) do
      {:ok, %{token: token, user: user}}
    else
      {:error, _} = error -> error
      {:inactive, reason} -> {:error, reason}
    end
  end

  defp fetch_me(token) do
    Client.query(@me_query, %{}, token: token)
  end

  defp fetch_token(email, password) do
    config = Application.get_env(:credit_radar, __MODULE__, [])

    url =
      config[:token_url] ||
        raise "Missing :token_url configuration for #{inspect(__MODULE__)} (GRAPHQL_AUTH_URL)"

    client_id =
      config[:client_id] ||
        raise "Missing :client_id configuration for #{inspect(__MODULE__)} (GRAPHQL_CLIENT_ID)"

    client_secret =
      config[:client_secret] ||
        raise "Missing :client_secret configuration for #{inspect(__MODULE__)} (GRAPHQL_CLIENT_SECRET)"

    body = [
      grant_type: "password",
      email: email,
      password: password,
      client_id: client_id,
      client_secret: client_secret
    ]

    case Req.post(url: String.trim_trailing(url, "/") <> @token_path, form: body) do
      {:ok, %Req.Response{status: status, body: %{"access_token" => token}}} when status in 200..299 ->
        {:ok, token}

      {:ok, %Req.Response{body: %{"error_description" => message}}} ->
        {:error, message}

      {:ok, %Req.Response{status: status, body: body}} ->
        {:error, %{status: status, body: body}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp ensure_active_user(%{"confirmed" => true} = user) do
    case get_in(user, ["activeSubscription", "actived"]) do
      true -> :ok
      _ -> {:inactive, "Assinatura inativa"}
    end
  end

  defp ensure_active_user(_), do: {:inactive, "Usuário não confirmado"}
end
