defmodule CreditRadar.Auth do
  @moduledoc """
  Handles authentication against the external OAuth API.

  Authentication is performed via OAuth password grant flow. The token
  is sufficient for authentication - no additional GraphQL queries are needed.
  """

  require Logger

  @token_path "/oauth/token"

  @doc """
  Performs the password grant against the OAuth endpoint.
  Returns `{:ok, %{token: token, user: map}}` or `{:error, reason}`.

  Note: Token is sufficient for authentication. User profile is created from email.
  """
  def authenticate(email, password) when is_binary(email) and is_binary(password) do
    case fetch_token(email, password) do
      {:ok, token} ->
        # Token is sufficient - create minimal user profile from email
        user = %{"id" => email, "email" => email, "name" => email}
        {:ok, %{token: token, user: user}}

      {:error, _} = error ->
        error
    end
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
      username: email,
      password: password,
      client_id: client_id,
      client_secret: client_secret
    ]

    response = Req.post(url: String.trim_trailing(url, "/") <> @token_path, form: body)

    log_http_response(:token, response)

    case response do
      {:ok, %Req.Response{status: status, body: %{"access_token" => token}}}
      when status in 200..299 ->
        {:ok, token}

      {:ok, %Req.Response{body: %{"error_description" => message}}} ->
        {:error, message}

      {:ok, %Req.Response{status: status, body: body}} ->
        {:error, %{status: status, body: body}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp log_http_response(step, {:ok, %Req.Response{} = resp}) do
    Logger.info("""
    [Auth] #{step} response
    status=#{resp.status}
    body=#{inspect(resp.body)}
    """)
  end

  defp log_http_response(step, {:error, reason}) do
    Logger.error("[Auth] #{step} request failed: #{inspect(reason)}")
  end
end
