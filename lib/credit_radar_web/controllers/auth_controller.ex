defmodule CreditRadarWeb.AuthController do
  use CreditRadarWeb, :controller

  alias CreditRadar.Auth
  alias CreditRadarWeb.UserAuth

  def new(conn, _params) do
    render(conn, :new, form: build_form(%{"email" => "", "password" => ""}))
  end

  def create(conn, %{"session" => %{"email" => email, "password" => password}}) do
    case Auth.authenticate(email, password) do
      {:ok, %{token: token, user: user}} ->
        session_user = %{
          "id" => user["id"],
          "name" => user["name"],
          "email" => user["email"],
          "token" => token
        }

        conn
        |> UserAuth.log_in_user(session_user)
        |> redirect(to: get_session(conn, :user_return_to) || ~p"/analise-credito")

      {:error, reason} ->
        render(conn, :new,
          form: build_form(%{"email" => email, "password" => ""}),
          error_message: format_error(reason)
        )
    end
  end

  def create(conn, _params) do
    render(conn, :new,
      form: build_form(%{"email" => "", "password" => ""}),
      error_message: "Informe e-mail e senha"
    )
  end

  def delete(conn, _params) do
    conn
    |> UserAuth.log_out_user()
    |> redirect(to: ~p"/login")
  end

  defp format_error(%{"message" => message}), do: message
  defp format_error(message) when is_binary(message), do: message
  defp format_error(_), do: "Não foi possível autenticar"

  defp build_form(params), do: Phoenix.Component.to_form(params, as: :session)
end
