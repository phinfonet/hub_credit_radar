defmodule CreditRadarWeb.AdminSessionController do
  @moduledoc """
  Controller para autenticação de administradores.
  """
  use CreditRadarWeb, :controller

  alias CreditRadar.Accounts
  alias CreditRadarWeb.AdminAuth

  def new(conn, _params) do
    # Se já está autenticado, redireciona para admin
    if conn.assigns[:current_admin] do
      redirect(conn, to: ~p"/admin/securities")
    else
      render(conn, :new, form: build_form(%{"email" => "", "password" => ""}))
    end
  end

  def create(conn, %{"session" => %{"email" => email, "password" => password}}) do
    case Accounts.authenticate_admin(email, password) do
      {:ok, admin} ->
        conn
        |> put_flash(:info, "Bem-vindo, #{admin.name}!")
        |> AdminAuth.log_in_admin(admin)
        |> redirect(to: ~p"/admin/securities")

      {:error, :invalid_credentials} ->
        render(conn, :new,
          form: build_form(%{"email" => email, "password" => ""}),
          error_message: "Email ou senha inválidos"
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
    |> put_flash(:info, "Logout realizado com sucesso.")
    |> AdminAuth.log_out_admin()
    |> redirect(to: ~p"/admin/login")
  end

  defp build_form(params), do: Phoenix.Component.to_form(params, as: :session)
end
