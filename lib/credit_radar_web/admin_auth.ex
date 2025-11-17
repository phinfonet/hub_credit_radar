defmodule CreditRadarWeb.AdminAuth do
  @moduledoc """
  Módulo para autenticação e gerenciamento de sessão de administradores.
  """
  use CreditRadarWeb, :verified_routes

  import Plug.Conn
  import Phoenix.Controller

  alias CreditRadar.Accounts

  @doc """
  Plug que busca o admin atual da sessão e o coloca em `conn.assigns.current_admin`.

  Se não houver admin na sessão, `current_admin` será `nil`.
  """
  def fetch_current_admin(conn, _opts) do
    admin_id = get_session(conn, :admin_id)
    admin = admin_id && Accounts.get_admin(admin_id)
    assign(conn, :current_admin, admin)
  end

  @doc """
  Plug que requer que o admin esteja autenticado.

  Se não houver admin na sessão, redireciona para a página de login.
  """
  def require_authenticated_admin(conn, _opts) do
    case conn.assigns[:current_admin] do
      nil ->
        conn
        |> put_flash(:error, "Você precisa estar autenticado para acessar esta página.")
        |> redirect(to: ~p"/admin/login")
        |> halt()

      _admin ->
        conn
    end
  end

  @doc """
  Faz login do admin armazenando seu ID na sessão.

  Retorna a conexão atualizada com o admin na sessão.
  """
  def log_in_admin(conn, admin) do
    conn
    |> put_session(:admin_id, admin.id)
    |> assign(:current_admin, admin)
    |> configure_session(renew: true)
  end

  @doc """
  Faz logout do admin removendo-o da sessão.

  Retorna a conexão com a sessão limpa.
  """
  def log_out_admin(conn) do
    conn
    |> clear_session()
    |> configure_session(renew: true)
  end
end
