defmodule CreditRadarWeb.AdminAuth do
  @moduledoc """
  Módulo para autenticação e gerenciamento de sessão de administradores.
  """
  use CreditRadarWeb, :verified_routes

  import Plug.Conn
  import Phoenix.Controller

  require Logger
  alias CreditRadar.Accounts
  alias Phoenix.LiveView

  @doc """
  Plug que busca o admin atual da sessão e o coloca em `conn.assigns.current_admin`.

  Se não houver admin na sessão, `current_admin` será `nil`.
  """
  def fetch_current_admin(conn, _opts) do
    admin_id = get_session(conn, :admin_id)
    Logger.debug("[AdminAuth] fetch_current_admin - admin_id from session: #{inspect(admin_id)}")

    admin = admin_id && Accounts.get_admin(admin_id)
    Logger.debug("[AdminAuth] fetch_current_admin - admin loaded: #{inspect(admin != nil)}")

    assign(conn, :current_admin, admin)
  end

  @doc """
  Plug que requer que o admin esteja autenticado.

  Se não houver admin na sessão, redireciona para a página de login.
  """
  def require_authenticated_admin(conn, _opts) do
    current_admin = conn.assigns[:current_admin]
    Logger.debug("[AdminAuth] require_authenticated_admin - current_admin present: #{inspect(current_admin != nil)}")

    case current_admin do
      nil ->
        Logger.warn("[AdminAuth] require_authenticated_admin - No admin found, redirecting to login")

        conn
        |> put_flash(:error, "Você precisa estar autenticado para acessar esta página.")
        |> redirect(to: ~p"/admin/login")
        |> halt()

      admin ->
        Logger.debug("[AdminAuth] require_authenticated_admin - Admin authenticated: #{admin.email}")
        conn
    end
  end

  @doc """
  Faz login do admin armazenando seu ID na sessão.

  Retorna a conexão atualizada com o admin na sessão.
  """
  def log_in_admin(conn, admin) do
    Logger.info("[AdminAuth] log_in_admin - Logging in admin: #{admin.email} (id: #{admin.id})")

    conn =
      conn
      |> put_session(:admin_id, admin.id)
      |> assign(:current_admin, admin)
      |> configure_session(renew: true)

    # Verify session was saved
    saved_id = get_session(conn, :admin_id)
    Logger.info("[AdminAuth] log_in_admin - Session saved with admin_id: #{inspect(saved_id)}")

    conn
  end

  @doc """
  Faz logout do admin removendo-o da sessão.

  Retorna a conexão com a sessão limpa.
  """
  def log_out_admin(conn) do
    admin_id_before = get_session(conn, :admin_id)
    Logger.info("[AdminAuth] log_out_admin - Logging out admin_id: #{inspect(admin_id_before)}")

    conn =
      conn
      |> delete_session(:admin_id)
      |> assign(:current_admin, nil)
      |> configure_session(renew: true)

    # Verify session was cleared
    admin_id_after = get_session(conn, :admin_id)
    Logger.info("[AdminAuth] log_out_admin - After logout, admin_id in session: #{inspect(admin_id_after)}")

    conn
  end

  @doc """
  LiveView hook que garante que o admin está autenticado.

  Uso: on_mount: {CreditRadarWeb.AdminAuth, :ensure_authenticated}
  """
  def on_mount(:ensure_authenticated, _params, session, socket) do
    admin_id = Map.get(session, :admin_id)

    case admin_id && Accounts.get_admin(admin_id) do
      nil ->
        socket =
          socket
          |> Phoenix.Component.assign(:current_admin, nil)
          |> LiveView.put_flash(:error, "Você precisa estar autenticado para acessar esta página.")

        {:halt, LiveView.redirect(socket, to: ~p"/admin/login")}

      admin ->
        {:cont, Phoenix.Component.assign(socket, :current_admin, admin)}
    end
  end
end
