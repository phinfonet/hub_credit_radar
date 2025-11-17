defmodule CreditRadarWeb.UserAuth do
  @moduledoc """
  Session helpers and plugs for authenticating end-users against the GraphQL API.
  """

  use CreditRadarWeb, :verified_routes
  import Plug.Conn
  import Phoenix.Controller
  alias Phoenix.LiveView

  @session_key "current_user"

  def fetch_current_user(conn, _opts) do
    user = get_session(conn, @session_key)
    assign(conn, :current_user, user)
  end

  def require_authenticated_user(conn, _opts) do
    if conn.assigns[:current_user] do
      conn
    else
      conn
      |> put_session(:user_return_to, current_path(conn))
      |> put_flash(:error, "Faça login para continuar.")
      |> redirect(to: ~p"/login")
      |> halt()
    end
  end

  def log_in_user(conn, user) do
    conn
    |> configure_session(renew: true)
    |> put_session(@session_key, user)
    |> assign(:current_user, user)
  end

  def log_out_user(conn) do
    conn
    |> configure_session(drop: true)
    |> assign(:current_user, nil)
  end

  # LiveView hook
  def on_mount(:ensure_authenticated, _params, session, socket) do
    case Map.get(session, @session_key) do
      nil ->
        {:halt, LiveView.redirect(socket, to: ~p"/login")}

      user ->
        {:cont, Phoenix.Component.assign(socket, current_user: user)}
    end
  end
end
