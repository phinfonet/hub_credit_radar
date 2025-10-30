defmodule CreditRadarWeb.PageController do
  use CreditRadarWeb, :controller

  def home(conn, _params) do
    render(conn, :home)
  end
end
