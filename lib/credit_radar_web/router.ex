defmodule CreditRadarWeb.Router do
  use CreditRadarWeb, :router

  pipeline :browser do
    plug :accepts, ["html"]
    plug :fetch_session
    plug :fetch_live_flash
    plug :put_root_layout, html: {CreditRadarWeb.Layouts, :root}
    plug :protect_from_forgery
    plug :put_secure_browser_headers
    plug :fetch_current_user
    plug :fetch_current_admin
  end

  pipeline :browser_protected do
    plug :require_authenticated_user
  end

  pipeline :admin_protected do
    plug :require_authenticated_admin
  end

  pipeline :api do
    plug :accepts, ["json"]
  end

  scope "/", CreditRadarWeb do
    pipe_through :browser

    get "/", PageController, :home
    get "/login", AuthController, :new
    post "/login", AuthController, :create
  end

  scope "/", CreditRadarWeb do
    pipe_through [:browser, :browser_protected]

    live "/analise-credito", Live.CreditAnalysisLive
    delete "/logout", AuthController, :delete
  end

  import Backpex.Router

  scope "/admin", CreditRadarWeb do
    pipe_through :browser

    get "/login", AdminSessionController, :new
    post "/login", AdminSessionController, :create
  end

  scope "/admin", CreditRadarWeb do
    pipe_through [:browser, :admin_protected]

    delete "/logout", AdminSessionController, :delete

    live_session :admin, on_mount: Backpex.InitAssigns do
      live_resources "/securities", Live.Admin.FixedIncomeSecurityLive,
        only: [:index, :show, :delete]
      live_resources "/assessments", Live.Admin.FixedIncomeAssessmentLive
      live_resources "/filter_rules", Live.Admin.FilterRuleLive
      live_resources "/executions", Live.Admin.ExecutionLive

      live_resources "/cdi_history", Live.Admin.CDIHistoryLive,
        only: [:index, :show, :resource_action]

      live_resources "/selic_history", Live.Admin.SelicHistoryLive,
        only: [:index, :show, :resource_action]

      live_resources "/cdi_projections", Live.Admin.CDIProjectionLive
      live_resources "/ipca_projections", Live.Admin.IPCAProjectionLive
    end

    backpex_routes()
  end

  # Other scopes may use custom stacks.
  # scope "/api", CreditRadarWeb do
  #   pipe_through :api
  # end

  # Enable LiveDashboard and Swoosh mailbox preview in development
  if Application.compile_env(:credit_radar, :dev_routes) do
    # If you want to use the LiveDashboard in production, you should put
    # it behind authentication and allow only admins to access it.
    # If your application does not have an admins-only section yet,
    # you can use Plug.BasicAuth to set up some basic authentication
    # as long as you are also using SSL (which you should anyway).
    import Phoenix.LiveDashboard.Router

    scope "/dev" do
      pipe_through :browser

      live_dashboard "/dashboard", metrics: CreditRadarWeb.Telemetry
      forward "/mailbox", Plug.Swoosh.MailboxPreview
    end
  end

  defp fetch_current_user(conn, opts),
    do: CreditRadarWeb.UserAuth.fetch_current_user(conn, opts)

  defp require_authenticated_user(conn, opts),
    do: CreditRadarWeb.UserAuth.require_authenticated_user(conn, opts)

  defp fetch_current_admin(conn, opts),
    do: CreditRadarWeb.AdminAuth.fetch_current_admin(conn, opts)

  defp require_authenticated_admin(conn, opts),
    do: CreditRadarWeb.AdminAuth.require_authenticated_admin(conn, opts)
end
