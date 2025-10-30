defmodule CreditRadarWeb.Router do
  use CreditRadarWeb, :router

  pipeline :browser do
    plug :accepts, ["html"]
    plug :fetch_session
    plug :fetch_live_flash
    plug :put_root_layout, html: {CreditRadarWeb.Layouts, :root}
    plug :protect_from_forgery
    plug :put_secure_browser_headers
  end

  pipeline :api do
    plug :accepts, ["json"]
  end

  scope "/", CreditRadarWeb do
    pipe_through :browser

    get "/", PageController, :home
    live "/analise-credito", Live.CreditAnalysisLive
  end

  import Backpex.Router

  scope "/admin", CreditRadarWeb do
    pipe_through :browser

    live_session :default, on_mount: Backpex.InitAssigns do
      live_resources "/securities", Live.Admin.FixedIncomeSecurityLive, only: [:index, :show]
      live_resources "/assessments", Live.Admin.FixedIncomeAssessmentLive
      live_resources "/filter_rules", Live.Admin.FilterRuleLive
      live_resources "/executions", Live.Admin.ExecutionLive
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
end
