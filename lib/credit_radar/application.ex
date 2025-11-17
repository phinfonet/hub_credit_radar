defmodule CreditRadar.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      CreditRadarWeb.Telemetry,
      CreditRadar.Repo,
      CreditRadar.HubRepo,
      CreditRadar.Cache,
      {CreditRadar.Ingestions.TaskSupervisor, name: CreditRadar.Ingestions.TaskSupervisor},
      {DNSCluster, query: Application.get_env(:credit_radar, :dns_cluster_query) || :ignore},
      {Phoenix.PubSub, name: CreditRadar.PubSub},
      # Start a worker by calling: CreditRadar.Worker.start_link(arg)
      # {CreditRadar.Worker, arg},
      # Start to serve requests, typically the last entry
      CreditRadarWeb.Endpoint
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: CreditRadar.Supervisor]
    Supervisor.start_link(children, opts)
  end

  # Tell Phoenix to update the endpoint configuration
  # whenever the application is updated.
  @impl true
  def config_change(changed, _new, removed) do
    CreditRadarWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end
