defmodule CreditRadar.Repo do
  use Ecto.Repo,
    otp_app: :credit_radar,
    adapter: Ecto.Adapters.Postgres
end
