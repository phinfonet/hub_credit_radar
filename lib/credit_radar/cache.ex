defmodule CreditRadar.Cache do
  use Nebulex.Cache,
    otp_app: :credit_radar,
    adapter: Nebulex.Adapters.Local
end
