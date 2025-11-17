defmodule CreditRadar.HubRepo do
  @moduledoc """
  Repositório read-only para acessar o banco de dados do Hub Backend (Rails).

  Usado apenas para autenticação de admins.
  NÃO deve ser usado para criar, atualizar ou deletar dados.
  """
  use Ecto.Repo,
    otp_app: :credit_radar,
    adapter: Ecto.Adapters.Postgres,
    read_only: true
end
