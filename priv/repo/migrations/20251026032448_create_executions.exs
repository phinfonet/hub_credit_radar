defmodule CreditRadar.Repo.Migrations.CreateExecutions do
  use Ecto.Migration

  def change do
    create table(:executions) do
      add :kind, :string
      add :status, :string
      add :started_at, :utc_datetime
      add :finished_at, :utc_datetime
      add :trigger, :string

      timestamps(type: :utc_datetime)
    end
  end
end
