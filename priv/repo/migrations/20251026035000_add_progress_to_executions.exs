defmodule CreditRadar.Repo.Migrations.AddProgressToExecutions do
  use Ecto.Migration

  def change do
    alter table(:executions) do
      add :progress, :integer, default: 0, null: false
    end
  end
end
