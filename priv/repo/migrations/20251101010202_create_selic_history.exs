defmodule CreditRadar.Repo.Migrations.CreateSelicHistory do
  use Ecto.Migration

  def change do
    create table(:selic_history) do
      add :reference_date, :date, null: false
      add :selic_value, :decimal, precision: 10, scale: 4, null: false

      timestamps(type: :utc_datetime)
    end

    create unique_index(:selic_history, [:reference_date])
  end
end
