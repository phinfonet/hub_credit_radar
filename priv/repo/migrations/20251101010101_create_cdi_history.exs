defmodule CreditRadar.Repo.Migrations.CreateCdiHistory do
  use Ecto.Migration

  def change do
    create table(:cdi_history) do
      add :reference_date, :date, null: false
      add :cdi_value, :decimal, precision: 10, scale: 4, null: false

      timestamps(type: :utc_datetime)
    end

    create unique_index(:cdi_history, [:reference_date])
  end
end
