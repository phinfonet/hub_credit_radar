defmodule CreditRadar.Repo.Migrations.CreateIpcaProjections do
  use Ecto.Migration

  def change do
    create table(:ipca_projections) do
      add :reference_date, :date, null: false
      add :current_month_ipca_value, :decimal, precision: 10, scale: 4
      add :year_ipca_projection_value, :decimal, precision: 10, scale: 4

      timestamps(type: :utc_datetime)
    end

    create unique_index(:ipca_projections, [:reference_date])
  end
end
