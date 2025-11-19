defmodule CreditRadar.Repo.Migrations.CreateIgpMProjections do
  use Ecto.Migration

  def change do
    create table(:igp_m_projections) do
      add :reference_date, :date, null: false
      add :current_month_igp_m_value, :decimal, precision: 10, scale: 4
      add :year_igp_m_projection_value, :decimal, precision: 10, scale: 4

      timestamps(type: :utc_datetime)
    end

    create unique_index(:igp_m_projections, [:reference_date])
  end
end
