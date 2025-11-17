defmodule CreditRadar.Repo.Migrations.CreateCdiProjections do
  use Ecto.Migration

  def change do
    create table(:cdi_projections) do
      add :reference_date,
          references(:cdi_history, column: :reference_date, type: :date, on_delete: :delete_all),
          null: false

      add :year_cdi_projection_value, :decimal, precision: 10, scale: 4

      timestamps(type: :utc_datetime)
    end

    create unique_index(:cdi_projections, [:reference_date])
  end
end
