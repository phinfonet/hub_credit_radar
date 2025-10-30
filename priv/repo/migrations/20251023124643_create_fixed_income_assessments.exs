defmodule CreditRadar.Repo.Migrations.CreateFixedIncomeAssessments do
  use Ecto.Migration

  def change do
    create table(:fixed_income_assessments) do
      add :issuer_quality, :string
      add :capital_structure, :string
      add :solvency_ratio, :decimal
      add :credit_spread, :decimal
      add :grade, :string
      add :recommendation, :string
      add :security_id, references(:fixed_income_securities, on_delete: :nothing)

      timestamps(type: :utc_datetime)
    end

    create index(:fixed_income_assessments, [:security_id])
  end
end
