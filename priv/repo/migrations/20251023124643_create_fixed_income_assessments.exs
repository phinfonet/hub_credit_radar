defmodule CreditRadar.Repo.Migrations.CreateFixedIncomeAssessments do
  use Ecto.Migration

  def change do
    create table(:fixed_income_assessments) do
      add :issuer_quality, :integer
      add :capital_structure, :integer
      add :solvency_ratio, :integer
      add :credit_spread, :integer
      add :grade, :string
      add :recommendation, :string
      add :security_id, references(:fixed_income_securities, on_delete: :delete_all)

      timestamps(type: :utc_datetime)
    end

    create index(:fixed_income_assessments, [:security_id])
  end
end
