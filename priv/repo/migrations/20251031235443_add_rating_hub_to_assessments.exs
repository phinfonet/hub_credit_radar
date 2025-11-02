defmodule CreditRadar.Repo.Migrations.AddRatingHubToAssessments do
  use Ecto.Migration

  def change do
    alter table(:fixed_income_assessments) do
      add :rating_hub, :decimal
    end
  end
end
