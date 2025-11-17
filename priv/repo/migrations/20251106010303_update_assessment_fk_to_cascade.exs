defmodule CreditRadar.Repo.Migrations.UpdateAssessmentFkToCascade do
  use Ecto.Migration

  def up do
    execute "ALTER TABLE fixed_income_assessments DROP CONSTRAINT IF EXISTS fixed_income_assessments_security_id_fkey"

    alter table(:fixed_income_assessments) do
      modify :security_id,
        references(:fixed_income_securities, on_delete: :delete_all),
        null: false
    end
  end

  def down do
    execute "ALTER TABLE fixed_income_assessments DROP CONSTRAINT IF EXISTS fixed_income_assessments_security_id_fkey"

    alter table(:fixed_income_assessments) do
      modify :security_id,
        references(:fixed_income_securities, on_delete: :nothing),
        null: false
    end
  end
end
