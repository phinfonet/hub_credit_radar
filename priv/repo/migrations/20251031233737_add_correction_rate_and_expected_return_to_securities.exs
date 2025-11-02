defmodule CreditRadar.Repo.Migrations.AddCorrectionRateAndExpectedReturnToSecurities do
  use Ecto.Migration

  def change do
    alter table(:fixed_income_securities) do
      add :correction_rate, :decimal
      add :expected_return, :decimal
    end
  end
end
