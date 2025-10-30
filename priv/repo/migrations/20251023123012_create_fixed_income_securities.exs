defmodule CreditRadar.Repo.Migrations.CreateFixedIncomeSecurities do
  use Ecto.Migration

  def change do
    create table(:fixed_income_securities) do
      add :issuer, :string
      add :security_type, :string
      add :series, :string
      add :issuing, :string
      add :benchmark_index, :string
      add :coupon_rate, :decimal
      add :credit_risk, :string
      add :code, :string
      add :duration, :integer
      add :reference_date, :date
      add :ntnb_reference_date, :date
      add :ntnb_reference, :string
      add :sync_source, :string

      timestamps(type: :utc_datetime)
    end
  end
end
