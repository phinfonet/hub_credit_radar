defmodule CreditRadar.Repo.Migrations.CreateIgnoreRules do
  use Ecto.Migration

  def change do
    create table(:ignore_rules) do
      add :security_code, :string

      timestamps(type: :utc_datetime)
    end
  end
end
