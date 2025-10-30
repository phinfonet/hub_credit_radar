defmodule CreditRadar.FixedIncome.Assessment do
  use Ecto.Schema
  import Ecto.Changeset

  schema "fixed_income_assessments" do
    field :issuer_quality, :string
    field :capital_structure, :string
    field :solvency_ratio, :decimal
    field :credit_spread, :decimal
    field :grade, :string
    field :recommendation, :string
    belongs_to :security, CreditRadar.FixedIncome.Security

    timestamps(type: :utc_datetime)
  end

  @doc false
  def changeset(assessment, attrs) do
    assessment
    |> cast(attrs, [:issuer_quality, :capital_structure, :solvency_ratio, :credit_spread, :grade, :recommendation, :security_id])
    |> validate_required([:issuer_quality, :capital_structure, :solvency_ratio, :credit_spread, :grade, :recommendation, :security_id])
    |> assoc_constraint(:security)
  end
end
