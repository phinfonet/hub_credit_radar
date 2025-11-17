defmodule CreditRadar.FixedIncome.Assessment do
  use Ecto.Schema
  import Ecto.Changeset

  schema "fixed_income_assessments" do
    field :issuer_quality, :integer
    field :capital_structure, :integer
    field :solvency_ratio, :integer
    field :credit_spread, :integer
    field :grade, Ecto.Enum, values: [:hy, :hg]
    field :recommendation, Ecto.Enum, values: [:enter, :not_enter]
    field :rating_hub, :decimal
    belongs_to :security, CreditRadar.FixedIncome.Security

    timestamps(type: :utc_datetime)
  end

  @doc false
  def changeset(assessment, attrs) do
    assessment
    |> cast(attrs, [
      :issuer_quality,
      :capital_structure,
      :solvency_ratio,
      :credit_spread,
      :grade,
      :recommendation,
      :security_id
    ])
    |> validate_required([
      :issuer_quality,
      :capital_structure,
      :solvency_ratio,
      :credit_spread,
      :grade,
      :recommendation,
      :security_id
    ])
    |> validate_inclusion(:issuer_quality, 1..5)
    |> validate_inclusion(:capital_structure, 1..5)
    |> validate_inclusion(:solvency_ratio, 1..5)
    |> validate_inclusion(:credit_spread, 1..5)
    |> assoc_constraint(:security)
  end
end
