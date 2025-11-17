defmodule CreditRadar.FixedIncome.IPCAProjection do
  use Ecto.Schema
  import Ecto.Changeset

  schema "ipca_projections" do
    field :reference_date, :date
    field :year_ipca_projection_value, :decimal

    timestamps(type: :utc_datetime)
  end

  @doc false
  def changeset(projection, attrs) do
    projection
    |> cast(attrs, [:reference_date, :year_ipca_projection_value])
    |> validate_required([:reference_date])
    |> unique_constraint(:reference_date)
  end
end
