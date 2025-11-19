defmodule CreditRadar.FixedIncome.IGPMProjection do
  use Ecto.Schema
  import Ecto.Changeset

  schema "igp_m_projections" do
    field :reference_date, :date
    field :current_month_igp_m_value, :decimal
    field :year_igp_m_projection_value, :decimal

    timestamps(type: :utc_datetime)
  end

  @doc false
  def changeset(projection, attrs) do
    projection
    |> cast(attrs, [:reference_date, :current_month_igp_m_value, :year_igp_m_projection_value])
    |> validate_required([:reference_date])
    |> unique_constraint(:reference_date)
  end
end
