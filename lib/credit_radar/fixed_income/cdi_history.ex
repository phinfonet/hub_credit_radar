defmodule CreditRadar.FixedIncome.CDIHistory do
  use Ecto.Schema
  import Ecto.Changeset

  schema "cdi_history" do
    field :reference_date, :date
    field :cdi_value, :decimal

    has_one :projection, CreditRadar.FixedIncome.CDIProjection,
      foreign_key: :reference_date,
      references: :reference_date

    timestamps(type: :utc_datetime)
  end

  @doc false
  def changeset(history, attrs) do
    history
    |> cast(attrs, [:reference_date, :cdi_value])
    |> validate_required([:reference_date, :cdi_value])
    |> unique_constraint(:reference_date)
  end
end
