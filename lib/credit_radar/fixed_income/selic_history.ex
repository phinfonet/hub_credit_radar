defmodule CreditRadar.FixedIncome.SelicHistory do
  use Ecto.Schema
  import Ecto.Changeset

  schema "selic_history" do
    field :reference_date, :date
    field :selic_value, :decimal

    timestamps(type: :utc_datetime)
  end

  @doc false
  def changeset(history, attrs) do
    history
    |> cast(attrs, [:reference_date, :selic_value])
    |> validate_required([:reference_date, :selic_value])
    |> unique_constraint(:reference_date)
  end
end
