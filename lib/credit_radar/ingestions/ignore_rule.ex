defmodule CreditRadar.Ingestions.IgnoreRule do
  use Ecto.Schema
  import Ecto.Changeset

  schema "ignore_rules" do
    field :security_code, :string

    timestamps(type: :utc_datetime)
  end

  @doc false
  def changeset(ignore_rule, attrs) do
    ignore_rule
    |> cast(attrs, [:security_code])
    |> validate_required([:security_code])
  end
end
