defmodule CreditRadar.Ingestions.Execution do
  use Ecto.Schema
  import Ecto.Changeset

  schema "executions" do
    field :kind, :string
    field :status, :string
    field :started_at, :utc_datetime
    field :finished_at, :utc_datetime
    field :trigger, :string
    field :progress, :integer, default: 0

    timestamps(type: :utc_datetime)
  end

  @doc false
  def changeset(execution, attrs) do
    execution
    |> cast(attrs, [:kind, :status, :started_at, :finished_at, :trigger, :progress])
    |> validate_required([:kind, :status, :trigger])
    |> validate_inclusion(:status, ["pending", "running", "completed", "failed"])
    |> validate_number(:progress, greater_than_or_equal_to: 0, less_than_or_equal_to: 100)
    |> validate_kind()
  end

  defp validate_kind(changeset) do
    validate_change(changeset, :kind, fn :kind, value ->
      if CreditRadar.Ingestions.valid_kind?(value), do: [], else: [kind: "tipo de ingestão inválido"]
    end)
  end
end
