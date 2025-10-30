defmodule CreditRadar.FixedIncome.Security do
  use Ecto.Schema
  import Ecto.Changeset

  schema "fixed_income_securities" do
    field :issuer, :string
    field :security_type, Ecto.Enum, values: [:cri, :cra, :debenture, :debenture_plus]
    field :series, :string
    field :issuing, :string
    field :benchmark_index, :string
    field :coupon_rate, :decimal
    field :credit_risk, :string
    field :code, :string
    field :duration, :integer
    field :reference_date, :date
    field :ntnb_reference, :string
    field :ntnb_reference_date, :date
    field :sync_source, Ecto.Enum, values: [:api]
    has_one :assessment, CreditRadar.FixedIncome.Assessment

    timestamps(type: :utc_datetime)
  end

  @doc false
  def changeset(security, attrs) do
    security
    |> cast(attrs, [
      :issuer,
      :security_type,
      :series,
      :issuing,
      :benchmark_index,
      :coupon_rate,
      :code,
      :credit_risk,
      :duration,
      :reference_date,
      :ntnb_reference,
      :ntnb_reference_date,
      :sync_source
    ])
    |> validate_required([:issuer, :security_type, :series, :issuing, :code, :credit_risk, :duration])
  end
end
