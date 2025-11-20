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
    field :correction_rate, :string
    field :expected_return, :decimal
    field :credit_risk, :string
    field :code, :string
    field :duration, :integer
    field :reference_date, :date
    field :ntnb_reference, :string
    field :ntnb_reference_date, :date
    field :sync_source, Ecto.Enum, values: [:api, :xls]
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
      :correction_rate,
      :expected_return,
      :code,
      :credit_risk,
      :duration,
      :reference_date,
      :ntnb_reference,
      :ntnb_reference_date,
      :sync_source
    ])
    |> normalize_string_fields()
    |> validate_required([:issuer, :security_type, :series, :issuing, :code, :duration])
  end

  defp normalize_string_fields(changeset) do
    # Normaliza campos de texto removendo espaços extras
    string_fields = [
      :issuer,
      :credit_risk,
      :code,
      :series,
      :issuing,
      :benchmark_index,
      :ntnb_reference,
      :correction_rate
    ]

    Enum.reduce(string_fields, changeset, fn field, acc ->
      # Pega o valor atual (change ou field existente)
      value = get_field(acc, field)

      case value do
        nil ->
          acc

        value when is_binary(value) ->
          # Remove espaços no início/fim e múltiplos espaços internos
          normalized = value |> String.trim() |> String.replace(~r/\s+/, " ")
          # Só atualiza se o valor mudou
          if normalized != value do
            put_change(acc, field, normalized)
          else
            acc
          end

        _ ->
          acc
      end
    end)
  end

end
