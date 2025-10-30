defmodule CreditRadarWeb.Live.Admin.FixedIncomeSecurityLive do
  use Backpex.LiveResource,
    adapter_config: [
      schema: CreditRadar.FixedIncome.Security,
      repo: CreditRadar.Repo,
      update_changeset: &CreditRadar.FixedIncome.security_update_changeset/3,
      create_changeset: &CreditRadar.FixedIncome.security_create_changeset/3
    ],
    layout: {CreditRadarWeb.Layouts, :admin}

  alias CreditRadarWeb.Live.Admin.FixedIncomeAssessmentLive

  @impl Backpex.LiveResource
  def singular_name, do: "Security"

  @impl Backpex.LiveResource
  def plural_name, do: "Securities"

  @impl Backpex.LiveResource
  def fields do
    [
      code: %{
        module: Backpex.Fields.Text,
        label: "Código do Ativo"
      },
      issuer: %{
        module: Backpex.Fields.Text,
        label: "Emissor"
      },
      security_type: %{
        module: Backpex.Fields.Text,
        label: "Tipo"
      },
      series: %{
        module: Backpex.Fields.Text,
        label: "Serie"
      },
      issuing: %{
        module: Backpex.Fields.Text,
        label: "Emissão"
      },
      benchmark_index: %{
        module: Backpex.Fields.Text,
        label: "Indice"
      },
      coupon_rate: %{
        module: Backpex.Fields.Number,
        label: "Coupon Rate"
      },
      credit_risk: %{
        module: Backpex.Fields.Text,
        label: "Risco de crédito"
      },
      duration: %{
        module: Backpex.Fields.Number,
        label: "Duration"
      },
      synced_at: %{
        module: Backpex.Fields.DateTime,
        label: "Synced At"
      },
      sync_source: %{
        module: Backpex.Fields.Text,
        label: "Sync Source"
      },
    ]
  end

  @impl Backpex.LiveResource
  def can?(_assigns, action, _item) when action in [:new, :create, :edit, :update, :delete], do: false
  def can?(_assigns, _action, _item), do: true
end
