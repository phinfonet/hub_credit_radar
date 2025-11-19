defmodule CreditRadarWeb.Live.Admin.IGPMProjectionLive do
  use Backpex.LiveResource,
    adapter_config: [
      schema: CreditRadar.FixedIncome.IGPMProjection,
      repo: CreditRadar.Repo,
      update_changeset: &CreditRadar.FixedIncome.igp_m_projection_changeset/3,
      create_changeset: &CreditRadar.FixedIncome.igp_m_projection_changeset/3
    ],
    layout: {CreditRadarWeb.Layouts, :admin}

  alias CreditRadarWeb.Backpex.Fields.MonthPicker

  @impl Backpex.LiveResource
  def singular_name, do: "Projeção IGP-M"

  @impl Backpex.LiveResource
  def plural_name, do: "Projeções IGP-M"

  @impl Backpex.LiveResource
  def fields do
    [
      reference_date: %{
        module: MonthPicker,
        label: "Mês de referência"
      },
      year_igp_m_projection_value: %{
        module: Backpex.Fields.Number,
        label: "IGP-M estimado (ano)"
      },
      updated_at: %{
        module: Backpex.Fields.DateTime,
        label: "Atualizado em",
        readonly: true
      }
    ]
  end

  @impl Backpex.LiveResource
  def create_button_label, do: "Nova projeção manual"

  def can?(_assigns, :delete, _item), do: false
  def can?(_assigns, _action, _item), do: true
end
