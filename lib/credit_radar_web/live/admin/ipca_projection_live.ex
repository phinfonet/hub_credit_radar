defmodule CreditRadarWeb.Live.Admin.IPCAProjectionLive do
  use Backpex.LiveResource,
    adapter_config: [
      schema: CreditRadar.FixedIncome.IPCAProjection,
      repo: CreditRadar.Repo,
      update_changeset: &CreditRadar.FixedIncome.ipca_projection_changeset/3,
      create_changeset: &CreditRadar.FixedIncome.ipca_projection_changeset/3
    ],
    layout: {CreditRadarWeb.Layouts, :admin}

  alias CreditRadarWeb.Backpex.Fields.MonthPicker

  @impl Backpex.LiveResource
  def singular_name, do: "Projeção IPCA"

  @impl Backpex.LiveResource
  def plural_name, do: "Projeções IPCA"

  @impl Backpex.LiveResource
  def fields do
    [
      reference_date: %{
        module: MonthPicker,
        label: "Mês de referência"
      },
      year_ipca_projection_value: %{
        module: Backpex.Fields.Number,
        label: "IPCA estimado (ano)"
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
