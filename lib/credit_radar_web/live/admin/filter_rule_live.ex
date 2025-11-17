defmodule CreditRadarWeb.Live.Admin.FilterRuleLive do
  use Backpex.LiveResource,
    adapter_config: [
      schema: CreditRadar.Ingestions.IgnoreRule,
      repo: CreditRadar.Repo,
      update_changeset: &CreditRadar.Ingestions.ignore_rule_update_changeset/3,
      create_changeset: &CreditRadar.Ingestions.ignore_rule_create_changeset/3
    ],
    layout: {CreditRadarWeb.Layouts, :admin}

  @impl Backpex.LiveResource
  def singular_name, do: "Filter Rule"

  @impl Backpex.LiveResource
  def plural_name, do: "Filter Rules"

  @impl Backpex.LiveResource
  def fields do
    [
      security_code: %{
        module: Backpex.Fields.Text,
        label: "Código do ativo"
      }
    ]
  end
end
