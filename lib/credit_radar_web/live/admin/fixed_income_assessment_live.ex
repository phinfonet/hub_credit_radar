defmodule CreditRadarWeb.Live.Admin.FixedIncomeAssessmentLive do
  use Backpex.LiveResource,
    adapter_config: [
      schema: CreditRadar.FixedIncome.Assessment,
      repo: CreditRadar.Repo,
      update_changeset: &CreditRadar.FixedIncome.assessment_update_changeset/3,
      create_changeset: &CreditRadar.FixedIncome.assessment_create_changeset/3
    ],
    layout: {CreditRadarWeb.Layouts, :admin}

  alias CreditRadar.FixedIncome
  alias CreditRadarWeb.Live.Admin.FixedIncomeSecurityLive

  @impl Backpex.LiveResource
  def singular_name, do: "Assessment"

  @impl Backpex.LiveResource
  def plural_name, do: "Assessments"

  @impl Backpex.LiveResource
  def fields do
    [
      security: %{
        module: Backpex.Fields.BelongsTo,
        label: "Security",
        display_field: :issuer,
        live_resource: FixedIncomeSecurityLive
      },
      issuer_quality: %{
        module: Backpex.Fields.Textarea,
        label: "Issuer Quality"
      },
      capital_structure: %{
        module: Backpex.Fields.Textarea,
        label: "Capital Structure"
      },
      solvency_ratio: %{
        module: Backpex.Fields.Number,
        label: "Solvency Ratio"
      },
      credit_spread: %{
        module: Backpex.Fields.Number,
        label: "Credit Spread"
      },
      grade: %{
        module: Backpex.Fields.Text,
        label: "Grade"
      },
      recommendation: %{
        module: Backpex.Fields.Textarea,
        label: "Recommendation"
      }
    ]
  end

  @impl Backpex.LiveResource
  def on_item_created(socket, assessment) do
    # Duplicar o assessment para todos os securities do mesmo emissor e reference_date
    FixedIncome.duplicate_assessment_to_issuer(assessment)
    socket
  end
end
