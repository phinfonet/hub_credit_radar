defmodule CreditRadarWeb.Live.Admin.FixedIncomeAssessmentLive do
  use Backpex.LiveResource,
    adapter_config: [
      schema: CreditRadar.FixedIncome.Assessment,
      repo: CreditRadar.Repo,
      update_changeset: &CreditRadar.FixedIncome.assessment_update_changeset/3,
      create_changeset: &CreditRadar.FixedIncome.assessment_create_changeset/3,
      item_query: &__MODULE__.item_query/3
    ],
    layout: {CreditRadarWeb.Layouts, :admin}

  import Ecto.Query
  alias CreditRadar.FixedIncome
  alias CreditRadar.FixedIncome.{Assessment, Security}
  alias CreditRadarWeb.Live.Admin.FixedIncomeSecurityLive

  @impl Backpex.LiveResource
  def singular_name, do: "Recomendação"

  @impl Backpex.LiveResource
  def plural_name, do: "Recomendações"

  @impl Backpex.LiveResource
  def fields do
    [
      security_code: %{
        module: Backpex.Fields.Text,
        label: "Código",
        render: fn assigns ->
          code = if assigns.item.security, do: assigns.item.security.code, else: "-"
          assigns = Map.put(assigns, :code, code)
          ~H"""
          <span><%= @code %></span>
          """
        end,
        only: [:index]
      },
      security: %{
        module: Backpex.Fields.BelongsTo,
        label: "Risco de Crédito",
        display_field: :credit_risk,
        live_resource: FixedIncomeSecurityLive,
        options_query: &__MODULE__.unique_securities_for_select/2
      },
      issuer_quality: %{
        module: Backpex.Fields.Select,
        label: "Issuer Quality",
        options: [{"1", 1}, {"2", 2}, {"3", 3}, {"4", 4}, {"5", 5}]
      },
      capital_structure: %{
        module: Backpex.Fields.Select,
        label: "Capital Structure",
        options: [{"1", 1}, {"2", 2}, {"3", 3}, {"4", 4}, {"5", 5}]
      },
      solvency_ratio: %{
        module: Backpex.Fields.Select,
        label: "Solvency Ratio",
        options: [{"1", 1}, {"2", 2}, {"3", 3}, {"4", 4}, {"5", 5}]
      },
      credit_spread: %{
        module: Backpex.Fields.Select,
        label: "Credit Spread",
        options: [{"1", 1}, {"2", 2}, {"3", 3}, {"4", 4}, {"5", 5}]
      },
      grade: %{
        module: Backpex.Fields.Select,
        label: "Grade",
        options: [{"HY", :hy}, {"HG", :hg}]
      },
      recommendation: %{
        module: Backpex.Fields.Select,
        label: "Recommendation",
        options: [{"Entrar", :enter}, {"Não Entrar", :not_enter}]
      }
    ]
  end

  @impl Backpex.LiveResource
  def on_item_created(socket, assessment) do
    # Duplicar o assessment para todos os securities do mesmo credit_risk e reference_date
    FixedIncome.duplicate_assessment_to_issuer(assessment)
    socket
  end

  @impl Backpex.LiveResource
  def on_item_updated(socket, assessment) do
    # Quando edita um assessment, atualiza todos os outros do mesmo credit_risk
    FixedIncome.update_assessments_by_credit_risk(assessment)
    socket
  end

  @doc """
  Custom item query to preload security association.
  """
  def item_query(query, _live_action, _assigns) do
    from a in query,
      preload: [:security]
  end

  @doc """
  Query para o dropdown de seleção de security.
  Retorna apenas um security por combinação (credit_risk, reference_date) para evitar duplicatas no select.

  IMPORTANTE: Isso SÓ afeta o dropdown ao criar um novo assessment.
  Na edição, o Backpex mostra o security já salvo normalmente, então não há problema.
  """
  def unique_securities_for_select(_schema, _params) do
    # Subquery para pegar o menor ID de cada combinação (credit_risk/originador, reference_date)
    subquery =
      from s in Security,
        group_by: [s.credit_risk, s.reference_date],
        select: %{
          id: min(s.id),
          credit_risk: s.credit_risk,
          reference_date: s.reference_date
        }

    # Query principal que retorna apenas os securities únicos
    from s in Security,
      join: sq in subquery(subquery),
      on: s.id == sq.id,
      order_by: [asc: s.credit_risk, desc: s.reference_date]
  end
end
