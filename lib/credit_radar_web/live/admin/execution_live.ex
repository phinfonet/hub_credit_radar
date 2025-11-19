defmodule CreditRadarWeb.Live.Admin.ExecutionLive do
  alias CreditRadar.Ingestions

  use Backpex.LiveResource,
    adapter_config: [
      schema: Ingestions.Execution,
      repo: CreditRadar.Repo,
      update_changeset: &Ingestions.execution_update_changeset/3,
      create_changeset: &Ingestions.execution_create_changeset/3,
      pubsub: [
        name: CreditRadar.PubSub,
        topic: fn
          # Subscribe to updates for individual executions
          %Ingestions.Execution{id: id} when not is_nil(id) ->
            "execution:#{id}"

          # No subscription for new records
          _ ->
            nil
        end,
        event: :execution_updated
      ]
    ],
    layout: {CreditRadarWeb.Layouts, :admin}

  @impl Backpex.LiveResource
  def singular_name, do: "Execution"

  @impl Backpex.LiveResource
  def plural_name, do: "Executions"

  @impl Backpex.LiveResource
  def fields do
    [
      kind: %{
        module: Backpex.Fields.Select,
        label: "Tipo de Ingestão",
        prompt: "Selecione o tipo da ingestão",
        options: Ingestions.ingestion_type_options()
      },
      status: %{
        module: Backpex.Fields.Text,
        label: "Status",
        readonly: true,
        only: [:index, :show]
      },
      progress: %{
        module: Backpex.Fields.Number,
        label: "Progresso (%)",
        readonly: true,
        only: [:index, :show]
      },
      trigger: %{
        module: Backpex.Fields.Text,
        label: "Disparo",
        readonly: true,
        only: [:index, :show]
      },
      started_at: %{
        module: Backpex.Fields.DateTime,
        label: "Iniciada em",
        only: [:index, :show]
      },
      finished_at: %{
        module: Backpex.Fields.DateTime,
        label: "Finalizada em",
        only: [:index, :show]
      }
    ]
  end

  @impl Backpex.LiveResource
  def on_item_created(socket, item) do
    _ = Ingestions.dispatch_execution(item)
    socket
  end

  @impl Backpex.LiveResource
  def can?(_assigns, action, _item) when action in [:edit, :update, :delete], do: false
  def can?(_assigns, _action, _item), do: true
end
