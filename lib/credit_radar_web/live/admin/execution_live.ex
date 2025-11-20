defmodule CreditRadarWeb.Live.Admin.ExecutionLive do
  alias CreditRadar.Ingestions

  use Backpex.LiveResource,
    adapter_config: [
      schema: Ingestions.Execution,
      repo: CreditRadar.Repo,
      update_changeset: &Ingestions.execution_update_changeset/3,
      create_changeset: &Ingestions.execution_create_changeset/3
    ],
    layout: {CreditRadarWeb.Layouts, :admin}

  import Phoenix.LiveView, only: [connected?: 1, push_event: 3]

  require Logger

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

  # Handle async info messages to subscribe to PubSub after mount
  @impl Phoenix.LiveView
  def handle_info(:after_mount, socket) do
    # Subscribe to a general executions topic for all updates
    Phoenix.PubSub.subscribe(CreditRadar.PubSub, "executions:updates")
    Logger.debug("Subscribed to executions:updates")
    {:noreply, socket}
  end

  # Handle PubSub messages for execution updates
  def handle_info({:execution_updated, execution}, socket) do
    Logger.debug("Received execution update for ##{execution.id}: #{execution.status} - #{execution.progress}%")

    # Trigger a process to reload items
    Process.send_after(self(), :reload_items, 100)

    {:noreply, socket}
  end

  def handle_info(:reload_items, socket) do
    # Force Backpex to reload items by sending the reload event
    {:noreply, push_event(socket, "backpex:reload", %{})}
  end

  def handle_info(msg, socket) do
    # Ignore unknown messages
    Logger.debug("Ignoring unknown message: #{inspect(msg)}")
    {:noreply, socket}
  end

  # Use on_mount callback to schedule PubSub subscription
  @impl Backpex.LiveResource
  def init_assigns(socket, _live_action) do
    # Schedule PubSub subscription after mount
    if connected?(socket) do
      send(self(), :after_mount)
    end

    socket
  end
end
