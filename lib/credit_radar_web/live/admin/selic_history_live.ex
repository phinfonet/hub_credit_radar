defmodule CreditRadarWeb.Live.Admin.SelicHistoryLive do
  use Backpex.LiveResource,
    adapter_config: [
      schema: CreditRadar.FixedIncome.SelicHistory,
      repo: CreditRadar.Repo,
      update_changeset: &CreditRadar.FixedIncome.selic_history_changeset/3,
      create_changeset: &CreditRadar.FixedIncome.selic_history_changeset/3
    ],
    layout: {CreditRadarWeb.Layouts, :admin}

  @impl Backpex.LiveResource
  def singular_name, do: "SELIC Diária"

  @impl Backpex.LiveResource
  def plural_name, do: "Histórico SELIC"

  @impl Backpex.LiveResource
  def resource_actions do
    [
      import_history: %{
        module: __MODULE__.ImportHistoryAction
      }
    ]
  end

  @impl Backpex.LiveResource
  def fields do
    [
      reference_date: %{
        module: Backpex.Fields.Date,
        label: "Data de referência"
      },
      selic_value: %{
        module: Backpex.Fields.Number,
        label: "SELIC (%)"
      },
      inserted_at: %{
        module: Backpex.Fields.DateTime,
        label: "Criado em",
        readonly: true
      }
    ]
  end

  @impl Backpex.LiveResource
  def can?(_assigns, action, _item) when action in [:new, :create, :edit, :update, :delete],
    do: false

  def can?(_assigns, _action, _item), do: true

  defmodule ImportHistoryAction do
    use Backpex.ResourceAction
    import Phoenix.LiveView, only: [put_flash: 3, assign: 3]

    alias CreditRadar.FixedIncome

    defstruct start_date: nil

    @impl Backpex.ResourceAction
    def changeset({data, _types}, attrs, metadata) when is_map(data) do
      struct = struct(__MODULE__, Map.take(data, [:start_date]))
      changeset(struct, attrs, metadata)
    end

    def changeset(struct \\ %__MODULE__{}, attrs, _metadata) do
      types = %{start_date: :date}

      {struct, types}
      |> Ecto.Changeset.cast(attrs, Map.keys(types))
      |> Ecto.Changeset.validate_required([:start_date])
    end

    @impl Backpex.ResourceAction
    def title, do: "Importar histórico via BCB"

    @impl Backpex.ResourceAction
    def label, do: "Sincronizar SELIC"

    @impl Backpex.ResourceAction
    def fields do
      [
        start_date: %{
          module: Backpex.Fields.Date,
          label: "Data inicial",
          type: :date,
          required: true
        }
      ]
    end

    @impl Backpex.ResourceAction
    def handle(socket, %{start_date: %Date{} = start_date}) do
      with {:ok, %{processed: processed}} <-
             FixedIncome.import_selic_history(%{
               "dataInicial" => format_bcb_date(start_date),
               "dataFinal" => format_bcb_date(Date.utc_today())
             }) do
        {:ok,
         socket
         |> assign(:item, %__MODULE__{start_date: start_date})
         |> put_flash(:info, "Histórico da SELIC sincronizado (#{processed} registros).")}
      else
        {:error, reason} ->
          {:error, put_base_error(socket, "Falha ao importar SELIC: #{format_reason(reason)}")}

        other ->
          {:error, put_base_error(socket, "Erro inesperado: #{inspect(other)}")}
      end
    end

    def handle(socket, _params), do: {:error, invalidate_start_date(socket)}

    defp format_bcb_date(%Date{} = date) do
      [year, month, day] =
        date
        |> Date.to_iso8601()
        |> String.split("-")

      "#{day}/#{month}/#{year}"
    end

    defp format_reason({:invalid_entry, _}), do: "formato inválido recebido do BCB"
    defp format_reason(:invalid_payload), do: "payload inválido"
    defp format_reason(reason) when is_atom(reason), do: Atom.to_string(reason)
    defp format_reason(reason), do: inspect(reason)

    defp invalidate_start_date(socket) do
      socket.assigns.form.source
      |> Ecto.Changeset.add_error(:start_date, "Informe uma data inicial válida")
    end

    defp put_base_error(socket, message) do
      socket.assigns.form.source
      |> Ecto.Changeset.add_error(:base, message)
    end
  end
end
