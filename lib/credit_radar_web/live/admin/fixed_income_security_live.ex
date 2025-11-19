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
  alias CreditRadar.Ingestions
  alias CreditRadar.Ingestions.Tasks.IngestCriCraXls
  alias CreditRadar.Ingestions.Tasks.IngestDebenturesXls
  alias CreditRadar.Repo

  require Logger

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
      }
    ]
  end

  @impl Backpex.LiveResource
  def can?(_assigns, action, _item) when action in [:new, :create, :edit, :update], do: false
  def can?(_assigns, _action, _item), do: true

  @impl Backpex.LiveResource
  def resource_actions do
    [
      upload_xls: %{
        module: __MODULE__.UploadXlsAction
      },
      upload_debentures_xls: %{
        module: __MODULE__.UploadDebenturesXlsAction
      }
    ]
  end

  defmodule UploadXlsAction do
    use Backpex.ResourceAction
    import Phoenix.LiveView, only: [put_flash: 3, push_navigate: 2]

    @impl Backpex.ResourceAction
    def title, do: "Upload XLS/XLSX"

    @impl Backpex.ResourceAction
    def label, do: "Upload CRI/CRA"

    @impl Backpex.ResourceAction
    def fields do
      [
        file: %{
          module: Backpex.Fields.Upload,
          label: "Arquivo XLS/XLSX",
          type: :upload,
          upload_key: :file,
          accept: ~w(.xls .xlsx),
          max_entries: 1,
          required: true,
          list_existing_files: fn _item -> [] end,
          put_upload_change: fn _socket,
                                params,
                                _item,
                                _uploaded_entries,
                                _removed_entries,
                                _action ->
            params
          end,
          consume_upload: &consume_upload/4,
          remove_uploads: fn _socket, _item, _field_name -> {:ok, []} end
        }
      ]
    end

    @impl Backpex.ResourceAction
    def changeset(change, attrs, _metadata) do
      Ecto.Changeset.cast(change, attrs, [:file])
    end

    @impl Backpex.ResourceAction
    def handle(socket, _params) do
      {:ok,
       socket
       |> put_flash(
         :info,
         "📤 Upload recebido! Estamos processando em segundo plano. Você pode navegar e acompanhar em Execuções."
       )}
    end

    defp consume_upload(_, entry, %{path: path}, _) do
      uuid = Map.get(entry, :uuid, Ecto.UUID.generate())
      name = Map.get(entry, :client_name, "")
      dest = Path.join(System.tmp_dir!(), "#{uuid}#{Path.extname(name)}")
      File.cp!(path, dest)
      start_async_ingestion(dest)
      {:ok, nil}
    end

    defp start_async_ingestion(file_path) do
      Task.Supervisor.start_child(CreditRadar.Ingestions.TaskSupervisor, fn ->
        Logger.info("⏳ Starting to process XLS file: #{file_path}")

        try do
          case IngestCriCraXls.run(nil, file_path) do
            {:ok, stats} ->
              total = stats.created + stats.updated

              Logger.info(
                "✅ CRI/CRA XLS ingestion completed successfully: #{total} títulos (#{stats.created} novos, #{stats.updated} atualizados, #{stats.skipped} pulados)"
              )

            {:error, reason} ->
              Logger.error("Failed to process XLS file: #{inspect(reason)}")
          end
        after
          File.rm(file_path)
        end
      end)
    end
  end

  defmodule UploadDebenturesXlsAction do
    use Backpex.ResourceAction
    import Phoenix.LiveView, only: [put_flash: 3, push_navigate: 2]

    alias CreditRadar.Ingestions
    alias CreditRadar.Workers.IngestDebenturesJob
    alias CreditRadar.Repo

    require Logger

    @impl Backpex.ResourceAction
    def title, do: "Upload XLS/XLSX"

    @impl Backpex.ResourceAction
    def label, do: "Upload Debêntures"

    @impl Backpex.ResourceAction
    def fields do
      [
        file: %{
          module: Backpex.Fields.Upload,
          label: "Arquivo XLS/XLSX",
          type: :upload,
          upload_key: :file,
          accept: ~w(.xls .xlsx),
          max_entries: 1,
          required: true,
          list_existing_files: fn _item -> [] end,
          put_upload_change: fn _socket,
                                params,
                                _item,
                                _uploaded_entries,
                                _removed_entries,
                                _action ->
            params
          end,
          consume_upload: &consume_upload/4,
          remove_uploads: fn _socket, _item, _field_name -> {:ok, []} end
        }
      ]
    end

    @impl Backpex.ResourceAction
    def changeset(change, attrs, _metadata) do
      Ecto.Changeset.cast(change, attrs, [:file])
    end

    @impl Backpex.ResourceAction
    def handle(socket, params) do
      Logger.info("🔵 UploadDebenturesXlsAction.handle called with params: #{inspect(params)}")

      {:ok,
       socket
       |> put_flash(
         :info,
         "📤 Upload recebido! Estamos processando em segundo plano. Você pode navegar e acompanhar em Execuções."
       )}
    end

    defp consume_upload(socket, entry, %{path: path}, action) do
      Logger.info("🔵 UploadDebenturesXlsAction.consume_upload called")
      Logger.info("🔵   - entry: #{inspect(entry)}")
      Logger.info("🔵   - path: #{path}")

      uuid = Map.get(entry, :uuid, Ecto.UUID.generate())
      name = Map.get(entry, :client_name, "")
      dest = Path.join(System.tmp_dir!(), "#{uuid}#{Path.extname(name)}")

      Logger.info("🔵 Copying file from #{path} to #{dest}")

      try do
        File.cp!(path, dest)
        Logger.info("✅ File copied successfully to #{dest}")
        Logger.info("🔵 File size: #{File.stat!(dest).size} bytes")
        start_async_ingestion(dest)
        Logger.info("✅ start_async_ingestion returned successfully")
        {:ok, nil}
      rescue
        error ->
          Logger.error("❌ Error in consume_upload: #{inspect(error)}")
          Logger.error("❌ Stacktrace: #{inspect(__STACKTRACE__)}")
          {:error, "Failed to process upload"}
      end
    end

    defp start_async_ingestion(file_path) do
      Logger.info("🟢 start_async_ingestion called with file_path: #{file_path}")
      Logger.info("🟢 File exists? #{File.exists?(file_path)}")

      # Create execution record for tracking progress
      Logger.info("🟢 Creating execution record...")

      execution_result =
        %Ingestions.Execution{}
        |> Ingestions.execution_create_changeset(%{
          "kind" => "debentures",
          "trigger" => "upload",
          "status" => "pending"
        })
        |> Repo.insert()

      case execution_result do
        {:ok, execution} ->
          Logger.info("✅ Created execution ##{execution.id} for Debentures XLS upload")
          Logger.info("🟢 Enqueuing Oban job...")

          # Enqueue Oban job to process the file
          job_result =
            %{execution_id: execution.id, file_path: file_path}
            |> IngestDebenturesJob.new()
            |> Oban.insert()

          case job_result do
            {:ok, job} ->
              Logger.info("✅ Oban job ##{job.id} enqueued for execution ##{execution.id}")

            {:error, reason} ->
              Logger.error("❌ Failed to enqueue Oban job: #{inspect(reason)}")
              # Clean up execution and file on error
              Repo.delete(execution)
              File.rm(file_path)
          end

        {:error, changeset} ->
          Logger.error("❌ Failed to create execution for upload")
          Logger.error("❌   - Errors: #{inspect(changeset.errors)}")
          Logger.error("❌   - Full changeset: #{inspect(changeset)}")
          File.rm(file_path)
      end

      Logger.info("🟢 start_async_ingestion completed")
    end
  end
end
