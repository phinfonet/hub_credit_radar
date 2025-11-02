defmodule CreditRadar.Ingestions do
  @moduledoc """
  Context helpers for ingestion-related entities such as filter rules and executions.
  Provides utilities to orchestrate ingestion tasks and manage execution lifecycles.
  """

  require Logger

  alias CreditRadar.Ingestions.{Execution, IgnoreRule, TaskSupervisor}
  alias CreditRadar.Repo

  @task_definitions [
    {:cri_cra, "CRI & CRA", CreditRadar.Ingestions.Tasks.IngestCriCra},
    {:debentures, "Debêntures", CreditRadar.Ingestions.Tasks.IngestDebentures},
    {:debentures_plus, "Debêntures Plus", CreditRadar.Ingestions.Tasks.IngestDebenturesPlus}
  ]

  @status_pending "pending"
  @status_running "running"
  @status_completed "completed"
  @status_failed "failed"
  @progress_default 0
  @progress_running 10
  @progress_completed 100

  @doc """
  Builds a changeset for creating a filter rule.
  """
  def ignore_rule_create_changeset(ignore_rule, attrs, _metadata \\ []) do
    IgnoreRule.changeset(ignore_rule, attrs)
  end

  @doc """
  Builds a changeset for updating a filter rule.
  """
  def ignore_rule_update_changeset(ignore_rule, attrs, _metadata \\ []) do
    IgnoreRule.changeset(ignore_rule, attrs)
  end

  @doc """
  Returns the select options that should be presented for ingestion executions.
  """
  def ingestion_type_options do
    Enum.map(@task_definitions, fn {key, label, _module} ->
      {label, Atom.to_string(key)}
    end)
  end

  @doc """
  Provides a human-readable label for a given ingestion kind.
  """
  def ingestion_label(kind) do
    case ingestion_entry(kind) do
      nil -> nil
      {_key, label, _module} -> label
    end
  end

  @doc """
  Checks if a given ingestion kind is supported.
  """
  def valid_kind?(kind) do
    case normalize_kind(kind) do
      {:ok, key} -> Enum.any?(@task_definitions, fn {candidate, _label, _module} -> candidate == key end)
      :error -> false
    end
  end

  @doc """
  Builds a changeset for creating an execution entry, injecting sensible defaults.
  """
  def execution_create_changeset(execution, attrs, _metadata \\ []) do
    attrs =
      attrs
      |> normalize_attrs()
      |> ensure_default(:status, @status_pending)
      |> ensure_default(:trigger, "manual")
      |> ensure_default(:started_at, nil)
      |> ensure_default(:finished_at, nil)
      |> ensure_default(:progress, @progress_default)

    Execution.changeset(execution, attrs)
  end

  @doc """
  Builds a changeset for updating an execution entry.
  """
  def execution_update_changeset(execution, attrs, _metadata \\ []) do
    attrs = normalize_attrs(attrs)
    Execution.changeset(execution, attrs)
  end

  @doc """
  Dispatches the ingestion task associated with the given execution.
  """
  def dispatch_execution(%Execution{} = execution) do
    with {:ok, module} <- ingestion_module(execution.kind),
         {:ok, _pid} <-
           Task.Supervisor.start_child(TaskSupervisor, fn -> run_execution(module, execution) end) do
      :ok
    else
      {:error, reason} ->
        Logger.error("Unable to dispatch ingestion for kind #{inspect(execution.kind)}: #{inspect(reason)}")
        mark_failed(execution)
        {:error, reason}
    end
  end

  @doc """
  Updates the progress percentage of a running execution.
  """
  def report_progress(%Execution{} = execution, progress) do
    do_report_progress(execution, progress)
  end

  def report_progress(execution_id, progress) when is_integer(execution_id) do
    case Repo.get(Execution, execution_id) do
      %Execution{} = execution -> do_report_progress(execution, progress)
      nil -> {:error, :not_found}
    end
  end

  defp run_execution(module, %Execution{} = execution) do
    Logger.info("🚀 Starting execution for module: #{inspect(module)}")

    # Ensure module is loaded
    case Code.ensure_loaded(module) do
      {:module, ^module} ->
        Logger.info("✓ Module #{inspect(module)} loaded successfully")
      {:error, reason} ->
        Logger.error("✗ Failed to load module #{inspect(module)}: #{inspect(reason)}")
    end

    {:ok, execution} =
      execution
      |> execution_update_changeset(%{
        status: @status_running,
        started_at: DateTime.utc_now(),
        progress: @progress_running
      })
      |> Repo.update()

    Logger.info("✓ Execution marked as running, now calling module.run()")

    # Debug: show all exported functions
    Logger.debug("Available functions: #{inspect(module.__info__(:functions))}")

    result =
      try do
        cond do
          function_exported?(module, :run, 1) ->
            Logger.info("→ Calling #{inspect(module)}.run/1 with execution")
            module.run(execution)
          function_exported?(module, :run, 0) ->
            Logger.info("→ Calling #{inspect(module)}.run/0")
            module.run()
          true ->
            Logger.error("✗ Module #{inspect(module)} does not export run/1 or run/0")
            Logger.error("✗ Exported functions: #{inspect(module.__info__(:functions))}")
            {:error, :not_implemented}
        end
      rescue
        exception ->
          Logger.error("💥 EXCEPTION in ingestion: #{inspect(exception)}")
          log_exception(exception, __STACKTRACE__)
          {:error, exception}
      catch
        kind, value ->
          Logger.error("💥 CRASH in ingestion: #{inspect({kind, value})}")
          {:error, {kind, value}}
      end

    Logger.info("📊 Ingestion result: #{inspect(result)}")

    status = finalize_status(result)
    Logger.info("📌 Final status: #{status}")

    latest_execution = Repo.get!(Execution, execution.id)

    final_progress =
      case status do
        @status_completed -> @progress_completed
        @status_failed -> latest_execution.progress || @progress_default
      end

    latest_execution
    |> execution_update_changeset(%{
      status: status,
      finished_at: DateTime.utc_now(),
      progress: final_progress
    })
    |> Repo.update()
  end

  defp do_report_progress(execution, progress) do
    normalized = normalize_progress(progress)

    execution
    |> execution_update_changeset(%{progress: normalized})
    |> Repo.update()
  end

  defp finalize_status(:ok), do: @status_completed
  defp finalize_status({:ok, _}), do: @status_completed
  defp finalize_status({:error, _}), do: @status_failed
  defp finalize_status(_), do: @status_failed

  defp mark_failed(%Execution{} = execution) do
    execution
    |> execution_update_changeset(%{
      status: @status_failed,
      finished_at: DateTime.utc_now(),
      progress: execution.progress || @progress_default
    })
    |> Repo.update()
  end

  defp log_exception(exception, stacktrace) do
    Logger.error(Exception.format(:error, exception, stacktrace))
  end

  defp ingestion_module(kind) when is_binary(kind) do
    case normalize_kind(kind) do
      {:ok, key} -> ingestion_module(key)
      :error -> {:error, :unknown_ingestion}
    end
  end

  defp ingestion_module(kind) when is_atom(kind) do
    case ingestion_entry(kind) do
      nil -> {:error, :unknown_ingestion}
      {_key, _label, module} -> {:ok, module}
    end
  end

  defp ingestion_module(%Execution{kind: kind}), do: ingestion_module(kind)
  defp ingestion_module(_), do: {:error, :unknown_ingestion}

  defp ingestion_entry(kind) when is_atom(kind) do
    Enum.find(@task_definitions, fn {key, _label, _module} -> key == kind end)
  end

  defp ingestion_entry(kind) when is_binary(kind) do
    with {:ok, key} <- normalize_kind(kind) do
      ingestion_entry(key)
    end
  end

  defp ingestion_entry(_), do: nil

  defp normalize_kind(kind) when is_atom(kind), do: {:ok, kind}

  defp normalize_kind(kind) when is_binary(kind) do
    try do
      {:ok, String.to_existing_atom(kind)}
    rescue
      ArgumentError -> :error
    end
  end

  defp normalize_kind(_), do: :error

  defp normalize_attrs(nil), do: %{}
  defp normalize_attrs(attrs) when is_map(attrs), do: attrs
  defp normalize_attrs(attrs) when is_list(attrs), do: Map.new(attrs)
  defp normalize_attrs(_), do: %{}

  defp ensure_default(attrs, key, value) do
    string_key = Atom.to_string(key)

    cond do
      Map.has_key?(attrs, key) -> attrs
      Map.has_key?(attrs, string_key) -> attrs
      true -> Map.put(attrs, string_key, value)
    end
  end

  defp normalize_progress(value) when is_integer(value), do: clamp(value, 0, 100)
  defp normalize_progress(value) when is_float(value), do: value |> round() |> clamp(0, 100)

  defp normalize_progress(value) when is_binary(value) do
    case Integer.parse(value) do
      {int, _rest} -> normalize_progress(int)
      :error -> @progress_default
    end
  end

  defp normalize_progress(_), do: @progress_default

  defp clamp(value, min_value, max_value) do
    value
    |> max(min_value)
    |> min(max_value)
  end
end
