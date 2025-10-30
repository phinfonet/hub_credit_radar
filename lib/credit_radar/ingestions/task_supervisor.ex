defmodule CreditRadar.Ingestions.TaskSupervisor do
  @moduledoc false

  @spec start_link(Keyword.t()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    opts
    |> Keyword.put_new(:name, __MODULE__)
    |> Task.Supervisor.start_link()
  end

  @spec child_spec(Keyword.t()) :: Supervisor.child_spec()
  def child_spec(opts) do
    opts
    |> Keyword.put_new(:name, __MODULE__)
    |> Task.Supervisor.child_spec()
  end
end
