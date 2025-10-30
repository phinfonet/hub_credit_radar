defmodule CreditRadar.IngestionsFixtures do
  @moduledoc """
  This module defines test helpers for creating
  entities via the `CreditRadar.Ingestions` context.
  """

  @doc """
  Generate a ignore_rule.
  """
  def ignore_rule_fixture(attrs \\ %{}) do
    {:ok, ignore_rule} =
      attrs
      |> Enum.into(%{
        code: "some code"
      })
      |> CreditRadar.Ingestions.create_ignore_rule()

    ignore_rule
  end

  @doc """
  Generate a execution.
  """
  def execution_fixture(attrs \\ %{}) do
    {:ok, execution} =
      attrs
      |> Enum.into(%{
        finished_at: ~U[2025-10-25 04:57:00Z],
        kind: "some kind",
        started_at: ~U[2025-10-25 04:57:00Z],
        status: "some status",
        trigger: "some trigger"
      })
      |> CreditRadar.Ingestions.create_execution()

    execution
  end
end
