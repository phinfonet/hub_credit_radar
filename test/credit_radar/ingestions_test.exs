defmodule CreditRadar.IngestionsTest do
  use CreditRadar.DataCase

  alias CreditRadar.FixedIncome
  alias CreditRadar.FixedIncome.Security
  alias CreditRadar.Ingestions
  alias CreditRadar.Ingestions.Tasks.IngestCriCra
  alias CreditRadar.Repo
  alias Decimal

  describe "ignore_rules" do
    alias CreditRadar.Ingestions.IgnoreRule

    import CreditRadar.IngestionsFixtures

    @invalid_attrs %{code: nil}

    test "list_ignore_rules/0 returns all ignore_rules" do
      ignore_rule = ignore_rule_fixture()
      assert Ingestions.list_ignore_rules() == [ignore_rule]
    end

    test "get_ignore_rule!/1 returns the ignore_rule with given id" do
      ignore_rule = ignore_rule_fixture()
      assert Ingestions.get_ignore_rule!(ignore_rule.id) == ignore_rule
    end

    test "create_ignore_rule/1 with valid data creates a ignore_rule" do
      valid_attrs = %{code: "some code"}

      assert {:ok, %IgnoreRule{} = ignore_rule} = Ingestions.create_ignore_rule(valid_attrs)
      assert ignore_rule.code == "some code"
    end

    test "create_ignore_rule/1 with invalid data returns error changeset" do
      assert {:error, %Ecto.Changeset{}} = Ingestions.create_ignore_rule(@invalid_attrs)
    end

    test "update_ignore_rule/2 with valid data updates the ignore_rule" do
      ignore_rule = ignore_rule_fixture()
      update_attrs = %{code: "some updated code"}

      assert {:ok, %IgnoreRule{} = ignore_rule} = Ingestions.update_ignore_rule(ignore_rule, update_attrs)
      assert ignore_rule.code == "some updated code"
    end

    test "update_ignore_rule/2 with invalid data returns error changeset" do
      ignore_rule = ignore_rule_fixture()
      assert {:error, %Ecto.Changeset{}} = Ingestions.update_ignore_rule(ignore_rule, @invalid_attrs)
      assert ignore_rule == Ingestions.get_ignore_rule!(ignore_rule.id)
    end

    test "delete_ignore_rule/1 deletes the ignore_rule" do
      ignore_rule = ignore_rule_fixture()
      assert {:ok, %IgnoreRule{}} = Ingestions.delete_ignore_rule(ignore_rule)
      assert_raise Ecto.NoResultsError, fn -> Ingestions.get_ignore_rule!(ignore_rule.id) end
    end

    test "change_ignore_rule/1 returns a ignore_rule changeset" do
      ignore_rule = ignore_rule_fixture()
      assert %Ecto.Changeset{} = Ingestions.change_ignore_rule(ignore_rule)
    end
  end

  describe "executions" do
    alias CreditRadar.Ingestions.Execution

    import CreditRadar.IngestionsFixtures

    @invalid_attrs %{status: nil, started_at: nil, kind: nil, finished_at: nil, trigger: nil}

    test "list_executions/0 returns all executions" do
      execution = execution_fixture()
      assert Ingestions.list_executions() == [execution]
    end

    test "get_execution!/1 returns the execution with given id" do
      execution = execution_fixture()
      assert Ingestions.get_execution!(execution.id) == execution
    end

    test "create_execution/1 with valid data creates a execution" do
      valid_attrs = %{status: "some status", started_at: ~U[2025-10-25 04:57:00Z], kind: "some kind", finished_at: ~U[2025-10-25 04:57:00Z], trigger: "some trigger"}

      assert {:ok, %Execution{} = execution} = Ingestions.create_execution(valid_attrs)
      assert execution.status == "some status"
      assert execution.started_at == ~U[2025-10-25 04:57:00Z]
      assert execution.kind == "some kind"
      assert execution.finished_at == ~U[2025-10-25 04:57:00Z]
      assert execution.trigger == "some trigger"
    end

    test "create_execution/1 with invalid data returns error changeset" do
      assert {:error, %Ecto.Changeset{}} = Ingestions.create_execution(@invalid_attrs)
    end

    test "update_execution/2 with valid data updates the execution" do
      execution = execution_fixture()
      update_attrs = %{status: "some updated status", started_at: ~U[2025-10-26 04:57:00Z], kind: "some updated kind", finished_at: ~U[2025-10-26 04:57:00Z], trigger: "some updated trigger"}

      assert {:ok, %Execution{} = execution} = Ingestions.update_execution(execution, update_attrs)
      assert execution.status == "some updated status"
      assert execution.started_at == ~U[2025-10-26 04:57:00Z]
      assert execution.kind == "some updated kind"
      assert execution.finished_at == ~U[2025-10-26 04:57:00Z]
      assert execution.trigger == "some updated trigger"
    end

    test "update_execution/2 with invalid data returns error changeset" do
      execution = execution_fixture()
      assert {:error, %Ecto.Changeset{}} = Ingestions.update_execution(execution, @invalid_attrs)
      assert execution == Ingestions.get_execution!(execution.id)
    end

    test "delete_execution/1 deletes the execution" do
      execution = execution_fixture()
      assert {:ok, %Execution{}} = Ingestions.delete_execution(execution)
      assert_raise Ecto.NoResultsError, fn -> Ingestions.get_execution!(execution.id) end
    end

    test "change_execution/1 returns a execution changeset" do
      execution = execution_fixture()
      assert %Ecto.Changeset{} = Ingestions.change_execution(execution)
    end
  end

  describe "persist_operations/1" do
    test "creates new securities from operations" do
      operations = [
        %{
          code: "CRI123",
          reference_date: ~D[2025-10-01],
          duration: Decimal.new("12"),
          issuing: "2024-01-01",
          issuer: "Issuer Alpha",
          credit_risk: "AAA",
          series: "A",
          security_type: :cri
        }
      ]

      assert {:ok, stats} = IngestCriCra.persist_operations(operations)
      assert stats.created == 1
      assert stats.updated == 0
      assert stats.skipped == 0

      security = Repo.get_by!(Security, code: "CRI123")
      assert security.security_type == :cri
      assert security.duration == 12
      assert security.sync_source == :api
    end

    test "updates existing securities when identifiers match" do
      existing_attrs = %{
        code: "CRI999",
        reference_date: ~D[2025-09-01],
        duration: 24,
        issuing: "2023-09-01",
        issuer: "Issuer Beta",
        credit_risk: "BBB",
        series: "B",
        security_type: :cri,
        sync_source: :api
      }

      %Security{}
      |> FixedIncome.security_create_changeset(existing_attrs)
      |> Repo.insert!()

      operations = [
        %{
          code: "CRI999",
          reference_date: ~D[2025-10-15],
          duration: Decimal.new("36"),
          issuing: "2023-09-01",
          issuer: "Issuer Beta Updated",
          credit_risk: "AA",
          series: "B",
          security_type: :cri
        }
      ]

      assert {:ok, stats} = IngestCriCra.persist_operations(operations)
      assert stats.created == 0
      assert stats.updated == 1
      assert stats.skipped == 0

      security = Repo.get_by!(Security, code: "CRI999")
      assert security.issuer == "Issuer Beta Updated"
      assert security.credit_risk == "AA"
      assert security.duration == 36
      assert security.reference_date == ~D[2025-10-15]
    end

    test "returns error when persistence fails" do
      operations = [
        %{
          code: nil,
          duration: Decimal.new("10"),
          issuing: "2023-01-01",
          issuer: "Issuer Gamma",
          credit_risk: "AA",
          series: "C",
          security_type: :cri
        }
      ]

      assert {:ok, stats} = IngestCriCra.persist_operations(operations)
      assert stats.skipped == 1
    end
  end
end
