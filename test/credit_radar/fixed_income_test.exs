defmodule CreditRadar.FixedIncomeTest do
  use CreditRadar.DataCase

  alias CreditRadar.FixedIncome
  alias CreditRadar.FixedIncome.{Assessment, Security}

  describe "duplicate_assessment_to_issuer/1" do
    test "duplicates assessment to all securities with same issuer and reference_date" do
      # Criar securities com o mesmo emissor e data de referência
      reference_date = ~D[2024-01-15]

      security1 = insert_security(%{
        issuer: "Empresa ABC",
        reference_date: reference_date,
        code: "ABC001"
      })

      security2 = insert_security(%{
        issuer: "Empresa ABC",
        reference_date: reference_date,
        code: "ABC002"
      })

      security3 = insert_security(%{
        issuer: "Empresa ABC",
        reference_date: reference_date,
        code: "ABC003"
      })

      # Criar assessment para o primeiro security
      assessment = insert_assessment(%{
        security_id: security1.id,
        issuer_quality: "Alta qualidade",
        capital_structure: "Estrutura sólida",
        solvency_ratio: Decimal.new("1.5"),
        credit_spread: Decimal.new("2.3"),
        grade: "A+",
        recommendation: "Comprar"
      })

      # Duplicar para os outros
      {:ok, _} = FixedIncome.duplicate_assessment_to_issuer(assessment)

      # Verificar que assessments foram criados para os outros securities
      assessment2 = Repo.get_by(Assessment, security_id: security2.id)
      assessment3 = Repo.get_by(Assessment, security_id: security3.id)

      assert assessment2 != nil
      assert assessment3 != nil

      # Verificar que os dados foram copiados corretamente
      assert assessment2.issuer_quality == "Alta qualidade"
      assert assessment2.capital_structure == "Estrutura sólida"
      assert Decimal.equal?(assessment2.solvency_ratio, Decimal.new("1.5"))
      assert Decimal.equal?(assessment2.credit_spread, Decimal.new("2.3"))
      assert assessment2.grade == "A+"
      assert assessment2.recommendation == "Comprar"
    end

    test "does not duplicate to securities with different issuer" do
      reference_date = ~D[2024-01-15]

      security1 = insert_security(%{
        issuer: "Empresa ABC",
        reference_date: reference_date,
        code: "ABC001"
      })

      security2 = insert_security(%{
        issuer: "Empresa XYZ",
        reference_date: reference_date,
        code: "XYZ001"
      })

      assessment = insert_assessment(%{
        security_id: security1.id,
        issuer_quality: "Alta qualidade",
        capital_structure: "Estrutura sólida",
        solvency_ratio: Decimal.new("1.5"),
        credit_spread: Decimal.new("2.3"),
        grade: "A+",
        recommendation: "Comprar"
      })

      {:ok, _} = FixedIncome.duplicate_assessment_to_issuer(assessment)

      # Verificar que NÃO foi criado assessment para o security de outro emissor
      assessment2 = Repo.get_by(Assessment, security_id: security2.id)
      assert assessment2 == nil
    end

    test "does not duplicate to securities with different reference_date" do
      security1 = insert_security(%{
        issuer: "Empresa ABC",
        reference_date: ~D[2024-01-15],
        code: "ABC001"
      })

      security2 = insert_security(%{
        issuer: "Empresa ABC",
        reference_date: ~D[2024-02-15],
        code: "ABC002"
      })

      assessment = insert_assessment(%{
        security_id: security1.id,
        issuer_quality: "Alta qualidade",
        capital_structure: "Estrutura sólida",
        solvency_ratio: Decimal.new("1.5"),
        credit_spread: Decimal.new("2.3"),
        grade: "A+",
        recommendation: "Comprar"
      })

      {:ok, _} = FixedIncome.duplicate_assessment_to_issuer(assessment)

      # Verificar que NÃO foi criado assessment para o security com data diferente
      assessment2 = Repo.get_by(Assessment, security_id: security2.id)
      assert assessment2 == nil
    end
  end

  # Helper functions
  defp insert_security(attrs) do
    default_attrs = %{
      issuer: "Default Issuer",
      security_type: :cri,
      series: "1",
      issuing: "2024-01",
      code: "DEFAULT001",
      credit_risk: "Baixo",
      duration: 12,
      reference_date: ~D[2024-01-01]
    }

    merged_attrs = Map.merge(default_attrs, attrs)

    %Security{}
    |> Security.changeset(merged_attrs)
    |> Repo.insert!()
  end

  defp insert_assessment(attrs) do
    %Assessment{}
    |> Assessment.changeset(attrs)
    |> Repo.insert!()
  end
end
