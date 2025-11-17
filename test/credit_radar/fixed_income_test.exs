defmodule CreditRadar.FixedIncomeTest do
  use CreditRadar.DataCase

  alias CreditRadar.FixedIncome

  alias CreditRadar.FixedIncome.{
    Assessment,
    CDIHistory,
    CDIProjection,
    IPCAProjection,
    Security,
    SelicHistory
  }

  describe "duplicate_assessment_to_issuer/1" do
    test "duplicates assessment to all securities with same issuer and reference_date" do
      # Criar securities com o mesmo emissor e data de referência
      reference_date = ~D[2024-01-15]

      security1 =
        insert_security(%{
          issuer: "Empresa ABC",
          reference_date: reference_date,
          code: "ABC001"
        })

      security2 =
        insert_security(%{
          issuer: "Empresa ABC",
          reference_date: reference_date,
          code: "ABC002"
        })

      security3 =
        insert_security(%{
          issuer: "Empresa ABC",
          reference_date: reference_date,
          code: "ABC003"
        })

      # Criar assessment para o primeiro security
      assessment =
        insert_assessment(%{
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

      security1 =
        insert_security(%{
          issuer: "Empresa ABC",
          reference_date: reference_date,
          code: "ABC001"
        })

      security2 =
        insert_security(%{
          issuer: "Empresa XYZ",
          reference_date: reference_date,
          code: "XYZ001"
        })

      assessment =
        insert_assessment(%{
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
      security1 =
        insert_security(%{
          issuer: "Empresa ABC",
          reference_date: ~D[2024-01-15],
          code: "ABC001"
        })

      security2 =
        insert_security(%{
          issuer: "Empresa ABC",
          reference_date: ~D[2024-02-15],
          code: "ABC002"
        })

      assessment =
        insert_assessment(%{
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

  describe "cdi_history_changeset/3" do
    test "validates required attributes" do
      changeset =
        FixedIncome.cdi_history_changeset(%CDIHistory{}, %{
          reference_date: ~D[2024-01-01],
          cdi_value: Decimal.new("10.1500")
        })

      assert changeset.valid?
    end

    test "errors when reference_date is missing" do
      changeset =
        FixedIncome.cdi_history_changeset(%CDIHistory{}, %{
          cdi_value: Decimal.new("10.1500")
        })

      refute changeset.valid?
      assert %{reference_date: ["can't be blank"]} = errors_on(changeset)
    end
  end

  describe "selic_history_changeset/3" do
    test "validates required attributes" do
      changeset =
        FixedIncome.selic_history_changeset(%SelicHistory{}, %{
          reference_date: ~D[2024-01-01],
          selic_value: Decimal.new("11.2500")
        })

      assert changeset.valid?
    end

    test "errors when selic_value is missing" do
      changeset =
        FixedIncome.selic_history_changeset(%SelicHistory{}, %{
          reference_date: ~D[2024-01-01]
        })

      refute changeset.valid?
      assert %{selic_value: ["can't be blank"]} = errors_on(changeset)
    end
  end

  describe "cdi_projection_changeset/3" do
    test "requires reference_date" do
      changeset =
        FixedIncome.cdi_projection_changeset(%CDIProjection{}, %{
          reference_date: ~D[2024-01-01]
        })

      assert changeset.valid?
    end

    test "errors when reference_date missing" do
      changeset =
        FixedIncome.cdi_projection_changeset(%CDIProjection{}, %{
          current_month_cdi_value: Decimal.new("10.15")
        })

      refute changeset.valid?
      assert %{reference_date: ["can't be blank"]} = errors_on(changeset)
    end
  end

  describe "import_cdi_history/2" do
    test "upserts normalized entries using provided fetch_fun" do
      fetch_fun = fn _params, _opts ->
        {:ok,
         [
           %{"data" => "01/01/2024", "valor" => "10,15"},
           %{"data" => "02/01/2024", "valor" => "10.25"}
         ]}
      end

      assert {:ok, %{processed: 2}} =
               FixedIncome.import_cdi_history(%{}, fetch_fun: fetch_fun)

      record = Repo.get_by!(CDIHistory, reference_date: ~D[2024-01-01])
      assert Decimal.equal?(record.cdi_value, Decimal.new("10.15"))
    end

    test "returns error when payload rows are invalid" do
      fetch_fun = fn _, _ -> {:ok, [%{"foo" => "bar"}]} end

      assert {:error, :invalid_entry} = FixedIncome.import_cdi_history(%{}, fetch_fun: fetch_fun)
    end
  end

  describe "import_selic_history/2" do
    test "persists SELIC entries" do
      fetch_fun = fn _params, _opts ->
        {:ok, [%{"data" => "05/01/2024", "valor" => "11.5000"}]}
      end

      assert {:ok, %{processed: 1}} =
               FixedIncome.import_selic_history(%{}, fetch_fun: fetch_fun)

      record = Repo.get_by!(SelicHistory, reference_date: ~D[2024-01-05])
      assert Decimal.equal?(record.selic_value, Decimal.new("11.5000"))
    end
  end

  describe "upsert_cdi_projection/1" do
    test "inserts a new projection" do
      attrs = %{
        reference_date: ~D[2024-01-01],
        current_month_selic_value: Decimal.new("11.0"),
        current_month_cdi_value: Decimal.new("10.5"),
        year_cdi_projection_value: Decimal.new("12.3")
      }

      assert {:ok, _} = FixedIncome.upsert_cdi_projection(attrs)

      projection = Repo.get_by!(CDIProjection, reference_date: ~D[2024-01-01])
      assert Decimal.equal?(projection.year_cdi_projection_value, Decimal.new("12.3"))
    end

    test "updates an existing projection" do
      attrs = %{
        reference_date: ~D[2024-02-01],
        current_month_cdi_value: Decimal.new("10.1")
      }

      {:ok, _} = FixedIncome.upsert_cdi_projection(attrs)

      update_attrs = %{
        reference_date: ~D[2024-02-01],
        current_month_cdi_value: Decimal.new("11.1"),
        year_cdi_projection_value: Decimal.new("12.8")
      }

      {:ok, _} = FixedIncome.upsert_cdi_projection(update_attrs)

      projection = Repo.get_by!(CDIProjection, reference_date: ~D[2024-02-01])
      assert Decimal.equal?(projection.current_month_cdi_value, Decimal.new("11.1"))
      assert Decimal.equal?(projection.year_cdi_projection_value, Decimal.new("12.8"))
    end
  end

  describe "upsert_ipca_projection/1" do
    test "inserts or updates projections" do
      attrs = %{
        reference_date: ~D[2024-03-01],
        current_month_ipca_value: Decimal.new("0.45"),
        year_ipca_projection_value: Decimal.new("4.10")
      }

      assert {:ok, _} = FixedIncome.upsert_ipca_projection(attrs)

      update_attrs = %{
        reference_date: ~D[2024-03-01],
        year_ipca_projection_value: Decimal.new("4.25")
      }

      assert {:ok, _} = FixedIncome.upsert_ipca_projection(update_attrs)

      record = Repo.get_by!(IPCAProjection, reference_date: ~D[2024-03-01])
      assert Decimal.equal?(record.year_ipca_projection_value, Decimal.new("4.25"))
    end
  end

  describe "expected_return_for/1" do
    test "returns CDI projection for di_plus securities" do
      Repo.insert!(%CDIProjection{
        reference_date: ~D[2024-01-01],
        year_cdi_projection_value: Decimal.new("12.34")
      })

      attrs = %{benchmark_index: "di_plus", reference_date: ~D[2024-01-01]}

      assert Decimal.equal?(FixedIncome.expected_return_for(attrs), Decimal.new("12.34"))
    end

    test "returns nil when projection missing" do
      attrs = %{benchmark_index: "di_plus", reference_date: ~D[2024-02-01]}

      assert FixedIncome.expected_return_for(attrs) == nil
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
