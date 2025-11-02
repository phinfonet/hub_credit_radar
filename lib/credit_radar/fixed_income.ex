defmodule CreditRadar.FixedIncome do
  @moduledoc """
  Context boundary for fixed income domain helpers.
  """
  import Ecto.Query
  alias CreditRadar.Repo
  alias CreditRadar.FixedIncome.{Assessment, Security}

  @doc """
  Builds a changeset for creating a security.
  """
  def security_create_changeset(security, attrs, _metadata \\ []) do
    Security.changeset(security, attrs)
  end

  @doc """
  Builds a changeset for updating a security.
  """
  def security_update_changeset(security, attrs, _metadata \\ []) do
    Security.changeset(security, attrs)
  end

  @doc """
  Builds a changeset for creating an assessment.
  """
  def assessment_create_changeset(assessment, attrs, _metadata \\ []) do
    assessment
    |> Assessment.changeset(attrs)
    |> calculate_rating_hub()
  end

  @doc """
  Duplicates an assessment to all other securities with the same credit_risk and reference_date.
  Called after an assessment is successfully created.

  The credit_risk (originador) represents the actual credit risk entity,
  not the issuer (securitizadora), so the assessment should be replicated
  to all securities from the same credit risk source.
  """
  def duplicate_assessment_to_issuer(assessment) do
    assessment = Repo.preload(assessment, :security)
    security = assessment.security

    if security && security.credit_risk do
      # Buscar todos os outros securities com mesmo credit_risk (originador) e reference_date
      other_securities =
        Security
        |> where([s], s.credit_risk == ^security.credit_risk)
        |> where([s], s.reference_date == ^security.reference_date)
        |> where([s], s.id != ^security.id)
        |> Repo.all()

      # Duplicar o assessment para cada security (apenas se não existir)
      Enum.each(other_securities, fn sec ->
        existing = Repo.get_by(Assessment, security_id: sec.id)

        if is_nil(existing) do
          %Assessment{
            issuer_quality: assessment.issuer_quality,
            capital_structure: assessment.capital_structure,
            solvency_ratio: assessment.solvency_ratio,
            credit_spread: assessment.credit_spread,
            grade: assessment.grade,
            recommendation: assessment.recommendation,
            security_id: sec.id
          }
          |> Repo.insert()
        end
      end)
    end

    {:ok, assessment}
  end

  @doc """
  Updates all assessments for securities with the same credit_risk and reference_date.
  Called after an assessment is edited to propagate changes to all related securities.
  """
  def update_assessments_by_credit_risk(assessment) do
    assessment = Repo.preload(assessment, :security)
    security = assessment.security

    if security && security.credit_risk do
      # Buscar todos os outros securities com mesmo credit_risk (originador) e reference_date
      other_security_ids =
        Security
        |> where([s], s.credit_risk == ^security.credit_risk)
        |> where([s], s.reference_date == ^security.reference_date)
        |> where([s], s.id != ^security.id)
        |> select([s], s.id)
        |> Repo.all()

      # Atualizar todos os assessments existentes desses securities
      Assessment
      |> where([a], a.security_id in ^other_security_ids)
      |> Repo.update_all(
        set: [
          issuer_quality: assessment.issuer_quality,
          capital_structure: assessment.capital_structure,
          solvency_ratio: assessment.solvency_ratio,
          credit_spread: assessment.credit_spread,
          grade: assessment.grade,
          recommendation: assessment.recommendation,
          rating_hub: assessment.rating_hub,
          updated_at: DateTime.utc_now()
        ]
      )
    end

    {:ok, assessment}
  end

  @doc """
  Builds a changeset for updating an assessment.
  """
  def assessment_update_changeset(assessment, attrs, _metadata \\ []) do
    assessment
    |> Assessment.changeset(attrs)
    |> calculate_rating_hub()
  end

  @doc """
  Lists securities with their assessments for analysis purposes.
  Supports filtering by security_type, benchmark_index, duration range, grade, and issuer.
  """
  def list_securities_with_assessments(filters \\ %{}) do
    Security
    |> join(:inner, [s], a in assoc(s, :assessment))
    |> apply_analysis_filters(filters)
    |> select([s, a], %{
      id: s.id,
      code: s.code,
      issuer: s.issuer,
      security_type: s.security_type,
      series: s.series,
      issuing: s.issuing,
      benchmark_index: s.benchmark_index,
      coupon_rate: s.coupon_rate,
      correction_rate: s.correction_rate,
      expected_return: s.expected_return,
      credit_risk: s.credit_risk,
      duration: s.duration,
      reference_date: s.reference_date,
      ntnb_reference: s.ntnb_reference,
      ntnb_reference_date: s.ntnb_reference_date,
      # Assessment fields
      assessment_id: a.id,
      issuer_quality: a.issuer_quality,
      capital_structure: a.capital_structure,
      solvency_ratio: a.solvency_ratio,
      credit_spread: a.credit_spread,
      grade: a.grade,
      recommendation: a.recommendation,
      rating_hub: a.rating_hub
    })
    |> order_by([s, a], [asc: s.issuer, asc: s.code])
    |> Repo.all()
  end

  @doc """
  Returns only securities that have assessments (for BI analysis).
  """
  def list_assessed_securities(filters \\ %{}) do
    Security
    |> join(:inner, [s], a in assoc(s, :assessment))
    |> apply_analysis_filters(filters)
    |> select([s, a], %{
      id: s.id,
      code: s.code,
      issuer: s.issuer,
      security_type: s.security_type,
      benchmark_index: s.benchmark_index,
      coupon_rate: s.coupon_rate,
      credit_risk: s.credit_risk,
      duration: s.duration,
      grade: a.grade,
      solvency_ratio: a.solvency_ratio,
      credit_spread: a.credit_spread,
      recommendation: a.recommendation
    })
    |> order_by([s, a], [asc: s.duration])
    |> Repo.all()
  end

  defp apply_analysis_filters(query, filters) do
    query
    |> filter_by_security_type(filters)
    |> filter_by_benchmark_index(filters)
    |> filter_by_grade(filters)
    |> filter_by_recommendation(filters)
    |> filter_by_issuer(filters)
  end

  defp filter_by_security_type(query, %{security_type: type}) when not is_nil(type) do
    where(query, [s], s.security_type == ^type)
  end

  defp filter_by_security_type(query, _), do: query

  defp filter_by_benchmark_index(query, %{benchmark_index: index}) when not is_nil(index) do
    where(query, [s], s.benchmark_index == ^index)
  end

  defp filter_by_benchmark_index(query, _), do: query

  defp filter_by_duration_range(query, %{duration_min: min, duration_max: max})
       when not is_nil(min) and not is_nil(max) do
    where(query, [s], s.duration >= ^min and s.duration <= ^max)
  end

  defp filter_by_duration_range(query, %{duration_min: min}) when not is_nil(min) do
    where(query, [s], s.duration >= ^min)
  end

  defp filter_by_duration_range(query, %{duration_max: max}) when not is_nil(max) do
    where(query, [s], s.duration <= ^max)
  end

  defp filter_by_duration_range(query, _), do: query

  defp filter_by_grade(query, %{grade: grade}) when not is_nil(grade) do
    where(query, [s, a], a.grade == ^grade)
  end

  defp filter_by_grade(query, _), do: query

  defp filter_by_recommendation(query, %{recommendation: recommendation}) when not is_nil(recommendation) do
    where(query, [s, a], a.recommendation == ^recommendation)
  end

  defp filter_by_recommendation(query, _), do: query

  defp filter_by_issuer(query, %{issuers: issuers}) when is_list(issuers) and length(issuers) > 0 do
    where(query, [s], s.issuer in ^issuers)
  end

  defp filter_by_issuer(query, %{issuer: issuer}) when not is_nil(issuer) and issuer != "" do
    where(query, [s], s.issuer == ^issuer)
  end

  defp filter_by_issuer(query, _), do: query

  @doc """
  Returns a list of unique issuers (emissores) from all securities.
  """
  def list_unique_issuers do
    Security
    |> select([s], s.issuer)
    |> distinct(true)
    |> where([s], not is_nil(s.issuer))
    |> order_by([s], asc: s.issuer)
    |> Repo.all()
  end

  @doc """
  Returns a list of unique benchmark indexes from all securities.
  """
  def list_unique_benchmark_indexes do
    Security
    |> select([s], s.benchmark_index)
    |> distinct(true)
    |> where([s], not is_nil(s.benchmark_index))
    |> order_by([s], asc: s.benchmark_index)
    |> Repo.all()
  end

  @doc """
  Returns a list of unique grades from all assessments.
  """
  def list_unique_grades do
    Assessment
    |> select([a], a.grade)
    |> distinct(true)
    |> where([a], not is_nil(a.grade))
    |> order_by([a], desc: a.grade)
    |> Repo.all()
  end

  # Calcula o Rating Hub automaticamente
  # Rating Hub = expected_return * média(issuer_quality, capital_structure, solvency_ratio, credit_spread)
  defp calculate_rating_hub(changeset) do
    security_id = Ecto.Changeset.get_field(changeset, :security_id)
    issuer_quality = Ecto.Changeset.get_field(changeset, :issuer_quality)
    capital_structure = Ecto.Changeset.get_field(changeset, :capital_structure)
    solvency_ratio = Ecto.Changeset.get_field(changeset, :solvency_ratio)
    credit_spread = Ecto.Changeset.get_field(changeset, :credit_spread)

    # Só calcula se tiver todos os valores necessários
    if security_id && issuer_quality && capital_structure && solvency_ratio && credit_spread do
      security = Repo.get(Security, security_id)

      if security && security.expected_return do
        # Média dos 4 campos
        avg = Decimal.div(
          Decimal.add(
            Decimal.add(Decimal.new(issuer_quality), Decimal.new(capital_structure)),
            Decimal.add(Decimal.new(solvency_ratio), Decimal.new(credit_spread))
          ),
          Decimal.new(4)
        )

        # Rating Hub = expected_return * avg
        rating_hub = Decimal.mult(security.expected_return, avg)

        Ecto.Changeset.put_change(changeset, :rating_hub, rating_hub)
      else
        changeset
      end
    else
      changeset
    end
  end
end
