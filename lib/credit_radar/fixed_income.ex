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
    Assessment.changeset(assessment, attrs)
  end

  @doc """
  Duplicates an assessment to all other securities with the same issuer and reference_date.
  Called after an assessment is successfully created.
  """
  def duplicate_assessment_to_issuer(assessment) do
    assessment = Repo.preload(assessment, :security)
    security = assessment.security

    if security do
      # Buscar todos os outros securities com mesmo emissor e reference_date
      other_securities =
        Security
        |> where([s], s.issuer == ^security.issuer)
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
  Builds a changeset for updating an assessment.
  """
  def assessment_update_changeset(assessment, attrs, _metadata \\ []) do
    Assessment.changeset(assessment, attrs)
  end

  @doc """
  Lists securities with their assessments for analysis purposes.
  Supports filtering by security_type, benchmark_index, duration range, grade, and issuer.
  """
  def list_securities_with_assessments(filters \\ %{}) do
    Security
    |> join(:left, [s], a in assoc(s, :assessment))
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
      recommendation: a.recommendation
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
end
