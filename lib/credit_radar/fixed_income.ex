defmodule CreditRadar.FixedIncome do
  @moduledoc """
  Context boundary for fixed income domain helpers.
  """
  import Ecto.Query
  alias CreditRadar.Repo

  alias CreditRadar.FixedIncome.{
    Assessment,
    CDIHistory,
    CDIProjection,
    IPCAProjection,
    Security,
    SelicHistory
  }

  alias CreditRadar.Integrations.BCB.Client, as: BCBClient

  @doc """
  Fetches CDI daily series straight from the Banco Central do Brasil API.
  """
  @spec fetch_cdi_series(map(), keyword()) ::
          {:ok, list()} | {:error, {:unexpected_status, non_neg_integer(), term()} | term()}
  def fetch_cdi_series(params \\ %{}, opts \\ []) do
    BCBClient.fetch_cdi_series(params, opts)
  end

  @doc """
  Fetches SELIC daily series straight from the Banco Central do Brasil API.
  """
  @spec fetch_selic_series(map(), keyword()) ::
          {:ok, list()} | {:error, {:unexpected_status, non_neg_integer(), term()} | term()}
  def fetch_selic_series(params \\ %{}, opts \\ []) do
    BCBClient.fetch_selic_series(params, opts)
  end

  @doc """
  Builds a changeset for persisting CDI history entries.
  """
  def cdi_history_changeset(history \\ %CDIHistory{}, attrs, _metadata \\ []) do
    CDIHistory.changeset(history, attrs)
  end

  @doc """
  Builds a changeset for persisting SELIC history entries.
  """
  def selic_history_changeset(history \\ %SelicHistory{}, attrs, _metadata \\ []) do
    SelicHistory.changeset(history, attrs)
  end

  @doc """
  Builds a changeset for persisting CDI projections.
  """
  def cdi_projection_changeset(projection \\ %CDIProjection{}, attrs, _metadata \\ []) do
    CDIProjection.changeset(projection, normalize_reference_month(attrs))
  end

  @doc """
  Builds a changeset for persisting IPCA projections.
  """
  def ipca_projection_changeset(projection \\ %IPCAProjection{}, attrs, _metadata \\ []) do
    IPCAProjection.changeset(projection, normalize_reference_month(attrs))
  end

  @doc """
  Computes the expected return override for a security attrs map based on benchmark data.

  - For `di_plus`, returns the latest CDI projection for the reference month plus the coupon spread
  - For `ipca`, returns the latest IPCA projection for the reference month plus the coupon spread
  """
  def expected_return_for(%{benchmark_index: _} = attrs) do
    with {:ok, details} <- expected_return_components(attrs) do
      combine_projection(details)
    else
      _ -> nil
    end
  end

  def expected_return_for(_attrs), do: nil

  @doc """
  Helper to inspect how the expected return is being derived for a given security.
  Returns a map with the stored value, coupon, projection and computed result.
  """
  def expected_return_snapshot(security_or_id) do
    with %Security{} = security <- load_security(security_or_id),
         {:ok, %{benchmark: benchmark, projection: projection, coupon: coupon, mode: mode}} <-
           expected_return_components(Map.from_struct(security)) do
      %{
        security_id: security.id,
        benchmark_index: benchmark,
        coupon_rate: coupon,
        projection_value: projection,
        stored_expected_return: security.expected_return,
        computed_expected_return: combine_projection(%{mode: mode, projection: projection, coupon: coupon})
      }
    else
      nil -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  defp expected_return_components(%{benchmark_index: benchmark} = attrs) do
    case normalize_benchmark(benchmark) do
      normalized when normalized in ["di_plus", "ipca"] ->
        schema =
          if normalized == "di_plus",
            do: {CDIProjection, :year_cdi_projection_value},
            else: {IPCAProjection, :year_ipca_projection_value}

        {mod, field} = schema
        info = projection_payload(attrs, mod, field, :add, &decimal_coupon/1)
        {:ok, Map.put(info, :benchmark, normalized)}

      "di_multiple" ->
        info = projection_payload(attrs, CDIProjection, :year_cdi_projection_value, :multiply, &ratio_coupon/1)
        {:ok, Map.put(info, :benchmark, "di_multiple")}

      _ ->
        {:error, :unsupported_benchmark}
    end
  end

  defp expected_return_components(_), do: {:error, :invalid_attrs}

  defp projection_payload(attrs, schema, value_field, mode, coupon_fun) do
    reference_date = resolve_reference_date(attrs)

    projection =
      case reference_date do
        %Date{} = date -> fetch_month_projection(schema, value_field, date)
        _ -> nil
      end

    coupon = coupon_fun.(attrs)

    %{mode: mode, projection: projection, coupon: coupon}
  end

  defp resolve_reference_date(attrs) do
    attrs
    |> get_first_present([:reference_date, "reference_date", :ntnb_reference_date, "ntnb_reference_date"])
    |> coerce_date()
  end

  defp get_first_present(map, keys) do
    Enum.find_value(keys, fn key ->
      case Map.get(map, key) do
        nil -> nil
        "" -> nil
        value -> value
      end
    end)
  end

  defp coerce_date(%Date{} = date), do: date

  defp coerce_date(%NaiveDateTime{} = dt), do: NaiveDateTime.to_date(dt)
  defp coerce_date(%DateTime{} = dt), do: DateTime.to_date(dt)

  defp coerce_date(value) when is_binary(value) do
    case Date.from_iso8601(value) do
      {:ok, date} -> date
      _ -> nil
    end
  end

  defp coerce_date(_), do: nil

  defp ensure_expected_return(security_map) do
    Map.update(security_map, :expected_return, nil, fn existing ->
      existing || expected_return_for(security_map)
    end)
  end

  defp fetch_month_projection(schema, value_field, %Date{} = reference_date) do
    month_start = Date.beginning_of_month(reference_date)
    month_end = Date.end_of_month(reference_date)

    schema
    |> where([p], p.reference_date >= ^month_start and p.reference_date <= ^month_end)
    |> order_by([p], [desc: p.reference_date, desc: p.inserted_at])
    |> limit(1)
    |> Repo.one()
    |> case do
      nil -> nil
      record -> Map.get(record, value_field)
    end
  end

  defp sum_projection_and_coupon(nil, nil), do: nil
  defp sum_projection_and_coupon(projection, nil), do: projection
  defp sum_projection_and_coupon(nil, coupon), do: coupon
  defp sum_projection_and_coupon(projection, coupon), do: Decimal.add(projection, coupon)

  defp multiply_projection_and_ratio(nil, _), do: nil
  defp multiply_projection_and_ratio(_, nil), do: nil
  defp multiply_projection_and_ratio(projection, ratio), do: Decimal.mult(projection, ratio)

  defp combine_projection(%{mode: :add, projection: projection, coupon: coupon}),
    do: sum_projection_and_coupon(projection, coupon)

  defp combine_projection(%{mode: :multiply, projection: projection, coupon: ratio}),
    do: multiply_projection_and_ratio(projection, ratio)

  defp combine_projection(_), do: nil

  defp decimal_coupon(attrs) do
    attrs
    |> coupon_raw_value()
    |> decimal_from()
  end

  defp ratio_coupon(attrs) do
    attrs
    |> coupon_raw_value()
    |> decimal_from()
    |> normalize_ratio()
  end

  defp coupon_raw_value(attrs) do
    Map.get(attrs, :coupon_rate) || Map.get(attrs, "coupon_rate")
  end

  defp normalize_ratio(nil), do: nil

  defp normalize_ratio(%Decimal{} = value) do
    case Decimal.compare(value, Decimal.new(2)) do
      :gt -> Decimal.div(value, Decimal.new(100))
      _ -> value
    end
  end

  defp normalize_ratio(other), do: other

  defp normalize_benchmark(benchmark) when is_atom(benchmark), do: Atom.to_string(benchmark)

  defp normalize_benchmark(benchmark) when is_binary(benchmark) do
    benchmark
    |> String.trim()
    |> String.downcase()
  end

  defp normalize_benchmark(_), do: nil

  defp normalize_reference_month(attrs) when is_map(attrs) do
    attrs
    |> normalize_reference_key(:reference_date)
    |> normalize_reference_key("reference_date")
  end

  defp normalize_reference_month(attrs), do: attrs

  defp normalize_reference_key(attrs, key) do
    case Map.fetch(attrs, key) do
      {:ok, value} ->
        Map.put(attrs, key, expand_month_value(value))

      :error ->
        attrs
    end
  end

  defp expand_month_value(value) when is_binary(value) do
    cond do
      value == "" -> nil
      String.length(value) == 7 -> value <> "-01"
      true -> value
    end
  end

  defp expand_month_value(%Date{} = date), do: date
  defp expand_month_value(%DateTime{} = datetime), do: DateTime.to_date(datetime)
  defp expand_month_value(%NaiveDateTime{} = datetime), do: NaiveDateTime.to_date(datetime)
  defp expand_month_value(other), do: other

  defp load_security(%Security{} = security), do: security

  defp load_security(id) do
    id
    |> parse_integer()
    |> case do
      nil -> nil
      int_id -> Repo.get(Security, int_id)
    end
  end

  defp parse_integer(id) when is_integer(id), do: id

  defp parse_integer(id) when is_binary(id) do
    case Integer.parse(id) do
      {int, _} -> int
      :error -> nil
    end
  end

  defp parse_integer(_), do: nil

  defp decimal_from(%Decimal{} = decimal), do: decimal
  defp decimal_from(value) when is_integer(value), do: Decimal.new(value)
  defp decimal_from(value) when is_float(value), do: Decimal.from_float(value)

  defp decimal_from(value) when is_binary(value) do
    case Decimal.parse(value) do
      {:ok, decimal} -> decimal
      :error -> nil
    end
  end

  defp decimal_from(_), do: nil

  defp decimal_percentage(value) do
    value
    |> decimal_from()
    |> normalize_percentage()
  end

  defp normalize_percentage(nil), do: nil

  defp normalize_percentage(%Decimal{} = decimal) do
    abs_val = Decimal.abs(decimal)

    case Decimal.compare(abs_val, Decimal.new(2)) do
      :gt -> Decimal.div(decimal, Decimal.new(100))
      _ -> decimal
    end
  end

  @doc """
  Inserts or updates a CDI projection entry based on the reference date.
  """
  def upsert_cdi_projection(attrs) when is_map(attrs) do
    %CDIProjection{}
    |> cdi_projection_changeset(attrs)
    |> Repo.insert(
      on_conflict: {:replace_all_except, [:id, :inserted_at]},
      conflict_target: :reference_date
    )
  end

  @doc """
  Inserts or updates an IPCA projection entry based on the reference date.
  """
  def upsert_ipca_projection(attrs) when is_map(attrs) do
    %IPCAProjection{}
    |> ipca_projection_changeset(attrs)
    |> Repo.insert(
      on_conflict: {:replace_all_except, [:id, :inserted_at]},
      conflict_target: :reference_date
    )
  end

  @doc """
  Imports (or refreshes) the CDI historical series using the BCB open data API.
  """
  def import_cdi_history(params \\ %{}, opts \\ []) do
    fetch_fun = Keyword.get(opts, :fetch_fun, &fetch_cdi_series/2)
    fetch_opts = Keyword.get(opts, :fetch_opts, [])

    with {:ok, payload} <- fetch_fun.(params, fetch_opts) do
      upsert_rate_series(payload, :cdi)
    end
  end

  @doc """
  Imports (or refreshes) the SELIC historical series using the BCB open data API.
  """
  def import_selic_history(params \\ %{}, opts \\ []) do
    fetch_fun = Keyword.get(opts, :fetch_fun, &fetch_selic_series/2)
    fetch_opts = Keyword.get(opts, :fetch_opts, [])

    with {:ok, payload} <- fetch_fun.(params, fetch_opts) do
      upsert_rate_series(payload, :selic)
    end
  end

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
    |> order_by([s, a], asc: s.issuer, asc: s.code)
    |> Repo.all()
    |> Enum.map(&ensure_expected_return/1)
    |> Enum.map(&ensure_rating_hub/1)
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
    |> order_by([s, a], asc: s.duration)
    |> Repo.all()
  end

  defp upsert_rate_series(payload, type) when is_list(payload) do
    Enum.reduce_while(payload, {:ok, %{processed: 0}}, fn row, {:ok, acc} ->
      with {:ok, attrs} <- normalize_rate_entry(row, type),
           {:ok, _record} <- persist_rate_entry(attrs, type) do
        {:cont, {:ok, %{processed: acc.processed + 1}}}
      else
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp upsert_rate_series(_payload, _type), do: {:error, :invalid_payload}

  defp normalize_rate_entry(%{"data" => date, "valor" => value}, :cdi) do
    with {:ok, reference_date} <- parse_bcb_date(date),
         {:ok, decimal} <- parse_bcb_decimal(value) do
      {:ok, %{reference_date: reference_date, cdi_value: decimal}}
    end
  end

  defp normalize_rate_entry(%{"data" => date, "valor" => value}, :selic) do
    with {:ok, reference_date} <- parse_bcb_date(date),
         {:ok, decimal} <- parse_bcb_decimal(value) do
      {:ok, %{reference_date: reference_date, selic_value: decimal}}
    end
  end

  defp normalize_rate_entry(_row, _type), do: {:error, :invalid_entry}

  defp persist_rate_entry(attrs, :cdi) do
    %CDIHistory{}
    |> cdi_history_changeset(attrs)
    |> Repo.insert(
      on_conflict: [
        set: [cdi_value: attrs.cdi_value, updated_at: DateTime.utc_now()]
      ],
      conflict_target: :reference_date
    )
  end

  defp persist_rate_entry(attrs, :selic) do
    %SelicHistory{}
    |> selic_history_changeset(attrs)
    |> Repo.insert(
      on_conflict: [
        set: [selic_value: attrs.selic_value, updated_at: DateTime.utc_now()]
      ],
      conflict_target: :reference_date
    )
  end

  defp parse_bcb_date(date_string) when is_binary(date_string) do
    with [day, month, year] <- String.split(date_string, "/"),
         {:ok, parsed} <-
           Date.from_iso8601(
             "#{year}-#{String.pad_leading(month, 2, "0")}-#{String.pad_leading(day, 2, "0")}"
           ) do
      {:ok, parsed}
    else
      _ -> {:error, :invalid_date}
    end
  end

  defp parse_bcb_date(_), do: {:error, :invalid_date}

  defp parse_bcb_decimal(value) when is_binary(value) do
    sanitized =
      if String.contains?(value, ",") do
        value
        |> String.replace(".", "")
        |> String.replace(",", ".")
      else
        value
      end

    case Decimal.new(sanitized) do
      %Decimal{} = decimal -> {:ok, decimal}
    end
  rescue
    _ -> {:error, :invalid_decimal}
  end

  defp parse_bcb_decimal(value) when is_number(value), do: {:ok, Decimal.new(value)}
  defp parse_bcb_decimal(_), do: {:error, :invalid_decimal}

  defp apply_analysis_filters(query, filters) do
    query
    |> filter_by_security_type(filters)
    |> filter_by_benchmark_index(filters)
    |> filter_by_grade(filters)
    |> filter_by_recommendation(filters)
    |> filter_by_issuer(filters)
    |> filter_by_credit_risk(filters)
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

  defp filter_by_recommendation(query, %{recommendation: recommendation})
       when not is_nil(recommendation) do
    where(query, [s, a], a.recommendation == ^recommendation)
  end

  defp filter_by_recommendation(query, _), do: query

  defp filter_by_issuer(query, %{issuers: issuers})
       when is_list(issuers) and length(issuers) > 0 do
    where(query, [s], s.issuer in ^issuers)
  end

  defp filter_by_issuer(query, %{issuer: issuer}) when not is_nil(issuer) and issuer != "" do
    where(query, [s], s.issuer == ^issuer)
  end

  defp filter_by_issuer(query, _), do: query

  defp filter_by_credit_risk(query, %{credit_risks: credit_risks})
       when is_list(credit_risks) and length(credit_risks) > 0 do
    where(query, [s], s.credit_risk in ^credit_risks)
  end

  defp filter_by_credit_risk(query, %{credit_risk: credit_risk})
       when not is_nil(credit_risk) and credit_risk != "" do
    where(query, [s], s.credit_risk == ^credit_risk)
  end

  defp filter_by_credit_risk(query, _), do: query

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
  Returns a list of unique credit risks (originadores) from all securities.
  """
  def list_unique_credit_risks do
    Security
    |> select([s], s.credit_risk)
    |> distinct(true)
    |> where([s], not is_nil(s.credit_risk))
    |> order_by([s], asc: s.credit_risk)
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
      expected_ratio =
        security_id
        |> load_security()
        |> expected_ratio_from_security()

      if expected_ratio do
        avg =
          Decimal.new(issuer_quality)
          |> Decimal.add(Decimal.new(capital_structure))
          |> Decimal.add(Decimal.new(solvency_ratio))
          |> Decimal.add(Decimal.new(credit_spread))
          |> Decimal.div(Decimal.new(4))

        rating_hub =
          expected_ratio
          |> Decimal.mult(avg)
          |> Decimal.mult(Decimal.new(10))

        Ecto.Changeset.put_change(changeset, :rating_hub, rating_hub)
      else
        changeset
      end
    else
      changeset
    end
  end

  defp expected_ratio_from_security(%Security{} = security) do
    decimal_percentage(security.expected_return) ||
      decimal_percentage(expected_return_for(security)) ||
      decimal_percentage(security.coupon_rate)
  end

  defp expected_ratio_from_security(_), do: nil

  defp ensure_rating_hub(security_map) do
    Map.update(security_map, :rating_hub, nil, fn existing ->
      existing || compute_rating_hub_from_map(security_map)
    end)
  end

  defp compute_rating_hub_from_map(%{
         issuer_quality: issuer_quality,
         capital_structure: capital_structure,
         solvency_ratio: solvency_ratio,
         credit_spread: credit_spread
       } = map)
       when not is_nil(issuer_quality) and not is_nil(capital_structure) and
              not is_nil(solvency_ratio) and not is_nil(credit_spread) do
    expected_ratio =
      decimal_percentage(map.expected_return) ||
        decimal_percentage(expected_return_for(map)) ||
        decimal_percentage(map.coupon_rate)

    with %Decimal{} = ratio <- expected_ratio do
      avg =
        [issuer_quality, capital_structure, solvency_ratio, credit_spread]
        |> Enum.reduce(Decimal.new(0), fn val, acc -> Decimal.add(acc, Decimal.new(val)) end)
        |> Decimal.div(Decimal.new(4))

      ratio
      |> Decimal.mult(avg)
      |> Decimal.mult(Decimal.new(10))
    end
  end

  defp compute_rating_hub_from_map(_), do: nil
end
