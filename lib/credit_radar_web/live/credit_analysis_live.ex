defmodule CreditRadarWeb.Live.CreditAnalysisLive do
  use CreditRadarWeb, :live_view

  alias CreditRadar.FixedIncome

  @impl true
  def mount(_params, _session, socket) do
    socket =
      socket
      |> assign(:page_title, "Análise de Crédito - Hub do Investidor")
      |> assign(:filters, %{})
      |> assign(:securities, [])
      |> assign(:selected_securities, MapSet.new())
      |> assign(:issuers, FixedIncome.list_unique_issuers())
      |> assign(:credit_risks, FixedIncome.list_unique_credit_risks())
      |> assign(:benchmark_indexes, FixedIncome.list_unique_benchmark_indexes())
      |> assign(:sort_by, :code)
      |> assign(:sort_order, :asc)
      |> load_securities()

    {:ok, socket}
  end

  @impl true
  def handle_params(params, _url, socket) do
    filters = parse_filters(params)

    socket =
      socket
      |> assign(:filters, filters)
      |> load_securities()

    {:noreply, socket}
  end

  @impl true
  def handle_event("apply_filters", params, socket) do
    filters = parse_filters(params)

    socket =
      socket
      |> assign(:filters, filters)
      |> load_securities()

    {:noreply, socket}
  end

  @impl true
  def handle_event("clear_filters", _params, socket) do
    {:noreply, push_patch(socket, to: ~p"/analise-credito")}
  end

  @impl true
  def handle_event("toggle_security", %{"id" => id}, socket) do
    security_id = String.to_integer(id)
    selected = socket.assigns.selected_securities

    new_selected =
      if MapSet.member?(selected, security_id) do
        MapSet.delete(selected, security_id)
      else
        MapSet.put(selected, security_id)
      end

    {:noreply, assign(socket, :selected_securities, new_selected)}
  end

  @impl true
  def handle_event("select_all", _params, socket) do
    all_ids = socket.assigns.securities |> Enum.map(& &1.id) |> MapSet.new()
    {:noreply, assign(socket, :selected_securities, all_ids)}
  end

  @impl true
  def handle_event("deselect_all", _params, socket) do
    {:noreply, assign(socket, :selected_securities, MapSet.new())}
  end

  @impl true
  def handle_event("sort", %{"field" => field}, socket) do
    field_atom = String.to_existing_atom(field)
    current_sort = socket.assigns.sort_by
    current_order = socket.assigns.sort_order

    # Toggle order if clicking same field, otherwise default to asc
    new_order =
      if field_atom == current_sort do
        if current_order == :asc, do: :desc, else: :asc
      else
        :asc
      end

    socket =
      socket
      |> assign(:sort_by, field_atom)
      |> assign(:sort_order, new_order)
      |> load_securities()

    {:noreply, socket}
  end

  defp load_securities(socket) do
    filters = socket.assigns.filters

    securities =
      FixedIncome.list_securities_with_assessments(filters)
      |> sort_securities(socket.assigns.sort_by, socket.assigns.sort_order)

    assign(socket, :securities, securities)
  end

  defp sort_securities(securities, field, order) do
    sorted = Enum.sort_by(securities, &sort_value(&1, field), order)
    if order == :desc, do: sorted, else: sorted
  end

  # Get sortable value from security map
  defp sort_value(security, :code), do: security.code || ""
  defp sort_value(security, :credit_risk), do: security.credit_risk || ""
  defp sort_value(security, :issuer), do: security.issuer || ""
  defp sort_value(security, :security_type), do: Atom.to_string(security.security_type)
  defp sort_value(security, :benchmark_index), do: security.benchmark_index || ""
  defp sort_value(security, :duration), do: security.duration || 0
  defp sort_value(security, :coupon_rate), do: decimal_to_float(security.coupon_rate)
  defp sort_value(security, :correction_rate), do: decimal_to_float(security.correction_rate)
  defp sort_value(security, :expected_return), do: decimal_to_float(security.expected_return)
  defp sort_value(security, :issuer_quality), do: security.issuer_quality || 0
  defp sort_value(security, :capital_structure), do: security.capital_structure || 0

  defp sort_value(security, :grade),
    do: (security.grade && Atom.to_string(security.grade)) || "zzz"

  defp sort_value(security, :rating_hub), do: decimal_to_float(security.rating_hub)
  defp sort_value(security, :solvency_ratio), do: security.solvency_ratio || 0
  defp sort_value(security, :credit_spread), do: security.credit_spread || 0

  defp sort_value(security, :recommendation),
    do: (security.recommendation && Atom.to_string(security.recommendation)) || "zzz"

  defp sort_value(_, _), do: 0

  defp format_benchmark(security) do
    case security.benchmark_index do
      "di_multiple" -> "CDI %"
      "di_plus" -> "CDI +"
      "ipca" -> "IPCA +"
      other when is_binary(other) -> String.upcase(other)
      _ -> "-"
    end
  end

  defp decimal_to_float(nil), do: 0.0
  defp decimal_to_float(%Decimal{} = d), do: Decimal.to_float(d)
  defp decimal_to_float(val) when is_number(val), do: val / 1.0
  defp decimal_to_float(_), do: 0.0

  defp parse_filters(params) do
    %{}
    |> put_string_filter(:security_type, params["security_type"])
    |> put_string_filter(:benchmark_index, params["benchmark_index"])
    |> put_atom_filter(:grade, params["grade"])
    |> put_atom_filter(:recommendation, params["recommendation"])
    |> put_credit_risks_filter(params["credit_risks"])
  end

  defp put_string_filter(filters, _key, nil), do: filters
  defp put_string_filter(filters, _key, ""), do: filters
  defp put_string_filter(filters, key, value), do: Map.put(filters, key, value)

  defp put_atom_filter(filters, _key, nil), do: filters
  defp put_atom_filter(filters, _key, ""), do: filters

  defp put_atom_filter(filters, key, value) when is_binary(value) do
    Map.put(filters, key, String.to_existing_atom(value))
  rescue
    ArgumentError -> filters
  end

  defp put_atom_filter(filters, _key, _value), do: filters

  defp put_integer_filter(filters, _key, nil), do: filters
  defp put_integer_filter(filters, _key, ""), do: filters

  defp put_integer_filter(filters, key, value) do
    case parse_integer(value) do
      nil -> filters
      parsed_value -> Map.put(filters, key, parsed_value)
    end
  end

  defp put_credit_risks_filter(filters, nil), do: filters
  defp put_credit_risks_filter(filters, []), do: filters

  defp put_credit_risks_filter(filters, credit_risks) when is_list(credit_risks) do
    cleaned = Enum.reject(credit_risks, &(&1 == "" or is_nil(&1)))

    if Enum.empty?(cleaned) do
      filters
    else
      Map.put(filters, :credit_risks, cleaned)
    end
  end

  defp put_credit_risks_filter(filters, _), do: filters

  defp parse_integer(value) when is_binary(value) do
    case Integer.parse(value) do
      {int, _} -> int
      :error -> nil
    end
  end

  defp parse_integer(_), do: nil

  # Helper to convert days to years (rounded to 2 decimals)
  defp days_to_years(nil), do: nil
  defp days_to_years(days) when is_integer(days), do: Float.round(days / 365.0, 2)
  defp days_to_years(_), do: nil

  defp chart_data(securities, selected_ids) do
    # Filter to selected or all if none selected
    data_securities =
      if MapSet.size(selected_ids) > 0 do
        Enum.filter(securities, &MapSet.member?(selected_ids, &1.id))
      else
        securities
      end

    # Only include securities with rating_hub for the chart
    assessed = Enum.filter(data_securities, & &1.rating_hub)

    series =
      Enum.map(assessed, fn sec ->
        duration_years = days_to_years(sec.duration)
        rating_hub_value = sec.rating_hub && Decimal.to_float(sec.rating_hub)

        %{
          value: [
            duration_years,
            rating_hub_value,
            sec.credit_risk,
            format_benchmark(sec),
            sec.code
          ],
          code: sec.code,
          credit_risk: sec.credit_risk,
          duration: sec.duration,
          duration_years: duration_years,
          rating_hub: rating_hub_value,
          grade: sec.grade && Atom.to_string(sec.grade),
          benchmark_display: format_benchmark(sec),
          security_type: Atom.to_string(sec.security_type),
          couponRate: sec.coupon_rate && Decimal.to_float(sec.coupon_rate)
        }
      end)

    %{
      title: "Análise: Duration (anos) vs Rating Hub",
      series: series,
      legend: ["CRI", "CRA", "Debêntures", "Debêntures Plus"]
    }
    |> Jason.encode!()
  end

  defp format_security_type(:cri), do: "CRI"
  defp format_security_type(:cra), do: "CRA"
  defp format_security_type(:debenture), do: "Debênture"
  defp format_security_type(:debenture_plus), do: "Debênture Plus"
  defp format_security_type(_), do: "-"

  defp format_decimal(nil), do: "-"

  defp format_decimal(%Decimal{} = decimal) do
    decimal
    |> Decimal.round(2)
    |> Decimal.to_string()
  end

  defp format_decimal(value) when is_number(value) do
    :erlang.float_to_binary(value / 1, decimals: 2)
  end

  defp format_decimal(_), do: "-"

  defp format_date(nil), do: "-"
  defp format_date(%Date{} = date), do: Calendar.strftime(date, "%d/%m/%Y")
  defp format_date(_), do: "-"

  # Helper to render sort icon
  defp sort_icon(assigns, field) do
    current_field = assigns[:sort_by]
    current_order = assigns[:sort_order]

    cond do
      current_field == field && current_order == :asc ->
        ~H"""
        <svg
          class="w-4 h-4 inline-block ml-1 text-teal-400"
          fill="none"
          stroke="currentColor"
          viewBox="0 0 24 24"
        >
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 15l7-7 7 7" />
        </svg>
        """

      current_field == field && current_order == :desc ->
        ~H"""
        <svg
          class="w-4 h-4 inline-block ml-1 text-teal-400"
          fill="none"
          stroke="currentColor"
          viewBox="0 0 24 24"
        >
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" />
        </svg>
        """

      true ->
        ~H"""
        <svg
          class="w-4 h-4 inline-block ml-1 text-gray-600 opacity-0 group-hover:opacity-100"
          fill="none"
          stroke="currentColor"
          viewBox="0 0 24 24"
        >
          <path
            stroke-linecap="round"
            stroke-linejoin="round"
            stroke-width="2"
            d="M8 9l4-4 4 4m0 6l-4 4-4-4"
          />
        </svg>
        """
    end
  end

  # Helper to get color classes based on rating value (1-5)
  # 1 = vermelho, 2 = laranja, 3 = amarelo, 4 = verde claro, 5 = verde
  defp rating_color_class(1), do: "bg-red-500/20 text-red-400 ring-1 ring-red-500/30"
  defp rating_color_class(2), do: "bg-orange-500/20 text-orange-400 ring-1 ring-orange-500/30"
  defp rating_color_class(3), do: "bg-yellow-500/20 text-yellow-400 ring-1 ring-yellow-500/30"
  defp rating_color_class(4), do: "bg-lime-500/20 text-lime-400 ring-1 ring-lime-500/30"
  defp rating_color_class(5), do: "bg-green-500/20 text-green-400 ring-1 ring-green-500/30"
  defp rating_color_class(_), do: "bg-gray-700 text-gray-400"
end
