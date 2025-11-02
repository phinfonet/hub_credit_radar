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
      |> assign(:benchmark_indexes, FixedIncome.list_unique_benchmark_indexes())
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
      |> push_patch(to: ~p"/analise-credito?#{filters}")

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

  defp load_securities(socket) do
    filters = socket.assigns.filters
    securities = FixedIncome.list_securities_with_assessments(filters)

    assign(socket, :securities, securities)
  end

  defp parse_filters(params) do
    %{}
    |> put_string_filter(:security_type, params["security_type"])
    |> put_string_filter(:benchmark_index, params["benchmark_index"])
    |> put_atom_filter(:grade, params["grade"])
    |> put_atom_filter(:recommendation, params["recommendation"])
    |> put_issuers_filter(params["issuers"])
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

  defp put_issuers_filter(filters, nil), do: filters
  defp put_issuers_filter(filters, []), do: filters

  defp put_issuers_filter(filters, issuers) when is_list(issuers) do
    cleaned = Enum.reject(issuers, &(&1 == "" or is_nil(&1)))

    if Enum.empty?(cleaned) do
      filters
    else
      Map.put(filters, :issuers, cleaned)
    end
  end

  defp put_issuers_filter(filters, _), do: filters

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
          value: [duration_years, rating_hub_value],
          code: sec.code,
          issuer: sec.issuer,
          duration: sec.duration,
          duration_years: duration_years,
          rating_hub: rating_hub_value,
          grade: sec.grade && Atom.to_string(sec.grade),
          benchmark_index: sec.benchmark_index,
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
end
