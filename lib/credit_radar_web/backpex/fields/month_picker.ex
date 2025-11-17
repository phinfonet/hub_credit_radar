defmodule CreditRadarWeb.Backpex.Fields.MonthPicker do
  @moduledoc """
  Custom Backpex field that uses select inputs for month/year and
  automatically stores the first day of the selected month.
  """
  use Backpex.Field

  alias Backpex.HTML.Layout
  alias Backpex.LiveResource

  @impl Backpex.Field
  def render_value(assigns) do
    display_value =
      assigns.value
      |> month_value()
      |> format_display()

    assigns = assign(assigns, :value, display_value)

    ~H"""
    <p class={@live_action in [:index, :resource_action] && "truncate"}>
      {@value}
    </p>
    """
  end

  @impl Backpex.Field
  def render_form(assigns) do
    field = assigns.form[assigns.name]
    {year, month} = split_year_month(field.value)

    assigns =
      assigns
      |> assign(:field, field)
      |> assign(:selected_year, year || Integer.to_string(current_year()))
      |> assign(:selected_month, month || "01")
      |> assign(:year_range, year_range(assigns))

    ~H"""
    <div>
      <Layout.field_container>
        <:label align={Backpex.Field.align_label(@field_options, assigns, :top)}>
          <Layout.input_label for={@field} text={@field_options[:label]} />
        </:label>
        <div class="flex gap-3">
          <select
            id={"#{@field.id}_month"}
            name={"#{@field.name}[month]"}
            class="select select-bordered flex-1"
            disabled={@readonly}
          >
            <%= for month_option <- month_options() do %>
              <option value={month_option} selected={month_option == @selected_month}>
                {month_option}
              </option>
            <% end %>
          </select>

          <select
            id={"#{@field.id}_year"}
            name={"#{@field.name}[year]"}
            class="select select-bordered flex-1"
            disabled={@readonly}
          >
            <%= for year <- @year_range do %>
              <option value={year} selected={Integer.to_string(year) == @selected_year}>
                {year}
              </option>
            <% end %>
          </select>
        </div>
        <input type="hidden" name={"#{@field.name}[day]"} value="01" />
      </Layout.field_container>
    </div>
    """
  end

  @impl Backpex.Field
  def render_index_form(assigns) do
    value = month_value(assigns.value)
    form = to_form(%{"value" => value}, as: :index_form)

    assigns =
      assigns
      |> assign_new(:form, fn -> form end)
      |> assign(:valid, Map.get(assigns, :valid, true))

    ~H"""
    <div>
      <.form for={@form} phx-change="update-field" phx-submit="update-field" phx-target={@myself}>
        <input
          type="month"
          id={"index-form-input-#{@name}-#{LiveResource.primary_value(@item, @live_resource)}"}
          name="index_form[value]"
          value={@form[:value].value}
          class={[
            "input input-sm",
            @valid && "not-hover:input-ghost",
            !@valid && "input-error bg-error/10"
          ]}
          phx-debounce="100"
          readonly={@readonly}
        />
      </.form>
    </div>
    """
  end

  @impl Phoenix.LiveComponent
  def handle_event("update-field", %{"index_form" => %{"value" => value}}, socket) do
    Backpex.Field.handle_index_editable(socket, value, %{socket.assigns.name => value})
  end

  defp month_value(%Date{} = date), do: Calendar.strftime(date, "%Y-%m")
  defp month_value(%DateTime{} = datetime), do: Calendar.strftime(DateTime.to_date(datetime), "%Y-%m")
  defp month_value(%NaiveDateTime{} = datetime), do: Calendar.strftime(NaiveDateTime.to_date(datetime), "%Y-%m")
  defp month_value(value) when is_binary(value), do: value
  defp month_value(_), do: ""

  defp format_display(""), do: ""

  defp format_display(<<year::binary-size(4), "-", month::binary-size(2)>>) do
    "#{month}/#{String.slice(year, 2, 2)}"
  end

  defp format_display(other), do: other

  defp split_year_month(value) do
    case month_value(value) do
      <<year::binary-size(4), "-", month::binary-size(2)>> -> {year, month}
      _ -> {nil, nil}
    end
  end

  defp month_options do
    for n <- 1..12, do: n |> Integer.to_string() |> String.pad_leading(2, "0")
  end

  defp year_range(assigns) do
    case assigns.field_options[:year_range] do
      {from, to} when is_integer(from) and is_integer(to) and from <= to ->
        from..to

      fun when is_function(fun, 1) ->
        fun.(assigns)

      _ ->
        start_year = current_year() - 5
        end_year = current_year() + 5
        start_year..end_year
    end
  end

  defp current_year do
    Date.utc_today().year
  end
end

