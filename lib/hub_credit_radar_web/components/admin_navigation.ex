defmodule HubCreditRadarWeb.Components.AdminNavigation do
  @moduledoc """
  Helper module for admin sidebar navigation items.
  Centralizes navigation structure and styling.
  """

  @doc """
  Returns the list of navigation items for the admin sidebar.
  Each item contains all necessary information for rendering.
  """
  def nav_items do
    [
      %{
        id: "sidebar-securities-link",
        path: "/admin/securities",
        icon: "hero-banknotes",
        title: "Riscos de Crédito",
        description: "Atualizado diariamente às 20h",
        active_bg: "bg-[#11394A]",
        active_shadow: "shadow-[#0ADC7D]/20",
        active_ring: "ring-[#0ADC7D]/60",
        gradient_from: "from-[#0ADC7D]",
        gradient_via: nil,
        gradient_to: "to-[#6E82FA]",
        icon_text_color: "text-white"
      },
      %{
        id: "sidebar-cdi-history-link",
        path: "/admin/cdi_history",
        icon: "hero-chart-bar-square",
        title: "Histórico CDI",
        description: "Série SGS 4391 sincronizada mensalmente",
        active_bg: "bg-[#162E4E]",
        active_shadow: "shadow-[#0ADC7D]/20",
        active_ring: "ring-[#0ADC7D]/60",
        gradient_from: "from-[#0ADC7D]",
        gradient_via: "via-[#3CD2AE]",
        gradient_to: "to-[#6E82FA]",
        icon_text_color: "text-[#041C18]"
      },
      %{
        id: "sidebar-selic-history-link",
        path: "/admin/selic_history",
        icon: "hero-sparkles",
        title: "Histórico SELIC",
        description: "Acompanhamento SGS 4390",
        active_bg: "bg-[#1A2F56]",
        active_shadow: "shadow-[#4BA5FF]/25",
        active_ring: "ring-[#4BA5FF]/60",
        gradient_from: "from-[#4BA5FF]",
        gradient_via: "via-[#6E82FA]",
        gradient_to: "to-[#0ADC7D]",
        icon_text_color: "text-white"
      },
      %{
        id: "sidebar-cdi-projections-link",
        path: "/admin/cdi_projections",
        icon: "hero-chart-bar",
        title: "Projeções CDI",
        description: "Pearson + statistics mensal",
        active_bg: "bg-[#261B4F]",
        active_shadow: "shadow-[#C084FC]/25",
        active_ring: "ring-[#C084FC]/60",
        gradient_from: "from-[#C084FC]",
        gradient_via: "via-[#8B5CF6]",
        gradient_to: "to-[#0ADC7D]",
        icon_text_color: "text-white"
      },
      %{
        id: "sidebar-ipca-projections-link",
        path: "/admin/ipca_projections",
        icon: "hero-arrow-trending-up",
        title: "Projeções IPCA",
        description: "Tracking de inflação esperada",
        active_bg: "bg-[#30213D]",
        active_shadow: "shadow-[#F472B6]/25",
        active_ring: "ring-[#F472B6]/60",
        gradient_from: "from-[#F472B6]",
        gradient_via: "via-[#DB2777]",
        gradient_to: "to-[#0ADC7D]",
        icon_text_color: "text-white"
      },
      %{
        id: "sidebar-igp-m-projections-link",
        path: "/admin/igp_m_projections",
        icon: "hero-presentation-chart-line",
        title: "Projeções IGP-M",
        description: "Índice de preços FGV",
        active_bg: "bg-[#3D2817]",
        active_shadow: "shadow-[#FB923C]/25",
        active_ring: "ring-[#FB923C]/60",
        gradient_from: "from-[#FB923C]",
        gradient_via: "via-[#F97316]",
        gradient_to: "to-[#0ADC7D]",
        icon_text_color: "text-white"
      },
      %{
        id: "sidebar-assessments-link",
        path: "/admin/assessments",
        icon: "hero-clipboard-document-check",
        title: "Recomendações",
        description: "Cadastre as recomendações do Hub pra cada risco de crédito",
        active_bg: "bg-[#1A2F56]",
        active_shadow: "shadow-[#6E82FA]/25",
        active_ring: "ring-[#6E82FA]/60",
        gradient_from: "from-[#6E82FA]",
        gradient_via: "via-[#5A7BFF]",
        gradient_to: "to-[#0ADC7D]",
        icon_text_color: "text-white"
      },
      %{
        id: "sidebar-filter-rules-link",
        path: "/admin/filter_rules",
        icon: "hero-adjustments-horizontal",
        title: "Filtrar Ativos",
        description: "Mantenha ativos não cobertos fora das ingestões",
        active_bg: "bg-[#164433]",
        active_shadow: "shadow-[#0ADC7D]/25",
        active_ring: "ring-[#0ADC7D]/60",
        gradient_from: "from-[#0ADC7D]",
        gradient_via: "via-[#44F3AA]",
        gradient_to: "to-[#6E82FA]",
        icon_text_color: "text-[#05110A]"
      },
      %{
        id: "sidebar-executions-link",
        path: "/admin/executions",
        icon: "hero-arrow-path",
        title: "Executions",
        description: "Monitore o histórico de ingestões",
        active_bg: "bg-[#12324B]",
        active_shadow: "shadow-[#29B6F6]/25",
        active_ring: "ring-[#29B6F6]/60",
        gradient_from: "from-[#29B6F6]",
        gradient_via: "via-[#4CC2FF]",
        gradient_to: "to-[#6E82FA]",
        icon_text_color: "text-white"
      }
    ]
  end

  @doc """
  Returns the CSS classes for a navigation link based on active state.
  """
  def link_classes(is_active, item) do
    base_classes = "group flex items-center gap-3 rounded-xl px-3 py-2 text-sm transition"

    if is_active do
      "#{base_classes} #{item.active_bg} text-white shadow-lg #{item.active_shadow} ring-1 #{item.active_ring}"
    else
      "#{base_classes} text-white/60 hover:bg-white/5 hover:text-white"
    end
  end

  @doc """
  Returns the CSS classes for a navigation icon based on active state.
  """
  def icon_classes(is_active, item) do
    base_classes = "flex size-9 items-center justify-center rounded-lg transition"

    if is_active do
      gradient_classes = [
        "bg-gradient-to-br",
        item.gradient_from,
        item.gradient_via,
        item.gradient_to,
        item.icon_text_color
      ]
      |> Enum.reject(&is_nil/1)
      |> Enum.join(" ")

      "#{base_classes} #{gradient_classes}"
    else
      "#{base_classes} bg-white/10 text-white/50 group-hover:text-white"
    end
  end

  @doc """
  Returns just the gradient classes for an icon.
  """
  def icon_gradient_classes(item) do
    [
      "bg-gradient-to-br",
      item.gradient_from,
      item.gradient_via,
      item.gradient_to,
      item.icon_text_color
    ]
    |> Enum.reject(&is_nil/1)
    |> Enum.join(" ")
  end
end
