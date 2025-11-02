import * as echarts from 'echarts';
import TomSelect from 'tom-select';

/**
 * ECharts Hook for Phoenix LiveView
 *
 * Renders interactive scatter plots and other chart types.
 * Receives chart configuration via phx-hook data attributes.
 */
export const EChartsHook = {
  mounted() {
    this.chart = echarts.init(this.el);

    // Initial render
    this.handleEvent("update-chart", (data) => {
      this.updateChart(data);
    });

    // Handle window resize
    this.resizeHandler = () => {
      if (this.chart) {
        this.chart.resize();
      }
    };
    window.addEventListener('resize', this.resizeHandler);

    // Initial data from server
    if (this.el.dataset.chartData) {
      try {
        const data = JSON.parse(this.el.dataset.chartData);
        this.updateChart(data);
      } catch (e) {
        console.error('Failed to parse chart data:', e);
      }
    }
  },

  updated() {
    // Update chart when data changes
    if (this.el.dataset.chartData) {
      try {
        const data = JSON.parse(this.el.dataset.chartData);
        this.updateChart(data);
      } catch (e) {
        console.error('Failed to parse chart data:', e);
      }
    }
  },

  updateChart(data) {
    if (!this.chart) return;

    const option = {
      title: {
        text: data.title || 'Análise de Crédito',
        left: 'center',
        textStyle: {
          color: '#E5E7EB',
          fontSize: 18,
          fontWeight: 'bold'
        }
      },
      tooltip: {
        trigger: 'item',
        backgroundColor: 'rgba(17, 24, 39, 0.95)',
        borderColor: '#374151',
        borderWidth: 1,
        textStyle: {
          color: '#E5E7EB'
        },
        formatter: (params) => {
          const item = params.data;
          return `
            <div style="padding: 8px;">
              <strong style="color: #0ADC7D;">${item.issuer}</strong><br/>
              <strong>Código:</strong> ${item.code}<br/>
              <strong>Duration:</strong> ${item.duration_years} anos (${item.duration} dias)<br/>
              <strong>Rating Hub:</strong> ${item.rating_hub ? item.rating_hub.toFixed(2) : 'N/A'}<br/>
              <strong>Grade:</strong> ${item.grade ? item.grade.toUpperCase() : 'N/A'}<br/>
              <strong>Benchmark:</strong> ${item.benchmark_index || 'N/A'}<br/>
              <strong>Tipo:</strong> ${item.security_type}<br/>
            </div>
          `;
        }
      },
      grid: {
        left: '10%',
        right: '10%',
        bottom: '15%',
        top: '15%',
        containLabel: true
      },
      xAxis: {
        type: 'value',
        name: 'Duration (anos)',
        nameLocation: 'middle',
        nameGap: 40,
        nameTextStyle: {
          color: '#9CA3AF',
          fontSize: 14,
          fontWeight: 'bold'
        },
        axisLine: {
          lineStyle: {
            color: '#374151'
          }
        },
        axisLabel: {
          color: '#9CA3AF',
          formatter: (value) => `${value} anos`
        },
        splitLine: {
          lineStyle: {
            color: '#1F2937',
            type: 'dashed'
          }
        }
      },
      yAxis: {
        type: 'value',
        name: 'Rating Hub',
        nameLocation: 'middle',
        nameGap: 50,
        nameTextStyle: {
          color: '#9CA3AF',
          fontSize: 14,
          fontWeight: 'bold'
        },
        axisLine: {
          lineStyle: {
            color: '#374151'
          }
        },
        axisLabel: {
          color: '#9CA3AF',
          formatter: (value) => value.toFixed(2)
        },
        splitLine: {
          lineStyle: {
            color: '#1F2937',
            type: 'dashed'
          }
        }
      },
      series: [{
        type: 'scatter',
        symbolSize: (data) => {
          // Size based on coupon rate or default
          return Math.max(8, Math.min(20, (data.couponRate || 10) * 1.5));
        },
        data: data.series || [],
        itemStyle: {
          color: (params) => {
            // Color based on security type
            const colors = {
              'cri': '#0ADC7D',
              'cra': '#6E82FA',
              'debenture': '#29B6F6',
              'debenture_plus': '#F59E0B'
            };
            return colors[params.data.security_type] || '#9CA3AF';
          },
          opacity: 0.8,
          borderColor: '#1F2937',
          borderWidth: 2
        },
        emphasis: {
          itemStyle: {
            opacity: 1,
            borderWidth: 3,
            shadowBlur: 10,
            shadowColor: 'rgba(10, 220, 125, 0.5)'
          }
        }
      }],
      legend: {
        data: data.legend || ['CRI', 'CRA', 'Debêntures', 'Debêntures Plus'],
        top: 'bottom',
        textStyle: {
          color: '#9CA3AF'
        }
      },
      backgroundColor: 'transparent'
    };

    this.chart.setOption(option, true);
  },

  destroyed() {
    if (this.resizeHandler) {
      window.removeEventListener('resize', this.resizeHandler);
    }
    if (this.chart) {
      this.chart.dispose();
      this.chart = null;
    }
  }
};

/**
 * TomSelect Hook for Phoenix LiveView
 *
 * Enhances multi-select dropdowns with search, tagging, and better UX.
 * Works seamlessly with LiveView forms.
 */
export const TomSelectHook = {
  mounted() {
    const selectElement = this.el.querySelector('select');
    if (!selectElement) return;

    this.select = new TomSelect(selectElement, {
      plugins: ['remove_button', 'clear_button'],
      maxOptions: null,
      placeholder: 'Selecione emissores...',
      allowEmptyOption: true,
      closeAfterSelect: false,
      hidePlaceholder: false,
      render: {
        no_results: function(data, escape) {
          return '<div class="no-results">Nenhum resultado encontrado para "' + escape(data.input) + '"</div>';
        },
      }
    });

    // Preserve selected values on LiveView updates
    this.handleEvent("preserve-selection", () => {
      if (this.select) {
        this.selectedValues = this.select.getValue();
      }
    });
  },

  updated() {
    // Restore selection after LiveView update
    if (this.select && this.selectedValues) {
      this.select.setValue(this.selectedValues, true);
      this.selectedValues = null;
    }
  },

  destroyed() {
    if (this.select) {
      this.select.destroy();
      this.select = null;
    }
  }
};

export default {
  ECharts: EChartsHook,
  TomSelect: TomSelectHook
};
