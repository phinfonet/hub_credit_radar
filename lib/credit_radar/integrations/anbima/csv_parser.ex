defmodule CreditRadar.Integrations.Anbima.CsvParser do
  @moduledoc """
  Parser for Anbima CSV exports.
  Handles downloading and parsing CSV data from Anbima's external download endpoints.
  """

  NimbleCSV.define(AnbimaCSV, separator: ";", escape: "\"")

  @doc """
  Fetches and parses CRI/CRA rates CSV from Anbima.

  URL: https://www.anbima.com.br/pt_br/anbima/TaxasCriCraExport/downloadExterno
  """
  def fetch_cri_cra_rates do
    url = "https://www.anbima.com.br/pt_br/anbima/TaxasCriCraExport/downloadExterno"

    case Req.get(url) do
      {:ok, %{status: 200, body: body}} ->
        {:ok, parse_csv(body)}

      {:ok, %{status: status}} ->
        {:error, {:unexpected_status, status}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Parses CSV content into a list of maps.
  """
  def parse_csv(csv_content) when is_binary(csv_content) do
    csv_content
    |> AnbimaCSV.parse_string(skip_headers: false)
    |> case do
      [headers | rows] ->
        # Normalizar headers (remover BOM, trim, lowercase)
        normalized_headers =
          headers
          |> Enum.map(&normalize_header/1)

        # Mapear cada row para um map
        Enum.map(rows, fn row ->
          normalized_headers
          |> Enum.zip(row)
          |> Map.new()
        end)

      [] ->
        []
    end
  end

  defp normalize_header(header) do
    header
    |> String.trim()
    |> String.replace("\uFEFF", "")  # Remove BOM
    |> String.downcase()
  end
end
