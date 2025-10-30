defmodule CreditRadar.Integrations.Anbima.Client do
  @moduledoc """
  Minimal wrapper around `Req` for ANBIMA secondary market endpoints.
  """

  alias CreditRadar.Integrations.Anbima.Auth

  # @base_url "https://api.anbima.com.br"
  @base_url "https://api-sandbox.anbima.com.br/feed/precos-indices"

  @doc """
  Calls `/v1/debentures/mercado-secundario`.
  """
  def fetch_debentures_secondary(params \\ %{}) do
    client()
    |> Req.get(url: "/v1/debentures/mercado-secundario")
  end


  @doc """
  Calls `/v1/debentures/mercado-secundario`.
  """
  def fetch_debentures_plus_secondary(params \\ %{}) do
    client()
    |> Req.get(url: "/v1/debentures/mercado-secundario")
  end

  @doc """
  Calls `/precos-indices/v1/cri-cra/mercado-secundario`.
  """
  def fetch_cri_cra_secondary(params \\ %{}) do
    client()
    |> Req.get(url: "/v1/cri-cra/mercado-secundario")
  end


  defp client() do
    client_id = Keyword.fetch!(credentials(), :client_id)
    token = Auth.fetch_token!()

    Req.new(
      base_url: @base_url,
      headers: [
        {"client_id", client_id},
        {"access_token", token},
        {"accept", "application/json"}
      ]
    )
  end


  defp credentials, do: Application.fetch_env!(:credit_radar, :anbima_credentials)
end
