defmodule CreditRadar.Integrations.BCB.ClientTest do
  use ExUnit.Case, async: true

  alias CreditRadar.Integrations.BCB.Client
  alias Req.Response

  describe "fetch_cdi_series/2" do
    test "returns the decoded payload on success and normalizes params" do
      request_fun = fn _client, opts ->
        params = Keyword.fetch!(opts, :params)
        url = Keyword.fetch!(opts, :url)

        assert url == "/dados/serie/bcdata.sgs.4391/dados"
        assert params["formato"] == "json"
        assert params["dataInicial"] == "01/01/2024"
        assert params["dataFinal"] == "05/01/2024"

        {:ok, %Response{status: 200, body: [%{"data" => "01/01/2024", "valor" => "10.15"}]}}
      end

      assert {:ok, [%{"data" => "01/01/2024", "valor" => "10.15"}]} =
               Client.fetch_cdi_series(
                 %{"dataFinal" => "05/01/2024", dataInicial: "01/01/2024"},
                 request_fun: request_fun
               )
    end

    test "returns error tuple when the API answers with an unexpected status" do
      request_fun = fn _client, _opts ->
        {:ok, %Response{status: 503, body: "maintenance"}}
      end

      assert {:error, {:unexpected_status, 503, "maintenance"}} =
               Client.fetch_cdi_series(%{}, request_fun: request_fun)
    end

    test "propagates lower level request errors" do
      request_fun = fn _client, _opts -> {:error, :timeout} end

      assert {:error, :timeout} = Client.fetch_cdi_series(%{}, request_fun: request_fun)
    end

    test "raises when params are not provided as a map" do
      assert_raise ArgumentError, fn ->
        Client.fetch_cdi_series([:a, :b], [])
      end
    end
  end

  describe "fetch_selic_series/2" do
    test "requests the SELIC endpoint and returns payload" do
      request_fun = fn _client, opts ->
        params = Keyword.fetch!(opts, :params)
        url = Keyword.fetch!(opts, :url)

        assert url == "/dados/serie/bcdata.sgs.4390/dados"
        assert params["formato"] == "json"

        {:ok, %Response{status: 200, body: [%{"data" => "02/01/2024", "valor" => "10.75"}]}}
      end

      assert {:ok, [%{"data" => "02/01/2024", "valor" => "10.75"}]} =
               Client.fetch_selic_series(%{}, request_fun: request_fun)
    end

    test "raises when params are not provided as a map" do
      assert_raise ArgumentError, fn ->
        Client.fetch_selic_series([:a, :b], [])
      end
    end
  end
end
