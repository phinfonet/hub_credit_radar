# Script for populating the database. You can run it as:
#
#     mix run priv/repo/seeds.exs
#
# Inside the script, you can read and write to any of your
# repositories directly:
#
#     CreditRadar.Repo.insert!(%CreditRadar.SomeSchema{})
#
# We recommend using the bang functions (`insert!`, `update!`
# and so on) as they will fail if something goes wrong.

alias CreditRadar.FixedIncome

# Seed IPCA projections for future months
# Based on market expectations (valores em %)
ipca_projections = [
  # 2025
  %{reference_date: "2025-01-01", year_ipca_projection_value: "4.85"},
  %{reference_date: "2025-02-01", year_ipca_projection_value: "4.78"},
  %{reference_date: "2025-03-01", year_ipca_projection_value: "4.72"},
  %{reference_date: "2025-04-01", year_ipca_projection_value: "4.65"},
  %{reference_date: "2025-05-01", year_ipca_projection_value: "4.60"},
  %{reference_date: "2025-06-01", year_ipca_projection_value: "4.55"},
  %{reference_date: "2025-07-01", year_ipca_projection_value: "4.50"},
  %{reference_date: "2025-08-01", year_ipca_projection_value: "4.45"},
  %{reference_date: "2025-09-01", year_ipca_projection_value: "4.42"},
  %{reference_date: "2025-10-01", year_ipca_projection_value: "4.40"},
  %{reference_date: "2025-11-01", year_ipca_projection_value: "4.38"},
  %{reference_date: "2025-12-01", year_ipca_projection_value: "4.35"},
  # 2026
  %{reference_date: "2026-01-01", year_ipca_projection_value: "4.25"},
  %{reference_date: "2026-02-01", year_ipca_projection_value: "4.20"},
  %{reference_date: "2026-03-01", year_ipca_projection_value: "4.15"},
  %{reference_date: "2026-04-01", year_ipca_projection_value: "4.10"},
  %{reference_date: "2026-05-01", year_ipca_projection_value: "4.05"},
  %{reference_date: "2026-06-01", year_ipca_projection_value: "4.00"},
  %{reference_date: "2026-07-01", year_ipca_projection_value: "3.95"},
  %{reference_date: "2026-08-01", year_ipca_projection_value: "3.92"},
  %{reference_date: "2026-09-01", year_ipca_projection_value: "3.90"},
  %{reference_date: "2026-10-01", year_ipca_projection_value: "3.88"},
  %{reference_date: "2026-11-01", year_ipca_projection_value: "3.85"},
  %{reference_date: "2026-12-01", year_ipca_projection_value: "3.82"}
]

IO.puts("Seeding IPCA projections...")

Enum.each(ipca_projections, fn attrs ->
  case FixedIncome.upsert_ipca_projection(attrs) do
    {:ok, projection} ->
      IO.puts("  ✓ IPCA projection for #{attrs.reference_date}: #{attrs.year_ipca_projection_value}%")

    {:error, changeset} ->
      IO.puts("  ✗ Failed to insert IPCA projection for #{attrs.reference_date}")
      IO.inspect(changeset.errors)
  end
end)

IO.puts("Seeding complete!")
