#!/usr/bin/env elixir

# Test if the module and function exist
IO.puts("Testing CreditRadar.Ingestions.Tasks.IngestCriCra module...")

# Load the application
Mix.install([])
Application.load(:credit_radar)

# Check if module exists
IO.inspect(Code.ensure_loaded(CreditRadar.Ingestions.Tasks.IngestCriCra), label: "Module loaded?")

# Check exported functions
if Code.ensure_loaded?(CreditRadar.Ingestions.Tasks.IngestCriCra) do
  IO.inspect(CreditRadar.Ingestions.Tasks.IngestCriCra.__info__(:functions),
    label: "Exported functions"
  )

  # Try to check function_exported?
  IO.inspect(function_exported?(CreditRadar.Ingestions.Tasks.IngestCriCra, :run, 0),
    label: "run/0 exported?"
  )

  IO.inspect(function_exported?(CreditRadar.Ingestions.Tasks.IngestCriCra, :run, 1),
    label: "run/1 exported?"
  )
else
  IO.puts("ERROR: Module not loaded!")
end
