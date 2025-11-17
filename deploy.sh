#!/bin/bash
set -e

echo "Pulling..."
git pull origin main

echo "Installing deps..."
mix deps.get  

echo "Installing npm..."
cd assets && npm install && cd ..

echo "Building assets..."
MIX_ENV=prod mix assets.deploy

echo "Building release..."
MIX_ENV=prod mix release --overwrite

echo "Running migrations..."
_build/prod/rel/credit_radar/bin/migrate

echo "Restarting..."
sudo systemctl restart credit_radar

echo "✅ Done!"
