#!/bin/bash
set -e

echo "Pulling latest code..."
git pull origin main

echo "Installing deps..."
mix deps.get

echo "Building assets..."
mix assets.deploy
mix deps.clean --only dev

echo "Building release..."
MIX_ENV=prod mix release --overwrite

echo "Restarting service..."
sudo systemctl restart credit_radar

echo "✅ Deploy done!"
