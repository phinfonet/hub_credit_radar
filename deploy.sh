#!/bin/bash
set -e

echo "Pulling latest code..."
git pull origin main

echo "Installing Mix deps..."
mix deps.get

echo "Installing npm deps..."
cd assets && npm install && cd ..

echo "Building assets..."
mix assets.deploy

echo "Building release..."
MIX_ENV=prod mix release --overwrite

echo "Restarting service..."
sudo systemctl restart sua_app

echo "✅ Deploy done!"
