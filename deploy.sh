#!/bin/bash
set -e

# Configuration
USE_SYSTEMD=${USE_SYSTEMD:-"auto"}  # auto, yes, no

echo "🚀 Starting deployment..."
echo "⚡ Using zero-downtime strategy: building new release while old server runs"

# Detect if systemd service exists and is active
SYSTEMD_ACTIVE=false
if systemctl is-active --quiet credit_radar 2>/dev/null; then
  SYSTEMD_ACTIVE=true
fi

# ========================================
# PHASE 1: Build new release (old server still running)
# ========================================
echo ""
echo "📦 Phase 1: Building new release (old server still running)..."

# Get dependencies and compile
echo "📦 Installing dependencies..."
mix deps.get --only prod

echo "🔨 Compiling application..."
MIX_ENV=prod mix compile

echo "🎨 Deploying assets..."
MIX_ENV=prod mix assets.deploy

echo "📦 Building release..."
MIX_ENV=prod mix release --overwrite

echo "✅ New release built successfully!"

# ========================================
# PHASE 2: Quick swap (minimal downtime)
# ========================================
echo ""
echo "⚡ Phase 2: Quick server swap (downtime starts now)..."
START_TIME=$(date +%s)

# Determine stop method
if [[ "$USE_SYSTEMD" == "auto" ]]; then
  if $SYSTEMD_ACTIVE; then
    echo "⏹️  Stopping systemd service..."
    sudo systemctl stop credit_radar
    sleep 2
  else
    echo "⏹️  Stopping current server..."
    if [ -f "_build/prod/rel/credit_radar/bin/credit_radar" ]; then
      _build/prod/rel/credit_radar/bin/credit_radar stop || echo "No running server to stop"
      sleep 2
    fi
    # Kill any beam processes (fallback)
    echo "🧹 Cleaning up any remaining processes..."
    pkill -9 -f "beam.*credit_radar" || echo "No beam processes to kill"
    sleep 1
  fi
elif [[ "$USE_SYSTEMD" == "yes" ]]; then
  echo "⏹️  Stopping systemd service..."
  sudo systemctl stop credit_radar
  sleep 2
else
  echo "⏹️  Stopping current server..."
  if [ -f "_build/prod/rel/credit_radar/bin/credit_radar" ]; then
    _build/prod/rel/credit_radar/bin/credit_radar stop || echo "No running server to stop"
    sleep 2
  fi
  echo "🧹 Cleaning up any remaining processes..."
  pkill -9 -f "beam.*credit_radar" || echo "No beam processes to kill"
  sleep 1
fi

# Run migrations
echo "🗄️  Running database migrations..."
_build/prod/rel/credit_radar/bin/migrate

# Start server
if [[ "$USE_SYSTEMD" == "yes" ]] || [[ "$USE_SYSTEMD" == "auto" && $SYSTEMD_ACTIVE == true ]]; then
  echo "🚀 Starting systemd service..."
  sudo systemctl start credit_radar
  sleep 3

  END_TIME=$(date +%s)
  DOWNTIME=$((END_TIME - START_TIME))

  if systemctl is-active --quiet credit_radar; then
    echo ""
    echo "✅ Deployment successful! Service is running."
    echo "⚡ Total downtime: ${DOWNTIME} seconds"
    echo ""
    echo "📊 Check status: sudo systemctl status credit_radar"
    echo "📊 Check logs: sudo journalctl -u credit_radar -f"
  else
    echo "❌ Service failed to start. Check logs for details."
    echo "🔍 Run: sudo journalctl -u credit_radar -n 50"
    exit 1
  fi
else
  echo "🚀 Starting server as daemon..."
  _build/prod/rel/credit_radar/bin/credit_radar daemon

  sleep 3

  END_TIME=$(date +%s)
  DOWNTIME=$((END_TIME - START_TIME))

  if _build/prod/rel/credit_radar/bin/credit_radar pid > /dev/null 2>&1; then
    PID=$(_build/prod/rel/credit_radar/bin/credit_radar pid)
    echo ""
    echo "✅ Deployment successful! Server running with PID: $PID"
    echo "⚡ Total downtime: ${DOWNTIME} seconds"
    echo ""
    echo "📊 Check logs: _build/prod/rel/credit_radar/log/"
  else
    echo "❌ Server failed to start. Check logs for details."
    exit 1
  fi
fi
