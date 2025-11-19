#!/bin/bash
set -e

echo "🧹 LIMPEZA COMPLETA E REBUILD..."

# Parar servidor
echo "⏹️  Parando servidor..."
sudo systemctl stop credit_radar 2>/dev/null || true
pkill -9 -f "beam.*credit_radar" || true
sleep 2

# Limpar TUDO
echo "🗑️  Removendo build antigo..."
rm -rf _build/prod
rm -rf deps
rm -rf priv/static/assets

# Atualizar código
echo "📥 Atualizando código do git..."
git fetch origin
git checkout claude/fix-assets-ipca-record-011LLFCLmscqXpws6P8FJFh2
git reset --hard origin/claude/fix-assets-ipca-record-011LLFCLmscqXpws6P8FJFh2

# Instalar deps
echo "📦 Instalando dependências..."
mix deps.get --only prod

# Compilar TUDO do zero
echo "🔨 Compilando do zero..."
MIX_ENV=prod mix compile --force

# Assets
echo "🎨 Compilando assets..."
cd assets && npm install && cd ..
MIX_ENV=prod mix assets.deploy

# Release
echo "📦 Criando release..."
MIX_ENV=prod mix release --overwrite

# Migrations
echo "🗄️  Rodando migrations..."
_build/prod/rel/credit_radar/bin/migrate

# Iniciar
echo "🚀 Iniciando servidor..."
sudo systemctl start credit_radar

sleep 3

# Verificar
if systemctl is-active --quiet credit_radar; then
  echo "✅ SUCESSO! Servidor rodando."
  echo ""
  echo "📊 Verificar logs:"
  echo "sudo journalctl -u credit_radar -f"
  echo ""
  echo "🌐 Testar acesso SEM LOGIN:"
  echo "curl -I http://localhost:4000/analise-credito"
else
  echo "❌ ERRO! Servidor não iniciou."
  echo "sudo journalctl -u credit_radar -n 50"
  exit 1
fi
