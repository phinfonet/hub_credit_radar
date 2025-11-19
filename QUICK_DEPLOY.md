# Deploy Rápido - Remover Autenticação de /analise-credito

## Status Atual

✅ O código já está correto no repositório
✅ A rota `/analise-credito` está configurada SEM autenticação
✅ Todas as mudanças já foram commitadas

## Problema

O servidor ainda está rodando a versão antiga que exige autenticação.

## Solução: Deploy no Servidor

### No servidor de produção, execute:

```bash
# 1. Entre no diretório do projeto
cd /home/user/hub_credit_radar

# 2. Atualize o código
git fetch origin
git checkout claude/fix-assets-ipca-record-011LLFCLmscqXpws6P8FJFh2
git pull origin claude/fix-assets-ipca-record-011LLFCLmscqXpws6P8FJFh2

# 3. Execute o deploy
./deploy.sh
```

### OU se preferir fazer manualmente:

```bash
# Parar servidor
sudo systemctl stop credit_radar

# Atualizar código
git pull origin claude/fix-assets-ipca-record-011LLFCLmscqXpws6P8FJFh2

# Compilar
MIX_ENV=prod mix deps.get --only prod
MIX_ENV=prod mix compile
MIX_ENV=prod mix assets.deploy
MIX_ENV=prod mix release --overwrite

# Iniciar servidor
sudo systemctl start credit_radar
```

## Verificar se funcionou

Após o deploy, acesse:
```
http://seu-servidor/analise-credito
```

Você deve conseguir acessar **sem precisar fazer login**!

## Se ainda pedir login

Limpe o cache do navegador ou tente em uma aba anônima.

## Configuração Atual no Código

```elixir
# lib/credit_radar_web/router.ex
scope "/", CreditRadarWeb do
  pipe_through :browser  # ← SEM autenticação!

  get "/", PageController, :home
  get "/login", AuthController, :new
  post "/login", AuthController, :create

  # Análise de crédito - sem autenticação (temporário)
  live "/analise-credito", Live.CreditAnalysisLive  # ← PÚBLICO
end
```

## Rotas Admin Continuam Protegidas

```elixir
scope "/admin", CreditRadarWeb do
  pipe_through [:browser, :admin_protected]  # ← COM autenticação!
  # ... todas as rotas admin continuam protegidas
end
```
