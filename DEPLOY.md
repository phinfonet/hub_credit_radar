# Deploy Guide - Credit Radar

Este guia explica como fazer deploy da aplicação em produção.

## Opções de Deploy

### 1. Deploy com Systemd (Recomendado para Produção)

O systemd gerencia o processo de forma robusta com restart automático.

#### Configuração Inicial

O arquivo de serviço já existe em `/etc/systemd/system/credit_radar.service`.

Se precisar recarregar após mudanças:

```bash
# Recarregar o systemd
sudo systemctl daemon-reload

# Habilitar o serviço para iniciar no boot (se ainda não estiver)
sudo systemctl enable credit_radar
```

#### Deploy

```bash
# Executar o script de deploy
./deploy.sh
```

O script detectará automaticamente que você está usando systemd e:
1. Parará o serviço atual
2. Fará o build da release
3. Rodará as migrations
4. Iniciará o serviço novamente

#### Comandos úteis

```bash
# Ver status do serviço
sudo systemctl status credit_radar

# Ver logs em tempo real
sudo journalctl -u credit_radar -f

# Ver últimas 50 linhas de log
sudo journalctl -u credit_radar -n 50

# Parar o serviço
sudo systemctl stop credit_radar

# Iniciar o serviço
sudo systemctl start credit_radar

# Reiniciar o serviço
sudo systemctl restart credit_radar
```

### 2. Deploy Manual (Sem Systemd)

Se preferir gerenciar o processo manualmente:

```bash
# Forçar deploy sem systemd
USE_SYSTEMD=no ./deploy.sh
```

Isto irá:
1. Matar processos existentes
2. Fazer o build
3. Iniciar o servidor como daemon

#### Comandos úteis

```bash
# Ver PID do processo
_build/prod/rel/credit_radar/bin/credit_radar pid

# Ver logs
tail -f _build/prod/rel/credit_radar/log/server.log

# Parar o servidor
_build/prod/rel/credit_radar/bin/credit_radar stop

# Reiniciar o servidor
_build/prod/rel/credit_radar/bin/credit_radar restart
```

## Configuração de Ambiente

Certifique-se de que as seguintes variáveis estão definidas:

### Obrigatórias

```bash
export DATABASE_URL="ecto://user:pass@host/database"
export HUB_DATABASE_URL="ecto://user:pass@host/hub_do_investidor_production"
export SECRET_KEY_BASE="sua-chave-secreta-aqui"
```

### Opcionais

```bash
export PHX_HOST="seu-dominio.com"
export PORT="4000"
export CHECK_ORIGIN="//seu-dominio.com,//outro-dominio.com"
```

## WebSocket Origin Configuration

A aplicação está configurada para aceitar conexões WebSocket de:
- O host configurado via `RENDER_EXTERNAL_HOSTNAME`
- O ELB da AWS: `credit-tracker-1380171241.us-east-2.elb.amazonaws.com`

Para adicionar mais origens:

```bash
# Via variável de ambiente (lista separada por vírgulas)
export CHECK_ORIGIN="//dominio1.com,//dominio2.com,//load-balancer.aws.com"
```

Ou edite `config/runtime.exs` diretamente.

## Troubleshooting

### Assets não carregam (404)

```bash
# Recompilar assets
MIX_ENV=prod mix assets.deploy
```

### Erro "Could not check origin"

Adicione a origem correta em `CHECK_ORIGIN`:

```bash
export CHECK_ORIGIN="//sua-origem-aqui.com"
```

### Servidor não inicia

```bash
# Via systemd
sudo journalctl -u credit_radar -n 100

# Via daemon
tail -n 100 _build/prod/rel/credit_radar/log/server_error.log
```

### Migrations falham

```bash
# Rodar migrations manualmente
_build/prod/rel/credit_radar/bin/migrate
```

## Estrutura de Logs

### Com Systemd
- Logs via journalctl: `sudo journalctl -u credit_radar -f`
- Arquivos: `_build/prod/rel/credit_radar/log/`

### Sem Systemd
- `_build/prod/rel/credit_radar/log/server.log`
- `_build/prod/rel/credit_radar/log/server_error.log`

## Health Check

Verifique se a aplicação está rodando:

```bash
curl http://localhost:4000
```

Ou acesse via navegador o endereço configurado.

## Atualizando o Serviço

Para atualizar após mudanças no código:

```bash
# Com Git
git pull origin main

# Deploy
./deploy.sh
```

O script automaticamente:
- Para o servidor atual
- Faz o build da nova versão
- Roda migrations
- Reinicia o servidor
