# Rolling Deployment Guide

## Quando usar este guia

Use este guia quando você tiver **múltiplas instâncias** do Credit Radar rodando atrás de um load balancer.

## Pré-requisitos

- Load balancer configurado (ALB, nginx, HAProxy, etc.)
- Múltiplas instâncias da aplicação
- Health checks configurados no load balancer
- Acesso SSH a todas as instâncias

## Estratégia de Rolling Deployment

### Objetivo

Atualizar todas as instâncias **sem nenhum downtime**, mantendo sempre pelo menos uma instância disponível.

### Processo

Para cada instância:

1. **Remover do load balancer**
   ```bash
   # Exemplo com AWS ALB
   aws elbv2 deregister-targets --target-group-arn <ARN> --targets Id=<instance-id>

   # Aguardar draining (conexões existentes terminarem)
   # Tempo típico: 30-60 segundos
   ```

2. **Deploy na instância**
   ```bash
   ssh user@instance-X
   cd /path/to/credit_radar
   ./deploy.sh
   ```

   O downtime dessa instância não afeta os usuários pois ela está fora do load balancer.

3. **Health check**
   ```bash
   # Verificar se a aplicação está saudável
   curl http://localhost:4000/health || curl http://localhost:4000/
   ```

4. **Adicionar de volta ao load balancer**
   ```bash
   # Exemplo com AWS ALB
   aws elbv2 register-targets --target-group-arn <ARN> --targets Id=<instance-id>

   # Aguardar health check passar (30-60 segundos)
   ```

5. **Repetir para próxima instância**

## Script Automatizado

```bash
#!/bin/bash
# rolling_deploy.sh

INSTANCES=("10.0.1.10" "10.0.1.11" "10.0.1.12")
DEPLOY_PATH="/home/ubuntu/credit_radar"
TARGET_GROUP_ARN="arn:aws:elasticloadbalancing:..."

for INSTANCE in "${INSTANCES[@]}"; do
  echo "🔄 Deploying to instance: $INSTANCE"

  # 1. Obter instance ID
  INSTANCE_ID=$(aws ec2 describe-instances \
    --filters "Name=private-ip-address,Values=$INSTANCE" \
    --query 'Reservations[0].Instances[0].InstanceId' \
    --output text)

  # 2. Remover do load balancer
  echo "⏸️  Removing from load balancer..."
  aws elbv2 deregister-targets \
    --target-group-arn "$TARGET_GROUP_ARN" \
    --targets Id="$INSTANCE_ID"

  # Aguardar draining
  sleep 60

  # 3. Deploy
  echo "🚀 Deploying..."
  ssh ubuntu@$INSTANCE "cd $DEPLOY_PATH && ./deploy.sh"

  # 4. Health check
  echo "🏥 Running health check..."
  ssh ubuntu@$INSTANCE "curl -f http://localhost:4000/ || exit 1"

  # 5. Adicionar de volta
  echo "✅ Adding back to load balancer..."
  aws elbv2 register-targets \
    --target-group-arn "$TARGET_GROUP_ARN" \
    --targets Id="$INSTANCE_ID"

  # Aguardar health check passar
  sleep 60

  echo "✅ Instance $INSTANCE deployed successfully"
  echo ""
done

echo "🎉 Rolling deployment completed!"
```

## Rollback Rápido

Se algo der errado durante o rolling deployment:

### Rollback de uma instância específica

```bash
# 1. Parar a versão com problema
ssh user@instance-X "cd /path/to/credit_radar && sudo systemctl stop credit_radar"

# 2. Reverter para release anterior
ssh user@instance-X "cd /path/to/credit_radar && git checkout <commit-anterior>"

# 3. Deploy da versão anterior
ssh user@instance-X "cd /path/to/credit_radar && ./deploy.sh"
```

### Rollback completo

Se detectar problema após deploy em algumas instâncias, **não continue** o rolling deployment:

1. Mantenha as instâncias já atualizadas fora do load balancer
2. Faça rollback nelas para a versão estável
3. Recoloque todas no load balancer

## Boas Práticas

### 1. Sempre teste primeiro

```bash
# Deploy em apenas uma instância (canary)
# Monitore por 5-10 minutos
# Se tudo OK, continue com as demais
```

### 2. Monitore durante o deploy

- Logs de erro
- Métricas de performance (response time, error rate)
- Health checks
- Conexões de banco de dados

### 3. Automatize health checks

```bash
# health_check.sh
#!/bin/bash
INSTANCE=$1
MAX_RETRIES=5
RETRY=0

while [ $RETRY -lt $MAX_RETRIES ]; do
  if curl -f http://$INSTANCE:4000/ > /dev/null 2>&1; then
    echo "✅ Health check passed"
    exit 0
  fi

  RETRY=$((RETRY + 1))
  echo "⏳ Health check failed, retry $RETRY/$MAX_RETRIES"
  sleep 10
done

echo "❌ Health check failed after $MAX_RETRIES attempts"
exit 1
```

### 4. Configurar draining adequado

No load balancer, configure:
- **Connection draining:** 60-120 segundos
- **Health check interval:** 10-30 segundos
- **Healthy threshold:** 2-3 checks consecutivos

## Migrações de Banco de Dados

### Migrações compatíveis (safe)

Execute normalmente. O deploy.sh já roda as migrações automaticamente.

### Migrações breaking changes

Quando a migração não é retrocompatível:

1. **Deploy em duas etapas:**

   **Etapa 1:** Deploy código compatível com ambas versões do schema
   ```bash
   # Código que funciona COM e SEM a nova coluna
   ./rolling_deploy.sh
   ```

   **Etapa 2:** Deploy com a migração
   ```bash
   # Agora todas instâncias suportam o novo schema
   # Rode a migração em uma instância
   ssh user@instance-1 "cd /path && _build/prod/rel/credit_radar/bin/migrate"

   # Deploy do código que usa o novo schema
   ./rolling_deploy.sh
   ```

2. **Blue-Green temporário:**
   - Crie um novo grupo de instâncias
   - Deploy completo no novo grupo (com migração)
   - Switch do load balancer
   - Desligue grupo antigo

## Configuração de Load Balancer

### AWS Application Load Balancer

```bash
# Target Group com configurações adequadas
aws elbv2 create-target-group \
  --name credit-radar-tg \
  --protocol HTTP \
  --port 4000 \
  --vpc-id vpc-xxx \
  --health-check-path / \
  --health-check-interval-seconds 30 \
  --healthy-threshold-count 2 \
  --unhealthy-threshold-count 3 \
  --deregistration-delay-connection-termination 120
```

### Nginx (para load balancer local)

```nginx
upstream credit_radar {
    least_conn;

    server 10.0.1.10:4000 max_fails=3 fail_timeout=30s;
    server 10.0.1.11:4000 max_fails=3 fail_timeout=30s;
    server 10.0.1.12:4000 max_fails=3 fail_timeout=30s;
}

server {
    listen 80;
    server_name credit-radar.com;

    location / {
        proxy_pass http://credit_radar;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # WebSocket support
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }

    # Health check endpoint (opcional)
    location /health {
        access_log off;
        return 200 "healthy\n";
    }
}
```

## Monitoramento

### Métricas importantes durante rolling deployment

1. **Request rate** - deve permanecer estável
2. **Error rate** - não deve aumentar
3. **Response time** - não deve degradar
4. **Active connections** - verificar draining adequado
5. **Database connections** - não deve esgotar o pool

### Alertas recomendados

- Error rate > 1%
- Response time P95 > 1s
- Health check failures
- Database connection pool > 80%

## Próximos Passos

Quando o tráfego crescer, considere:

1. **Blue-Green Deployment** - Para rollback instantâneo
2. **Canary Releases** - Deploy gradual (5% → 25% → 100%)
3. **Feature Flags** - Habilitar features sem deploy
4. **A/B Testing** - Testar mudanças com grupo de usuários

## Troubleshooting

### Instância não passa no health check após deploy

```bash
# Ver logs
ssh user@instance "sudo journalctl -u credit_radar -n 100"

# Verificar se está rodando
ssh user@instance "sudo systemctl status credit_radar"

# Testar localmente
ssh user@instance "curl -v http://localhost:4000/"
```

### Load balancer não remove conexões

- Aumentar `deregistration-delay`
- Verificar se aplicação fecha conexões gracefully
- Implementar graceful shutdown no Elixir (já tem no Phoenix)

### Migração trava outras instâncias

- Use `pg_try_advisory_lock` para lock de migração
- Ou rode migrações antes do rolling deployment
- Configure timeout adequado nas migrações

---

**Dúvidas?** Verifique os logs e monitore as métricas. Rolling deployment deve ser seguro e previsível.
