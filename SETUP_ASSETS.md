# Configuração de Assets - Credit Radar

## Problema Identificado

O erro "Not Found" ao clicar nos botões do Backpex ocorre porque **os assets JavaScript e CSS não foram compilados**.

Os diretórios existem mas estão vazios:
- `/priv/static/assets/js/` - vazio
- `/priv/static/assets/css/` - vazio

## Solução

Execute os seguintes comandos **no seu ambiente local** (onde o Elixir/Mix está instalado):

### 1. Instalar dependências

```bash
# Instalar dependências Elixir
mix deps.get

# Instalar dependências NPM
cd assets && npm install && cd ..
```

### 2. Compilar os assets

```bash
# Compilar CSS e JavaScript
mix assets.build
```

Este comando irá:
- Compilar o CSS do Tailwind (incluindo estilos do Backpex)
- Compilar o JavaScript do Backpex e hooks customizados
- Gerar os arquivos em `priv/static/assets/`

### 3. Popular o banco com dados de IPCA

```bash
mix run priv/repo/seeds.exs
```

Isto irá criar 24 registros de projeções de IPCA (2025-2026).

### 4. Reiniciar o servidor

```bash
# Parar o servidor se estiver rodando (Ctrl+C duas vezes)

# Iniciar novamente
mix phx.server
```

## Verificando se funcionou

Após compilar os assets, verifique que os seguintes arquivos foram criados:

```bash
ls -lh priv/static/assets/js/
# Deve mostrar: app.js (e possivelmente app.js.map)

ls -lh priv/static/assets/css/
# Deve mostrar: app.css
```

## Assets incluídos

Após a compilação, os seguintes recursos estarão disponíveis:

- **Backpex UI**: Toda a interface administrativa do Backpex
- **ECharts**: Gráficos interativos de scatter plot
- **TomSelect**: Multi-select aprimorado para filtros
- **Tailwind CSS**: Framework CSS com tema dark customizado
- **Phoenix LiveView**: Funcionalidades de tempo real

## Troubleshooting

### Se ainda houver erro "Not Found":

1. Verifique se o servidor está rodando:
   ```bash
   ps aux | grep beam
   ```

2. Limpe e recompile:
   ```bash
   mix clean
   mix deps.clean backpex
   mix deps.get
   mix assets.build
   ```

3. Verifique os logs do servidor para erros específicos

### Se o Backpex não estiver salvando dados:

Certifique-se de que:
- O banco de dados está acessível
- As migrations foram executadas: `mix ecto.migrate`
- O usuário admin está autenticado corretamente

## Dependências JavaScript instaladas

✅ Backpex (via file:../deps/backpex)
✅ Phoenix LiveView
✅ ECharts para visualizações
✅ TomSelect para multi-select
✅ Phoenix HTML para helpers

Todas as dependências já foram adicionadas ao `assets/package.json`.
