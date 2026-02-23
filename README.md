# superset-migrator 🚀

Ferramenta interativa para migrar dashboards do Apache Superset entre ambientes
(devel → staging → production), substituindo automaticamente as conexões de banco de dados.

## O problema resolvido

O export/import nativo do Superset inclui `sqlalchemy_uri` hard-coded.
Esta ferramenta intercepta o ZIP, troca as URIs pelo ambiente de destino,
e todo o gerenciamento de credenciais é feito via menus — sem editar YAML manualmente.

## Instalação

```bash
# Clonar o repositório
git clone https://github.com/yourusername/superset-migrator
cd superset-migrator

# Instalar (recomendado: ambiente virtual)
python -m venv .venv
source .venv/bin/activate     # Linux/macOS
.venv\Scripts\activate        # Windows

pip install -e .
```

## Uso

### Menu interativo (recomendado)

```bash
superset-migrator
```

Na primeira execução, um wizard guia o cadastro dos ambientes:

```
╭──────────────────────────────────────╮
│         superset-migrator            │
│  Migração de dashboards entre ambientes │
╰──────────────────────────────────────╯

Primeira execução!
Nenhum ambiente configurado. Vamos cadastrar pelo menos dois ambientes...

Ambiente de ORIGEM:
? Nome do ambiente: devel
? URL do Superset: http://superset-devel:8088
? Usuário: admin
? Senha: ****

Ambiente de DESTINO:
? Nome do ambiente: staging
? URL do Superset: http://superset-staging:8088
...
```

### Menu principal

```
? Menu principal  [origem: devel]
  🚀  Exportar dashboard (gerar ZIP)
  📤  Enviar ZIP para ambiente
  📋  Listar dashboards
  ⚙️   Gerenciar ambientes
  🗄️   Gerenciar bancos de dados
  ❌  Sair
```

### Fluxo de exportação (principal)

Suporta **seleção múltipla de dashboards**:

1. **Selecionar dashboards** — lista com checkbox ou busca por título
2. **Selecionar ambiente de destino**
3. **Resolver bancos sem mapeamento** (se houver) — cadastra URIs na hora
4. **Gerar ZIPs** — um arquivo por dashboard

```
? Como deseja localizar os dashboards?
  📋  Listar todos e selecionar

? Selecione os dashboards (espaço para marcar, enter para confirmar):
  ◉ [1] Dashboard de Vendas  [slug: vendas]
  ◉ [2] Dashboard Financeiro  [slug: financeiro]
  ◯ [3] Dashboard de RH

📦 2 dashboard(s) selecionado(s)

Exportando 2 dashboard(s) para 'staging':

(1/2) Dashboard de Vendas
  ✓ dashboard_1_vendas_staging_20240220.zip
(2/2) Dashboard Financeiro
  ✓ dashboard_2_financeiro_staging_20240220.zip

╭─────────────────────────────────────────────────╮
│ ✓ 2 dashboard(s) exportado(s) com sucesso!     │
╰─────────────────────────────────────────────────╯
```

### Fluxo de importação (secundário)

Suporta **importação de múltiplos arquivos**:

1. **Escolher modo** — único arquivo ou múltiplos de um diretório
2. **Selecionar arquivos** — com checkbox para múltipla seleção
3. **Selecionar ambiente de destino**
4. **Confirmar sobrescrita**
5. **Importar via API** + sincronizar datasets automaticamente

```
? Modo de importação:
  📄  Importar um único arquivo
❯ 📁  Importar múltiplos arquivos de um diretório

? Diretório com os arquivos ZIP: ./exports
? Selecione os arquivos (3 encontrados):
  ◉ dashboard_1_vendas_staging.zip
  ◉ dashboard_2_financeiro_staging.zip
  ◯ dashboard_3_rh_staging.zip

Importando 2 arquivo(s) para 'staging':

(1/2) dashboard_1_vendas_staging.zip
  ✓ Importado
(2/2) dashboard_2_financeiro_staging.zip
  ✓ Importado

✓ 2 dashboard(s) importado(s) com sucesso!
? Sincronizar colunas dos 5 dataset(s)? Sim
```

#### Busca por slug

O Superset permite configurar URLs customizadas para dashboards
(ex: `emv_transacoes_mancuso`). A ferramenta usa essa URL como chave:

```
? Como deseja localizar o dashboard?
❯ 🔍  Buscar por slug (URL customizada)
  📋  Listar todos e selecionar
  🔎  Buscar por título

? Slug do dashboard: emv_transacoes_mancuso
✓ Dashboard encontrado: EMV Transações Mancuso (ID: 42)
```

#### Primeiro banco de dados encontrado

Se o dashboard usa um banco que ainda não tem mapeamento:

```
┌─ Atenção ──────────────────────────────────────────────────┐
│ Os seguintes bancos não têm mapeamento para 'staging':      │
└────────────────────────────────────────────────────────────┘
  • SQL Server Principal
    URI atual (origem): mssql+pyodbc://sa:***@devel-sql:1433/MyDB?...

? Deseja cadastrar as URIs agora para continuar? Sim

Banco: SQL Server Principal  →  Ambiente: staging
? SQLAlchemy URI: mssql+pyodbc://sa:pass@staging-sql:1433/MyDB?driver=...

✓ Mapeamentos salvos para uso futuro.
```

Os mapeamentos são salvos em `~/.superset-migrator/config.yaml` e
reutilizados automaticamente em migrações futuras.

### Gerenciar bancos de dados

```
? O que deseja fazer?
❯ [+] Adicionar/editar banco
  [-] Remover banco
  ← Voltar
```

A tabela de bancos mostra o status de cada ambiente:

```
╭──────────────────────────────────────────────────────────────────╮
│ Banco (nome no Superset)  │ devel      │ staging    │ production  │
├───────────────────────────┼────────────┼────────────┼─────────────┤
│ SQL Server Principal      │ ✓ sa:***@… │ ✓ sa:***@… │ ✗ não mapeado│
│ ClickHouse Analytics      │ ✓ user:***  │ ✓ user:***  │ ✓ user:***   │
╰──────────────────────────────────────────────────────────────────╯
```

### Sincronização de datasets

Após importar um dashboard, o Superset pode não sincronizar automaticamente as colunas dos datasets (especialmente quando a conexão do banco mudou). O aplicativo oferece sincronização automática:

```
✓ Dashboard importado com sucesso no ambiente 'staging'!
? Sincronizar colunas dos 3 dataset(s)? (Sim)

🔄 Sincronizando 3 dataset(s)...
  ✓ dataset_vendas
  ✓ dataset_clientes
  ✓ dataset_produtos

✓ Todos os 3 dataset(s) sincronizados!
```

### Modo CI/CD (sem interação)

Para uso em pipelines automatizados:

```bash
# Migra por slug e importa automaticamente
superset-migrator migrate --slug emv_transacoes_mancuso --to staging --push

# Gera apenas o ZIP (sem importar)
superset-migrator migrate --slug meu-dashboard --to production --output ./exports/

# Importa um ZIP já existente (com sincronização automática)
superset-migrator import-zip dashboard_42_staging.zip --env staging

# Importa sem sincronizar datasets
superset-migrator import-zip dashboard_42_staging.zip --env staging --no-sync
```

## Estrutura do ZIP do Superset

```
export_abc123/
├── metadata.yaml
├── databases/
│   └── SQL_Server_Principal.yaml    ← sqlalchemy_uri substituída aqui
├── datasets/
│   └── SQL_Server_Principal/
│       └── minha_tabela.yaml
├── charts/
│   └── Meu_Grafico.yaml
└── dashboards/
    └── Meu_Dashboard.yaml
```

Apenas os arquivos em `databases/` são modificados. O resto é preservado integralmente.

## Arquivo de configuração

Fica em `~/.superset-migrator/config.yaml` e é gerenciado pela própria ferramenta.
Você pode editá-lo manualmente se preferir:

```yaml
environments:
  devel:
    url: http://superset-devel:8088
    username: admin
    password: admin
    is_source: true
  staging:
    url: http://superset-staging:8088
    username: admin
    password: admin

database_mappings:
  - name: SQL Server Principal
    environments:
      devel:
        sqlalchemy_uri: mssql+pyodbc://sa:pass@devel-sql:1433/MyDB?driver=ODBC+Driver+17+for+SQL+Server
      staging:
        sqlalchemy_uri: mssql+pyodbc://sa:pass@staging-sql:1433/MyDB?driver=ODBC+Driver+17+for+SQL+Server
```

## Dependências

| Pacote       | Versão | Função                          |
|--------------|--------|---------------------------------|
| `click`      | ≥8.1   | Estrutura do CLI                |
| `httpx`      | ≥0.27  | Requisições HTTP à API Superset |
| `pyyaml`     | ≥6.0   | Leitura/escrita do config       |
| `questionary`| ≥2.0   | Prompts interativos             |
| `rich`       | ≥13.0  | Tabelas, painéis e formatação   |
