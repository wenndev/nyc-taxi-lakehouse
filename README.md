# NYC Taxi Lakehouse

Pipeline Lakehouse para analisar corridas de taxi amarelo de Nova York junto com dados climaticos historicos da NOAA.

O objetivo analitico do projeto e responder:

```text
As condicoes climaticas afetam a demanda por taxi em Nova York?
```

## Status

O repositorio possui duas fases do mesmo projeto:

| Versao | Periodo | Status | Descricao |
|---|---:|---|---|
| V1 | 2024 | Preservada | Projeto original em Azure, ADF, Databricks, PySpark e Delta Lake. |
| V2/V2.5 | 2025 | Em refatoracao | Reconstrucao local-first com Poetry, PySpark e Delta Lake, preparada para voltar ao Azure depois. |

A V1 gerou a Gold publicada no Kaggle:

[NYC Taxi Trips 2024 - Gold Layer Star Schema](https://www.kaggle.com/datasets/delzin/nyc-taxi-trips-2024-gold-layer-star-schema)

A V2 esta sendo desenvolvida em [v2/](v2/README.md). O historico tecnico da refatoracao esta em [v2/docs/historico_refatoracao_v2.md](v2/docs/historico_refatoracao_v2.md).

Plano de retorno para Azure com ADF orquestrando Databricks:

[v2/docs/plano_cloud_adf_databricks.md](v2/docs/plano_cloud_adf_databricks.md)

Roteiro operacional para recriar a infraestrutura e executar a V2:

[v2/docs/plano_execucao_azure_v2.md](v2/docs/plano_execucao_azure_v2.md)

## Fontes

### NYC TLC

Dados publicos de corridas de taxi amarelo.

Na V2, o ano usado e 2025. Os arquivos seguem o padrao:

```text
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-02.parquet
...
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-12.parquet
```

### NOAA

Dados climaticos da API CDO da NOAA.

Na V2.5, a estrategia atual usa varias estacoes NOAA dentro do recorte de NYC:

```text
NOAA varias estacoes de NYC
  -> paginacao por offset
  -> Silver NOAA com 1 linha por estacao/dia
  -> agregacao diaria de clima NYC
  -> dim_clima com 1 linha por data
```

A regra principal da modelagem e que a `fact_trips` nao deve juntar diretamente
com varias estacoes NOAA. Antes do join, o clima precisa ser consolidado para uma
linha por dia. Isso evita duplicar corridas.

Modelagem proposta:

[v2/docs/star_schema_v2.5.excalidraw](v2/docs/star_schema_v2.5.excalidraw)

Preview em PNG:

[v2/docs/star_schema_v2.5.excalidraw.png](v2/docs/star_schema_v2.5.excalidraw.png)

## Arquitetura

A arquitetura segue o modelo Medallion:

```text
raw
  -> bronze
  -> silver
  -> gold
```

Na V2 local:

```text
v2/data/raw/...
v2/data/delta/bronze/...
v2/data/delta/silver/...
v2/data/delta/gold/...
```

No Azure depois, a ideia e trocar os caminhos locais por caminhos ADLS:

```text
abfss://raw@<storage>.dfs.core.windows.net/...
abfss://lakehouse@<storage>.dfs.core.windows.net/bronze/...
abfss://lakehouse@<storage>.dfs.core.windows.net/silver/...
abfss://lakehouse@<storage>.dfs.core.windows.net/gold/...
```

A logica PySpark deve ser reaproveitada. O que muda na cloud e principalmente configuracao, secrets, orquestracao e infraestrutura.

## Estrutura

```text
nyc-taxi-lakehouse/
  v1/
    bronze/
    silver/
    gold/
    pipeline/
    data/
    docs/

  v2/
    config/
      paths.py
      sources.py
      spark.py

    pipelines/
      ingestion/
      bronze/
      silver/
      gold/

    notebooks/
    docs/
    data/
      raw/      # ignorado no Git
      delta/    # ignorado no Git

  pyproject.toml
  README.md
```

## Como Rodar a V2 Localmente

Instalar dependencias:

```bash
poetry install
```

Baixar NYC TLC 2025:

```bash
poetry run ingest-nyc-tlc
```

Configurar token NOAA localmente.

Opcao recomendada: criar `.env` a partir do exemplo:

```bash
cp .env.example .env
```

Depois preencha:

```text
NOAA_TOKEN=seu_token_aqui
```

O arquivo `.env` esta no `.gitignore` e nao deve ser commitado.

Baixar NOAA 2025 com paginacao para a V2.5, usando varias estacoes de NYC:

```bash
poetry run ingest-noaa-weather \
  --year 2025 \
  --locationid CITY:US360019
```

Com `locationid CITY:US360019`, a saida padrao fica separada em:

```text
v2/data/raw/noaa/ghcnd_nyc/2025
```

Criar Bronze:

```bash
poetry run bronze-nyc-tlc --skip-count
poetry run bronze-noaa-weather --skip-count
```

Criar Silver:

```bash
poetry run silver-nyc-tlc --skip-count
poetry run silver-noaa-weather
```

Criar Gold diaria:

```bash
poetry run gold-daily-weather-demand \
  --noaa-input v2/data/delta/silver/noaa/ghcnd_nyc/2025
```

Criar Gold dimensional:

```bash
poetry run gold-star-schema \
  --noaa-input v2/data/delta/silver/noaa/ghcnd_nyc/2025
```

## Teste Local Leve

A Silver completa da TLC pode ser pesada localmente. Para validar sem travar a maquina:

```bash
poetry run bronze-nyc-tlc \
  --input v2/data/raw/nyc_tlc/yellow/2025/yellow_tripdata_2025-01.parquet \
  --output v2/data/delta/dev/bronze/nyc_tlc/yellow_sample/2025_01 \
  --limit 100000 \
  --skip-count

poetry run silver-nyc-tlc \
  --input v2/data/delta/dev/bronze/nyc_tlc/yellow_sample/2025_01 \
  --output v2/data/delta/dev/silver/nyc_tlc/yellow_sample/2025_01 \
  --skip-count

poetry run gold-daily-weather-demand \
  --tlc-input v2/data/delta/dev/silver/nyc_tlc/yellow_sample/2025_01 \
  --output v2/data/delta/dev/gold/daily_weather_demand/2025_01

poetry run gold-star-schema \
  --tlc-input v2/data/delta/dev/silver/nyc_tlc/yellow_sample/2025_01 \
  --output v2/data/delta/dev/gold/star_schema/2025_01 \
  --skip-count
```

## Validacoes Atuais da V2

TLC 2025:

```text
dias_2025 = 365
```

NOAA 2025:

```text
Fluxo V2.5 atual: usar locationid CITY:US360019.
Validar apos a ingestao:
downloaded_results == expected_count
dias distintos = 365
periodo = 2025-01-01 ate 2025-12-31
```

Gold diaria dev:

```text
linhas = 365
dias_sem_clima = 0
```

Gold Star Schema dev:

```text
dim_data = 365
dim_clima = 365
fact_trips.clima_id_nulo = 0
chaves_orfas = 0
```

## Principais Melhorias da V2

- Separacao clara entre V1 preservada e V2 refatorada.
- Projeto local-first com Poetry.
- Ingestao NYC TLC 2025 automatizada.
- Ingestao NOAA com paginacao por `offset`.
- Fluxo V2.5 para varias estacoes NOAA de NYC.
- Bronze e Silver em Delta Lake para TLC e NOAA.
- Silver TLC com colunas temporais, duracao, distancia em km, pagamento, flags e categorias.
- Silver NOAA com clima diario em uma linha por estacao/data.
- Gold consolida a NOAA para uma linha de clima NYC por data.
- Gold diaria usando calendario completo de 2025 como base.
- Gold dimensional com `dim_data`, `dim_clima`, `dim_localizacao` e `fact_trips`.
- Wrappers Databricks para Ingestion, Bronze, Silver e Gold.

## Problemas da V1 Que a V2 Resolve

Na V1, a `dim_clima` ficou com apenas 38 registros, causando grande quantidade de `clima_id` nulo na fato.

Na V2:

- a NOAA usa paginacao;
- a cobertura climatica tem 365 dias;
- a V2.5 consolida varias estacoes antes de ligar clima com corridas;
- a Gold diaria usa calendario completo;
- a Gold dimensional liga `fact_trips` com `dim_clima` por data;
- o join entre clima e demanda ocorre por `data`;
- dias incompletos ficam marcados por flag.

## Proximos Passos

- Criar dicionario de dados.
- Rodar a ingestao NOAA V2.5 completa com `CITY:US360019`.
- Validar Bronze, Silver e Gold com varias estacoes NOAA.
- Recriar infraestrutura Azure seguindo `v2/docs/plano_execucao_azure_v2.md`.
- Executar Silver e Gold completas no Databricks.
- Enriquecer `dim_localizacao` com taxi zone lookup.
- Criar camada ML usando a Gold diaria.
- Adicionar CI/CD com GitHub Actions para deploy de notebooks/scripts no Databricks.

## Observacoes Importantes

Os dados em `v2/data/raw` e `v2/data/delta` nao sao versionados no Git. Para continuar em outra maquina, sera necessario rebaixar os dados ou copiar essas pastas manualmente.

Tokens e credenciais nao devem ser commitados. Localmente use variavel de ambiente. No Azure, use Key Vault e Secret Scope do Databricks.
