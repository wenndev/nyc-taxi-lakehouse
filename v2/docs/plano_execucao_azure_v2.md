# Plano De Execucao Azure V2

Este documento e um roteiro operacional para recriar a infraestrutura Azure e
executar a V2 do projeto.

A logica principal ja esta nos scripts PySpark em `v2/pipelines`. Na cloud, o
objetivo e mudar infraestrutura, caminhos, secrets e orquestracao, sem reescrever
as regras de Bronze, Silver e Gold.

## Objetivo

Executar o pipeline completo de 2025 no Azure:

```text
NYC TLC + NOAA
  -> raw
  -> bronze
  -> silver
  -> gold
```

Pergunta analitica:

```text
As condicoes climaticas afetam a demanda por taxi em Nova York?
```

## Decisoes Da V2

- Ano de referencia: `2025`.
- Taxi: NYC TLC Yellow Taxi.
- Clima: NOAA GHCND.
- Estacao climatica: Central Park, `GHCND:USW00094728`.
- Camadas: Raw, Bronze, Silver e Gold.
- Formato das camadas tratadas: Delta Lake.
- Orquestracao cloud: Azure Data Factory chamando notebooks Databricks.
- Paginacao NOAA: fica no codigo Python/Databricks, nao no ADF.

## Recursos Azure

Criar ou configurar:

```text
1. Resource Group
2. Storage Account com ADLS Gen2
3. Containers ou volumes para raw e delta
4. Azure Databricks Workspace
5. Databricks Cluster ou Jobs Cluster
6. Key Vault
7. Databricks Secret Scope
8. Azure Data Factory
9. Linked Services do ADF para Databricks
10. Pipeline ADF com atividades Databricks Notebook
```

## Deploy Do Codigo

Os notebooks em `v2/databricks/notebooks` importam as funcoes em
`v2/pipelines`. No Databricks, garanta que o repositorio esteja disponivel com a
pasta `v2` preservada.

Opcao mais simples para a primeira execucao:

```text
Databricks Repos conectado ao GitHub
```

Alternativa futura:

```text
build/deploy como wheel no cluster ou job
```

## Caminhos Cloud

Opcao recomendada para os downloaders Python no Databricks:

```text
/Volumes/<catalog>/<schema>/<volume>/raw/nyc_tlc/yellow/2025
/Volumes/<catalog>/<schema>/<volume>/raw/noaa/ghcnd/2025
/Volumes/<catalog>/<schema>/<volume>/delta/bronze/nyc_tlc/yellow/2025
/Volumes/<catalog>/<schema>/<volume>/delta/bronze/noaa/ghcnd/2025
/Volumes/<catalog>/<schema>/<volume>/delta/silver/nyc_tlc/yellow/2025
/Volumes/<catalog>/<schema>/<volume>/delta/silver/noaa/ghcnd/2025
/Volumes/<catalog>/<schema>/<volume>/delta/gold/daily_weather_demand/2025
/Volumes/<catalog>/<schema>/<volume>/delta/gold/star_schema/2025
```

Alternativa:

```text
/dbfs/mnt/raw/...
/dbfs/mnt/delta/...
```

Evitar passar `abfss://...` diretamente para os scripts de ingestion Python,
porque eles escrevem arquivos via filesystem. `abfss://...` pode ser usado com
Spark nas camadas Bronze, Silver e Gold, se o cluster estiver configurado para
acessar o ADLS.

## Secrets

Localmente:

```text
.env
NOAA_TOKEN=...
```

No Azure:

```text
Key Vault
  -> noaa-token

Databricks Secret Scope
  -> scope: kv-lakehouse
  -> key: noaa-token
```

O token NOAA nao deve ir para:

- codigo;
- README;
- ADF em texto puro;
- parametros visiveis de pipeline;
- commit Git.

## Ordem Do Pipeline

Ordem recomendada no ADF:

```text
1. ingest_nyc_tlc
2. ingest_noaa_weather
3. bronze_nyc_tlc
4. bronze_noaa_weather
5. silver_nyc_tlc
6. silver_noaa_weather
7. gold_daily_weather_demand
8. gold_star_schema
```

## Notebooks Databricks

Os notebooks ficam em:

```text
v2/databricks/notebooks/
```

Lista:

```text
ingest_nyc_tlc.py
ingest_noaa_weather.py
bronze_nyc_tlc.py
bronze_noaa_weather.py
silver_nyc_tlc.py
silver_noaa_weather.py
gold_daily_weather_demand.py
gold_star_schema.py
```

Eles sao wrappers. A regra de negocio fica em:

```text
v2/pipelines/
```

## Parametros ADF

### ingest_nyc_tlc

```text
year=2025
start_month=1
end_month=12
output=/Volumes/<catalog>/<schema>/<volume>/raw/nyc_tlc/yellow/2025
overwrite=false
dry_run=false
```

### ingest_noaa_weather

```text
year=2025
stationid=GHCND:USW00094728
output=/Volumes/<catalog>/<schema>/<volume>/raw/noaa/ghcnd/2025
secret_scope=kv-lakehouse
secret_key=noaa-token
datasetid=GHCND
datatypeids=PRCP,TMAX,TMIN,SNOW,SNWD
units=metric
limit=1000
initial_offset=1
overwrite=false
dry_run=false
```

### bronze_nyc_tlc

```text
year=2025
input=/Volumes/<catalog>/<schema>/<volume>/raw/nyc_tlc/yellow/2025
output=/Volumes/<catalog>/<schema>/<volume>/delta/bronze/nyc_tlc/yellow/2025
mode=overwrite
skip_count=true
dry_run=false
```

### bronze_noaa_weather

```text
year=2025
datasetid=GHCND
input=/Volumes/<catalog>/<schema>/<volume>/raw/noaa/ghcnd/2025
output=/Volumes/<catalog>/<schema>/<volume>/delta/bronze/noaa/ghcnd/2025
mode=overwrite
skip_count=true
dry_run=false
```

### silver_nyc_tlc

```text
year=2025
input=/Volumes/<catalog>/<schema>/<volume>/delta/bronze/nyc_tlc/yellow/2025
output=/Volumes/<catalog>/<schema>/<volume>/delta/silver/nyc_tlc/yellow/2025
mode=overwrite
start_date=2025-01-01
end_date=2026-01-01
skip_count=true
dry_run=false
```

### silver_noaa_weather

```text
year=2025
datasetid=GHCND
input=/Volumes/<catalog>/<schema>/<volume>/delta/bronze/noaa/ghcnd/2025
output=/Volumes/<catalog>/<schema>/<volume>/delta/silver/noaa/ghcnd/2025
mode=overwrite
skip_count=false
dry_run=false
```

### gold_daily_weather_demand

```text
year=2025
tlc_input=/Volumes/<catalog>/<schema>/<volume>/delta/silver/nyc_tlc/yellow/2025
noaa_input=/Volumes/<catalog>/<schema>/<volume>/delta/silver/noaa/ghcnd/2025
output=/Volumes/<catalog>/<schema>/<volume>/delta/gold/daily_weather_demand/2025
mode=overwrite
skip_count=false
dry_run=false
```

### gold_star_schema

```text
year=2025
tlc_input=/Volumes/<catalog>/<schema>/<volume>/delta/silver/nyc_tlc/yellow/2025
noaa_input=/Volumes/<catalog>/<schema>/<volume>/delta/silver/noaa/ghcnd/2025
output=/Volumes/<catalog>/<schema>/<volume>/delta/gold/star_schema/2025
mode=overwrite
skip_count=true
dry_run=false
```

## Validacoes Apos Executar

Validar NOAA raw:

```text
downloaded_results == expected_count
distinct_dates == 365
data_min == 2025-01-01
data_max == 2025-12-31
```

Validar Silver NOAA:

```text
linhas == 365
dias_distintos == 365
dias_clima_incompleto esperado: 1
```

Validar Gold diaria:

```text
linhas == 365
dias_sem_clima == 0
```

Validar Gold Star Schema:

```text
dim_data.data_id sem duplicidade
dim_clima.clima_id sem duplicidade
fact_trips.data_id_nulo == 0
fact_trips.clima_id_nulo == 0
fact_trips.localizacao_partida_id_nulo == 0
fact_trips.localizacao_chegada_id_nulo == 0
chaves_orfas == 0
```

## Execucao Recomendada

1. Rodar os notebooks com `dry_run=true` para conferir caminhos e parametros.
2. Rodar `ingest_noaa_weather` e verificar se a paginacao baixou tudo.
3. Rodar `ingest_nyc_tlc` para os 12 meses.
4. Rodar Bronze NOAA e Silver NOAA primeiro, porque sao leves.
5. Rodar Bronze TLC.
6. Rodar Silver TLC com `skip_count=true`.
7. Rodar Gold diaria.
8. Rodar Gold Star Schema com `skip_count=true`.
9. Rodar queries de validacao no Databricks.
10. So depois pensar em OPTIMIZE, ZORDER, incremental e ML.

## Pontos De Atencao

- A Silver TLC completa e a Gold Star completa podem ser pesadas localmente.
- No Databricks, usar cluster com memoria suficiente para a TLC 2025 completa.
- `skip_count=true` evita disparar contagens caras durante processamento pesado.
- A V2 ainda nao implementa pipeline incremental; atualmente o padrao e
  `overwrite`.
- `OPTIMIZE` e `ZORDER` devem ser aplicados depois da primeira execucao full,
  nao antes.
- A `dim_localizacao` atual usa IDs da NYC TLC; ainda nao esta enriquecida com
  nomes de zonas/bairros.

## Depois Da Primeira Execucao Full

Melhorias para uma segunda etapa:

```text
1. Criar dicionario de dados
2. Adicionar taxi zone lookup na dim_localizacao
3. Criar checks automatizados de qualidade
4. Definir particionamento fisico das tabelas Delta
5. Aplicar OPTIMIZE/ZORDER no Databricks
6. Planejar processamento incremental
7. Criar camada ML a partir da Gold diaria
```
