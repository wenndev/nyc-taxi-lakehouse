# NYC Taxi Lakehouse V2 / V2.5

V2 e a refatoracao local-first do projeto.

Nesta versao, o projeto saiu do recorte de 2024 da V1 e passou a trabalhar com
dados de 2025. A logica principal continua sendo Bronze, Silver e Gold, mas agora
com codigo PySpark reaproveitavel localmente e depois no Azure Databricks.

A V2.5 e o fluxo atual dentro da V2: buscar varias estacoes NOAA de NYC com
paginacao, consolidar o clima por dia e manter a `dim_clima` com 1 linha por
data. O nome V2.5 marca uma evolucao de modelagem dentro da V2, nao uma nova
estrutura de projeto.

## Como Ler Esta V2

Se voce esta estudando o projeto, siga esta trilha:

```text
1. README.md
2. v2/README.md
3. v2/docs/00_visao_geral.md
4. v2/docs/02_dicionario_dados.md
5. v2/pipelines/ingestion/README.md
6. v2/pipelines/bronze/README.md
7. v2/pipelines/silver/README.md
8. v2/pipelines/quality/README.md
9. v2/pipelines/gold/README.md
10. v2/pipelines/dev/README.md
11. v2/docs/01_runbook_execucao.md
12. v2/databricks/README.md
13. v2/docs/03_plano_azure_databricks.md
14. v2/docs/04_orquestracao_adf_databricks.md
```

Mapa da pasta de documentos:

```text
v2/docs/README.md
```

Documentos de apoio:

```text
v2/docs/05_referencias_tecnicas.md
v2/docs/06_historico_refatoracao.md
```

Modelagem proposta V2.5:

```text
v2/docs/star_schema_v2.5.excalidraw
v2/docs/star_schema_v2.5.excalidraw.png
```

Arquitetura original da V1 mantida como referencia historica/comparativa:

```text
v2/docs/arquitetura_v1.jpg
```

Importante: `v2/data/raw` e `v2/data/delta` nao sao versionados no Git. Para
continuar em outra maquina, rebaixe os dados ou copie essas pastas manualmente.

## Estrutura Principal

```text
v2/
  config/
    spark.py
    paths.py
    sources.py

  pipelines/
    ingestion/
      download_nyc_tlc.py
      download_taxi_zone_lookup.py
      download_noaa_weather.py
    bronze/
      bronze_nyc_tlc.py
      bronze_taxi_zone_lookup.py
      bronze_noaa_weather.py
    silver/
      silver_nyc_tlc.py
      validate_silver_nyc_tlc.py
      silver_taxi_zone_lookup.py
      validate_silver_taxi_zone_lookup.py
      silver_noaa_weather.py
      validate_silver_noaa_weather.py
      validate_silver_common.py
    quality/
      config.py
      exceptions.py
      models.py
      validators.py
      storage.py
    gold/
      weather_consolidation.py
      gold_star_schema.py
      validate_gold_star_schema.py
      gold_daily_weather_demand.py
      validate_gold_daily_weather_demand.py
    dev/
      run_dev_sample.py
  databricks/
    notebooks/
      ingest_nyc_tlc.py
      ingest_taxi_zone_lookup.py
      ingest_noaa_weather.py
      bronze_nyc_tlc.py
      bronze_taxi_zone_lookup.py
      bronze_noaa_weather.py
      silver_nyc_tlc.py
      silver_taxi_zone_lookup.py
      silver_noaa_weather.py
      validate_silver_nyc_tlc.py
      validate_silver_taxi_zone_lookup.py
      validate_silver_noaa_weather.py
      gold_star_schema.py
      validate_gold_star_schema.py
      gold_daily_weather_demand.py
      validate_gold_daily_weather_demand.py

  notebooks/
    01_inspect_bronze_nyc_tlc.ipynb
    02_inspect_silver_nyc_tlc.ipynb
    03_inspect_gold_star_schema.ipynb
    04_eda_weather_demand.ipynb

  data/
    raw/
      nyc_tlc/
        yellow/
          2025/
        taxi_zone_lookup/
      noaa/
        ghcnd_nyc/
          2025/
    delta/
      bronze/
      silver/
      gold/
        star_schema/
        daily_weather_demand/
      quarantine/
      monitoring/
```

## Testes

Rodar a suite automatizada da V2:

```bash
poetry run python -m unittest discover -s tests -p 'test_*.py' -t .
```

Os testes usam DataFrames Spark pequenos e validam regras de ingestion, Data
Quality, Silver, Gold diaria e Star Schema.

Rodar o fluxo dev local de ponta a ponta:

```bash
poetry run run-v2-dev-sample --dry-run
poetry run run-v2-dev-sample
```

Esse comando valida uma amostra da TLC e reaproveita Silver NOAA e Taxi Zone
Lookup ja publicadas localmente.

## Configuracao Por Ambiente

A V2 agora possui uma base simples de configuracao em `v2/config/settings.py`.
Por padrao, tudo continua rodando localmente em:

```text
v2/data/raw
v2/data/delta
```

Variaveis uteis para testes locais ou preparacao cloud:

```bash
export NYC_TAXI_ENV=local
export NYC_TAXI_STORAGE_MODE=local
export NYC_TAXI_RAW_ROOT=v2/data/raw
export NYC_TAXI_DELTA_ROOT=v2/data/delta
export NYC_TAXI_SPARK_MASTER='local[1]'
export NYC_TAXI_SPARK_SHUFFLE_PARTITIONS=16
export NYC_TAXI_LOG_LEVEL=INFO
```

No Databricks, os wrappers continuam recebendo caminhos por parametro. A ideia
e usar o mesmo codigo PySpark e trocar apenas configuracao/caminhos:

```bash
export NYC_TAXI_ENV=databricks
export NYC_TAXI_STORAGE_MODE=databricks_volume
export NYC_TAXI_SPARK_MASTER=
```

As execucoes tambem passam a usar `pipeline_run_id`. Localmente ele e gerado
automaticamente; no Azure/ADF pode vir de parametro ou variavel de ambiente:

```bash
export PIPELINE_RUN_ID=manual-test-001
export ADF_PIPELINE_RUN_ID=<run_id_do_adf>
```

## Escrita Delta E Idempotencia

As escritas Delta da V2 passam por `v2/platform/delta.py`.

Isso cria um ponto unico para controlar:

```text
overwrite
append
overwriteSchema
mergeSchema
partitionBy
replaceWhere
```

Hoje o comportamento padrao continua simples:

```text
mode=overwrite -> reprocessa a saida inteira daquele caminho
mode=append    -> adiciona novos arquivos Delta naquele caminho
```

Para execucao local/dev, `overwrite` continua sendo o modo recomendado porque
evita duplicidade ao rodar o mesmo comando mais de uma vez.

A V2 ja escreve as tabelas temporais com particionamento por ano e mes:

```text
Silver NYC TLC              -> partitionBy=["ano", "mes"]
Silver NOAA                 -> partitionBy=["ano", "mes"]
Gold Star Schema/fact_trips -> partitionBy=["ano", "mes"]
Gold daily_weather_demand   -> partitionBy=["ano", "mes"]
```

Bronze e dimensoes pequenas ficam sem particionamento por enquanto. A Bronze
preserva melhor o dado de entrada e as dimensoes pequenas nao ganham muito com
pastas particionadas.

Para execucao incremental/backfill, os pipelines temporais aceitam
`replace_month` e, opcionalmente, `replace_year`. Quando `replace_year` nao e
informado, o pipeline usa o valor de `year`.

```text
year=2025
replace_month=1

replaceWhere gerado:
ano = 2025 AND mes = 1
```

Isso permite reprocessar apenas uma particao mensal sem sobrescrever todos os
meses ja publicados naquele caminho Delta.

Exemplo local:

```bash
poetry run silver-nyc-tlc \
  --year 2025 \
  --replace-month 1 \
  --skip-count

poetry run gold-daily-weather-demand \
  --year 2025 \
  --replace-month 1 \
  --skip-count
```

## Ingestion

Baixar ou conferir os arquivos Parquet da NYC TLC 2025.

Conferir URLs sem baixar:

```bash
poetry run ingest-nyc-tlc --dry-run
```

Baixar todos os meses de 2025:

```bash
poetry run ingest-nyc-tlc
```

Baixar somente janeiro para testar:

```bash
poetry run ingest-nyc-tlc --start-month 1 --end-month 1
```

Baixar Taxi Zone Lookup para enriquecer `dim_localizacao`:

```bash
poetry run ingest-taxi-zone-lookup --dry-run
poetry run ingest-taxi-zone-lookup
```

Baixar clima da NOAA para 2025 usando varias estacoes de NYC. A regra de
modelagem e manter a Gold com 1 linha de clima por data:

```bash
poetry run ingest-noaa-weather \
  --year 2025 \
  --locationid CITY:US360019
```

Esse comando usa `datasetid=GHCND` na API, mas salva por padrao em:

```text
v2/data/raw/noaa/ghcnd_nyc/2025
```

```text
NOAA varias estacoes
  -> Silver NOAA: 1 linha por estacao/dia
  -> Gold/dim_clima: clima NYC consolidado, 1 linha por data
  -> fact_trips: join por data/clima_id sem duplicar corridas
```

O token local pode ficar em `.env`:

```text
NOAA_TOKEN=seu_token_aqui
```

## Bronze

Depois que os dados estiverem em `v2/data/raw`, a Bronze converte os brutos para
Delta Lake.

Criar Bronze NYC TLC:

```bash
poetry run bronze-nyc-tlc --dry-run
poetry run bronze-nyc-tlc
```

Criar Bronze Taxi Zone Lookup:

```bash
poetry run bronze-taxi-zone-lookup --dry-run
poetry run bronze-taxi-zone-lookup
```

Criar Bronze NOAA:

```bash
poetry run bronze-noaa-weather --dry-run
poetry run bronze-noaa-weather
```

## Silver

A Silver le `v2/data/delta/bronze`, limpa/enriquece os dados e salva em
`v2/data/delta/silver`.

Criar Silver NYC TLC:

```bash
poetry run silver-nyc-tlc --dry-run
poetry run silver-nyc-tlc --skip-count
```

A Silver NYC TLC executa Data Quality depois do rename das colunas e antes das
colunas derivadas. Registros invalidos vao para quarentena e metricas vao para
monitoring:

```text
v2/data/delta/quarantine/nyc_tlc/yellow/2025
v2/data/delta/monitoring/quality/nyc_tlc/yellow/2025
```

Criar Silver Taxi Zone Lookup:

```bash
poetry run silver-taxi-zone-lookup --dry-run
poetry run silver-taxi-zone-lookup
poetry run validate-silver-taxi-zone-lookup
```

A Silver Taxi Zone Lookup tambem roda Data Quality. Essa fonte enriquece a
`dim_localizacao` com borough, zona e zona de servico:

```text
v2/data/delta/quarantine/nyc_tlc/taxi_zone_lookup
v2/data/delta/monitoring/quality/nyc_tlc/taxi_zone_lookup
```

Criar Silver NOAA:

```bash
poetry run silver-noaa-weather --dry-run
poetry run silver-noaa-weather
poetry run validate-silver-noaa-weather
```

A Silver NOAA executa Data Quality antes de publicar os dados tratados. Registros
invalidos vao para quarentena e metricas vao para monitoring:

```text
v2/data/delta/quarantine/noaa/ghcnd_nyc/2025
v2/data/delta/monitoring/quality/noaa/ghcnd_nyc/2025
```

Validar Silver TLC antes de gerar a Gold:

```bash
poetry run validate-silver-nyc-tlc --expected-days 365
```

Para amostra local, informe a quantidade esperada de dias da amostra ou remova
`--expected-days`.

## Gold

A Gold V2 tem duas saidas principais:

```text
gold/star_schema/2025
gold/daily_weather_demand/2025
```

O Star Schema e a entrega dimensional principal para BI. A
`daily_weather_demand` e uma tabela diaria para EDA/ML. As duas saidas leem dados
da Silver; a tabela diaria nao depende fisicamente do Star Schema.

Na escrita Delta, `fact_trips` e `daily_weather_demand` ficam particionadas por
`ano` e `mes`. As dimensoes ficam sem particionamento porque sao pequenas.

Criar Gold dimensional:

```bash
poetry run gold-star-schema --dry-run
poetry run gold-star-schema \
  --taxi-zone-lookup-input v2/data/delta/silver/nyc_tlc/taxi_zone_lookup \
  --noaa-input v2/data/delta/silver/noaa/ghcnd_nyc/2025
```

Validar a Gold dimensional:

```bash
poetry run validate-gold-star-schema --dry-run
poetry run validate-gold-star-schema
```

Criar Gold diaria:

```bash
poetry run gold-daily-weather-demand --dry-run
poetry run gold-daily-weather-demand \
  --noaa-input v2/data/delta/silver/noaa/ghcnd_nyc/2025
```

Validar a Gold diaria, base para EDA/ML:

```bash
poetry run validate-gold-daily-weather-demand --dry-run
poetry run validate-gold-daily-weather-demand
```

Teste local leve com amostra da TLC:

```bash
poetry run gold-star-schema \
  --tlc-input v2/data/delta/dev/silver/nyc_tlc/yellow_sample/2025_01 \
  --taxi-zone-lookup-input v2/data/delta/silver/nyc_tlc/taxi_zone_lookup \
  --output v2/data/delta/dev/gold/star_schema/2025_01 \
  --skip-count

poetry run validate-gold-star-schema \
  --input v2/data/delta/dev/gold/star_schema/2025_01 \
  --expected-locations 265 \
  --min-fact-rows 1

poetry run gold-daily-weather-demand \
  --tlc-input v2/data/delta/dev/silver/nyc_tlc/yellow_sample/2025_01 \
  --output v2/data/delta/dev/gold/daily_weather_demand/2025_01 \
  --skip-count

poetry run validate-gold-daily-weather-demand \
  --input v2/data/delta/dev/gold/daily_weather_demand/2025_01 \
  --min-days-with-demand 1 \
  --min-total-trips 1
```

Resultado esperado para o ano completo:

```text
365 linhas
1 linha por dia
join por data entre demanda diaria e clima diario
```

Limite consciente da V2.5:

```text
1 registro de clima diario representa todas as corridas daquele dia.
Mudancas de clima ao longo do dia nao sao modeladas nesta versao.
```

Evolucao futura V3:

```text
clima por hora ou faixa horaria
demanda por hora
join por data + hora
possivel evolucao para clima por zona/borough usando estacao mais proxima
```

Resultado esperado para a Gold dimensional:

```text
dim_data com 365 dias
dim_clima com 365 dias
fact_trips sem clima_id nulo
fact_trips sem chaves orfas
```

## Validacao Atual V2.5

NOAA raw:

```text
escopo = CITY:US360019
paginas = 76
downloaded_results = 75991
expected_count = 75991
```

Silver NOAA:

```text
linhas = 33074
dias = 365
estacoes = 124
periodo = 2025-01-01 ate 2025-12-31
grao = 1 linha por estacao/data
```

Clima consolidado para Gold:

```text
linhas = 365
dias_distintos = 365
min_estacoes_dia = 75
max_estacoes_dia = 108
media_estacoes_dia = 90.61
dias_clima_incompleto = 0
```

Gold Star Schema dev:

```text
dim_data = 365
dim_clima = 365
dim_localizacao = 265
fact_trips = 97060
fact_trips.clima_id_nulo = 0
```

## Evolucoes da V1 ja enderecadas

- Paginacao da API NOAA com `offset`.
- Desenho V2.5 para clima NYC consolidado usando varias estacoes NOAA.
- Data Quality modular para NOAA e NYC TLC entre Bronze e Silver.
- Taxi Zone Lookup com Bronze, Silver e Data Quality para enriquecer `dim_localizacao`.
- Silver TLC com colunas temporais, duracao, passageiros, pagamento e flags.
- Gold diaria usando calendario completo para evitar perda de dias no join.
- Gold dimensional com `dim_data`, `dim_clima`, `dim_localizacao` enriquecida e `fact_trips`.
- Particionamento Delta por `ano` e `mes` nas tabelas temporais.
- Reprocessamento mensal idempotente com `replace_month` e `replaceWhere`.
- Wrappers Databricks preparados para receber parametros do ADF.

## Visualizacao com Jupyter

Abrir JupyterLab:

```bash
poetry run jupyter lab
```

Notebook inicial:

```text
v2/notebooks/01_inspect_bronze_nyc_tlc.ipynb
```

Notebooks de inspecao:

```text
v2/notebooks/01_inspect_bronze_nyc_tlc.ipynb
v2/notebooks/02_inspect_silver_nyc_tlc.ipynb
v2/notebooks/03_inspect_gold_star_schema.ipynb
v2/notebooks/04_eda_weather_demand.ipynb
```
