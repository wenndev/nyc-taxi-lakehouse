# NYC Taxi Lakehouse V2 / V2.5

V2 e a refatoracao local-first do projeto.

Nesta versao, o projeto saiu do recorte de 2024 da V1 e passou a trabalhar com
dados de 2025. A logica principal continua sendo Bronze, Silver e Gold, mas agora
com codigo PySpark reaproveitavel localmente e depois no Azure Databricks.

A V2.5 e o fluxo atual dentro da V2: buscar varias estacoes NOAA de NYC com
paginacao, consolidar o clima por dia e manter a `dim_clima` com 1 linha por
data. O nome V2.5 marca uma evolucao de modelagem dentro da V2, nao uma nova
estrutura de projeto.

Leia primeiro, para entender o projeto inteiro do zero:

```text
v2/docs/00_visao_geral.md
```

Mapa da documentacao:

```text
v2/docs/README.md
```

Historico tecnico da refatoracao:

```text
v2/docs/06_historico_refatoracao.md
```

Plano cloud com ADF orquestrando Databricks:

```text
v2/docs/04_orquestracao_adf_databricks.md
```

Roteiro operacional para recriar Azure e executar a V2:

```text
v2/docs/03_plano_azure_databricks.md
```

Runbook de execucao local, dev e futura cloud:

```text
v2/docs/01_runbook_execucao.md
```

Referencias praticas de Databricks, Spark e Azure:

```text
v2/docs/05_referencias_tecnicas.md
```

Dicionario de dados da V2:

```text
v2/docs/02_dicionario_dados.md
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

## Estrutura inicial

```text
v2/
  config/
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
      models.py
      validators.py
      storage.py
    gold/
      gold_daily_weather_demand.py
      validate_gold_daily_weather_demand.py
      gold_star_schema.py
      validate_gold_star_schema.py
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
      gold_daily_weather_demand.py
      validate_gold_daily_weather_demand.py
      gold_star_schema.py
      validate_gold_star_schema.py

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

A primeira Gold V2 alinha todos os dias de 2025 usando um calendario completo como
base. Ela junta demanda diaria da TLC com clima diario da NOAA.

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

Teste local leve com amostra da TLC:

```bash
poetry run gold-daily-weather-demand \
  --tlc-input v2/data/delta/dev/silver/nyc_tlc/yellow_sample/2025_01 \
  --output v2/data/delta/dev/gold/daily_weather_demand/2025_01

poetry run validate-gold-daily-weather-demand \
  --input v2/data/delta/dev/gold/daily_weather_demand/2025_01 \
  --min-days-with-demand 1 \
  --min-total-trips 1

poetry run gold-star-schema \
  --tlc-input v2/data/delta/dev/silver/nyc_tlc/yellow_sample/2025_01 \
  --output v2/data/delta/dev/gold/star_schema/2025_01 \
  --skip-count

poetry run validate-gold-star-schema \
  --input v2/data/delta/dev/gold/star_schema/2025_01 \
  --expected-locations 265 \
  --min-fact-rows 1
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
fact_trips = 97065
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
