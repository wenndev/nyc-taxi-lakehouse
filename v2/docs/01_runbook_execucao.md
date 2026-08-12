# Runbook De Execucao NYC Taxi Lakehouse V2 / V2.5

Este runbook e o manual operacional do projeto. Ele explica a ordem correta para
executar a V2 localmente e como essa mesma ordem deve virar pipeline no Azure
Databricks com orquestracao pelo Azure Data Factory.

Documento de entendimento conceitual:

```text
v2/docs/00_visao_geral.md
```

Este arquivo aqui tem outro objetivo: dizer o que rodar, em qual ordem, quais
caminhos usar e quais validacoes precisam passar antes de seguir.

## 1. Objetivo Do Runbook

Executar o pipeline V2 / V2.5:

```text
NYC TLC Yellow Taxi 2025
Taxi Zone Lookup
NOAA GHCND NYC 2025
  -> raw
  -> bronze
  -> silver
  -> quality gates
  -> gold star schema
  -> gold daily weather demand
```

Pergunta analitica final:

```text
As condicoes climaticas afetam a demanda por taxi em Nova York?
```

## 2. Principios Operacionais

Antes de executar, mantenha estas regras em mente:

- Raw guarda os arquivos baixados.
- Bronze converte raw para Delta mantendo o dado quase bruto.
- Silver limpa, padroniza, aplica Data Quality e enriquece os dados.
- Gold entrega tabelas modeladas para Power BI, EDA e ML.
- Data Quality durante a Silver separa validos, invalidos, quarantine e metrics.
- Validadores pos-Silver e pos-Gold confirmam se a tabela publicada esta pronta.
- Localmente, prefira sample/dev para testar logica.
- A execucao completa de TLC deve acontecer no Databricks.
- NOAA deve ser baixada com paginacao por offset.
- A fato nao deve juntar direto com varias estacoes NOAA.
- O clima deve ser consolidado para 1 linha por data antes do join com corridas.

## 3. Estrutura De Pastas Usada Na V2

```text
v2/
  config/
    paths.py
    sources.py
    spark.py

  pipelines/
    ingestion/
    bronze/
    silver/
    quality/
    gold/

  databricks/
    notebooks/

  notebooks/

  docs/

  data/
    raw/
    delta/
      bronze/
      silver/
      gold/
      quarantine/
      monitoring/
```

Importante:

```text
v2/data/raw
v2/data/delta
```

nao devem ser versionados no Git. Eles sao dados locais.

## 4. Pre-Requisitos Locais

Ambiente local esperado:

```text
Python 3.11
Poetry
Java compativel com PySpark
PySpark
Delta Lake
```

Instalar dependencias:

```bash
poetry install
```

Verificar comandos disponiveis:

```bash
poetry run ingest-nyc-tlc --dry-run
poetry run bronze-nyc-tlc --dry-run
poetry run silver-nyc-tlc --dry-run
poetry run gold-star-schema --dry-run
```

Token NOAA local em `.env`:

```text
NOAA_TOKEN=seu_token
```

O arquivo `.env` nao deve ir para o Git.

## 5. Fontes Do Projeto

### NYC TLC Yellow Taxi

Fonte publica:

```text
https://d37ci6vzurychx.cloudfront.net/trip-data
```

Padrao de arquivo:

```text
yellow_tripdata_2025-01.parquet
yellow_tripdata_2025-02.parquet
...
yellow_tripdata_2025-12.parquet
```

### Taxi Zone Lookup

Fonte publica:

```text
https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

### NOAA GHCND

API:

```text
https://www.ncei.noaa.gov/cdo-web/api/v2/data
```

Escopo V2.5:

```text
datasetid=GHCND
locationid=CITY:US360019
datatypeid=PRCP,TMAX,TMIN,SNOW,SNWD
units=metric
year=2025
```

Regra de paginacao:

```text
limit=1000
offset=1
offset=1001
offset=2001
...
ate downloaded_results == expected_count
```

## 6. Ordem Oficial Local Completa

Use esta ordem quando quiser rodar o pipeline local completo. Em maquina local,
a TLC completa pode ser pesada. Use com cuidado.

### 6.1 Ingestao Raw

NYC TLC:

```bash
poetry run ingest-nyc-tlc
```

Taxi Zone Lookup:

```bash
poetry run ingest-taxi-zone-lookup
```

NOAA NYC paginada:

```bash
poetry run ingest-noaa-weather \
  --year 2025 \
  --locationid CITY:US360019
```

Validacao manual esperada para NOAA:

```text
downloaded_results == expected_count
status = success
```

Resultado ja observado na V2.5:

```text
pages = 76
downloaded_results = 75991
expected_count = 75991
```

### 6.2 Bronze

NYC TLC:

```bash
poetry run bronze-nyc-tlc --skip-count
```

Taxi Zone Lookup:

```bash
poetry run bronze-taxi-zone-lookup --skip-count
```

NOAA:

```bash
poetry run bronze-noaa-weather --skip-count
```

Gate para seguir:

```text
Bronze Delta salvo com _delta_log
schema impresso sem erro
```

### 6.3 Silver

NYC TLC:

```bash
poetry run silver-nyc-tlc --skip-count
```

Taxi Zone Lookup:

```bash
poetry run silver-taxi-zone-lookup --skip-count
```

NOAA:

```bash
poetry run silver-noaa-weather --skip-count
```

Gate para seguir:

```text
Data Quality nao pode retornar FAIL
Silver Delta deve ser salva
quarantine e monitoring devem ser gerados
```

### 6.4 Validacao Pos-Silver

NYC TLC:

```bash
poetry run validate-silver-nyc-tlc --expected-days 365
```

Taxi Zone Lookup:

```bash
poetry run validate-silver-taxi-zone-lookup
```

NOAA:

```bash
poetry run validate-silver-noaa-weather
```

Gate para seguir:

```text
Todos os validadores Silver devem retornar PASS
```

### 6.5 Gold Star Schema

Criar:

```bash
poetry run gold-star-schema \
  --taxi-zone-lookup-input v2/data/delta/silver/nyc_tlc/taxi_zone_lookup \
  --noaa-input v2/data/delta/silver/noaa/ghcnd_nyc/2025 \
  --skip-count
```

Validar:

```bash
poetry run validate-gold-star-schema
```

Gate para seguir:

```text
dim_data = 365
dim_clima = 365
dim_localizacao = 265
fact_trips > 0
fact_trips.clima_id nulo = 0
chaves orfas = 0
```

### 6.6 Gold Daily Weather Demand

Criar:

```bash
poetry run gold-daily-weather-demand \
  --noaa-input v2/data/delta/silver/noaa/ghcnd_nyc/2025 \
  --skip-count
```

Validar:

```bash
poetry run validate-gold-daily-weather-demand
```

Gate final:

```text
daily_rows = 365
daily_days_without_weather = 0
daily_incomplete_alignment = 0
daily_total_trips > 0
```

## 7. Ordem Local Dev Com Sample

Use este fluxo para testar sem travar a maquina.

Comando automatico recomendado:

```bash
poetry run run-v2-dev-sample --dry-run
poetry run run-v2-dev-sample
```

Esse comando roda a Bronze TLC sample, Silver TLC sample, validadores Silver,
Gold Star Schema dev, validador da Gold dimensional, Gold Daily Weather Demand
dev e validador da Gold diaria.

O passo a passo manual equivalente fica abaixo.

### 7.1 Silver TLC Dev

```bash
poetry run silver-nyc-tlc \
  --input v2/data/delta/dev/bronze/nyc_tlc/yellow_sample/2025_01 \
  --output v2/data/delta/dev/silver/nyc_tlc/yellow_sample/2025_01 \
  --start-date 2025-01-01 \
  --end-date 2025-02-01 \
  --skip-count
```

Validar:

```bash
poetry run validate-silver-nyc-tlc \
  --input v2/data/delta/dev/silver/nyc_tlc/yellow_sample/2025_01 \
  --expected-days 2
```

Resultado esperado no dev atual:

```text
Silver NYC TLC validation: PASS
tlc_rows = 97060
tlc_out_of_year_rows = 0
tlc_distinct_days = 2
```

### 7.2 Validar Silver NOAA E Lookup

```bash
poetry run validate-silver-taxi-zone-lookup
poetry run validate-silver-noaa-weather
```

Resultados esperados:

```text
Taxi Zone Lookup = PASS
NOAA = PASS
NOAA distinct days = 365
NOAA distinct stations = 124
```

### 7.3 Gold Star Schema Dev

```bash
poetry run gold-star-schema \
  --tlc-input v2/data/delta/dev/silver/nyc_tlc/yellow_sample/2025_01 \
  --noaa-input v2/data/delta/silver/noaa/ghcnd_nyc/2025 \
  --taxi-zone-lookup-input v2/data/delta/silver/nyc_tlc/taxi_zone_lookup \
  --output v2/data/delta/dev/gold/star_schema/2025_01 \
  --skip-count
```

Validar:

```bash
poetry run validate-gold-star-schema \
  --input v2/data/delta/dev/gold/star_schema/2025_01
```

Resultado esperado:

```text
Gold Star Schema validation: PASS
dim_data = 365
dim_clima = 365
dim_localizacao = 265
fact_trips = 97060
fact_trips_null_clima_id = 0
```

### 7.4 Gold Daily Weather Demand Dev

```bash
poetry run gold-daily-weather-demand \
  --tlc-input v2/data/delta/dev/silver/nyc_tlc/yellow_sample/2025_01 \
  --noaa-input v2/data/delta/silver/noaa/ghcnd_nyc/2025 \
  --output v2/data/delta/dev/gold/daily_weather_demand/2025_01 \
  --skip-count
```

Validar:

```bash
poetry run validate-gold-daily-weather-demand \
  --input v2/data/delta/dev/gold/daily_weather_demand/2025_01
```

Resultado esperado:

```text
Gold Daily Weather Demand validation: PASS
daily_rows = 365
daily_total_trips = 97060
daily_days_with_demand = 2
daily_days_without_weather = 0
```

## 8. Como Interpretar Cada Gate

### Data Quality WARNING

`WARNING` nao significa necessariamente erro.

Exemplo observado:

```text
total_records = 99979
valid_records = 97060
invalid_records = 2919
quality_percentage = 97.08
status = WARNING
```

Significado:

```text
registros invalidos foram separados
registros validos seguiram para Silver
quarantine foi preenchida
metrics foi preenchida
pipeline pode continuar
```

### Data Quality FAIL

`FAIL` deve parar o pipeline.

Possiveis causas:

```text
schema critico ausente
dataset vazio
erro estrutural
regra critica violada
```

### Validador FAIL

Validador `FAIL` significa que a tabela publicada nao deve alimentar a proxima
camada.

Exemplo real:

```text
tlc_out_of_year_rows = 21
tlc_distinct_days = 3
```

Correcao aplicada:

```text
rerodar Silver TLC com filtro de data
```

## 9. Camadas De Saida

### Quarantine

Registros invalidos:

```text
v2/data/delta/quarantine/nyc_tlc/yellow/2025
v2/data/delta/quarantine/nyc_tlc/taxi_zone_lookup
v2/data/delta/quarantine/noaa/ghcnd_nyc/2025
```

### Monitoring

Metricas de qualidade:

```text
v2/data/delta/monitoring/quality/nyc_tlc/yellow/2025
v2/data/delta/monitoring/quality/nyc_tlc/taxi_zone_lookup
v2/data/delta/monitoring/quality/noaa/ghcnd_nyc/2025
```

### Gold Star Schema

Modelo dimensional:

```text
v2/data/delta/gold/star_schema/2025/
  dim_data
  dim_clima
  dim_localizacao
  fact_trips
```

### Gold Daily Weather Demand

Tabela diaria para EDA/ML:

```text
v2/data/delta/gold/daily_weather_demand/2025
```

No dev atual:

```text
v2/data/delta/dev/gold/daily_weather_demand/2025_01
```

## 10. Regras De Modelagem Da V2.5

A V2.5 e uma evolucao de modelagem dentro da pasta `v2`. Ela nao cria uma nova
estrutura de projeto. O objetivo foi melhorar a V2 usando varias estacoes NOAA
de NYC, mas mantendo a Gold em grao diario para evitar duplicacao de corridas.

### NOAA

```text
Silver NOAA = 1 linha por estacao/dia
```

### Clima Na Gold

```text
dim_clima = 1 linha por data
escopo_clima = NYC_consolidado
```

### Fato

```text
fact_trips junta com dim_clima por data
```

Regra importante:

```text
fact_trips nao junta direto com varias estacoes NOAA
```

Motivo:

```text
evitar duplicar corridas
```

Limite consciente:

```text
1 registro de clima diario representa todas as corridas daquele dia.
A V2.5 nao captura mudancas de clima ao longo do dia.
```

Evolucao futura V3:

```text
clima por hora ou faixa horaria
demanda por hora
join por data + hora
possivel evolucao para clima por zona/borough usando estacao mais proxima
```

## 11. Preparacao Para Databricks

Os notebooks Databricks ficam em:

```text
v2/databricks/notebooks/
```

Eles sao wrappers. A regra de negocio continua em:

```text
v2/pipelines/
```

Isso significa:

```text
Databricks notebook recebe parametros
Databricks notebook chama funcao Python/PySpark
funcao Python/PySpark executa a logica
```

Notebooks existentes:

```text
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
```

Esses wrappers permitem que o ADF pare o pipeline logo depois da Silver caso
algum contrato de dados falhe:

```text
validate_silver_nyc_tlc.py
validate_silver_taxi_zone_lookup.py
validate_silver_noaa_weather.py
```

## 12. Caminhos Recomendados Na Cloud

Opcao recomendada com Volumes:

```text
/Volumes/<catalog>/<schema>/<volume>/raw/nyc_tlc/yellow/2025
/Volumes/<catalog>/<schema>/<volume>/raw/nyc_tlc/taxi_zone_lookup
/Volumes/<catalog>/<schema>/<volume>/raw/noaa/ghcnd_nyc/2025

/Volumes/<catalog>/<schema>/<volume>/delta/bronze/nyc_tlc/yellow/2025
/Volumes/<catalog>/<schema>/<volume>/delta/bronze/nyc_tlc/taxi_zone_lookup
/Volumes/<catalog>/<schema>/<volume>/delta/bronze/noaa/ghcnd_nyc/2025

/Volumes/<catalog>/<schema>/<volume>/delta/silver/nyc_tlc/yellow/2025
/Volumes/<catalog>/<schema>/<volume>/delta/silver/nyc_tlc/taxi_zone_lookup
/Volumes/<catalog>/<schema>/<volume>/delta/silver/noaa/ghcnd_nyc/2025

/Volumes/<catalog>/<schema>/<volume>/delta/gold/star_schema/2025
/Volumes/<catalog>/<schema>/<volume>/delta/gold/daily_weather_demand/2025

/Volumes/<catalog>/<schema>/<volume>/delta/quarantine/nyc_tlc/yellow/2025
/Volumes/<catalog>/<schema>/<volume>/delta/quarantine/nyc_tlc/taxi_zone_lookup
/Volumes/<catalog>/<schema>/<volume>/delta/quarantine/noaa/ghcnd_nyc/2025

/Volumes/<catalog>/<schema>/<volume>/delta/monitoring/quality/nyc_tlc/yellow/2025
/Volumes/<catalog>/<schema>/<volume>/delta/monitoring/quality/nyc_tlc/taxi_zone_lookup
/Volumes/<catalog>/<schema>/<volume>/delta/monitoring/quality/noaa/ghcnd_nyc/2025
```

Alternativa com mount:

```text
/dbfs/mnt/raw/...
/dbfs/mnt/delta/...
```

Evite passar `abfss://...` diretamente para scripts de ingestion Python.

Motivo:

```text
downloaders Python escrevem via filesystem
abfss funciona melhor em leituras/escritas Spark
```

## 13. Ordem Recomendada No Azure Data Factory

Pipeline ADF chamando notebooks Databricks:

```text
1. ingest_nyc_tlc
2. ingest_taxi_zone_lookup
3. ingest_noaa_weather
4. bronze_nyc_tlc
5. bronze_taxi_zone_lookup
6. bronze_noaa_weather
7. silver_nyc_tlc
8. silver_taxi_zone_lookup
9. silver_noaa_weather
10. validate_silver_nyc_tlc
11. validate_silver_taxi_zone_lookup
12. validate_silver_noaa_weather
13. gold_star_schema
14. validate_gold_star_schema
15. gold_daily_weather_demand
16. validate_gold_daily_weather_demand
```

Observacao:

```text
os passos 10, 11 e 12 usam os wrappers Databricks validate_silver_*
criados em v2/databricks/notebooks/
```

Eles devem ser configurados como gates no ADF para impedir que a Gold rode com
Silver fora do contrato esperado.

## 14. Parametros ADF Por Etapa

### ingest_nyc_tlc

```text
year=2025
start_month=1
end_month=12
output=/Volumes/<catalog>/<schema>/<volume>/raw/nyc_tlc/yellow/2025
overwrite=false
dry_run=false
```

### ingest_taxi_zone_lookup

```text
output=/Volumes/<catalog>/<schema>/<volume>/raw/nyc_tlc/taxi_zone_lookup
overwrite=false
dry_run=false
```

### ingest_noaa_weather

```text
year=2025
locationid=CITY:US360019
output=/Volumes/<catalog>/<schema>/<volume>/raw/noaa/ghcnd_nyc/2025
secret_scope=kv-lakehouse
secret_key=noaa-token
datasetid=GHCND
storage_datasetid=GHCND_NYC
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

### bronze_taxi_zone_lookup

```text
input=/Volumes/<catalog>/<schema>/<volume>/raw/nyc_tlc/taxi_zone_lookup/taxi_zone_lookup.csv
output=/Volumes/<catalog>/<schema>/<volume>/delta/bronze/nyc_tlc/taxi_zone_lookup
mode=overwrite
skip_count=true
dry_run=false
```

### bronze_noaa_weather

```text
year=2025
datasetid=GHCND_NYC
input=/Volumes/<catalog>/<schema>/<volume>/raw/noaa/ghcnd_nyc/2025
output=/Volumes/<catalog>/<schema>/<volume>/delta/bronze/noaa/ghcnd_nyc/2025
mode=overwrite
skip_count=true
dry_run=false
```

### silver_nyc_tlc

```text
year=2025
input=/Volumes/<catalog>/<schema>/<volume>/delta/bronze/nyc_tlc/yellow/2025
output=/Volumes/<catalog>/<schema>/<volume>/delta/silver/nyc_tlc/yellow/2025
quarantine_output=/Volumes/<catalog>/<schema>/<volume>/delta/quarantine/nyc_tlc/yellow/2025
metrics_output=/Volumes/<catalog>/<schema>/<volume>/delta/monitoring/quality/nyc_tlc/yellow/2025
pipeline_run_id=@{pipeline().RunId}
mode=overwrite
start_date=2025-01-01
end_date=2026-01-01
skip_quality=false
skip_count=true
dry_run=false
```

### silver_taxi_zone_lookup

```text
input=/Volumes/<catalog>/<schema>/<volume>/delta/bronze/nyc_tlc/taxi_zone_lookup
output=/Volumes/<catalog>/<schema>/<volume>/delta/silver/nyc_tlc/taxi_zone_lookup
quarantine_output=/Volumes/<catalog>/<schema>/<volume>/delta/quarantine/nyc_tlc/taxi_zone_lookup
metrics_output=/Volumes/<catalog>/<schema>/<volume>/delta/monitoring/quality/nyc_tlc/taxi_zone_lookup
pipeline_run_id=@{pipeline().RunId}
mode=overwrite
skip_quality=false
skip_count=true
dry_run=false
```

### silver_noaa_weather

```text
year=2025
datasetid=GHCND_NYC
input=/Volumes/<catalog>/<schema>/<volume>/delta/bronze/noaa/ghcnd_nyc/2025
output=/Volumes/<catalog>/<schema>/<volume>/delta/silver/noaa/ghcnd_nyc/2025
quarantine_output=/Volumes/<catalog>/<schema>/<volume>/delta/quarantine/noaa/ghcnd_nyc/2025
metrics_output=/Volumes/<catalog>/<schema>/<volume>/delta/monitoring/quality/noaa/ghcnd_nyc/2025
pipeline_run_id=@{pipeline().RunId}
mode=overwrite
skip_quality=false
skip_count=true
dry_run=false
```

### validate_silver_nyc_tlc

```text
year=2025
input=/Volumes/<catalog>/<schema>/<volume>/delta/silver/nyc_tlc/yellow/2025
expected_days=365
min_rows=1
dry_run=false
```

### validate_silver_taxi_zone_lookup

```text
input=/Volumes/<catalog>/<schema>/<volume>/delta/silver/nyc_tlc/taxi_zone_lookup
expected_locations=265
dry_run=false
```

### validate_silver_noaa_weather

```text
year=2025
datasetid=GHCND_NYC
input=/Volumes/<catalog>/<schema>/<volume>/delta/silver/noaa/ghcnd_nyc/2025
expected_days=365
min_rows=365
min_stations=2
dry_run=false
```

### gold_star_schema

```text
year=2025
tlc_input=/Volumes/<catalog>/<schema>/<volume>/delta/silver/nyc_tlc/yellow/2025
noaa_input=/Volumes/<catalog>/<schema>/<volume>/delta/silver/noaa/ghcnd_nyc/2025
taxi_zone_lookup_input=/Volumes/<catalog>/<schema>/<volume>/delta/silver/nyc_tlc/taxi_zone_lookup
output=/Volumes/<catalog>/<schema>/<volume>/delta/gold/star_schema/2025
mode=overwrite
skip_count=true
dry_run=false
```

### validate_gold_star_schema

```text
year=2025
input=/Volumes/<catalog>/<schema>/<volume>/delta/gold/star_schema/2025
expected_days=365
expected_locations=265
min_fact_rows=1
allow_incomplete_weather=false
dry_run=false
```

### gold_daily_weather_demand

```text
year=2025
tlc_input=/Volumes/<catalog>/<schema>/<volume>/delta/silver/nyc_tlc/yellow/2025
noaa_input=/Volumes/<catalog>/<schema>/<volume>/delta/silver/noaa/ghcnd_nyc/2025
output=/Volumes/<catalog>/<schema>/<volume>/delta/gold/daily_weather_demand/2025
mode=overwrite
skip_count=true
dry_run=false
```

### validate_gold_daily_weather_demand

```text
year=2025
input=/Volumes/<catalog>/<schema>/<volume>/delta/gold/daily_weather_demand/2025
expected_days=365
min_days_with_demand=1
min_total_trips=1
allow_incomplete_weather=false
dry_run=false
```

## 15. Secrets No Azure

Local:

```text
.env
NOAA_TOKEN=...
```

Azure recomendado:

```text
Key Vault
  secret: noaa-token

Databricks Secret Scope
  scope: kv-lakehouse
  key: noaa-token
```

No notebook Databricks:

```python
dbutils.secrets.get(scope="kv-lakehouse", key="noaa-token")
```

Nao colocar token em:

```text
codigo
README
ADF em texto puro
Git
prints
notebooks commitados
```

## 16. Checklist Antes De Ir Para Azure

Antes de ativar o trial Azure, conferir:

```text
[ ] projeto local roda com sample dev
[ ] Silver TLC dev PASS
[ ] Silver NOAA PASS
[ ] Silver Taxi Zone Lookup PASS
[ ] Gold Star Schema dev PASS
[ ] Gold Daily Weather Demand dev PASS
[ ] token NOAA removido de qualquer arquivo versionado
[ ] .env ignorado pelo Git
[ ] README e docs explicam V2/V2.5
[ ] runbook atualizado
[ ] dicionario de dados atualizado
[ ] star schema v2.5 atualizado
[ ] decidir caminho cloud: Volumes ou /dbfs/mnt
[ ] testar wrappers validate_silver_* no Databricks real
```

## 17. Checklist Depois De Criar Azure

Criar recursos:

```text
[ ] Resource Group
[ ] Storage Account com ADLS Gen2
[ ] containers ou volumes
[ ] Databricks Workspace
[ ] cluster ou jobs cluster
[ ] Key Vault
[ ] Secret Scope Databricks
[ ] Azure Data Factory
[ ] Linked Service ADF -> Databricks
[ ] Git folder Databricks conectado ao repo
```

Executar primeiro em dry-run quando possivel:

```text
[ ] ingestion dry-run
[ ] bronze dry-run
[ ] silver dry-run
[ ] gold dry-run
```

Depois executar:

```text
[ ] ingestion raw
[ ] bronze
[ ] silver + data quality
[ ] validate silver
[ ] gold
[ ] validate gold
```

## 18. Troubleshooting

### Spark local travando

Use:

```text
--skip-count
sample dev
local[1]
```

Evite contar dados grandes localmente.

### PATH_NOT_FOUND

Conferir se a camada anterior foi executada.

Exemplo:

```text
Silver nao existe
-> rodar Bronze primeiro
-> rodar Silver depois
```

### NOAA incompleta

Conferir `_manifest.json`:

```text
downloaded_results
expected_count
download_complete
status
```

Esperado:

```text
downloaded_results == expected_count
download_complete = true
status = success
```

### `clima_id` nulo na fact

Verificar:

```text
Silver NOAA cobre 365 dias?
dim_clima tem 365 dias?
fact_trips esta filtrada para 2025?
join com dim_clima usa data?
```

### Duplicacao de corridas na Gold

Verificar se a fato juntou com:

```text
dim_clima consolidada por data
```

e nao com:

```text
Silver NOAA estacao/dia
```

### Dados TLC fora do ano

Rodar Silver com:

```bash
--start-date 2025-01-01
--end-date 2026-01-01
```

No sample de janeiro:

```bash
--start-date 2025-01-01
--end-date 2025-02-01
```

## 19. Proximo Uso Do Data Mart

Tabela recomendada para EDA/ML:

```text
gold/daily_weather_demand/2025
```

Perguntas iniciais:

```text
demanda media em dias com chuva vs sem chuva
correlacao entre precipitacao e qtd_corridas
correlacao entre temperatura e qtd_corridas
demanda em dias com neve
efeito de fim de semana
efeito de mes e sazonalidade
```

Para ML, o alvo natural e:

```text
qtd_corridas
```

Features candidatas:

```text
precipitacao_media_mm
precipitacao_max_mm
temp_media_c
temp_max_media_c
temp_min_media_c
teve_chuva
teve_neve
categoria_chuva
categoria_temperatura
fim_de_semana
mes
dia_semana_num
```

Importante:

```text
ML real deve usar TLC completa, nao apenas sample de 2 dias
```

## 20. Estado Atual Do Projeto Local

Estado validado no ciclo dev:

```text
Silver NYC TLC validation: PASS
Silver NOAA Weather validation: PASS
Silver Taxi Zone Lookup validation: PASS
Gold Star Schema validation: PASS
Gold Daily Weather Demand validation: PASS
```

Numeros observados:

```text
Silver TLC dev = 97060 corridas
Silver NOAA = 33074 linhas
Silver NOAA = 365 dias
Silver NOAA = 124 estacoes
dim_data = 365
dim_clima = 365
dim_localizacao = 265
fact_trips dev = 97060
daily_weather_demand dev = 365 linhas
daily_total_trips dev = 97060
daily_days_without_weather = 0
```

## 21. Proximo Passo Recomendado

Antes de Azure:

```text
1. rodar testes automatizados da V2
2. revisar notebooks Databricks atuais
3. criar ou revisar plano ADF final
4. manter `arquitetura_v1.jpg` como referencia historica da V1
5. depois iniciar infra Azure
```

Comando oficial de testes:

```bash
poetry run python -m unittest discover -s tests -p 'test_*.py' -t .
```

Quando Azure estiver pronto:

```text
1. subir repo no Databricks
2. configurar secrets
3. configurar paths cloud
4. rodar ingestion NOAA primeiro em dry-run
5. rodar pipeline completo com ADF
6. validar Gold
7. seguir para EDA, Power BI e ML
```
