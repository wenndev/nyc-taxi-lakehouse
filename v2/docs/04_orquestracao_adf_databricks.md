# Plano Cloud: ADF Orquestrando Databricks

Escolha definida: o Azure Data Factory vai orquestrar, mas a logica de ingestao
e paginacao fica no Databricks/Python.

A modelagem climatica da V2.5 e diaria: varias estacoes NOAA de NYC sao
consolidadas em 1 registro de clima por data antes de ligar clima com corridas.
Analise horaria fica planejada para uma futura V3.

## Desenho

```text
ADF Pipeline
  -> Databricks job: ingest NYC TLC
  -> Databricks job: ingest Taxi Zone Lookup
  -> Databricks job: ingest NOAA com offset
  -> Databricks job: Bronze
  -> Databricks job: Silver com Data Quality TLC, Lookup e NOAA
  -> Databricks job: Gold
```

## Por Que Nao Fazer a Paginacao NOAA Toda no ADF

Daria para fazer, mas ficaria mais trabalhoso:

- Web Activity para primeira chamada;
- leitura de `metadata.resultset.count`;
- calculo manual dos offsets;
- ForEach para cada offset;
- Copy Activity para cada pagina JSON.

Como a paginacao ja esta funcionando no script Python, e melhor deixar o ADF
apenas chamar o job Databricks.

## Job Databricks NYC TLC

Notebook:

```text
v2/databricks/notebooks/ingest_nyc_tlc.py
```

Parametros `baseParameters` no ADF:

```text
year=2025
start_month=1
end_month=12
output=/Volumes/<catalog>/<schema>/<volume>/raw/nyc_tlc/yellow/2025
overwrite=false
dry_run=false
```

## Job Databricks Taxi Zone Lookup

Notebook:

```text
v2/databricks/notebooks/ingest_taxi_zone_lookup.py
```

Parametros `baseParameters` no ADF:

```text
output=/Volumes/<catalog>/<schema>/<volume>/raw/nyc_tlc/taxi_zone_lookup
overwrite=false
dry_run=false
```

## Onde Fica a Paginacao

No script:

```text
v2/pipelines/ingestion/download_noaa_weather.py
```

Regra:

```text
limit=1000
offset=1
offset=1001
offset=2001
...
ate downloaded_results == expected_count
```

Validacao esperada:

```text
downloaded_results == expected_count
pagina 1 offset=1
pagina 2 offset=1001
pagina 3 offset=2001
...
ate a ultima pagina retornada pela API
```

## Caminhos no Databricks

Os scripts de ingestao sao downloaders Python puros. Eles escrevem arquivos via
filesystem. Por isso, para raw ingestion no Databricks, use um caminho de arquivo
montado ou volume:

```text
/Volumes/<catalog>/<schema>/<volume>/raw/nyc_tlc/yellow/2025
/Volumes/<catalog>/<schema>/<volume>/raw/nyc_tlc/taxi_zone_lookup
/Volumes/<catalog>/<schema>/<volume>/raw/noaa/ghcnd_nyc/2025
/dbfs/mnt/raw/nyc_tlc/yellow/2025
/dbfs/mnt/raw/nyc_tlc/taxi_zone_lookup
/dbfs/mnt/raw/noaa/ghcnd_nyc/2025
```

Evite passar `abfss://...` diretamente para os scripts de ingestion Python.
`abfss://` deve ser usado com Spark/Delta nas camadas Bronze, Silver e Gold, ou
entao por meio de volumes/mounts acessiveis como filesystem.

## Secrets

Local:

```text
.env
NOAA_TOKEN=...
```

Azure/Databricks:

```python
token = dbutils.secrets.get(scope="kv-lakehouse", key="noaa-token")
```

O token nao deve ir para ADF em texto puro e nao deve ser commitado.

## Parametros Esperados no Job Databricks NOAA

Notebook:

```text
v2/databricks/notebooks/ingest_noaa_weather.py
```

Parametros `baseParameters` no ADF:

```text
year=2025
locationid=CITY:US360019
output=/Volumes/<catalog>/<schema>/<volume>/raw/noaa/ghcnd_nyc/2025
secret_scope=kv-lakehouse
secret_key=noaa-token
storage_datasetid=GHCND_NYC
```

Parametros opcionais:

```text
start_date=
end_date=
datasetid=GHCND
datatypeids=PRCP,TMAX,TMIN,SNOW,SNWD
units=metric
limit=1000
initial_offset=1
overwrite=false
dry_run=false
```

## Ordem Recomendada dos Jobs

1. `ingest-nyc-tlc`
2. `ingest-taxi-zone-lookup`
3. `ingest-noaa-weather`
4. `bronze-nyc-tlc`
5. `bronze-taxi-zone-lookup`
6. `bronze-noaa-weather`
7. `silver-nyc-tlc` com Data Quality TLC
8. `silver-taxi-zone-lookup` com Data Quality Lookup
9. `silver-noaa-weather` com Data Quality NOAA
10. `validate-silver-nyc-tlc`
11. `validate-silver-taxi-zone-lookup`
12. `validate-silver-noaa-weather`
13. `gold-star-schema`
14. `validate-gold-star-schema`
15. `gold-daily-weather-demand`
16. `validate-gold-daily-weather-demand`

## O Que Ainda Falta Fazer Quando o Azure Voltar

- Criar Storage Account/ADLS.
- Definir containers ou volumes para `raw`, `bronze`, `silver` e `gold`.
- Criar Key Vault e secret do token NOAA.
- Criar Secret Scope no Databricks.
- Criar cluster/job Databricks.
- Fazer ADF chamar os jobs Databricks em sequencia.
- Ajustar parametros de caminhos para apontar para a cloud.
- Passar `quarantine_output`, `metrics_output` e `pipeline_run_id` para as
  Silvers TLC, Taxi Zone Lookup e NOAA no Databricks.
- Configurar os notebooks `validate_silver_*` como gates depois da Silver.

Roteiro operacional detalhado:

```text
v2/docs/03_plano_azure_databricks.md
```
