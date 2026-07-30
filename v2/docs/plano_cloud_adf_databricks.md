# Plano Cloud: ADF Orquestrando Databricks

Escolha definida: o Azure Data Factory vai orquestrar, mas a logica de ingestao
e paginacao fica no Databricks/Python.

## Desenho

```text
ADF Pipeline
  -> Databricks job: ingest NYC TLC
  -> Databricks job: ingest NOAA com offset
  -> Databricks job: Bronze
  -> Databricks job: Silver
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

Teste local ja validado:

```text
expected=1824
pagina 1 offset=1    -> 1000 registros
pagina 2 offset=1001 -> 824 registros
total=1824
```

## Caminhos no Databricks

Os scripts de ingestao sao downloaders Python puros. Eles escrevem arquivos via
filesystem. Por isso, para raw ingestion no Databricks, use um caminho de arquivo
montado ou volume:

```text
/Volumes/<catalog>/<schema>/<volume>/raw/noaa/ghcnd/2025
/dbfs/mnt/raw/noaa/ghcnd/2025
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
stationid=GHCND:USW00094728
output=/Volumes/<catalog>/<schema>/<volume>/raw/noaa/ghcnd/2025
secret_scope=kv-lakehouse
secret_key=noaa-token
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
2. `ingest-noaa-weather`
3. `bronze-nyc-tlc`
4. `bronze-noaa-weather`
5. `silver-nyc-tlc`
6. `silver-noaa-weather`
7. `gold-daily-weather-demand`
8. `gold-star-schema`

## O Que Ainda Falta Fazer Quando o Azure Voltar

- Criar Storage Account/ADLS.
- Definir containers ou volumes para `raw`, `bronze`, `silver` e `gold`.
- Criar Key Vault e secret do token NOAA.
- Criar Secret Scope no Databricks.
- Criar cluster/job Databricks.
- Fazer ADF chamar os jobs Databricks em sequencia.
- Ajustar parametros de caminhos para apontar para a cloud.

Roteiro operacional detalhado:

```text
v2/docs/plano_execucao_azure_v2.md
```
