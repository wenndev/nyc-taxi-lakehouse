# NYC Taxi Lakehouse V2

V2 e a refatoracao local-first do projeto.

Nesta versao, o projeto saiu do recorte de 2024 da V1 e passou a trabalhar com
dados de 2025. A logica principal continua sendo Bronze, Silver e Gold, mas agora
com codigo PySpark reaproveitavel localmente e depois no Azure Databricks.

Historico tecnico da refatoracao:

```text
v2/docs/historico_refatoracao_v2.md
```

Plano cloud com ADF orquestrando Databricks:

```text
v2/docs/plano_cloud_adf_databricks.md
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
      download_noaa_weather.py
    bronze/
      bronze_nyc_tlc.py
      bronze_noaa_weather.py
    silver/
      silver_nyc_tlc.py
      silver_noaa_weather.py
    gold/
      gold_daily_weather_demand.py

  notebooks/
    01_inspect_bronze_nyc_tlc.ipynb
    02_inspect_silver_nyc_tlc.ipynb

  data/
    raw/
      nyc_tlc/
        yellow/
          2025/
      noaa/
        ghcnd/
          2025/
    delta/
      bronze/
      silver/
      gold/
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

Baixar clima da NOAA para 2025, usando a estacao Central Park:

```bash
poetry run ingest-noaa-weather \
  --year 2025 \
  --stationid GHCND:USW00094728
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

Criar Silver NOAA:

```bash
poetry run silver-noaa-weather --dry-run
poetry run silver-noaa-weather
```

## Gold

A primeira Gold V2 alinha todos os dias de 2025 usando um calendario completo como
base. Ela junta demanda diaria da TLC com clima diario da NOAA.

```bash
poetry run gold-daily-weather-demand --dry-run
poetry run gold-daily-weather-demand
```

Teste local leve com amostra da TLC:

```bash
poetry run gold-daily-weather-demand \
  --tlc-input v2/data/delta/dev/silver/nyc_tlc/yellow_sample/2025_01 \
  --output v2/data/delta/dev/gold/daily_weather_demand/2025_01
```

Resultado esperado para o ano completo:

```text
365 linhas
1 linha por dia
join por data entre demanda diaria e clima diario
```

## Evolucoes da V1 ja enderecadas

- Paginacao da API NOAA com `offset`.
- Clima 2025 com 365 dias para Central Park.
- Silver TLC com colunas temporais, duracao, passageiros, pagamento e flags.
- Gold diaria usando calendario completo para evitar perda de dias no join.

## Visualizacao com Jupyter

Abrir JupyterLab:

```bash
poetry run jupyter lab
```

Notebook inicial:

```text
v2/notebooks/01_inspect_bronze_nyc_tlc.ipynb
```
