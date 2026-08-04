# Ingestion V2

Scripts que baixam dados brutos para `v2/data/raw`.

## NYC TLC

```bash
poetry run ingest-nyc-tlc --dry-run
poetry run ingest-nyc-tlc
```

## Taxi Zone Lookup

Baixa o CSV oficial pequeno usado para enriquecer a `dim_localizacao`.

```bash
poetry run ingest-taxi-zone-lookup --dry-run
poetry run ingest-taxi-zone-lookup
```

Saida padrao:

```text
v2/data/raw/nyc_tlc/taxi_zone_lookup/taxi_zone_lookup.csv
```

## NOAA

A ingestion NOAA usa a API CDO v2 e salva uma pagina por arquivo JSON.

Antes de rodar, configure o token:

```bash
export NOAA_TOKEN="seu_token"
```

Conferir a request V2.5 com varias estacoes de NYC:

```bash
poetry run ingest-noaa-weather \
  --year 2025 \
  --locationid CITY:US360019 \
  --dry-run
```

Baixar V2.5 usando paginacao:

```bash
poetry run ingest-noaa-weather \
  --year 2025 \
  --locationid CITY:US360019
```

Parametros importantes:

```text
--datasetid     dataset NOAA, padrao GHCND
--datatypeid    pode repetir; padrao PRCP, TMAX, TMIN, SNOW, SNWD
--stationid     pode repetir
--locationid    pode repetir
--limit         maximo 1000 na API CDO
--output        destino raw local
--storage-datasetid nome usado apenas para organizar a pasta local/cloud
```

Saida padrao V2.5 com `locationid CITY:US360019`:

```text
v2/data/raw/noaa/ghcnd_nyc/2025/
  page_000001_offset_000000001.json
  page_000002_offset_000001001.json
  ...
  _manifest.json
```
