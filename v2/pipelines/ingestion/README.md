# Ingestion V2

Scripts que baixam dados brutos para `v2/data/raw`.

## NYC TLC

```bash
poetry run ingest-nyc-tlc --dry-run
poetry run ingest-nyc-tlc
```

## NOAA

A ingestion NOAA usa a API CDO v2 e salva uma pagina por arquivo JSON.

Antes de rodar, configure o token:

```bash
export NOAA_TOKEN="seu_token"
```

Conferir a request sem chamar a API:

```bash
poetry run ingest-noaa-weather \
  --year 2025 \
  --locationid CITY:US360019 \
  --dry-run
```

Baixar usando paginacao:

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
```

Saida padrao:

```text
v2/data/raw/noaa/ghcnd/2025/
  page_000001_offset_000000001.json
  page_000002_offset_000001001.json
  _manifest.json
```
