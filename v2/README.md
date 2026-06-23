# NYC Taxi Lakehouse V2

V2 e a refatoracao local do projeto.

Nesta primeira etapa, o foco e apenas consumir os arquivos reais da NYC TLC de 2025.

## Estrutura inicial

```text
v2/
  config/
    paths.py
    sources.py

  pipelines/
    ingestion/
      download_nyc_tlc.py
    bronze/
      bronze_nyc_tlc.py
    silver/
      silver_nyc_tlc.py

  notebooks/
    01_inspect_bronze_nyc_tlc.ipynb
    02_inspect_silver_nyc_tlc.ipynb

  data/
    raw/
      nyc_tlc/
        yellow/
          2025/
    delta/
```

## Primeira coisa a fazer

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

## Depois

Depois que os arquivos estiverem em `v2/data/raw/nyc_tlc/yellow/2025`, a proxima etapa sera criar a Bronze V2 em Delta Lake.

Criar Bronze NYC TLC:

```bash
poetry run bronze-nyc-tlc --dry-run
poetry run bronze-nyc-tlc
```

Proxima etapa:

```text
v2/pipelines/silver/
```

A Silver vai ler `v2/data/delta/bronze` e salvar em `v2/data/delta/silver`.

Criar Silver NYC TLC:

```bash
poetry run silver-nyc-tlc --dry-run
poetry run silver-nyc-tlc --skip-count
```

## Visualizacao com Jupyter

Abrir JupyterLab:

```bash
poetry run jupyter lab
```

Notebook inicial:

```text
v2/notebooks/01_inspect_bronze_nyc_tlc.ipynb
```
