# Bronze V2

A Bronze V2 le os dados brutos em `v2/data/raw` e grava tabelas em Delta Lake dentro de `v2/data/delta`.

## NYC TLC

Entrada:

```text
v2/data/raw/nyc_tlc/yellow/2025/*.parquet
```

Saida:

```text
v2/data/delta/bronze/nyc_tlc/yellow/2025
```

Conferir caminhos sem executar Spark:

```bash
poetry run bronze-nyc-tlc --dry-run
```

Criar Bronze:

```bash
poetry run bronze-nyc-tlc
```

Criar Bronze sem fazer `count()` no final:

```bash
poetry run bronze-nyc-tlc --skip-count
```

Responsabilidade desta etapa:

- ler Parquets brutos da NYC TLC;
- converter `passenger_count`, `RatecodeID` e `payment_type` para inteiro;
- salvar em Delta.

O tratamento de nulos, filtros, deduplicacao e renomeacao ficam para a Silver.

## NOAA Weather

Entrada:

```text
v2/data/raw/noaa/ghcnd/2025/page_*.json
```

Saida:

```text
v2/data/delta/bronze/noaa/ghcnd/2025
```

Conferir caminhos sem executar Spark:

```bash
poetry run bronze-noaa-weather --dry-run
```

Criar Bronze:

```bash
poetry run bronze-noaa-weather
```

Responsabilidade desta etapa:

- ler os JSONs brutos paginados da NOAA;
- preservar `metadata` e `results`;
- adicionar arquivo de origem e timestamp de processamento;
- salvar em Delta.
