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

## Taxi Zone Lookup

Entrada:

```text
v2/data/raw/nyc_tlc/taxi_zone_lookup/taxi_zone_lookup.csv
```

Saida:

```text
v2/data/delta/bronze/nyc_tlc/taxi_zone_lookup
```

Comando:

```bash
poetry run bronze-taxi-zone-lookup --dry-run
poetry run bronze-taxi-zone-lookup
```

Responsabilidade desta etapa:

- ler o CSV oficial Taxi Zone Lookup;
- preservar as colunas originais;
- salvar em Delta.

## NOAA Weather

Entrada:

```text
v2/data/raw/noaa/ghcnd_nyc/2025/page_*.json
```

Saida:

```text
v2/data/delta/bronze/noaa/ghcnd_nyc/2025
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
- exigir `_manifest.json` do endpoint CDO com `units=metric` e registrar
  `unidades_noaa=metric` em cada pagina Bronze;
- salvar em Delta.

O manifesto e lido pelo filesystem Hadoop da sessao Spark, pois o leitor JSON
ignora arquivos com prefixo `_`. Isso usa a configuracao de storage da sessao;
o acesso S3 ainda precisa ser validado no Glue.

Nao ha conversao numerica nesta camada. Manifesto ausente, unidade desconhecida
ou `standard` impedem a escrita. O cliente de ingestao ainda permite baixar
`standard` para RAW, mas esse lote nao pode entrar no pipeline metrico atual.
A verificacao de unidades nao substitui a verificacao de completude do lote.
