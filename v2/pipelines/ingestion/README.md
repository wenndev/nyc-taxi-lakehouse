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
--max-retries   quantidade de tentativas por request
--sleep-seconds base do backoff entre tentativas
--retry-jitter-seconds variacao aleatoria somada ao backoff
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

### Retomada e reinicio do lote NOAA

Para retomar uma execucao interrompida, repita o mesmo comando, sem
`--overwrite`. O downloader compara o manifesto antes de reutilizar paginas:
endpoint, dataset, datas, tipos de dado, estacoes/localidades, unidades,
`limit`, `initial_offset` e `storage_datasetid` devem corresponder. A ordem dos
filtros e as configuracoes de retry nao mudam a identidade da consulta.

Se a consulta for diferente, a execucao falha antes de alterar o lote existente
ou chamar a API. Use outro `--output` para preservar esse lote. Paginas sem
manifesto, ou com manifesto invalido, tambem nao sao reutilizadas automaticamente.
Manifestos antigos que ainda contenham a identidade completa da consulta podem
ser reutilizados mesmo sem os campos mais novos de status.

`--overwrite` significa reiniciar o lote: remove o manifesto anterior e somente
os arquivos `page_*_offset_*.json` do destino antes de baixar novamente. Isso
evita sobras quando a nova consulta retorna menos paginas. Nao ha rollback do
lote anterior: para preserva-lo, prefira um novo diretorio. Outros arquivos no
destino nao sao removidos.

O manifesto e gravado antes da primeira chamada HTTP e registra:

- `in_progress`: execucao iniciada; ainda nao pode ser publicada.
- `failed`: erro durante a execucao; as paginas ja salvas permitem uma nova tentativa.
- `incomplete`: contagem recebida diferente da esperada; retorno diferente de zero.
- `success` com `download_complete=true`: todas as observacoes esperadas foram contadas.

Uma interrupcao abrupta pode deixar `in_progress`; repetir a mesma consulta
revalida as paginas presentes. Se a interrupcao ocorrer durante a limpeza de
`--overwrite`, podem restar paginas sem manifesto: reinicie com `--overwrite`
ou use outro destino, sem associar essas paginas a uma consulta nova.

Metadados de pagina inconsistentes, alteracao do total entre paginas e arquivos
extras fora da sequencia impedem sucesso. Uma pagina curta pode deixar o lote
`incomplete`; reinicie o download para solicita-la novamente. Nao execute
Bronze sobre lotes `failed`, `incomplete` ou `in_progress`: a Bronze consulta o
manifesto para validar unidades, mas ainda nao bloqueia por status ou
completude automaticamente.

Execute apenas um downloader por diretorio de destino. Esta protecao nao e um
lock para execucoes concorrentes, um checksum dos arquivos ou uma garantia de
snapshot imutavel da API. Cobertura de dias/estacoes continua sendo validada nas
etapas de qualidade.

Testes sem API real:

```bash
poetry run python -m unittest tests.v2.pipelines.ingestion.test_download_noaa_weather -v
```
