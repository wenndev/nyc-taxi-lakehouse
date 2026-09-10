# Confiabilidade E DataOps V2 / V2.5

Este documento explica como a V2 lida com falhas, reprocessamento e
observabilidade. A ideia e responder uma pergunta simples:

```text
Se o pipeline falhar, ele sabe se recuperar?
```

Na V2 local, a resposta e: parcialmente sim. O projeto ja tem varias bases de
confiabilidade. Na cloud, Azure Data Factory, Databricks Jobs e Azure Monitor
devem complementar essa logica com retries, alertas e historico operacional.

## 1. Por Que Isso Importa

Um pipeline de dados nao deve ser avaliado apenas por "rodou uma vez".

Um pipeline mais maduro precisa conseguir:

```text
1. identificar falhas
2. evitar duplicidade
3. separar dado ruim
4. reprocessar partes especificas
5. registrar metricas
6. permitir investigacao
7. voltar para um estado confiavel
```

No projeto NYC Taxi Lakehouse, isso importa principalmente por causa de:

```text
NOAA API paginada
volume alto da NYC TLC
Gold usada para BI e ML
necessidade de clima completo em 365 dias
risco de duplicar corridas se o join com clima for feito errado
```

## 2. Mapa De Confiabilidade Atual

| Tema | Status Na V2 | Onde Aparece |
|---|---|---|
| Retry | Implementado na NOAA | `download_noaa_weather.py` |
| Backoff | Implementado na NOAA | espera cresce por tentativa |
| Jitter | Implementado como opcional | `--retry-jitter-seconds` |
| Idempotencia | Implementada nas escritas Delta | `overwrite`, `replaceWhere` |
| Reprocessamento mensal | Implementado | `replace_month` |
| Deduplicacao | Implementada | Silver NOAA, Silver TLC, lookup, Gold |
| Data Quality | Implementada | `v2/pipelines/quality` |
| Quarantine | Implementada | `v2/data/delta/quarantine/...` |
| Metricas | Implementadas | `v2/data/delta/monitoring/quality/...` |
| Validadores | Implementados | Silver e Gold |
| Checkpoint operacional | Parcial | Delta log e manifestos |
| Alertas | Planejado para Azure | ADF, Databricks, Azure Monitor |
| SLA/SLO | Documentado aqui | criterios abaixo |

## 3. Retry, Backoff E Jitter

A ingestion da NOAA usa API. APIs podem falhar por timeout, instabilidade ou
limite temporario.

Por isso, a V2 possui:

```text
max_retries
sleep_seconds
retry_jitter_seconds
```

Exemplo:

```bash
poetry run ingest-noaa-weather \
  --year 2025 \
  --locationid CITY:US360019 \
  --max-retries 3 \
  --sleep-seconds 0.25 \
  --retry-jitter-seconds 1.0
```

Logica:

```text
espera = sleep_seconds * tentativa + jitter aleatorio
```

Isso reduz o risco de varias tentativas baterem na API exatamente no mesmo
intervalo.

## 4. Idempotencia

Idempotencia significa poder rodar novamente sem duplicar ou corromper dados.

Na V2, isso acontece de duas formas:

```text
1. overwrite controlado para cargas completas
2. replaceWhere para reprocessamento mensal
```

Exemplo:

```text
replace_month=1
replaceWhere = ano = 2025 AND mes = 1
```

Assim, se janeiro precisar ser corrigido, o pipeline substitui apenas a particao
de janeiro. Os outros meses publicados continuam preservados.

## 5. Deduplicacao

A deduplicacao evita publicar registros repetidos.

Na V2:

```text
Silver NOAA:
  1 linha por data_clima, id_estacao, tipo_dado

Silver TLC:
  remove duplicatas de negocio nas corridas

Silver Taxi Zone Lookup:
  1 linha por location_id

Gold dim_localizacao:
  1 linha por location_id enriquecido
```

## 6. Data Quality E Quarantine

A Data Quality acontece principalmente na Silver.

Fluxo:

```text
Bronze
  -> regras de qualidade
  -> validos seguem para Silver
  -> invalidos vao para quarantine
  -> metricas vao para monitoring
```

Se uma regra critica falhar, a Silver nao deve ser publicada.

Exemplos de falhas:

```text
TLC:
  data_hora_partida nula
  localizacao invalida
  valor total invalido
  distancia negativa

NOAA:
  data_clima nula
  id_estacao nulo
  temperatura fora de faixa plausivel
  duplicidade por estacao/data

Taxi Zone Lookup:
  location_id nulo
  location_id duplicado
  zona vazia
```

## 7. Manifestos E Rastreabilidade

A ingestion NOAA gera `_manifest.json`.

O manifesto identifica a consulta antes de salvar paginas. A retomada recusa
outro periodo, escopo ou paginacao no mesmo destino. `--overwrite` reinicia o
lote e remove suas paginas antigas; nao e uma troca atomica de versoes do RAW.
Estados `failed`, `incomplete` e `in_progress` nao autorizam publicar Bronze.
Detalhes e limites estao no [guia de ingestao](../pipelines/ingestion/README.md#retomada-e-reinicio-do-lote-noaa).

Ele registra:

```text
fonte
endpoint
parametros
limit
initial_offset
sleep_seconds
max_retries
retry_jitter_seconds
paginas baixadas
downloaded_results
expected_count
status
download_complete
```

Esse arquivo ajuda a provar que a paginacao terminou corretamente.

Regra pratica:

```text
downloaded_results == expected_count
download_complete == true
```

## 8. SLOs Do Projeto

SLO e uma meta objetiva de qualidade/confiabilidade do pipeline.

Para esta V2, os SLOs recomendados sao:

```text
SLO NOAA Raw:
  _manifest.json com download_complete = true
  downloaded_results igual a expected_count

SLO Silver NOAA:
  365 dias no ano
  pelo menos 2 estacoes
  0 datas nulas
  0 ids de estacao nulos
  0 duplicatas por estacao/dia

SLO Silver Taxi Zone Lookup:
  265 localizacoes
  0 location_id duplicado
  0 location_id nulo

SLO Gold Star Schema:
  dim_data com 365 dias
  dim_clima com 365 dias
  dim_localizacao com 265 zonas
  fact_trips sem clima_id nulo
  fact_trips sem chaves orfas

SLO Gold Daily Weather Demand:
  365 linhas
  1 linha por data
  0 dias sem clima
  qtd_corridas nao negativa
```

## 9. Recuperacao De Falhas

### Falha Na API NOAA

Sinais:

```text
timeout
HTTPError
URLError
download_complete = false
downloaded_results diferente de expected_count
```

Acao:

```bash
poetry run ingest-noaa-weather \
  --year 2025 \
  --locationid CITY:US360019 \
  --max-retries 3 \
  --sleep-seconds 0.25 \
  --retry-jitter-seconds 1.0
```

Se o raw ficou parcial e precisa baixar novamente:

```bash
poetry run ingest-noaa-weather \
  --year 2025 \
  --locationid CITY:US360019 \
  --overwrite
```

Depois, conferir `_manifest.json`.

### Falha De Data Quality Na Silver

Sinais:

```text
status = FAIL
Silver nao publicada
quarantine preenchida
metrics preenchida
```

Acao:

```text
1. abrir metrics
2. identificar regra que falhou
3. abrir quarantine
4. decidir se o problema e dado ruim ou regra muito rigida
5. corrigir a regra ou a origem
6. reprocessar a Silver
```

### Falha Em Um Mes Especifico

Usar `replace_month`.

Exemplo Silver TLC:

```bash
poetry run silver-nyc-tlc \
  --year 2025 \
  --replace-month 1 \
  --skip-count
```

Exemplo Gold:

```bash
poetry run gold-star-schema \
  --year 2025 \
  --replace-month 1 \
  --skip-count
```

### Falha Na Gold

Sinais:

```text
dim_data diferente de 365 dias
dim_clima diferente de 365 dias
fact_trips com clima_id nulo
fact_trips com chave orfa
daily_weather_demand com dia sem clima
```

Acao:

```text
1. validar Silver NOAA
2. validar Silver TLC
3. validar Taxi Zone Lookup
4. reprocessar Gold
5. rodar validadores Gold novamente
```

## 10. O Que Fica Para Azure

Quando o projeto for para Azure, estes pontos devem sair do nivel "local/manual"
e virar configuracao operacional:

```text
ADF:
  retry policy por activity
  timeout por activity
  dependencia entre jobs
  parametros por ambiente

Databricks Jobs:
  retries por task
  cluster auto-termination
  logs por run
  task values ou saidas JSON

Azure Monitor / Log Analytics:
  metricas de falha
  logs de pipeline
  alertas por erro
  alertas de custo

Key Vault:
  token NOAA fora do codigo
  secrets acessados por Secret Scope
```

## 11. Definicao De Pronto

Antes de considerar uma execucao pronta para BI/ML:

```text
1. ingestion NOAA com manifest completo
2. Bronze publicada
3. Silver publicada com Data Quality
4. validadores Silver com PASS
5. Gold Star Schema com PASS
6. Gold daily_weather_demand com PASS
7. sem clima_id nulo na fact
8. sem dias sem clima na base diaria
9. metricas e quarantine disponiveis para auditoria
```

## 12. Resumo

A V2 ja nao e apenas um pipeline que "roda".

Ela tem mecanismos para:

```text
detectar problema
evitar duplicidade
isolar dado ruim
reprocessar recortes
validar tabelas publicadas
registrar o que foi baixado
preparar alertas e monitoramento na cloud
```

O proximo salto de maturidade acontece no Azure, conectando essa base local com
ADF, Databricks Jobs, Key Vault e Azure Monitor.
