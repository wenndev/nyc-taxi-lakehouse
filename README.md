# NYC Taxi Lakehouse

Pipeline Lakehouse para analisar corridas de taxi amarelo de Nova York junto com dados climaticos historicos da NOAA.

O objetivo analitico do projeto e responder:

```text
As condicoes climaticas afetam a demanda por taxi em Nova York?
```

## Status

O repositorio possui duas fases do mesmo projeto:

| Versao | Periodo | Status | Descricao |
|---|---:|---|---|
| V1 | 2024 | Preservada | Projeto original em Azure, ADF, Databricks, PySpark e Delta Lake. |
| V2/V2.5 | 2025 | Preparada para cloud | Reconstrucao local-first com Poetry, PySpark e Delta Lake, preparada para execucao full no Azure Databricks. |

A V1 gerou a Gold publicada no Kaggle:

[NYC Taxi Trips 2024 - Gold Layer Star Schema](https://www.kaggle.com/datasets/delzin/nyc-taxi-trips-2024-gold-layer-star-schema)

A V2 esta sendo desenvolvida em [v2/](v2/README.md).

## Como Ler A Documentacao

Se voce esta chegando agora no projeto, a ordem mais natural e:

1. Este `README.md` - contexto geral do repositorio.
2. [v2/README.md](v2/README.md) - estrutura da V2 e comandos principais.
3. [v2/docs/00_visao_geral.md](v2/docs/00_visao_geral.md) - historia da V1, problema da NOAA e desenho V2/V2.5.
4. [v2/docs/02_dicionario_dados.md](v2/docs/02_dicionario_dados.md) - tabelas, campos e regras de negocio.
5. READMEs das camadas em `v2/pipelines/` - ingestion, bronze, silver, quality, gold e dev.
6. [v2/docs/01_runbook_execucao.md](v2/docs/01_runbook_execucao.md) - ordem para executar e validar.
7. [v2/databricks/README.md](v2/databricks/README.md) - wrappers preparados para Databricks.
8. [v2/docs/03_plano_azure_databricks.md](v2/docs/03_plano_azure_databricks.md) - plano de retorno para Azure.
9. [v2/docs/04_orquestracao_adf_databricks.md](v2/docs/04_orquestracao_adf_databricks.md) - ADF orquestrando Databricks.
10. [v2/docs/08_arquitetura_azure_v2.md](v2/docs/08_arquitetura_azure_v2.md) - explicacao da arquitetura Azure V2.
11. [v2/docs/arquitetura_azure_v2.excalidraw](v2/docs/arquitetura_azure_v2.excalidraw) - diagrama editavel no Excalidraw.
12. [v2/docs/09_migracao_aws_local_first.md](v2/docs/09_migracao_aws_local_first.md) - ponte entre execucao local e AWS.
13. [v2/docs/07_confiabilidade_dataops.md](v2/docs/07_confiabilidade_dataops.md) - confiabilidade, recuperacao e SLOs.

Arquivos de apoio:

- [v2/docs/README.md](v2/docs/README.md) - mapa da pasta de documentos.
- [v2/docs/05_referencias_tecnicas.md](v2/docs/05_referencias_tecnicas.md) - notas praticas de Spark, Delta, Databricks e Azure.
- [v2/docs/08_arquitetura_azure_v2.md](v2/docs/08_arquitetura_azure_v2.md) - arquitetura planejada para Azure.
- [v2/docs/arquitetura_azure_v2.excalidraw](v2/docs/arquitetura_azure_v2.excalidraw) - versao editavel do diagrama de arquitetura.
- [v2/docs/09_migracao_aws_local_first.md](v2/docs/09_migracao_aws_local_first.md) - plano de retomada em casa e migracao incremental para AWS.
- [v2/docs/07_confiabilidade_dataops.md](v2/docs/07_confiabilidade_dataops.md) - confiabilidade, recuperacao e SLOs da V2.
- [v2/docs/06_historico_refatoracao.md](v2/docs/06_historico_refatoracao.md) - diario tecnico da refatoracao.

## Fontes

### NYC TLC

Dados publicos de corridas de taxi amarelo e referencia oficial de zonas.

Na V2, o ano usado e 2025. Os arquivos seguem o padrao:

```text
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-02.parquet
...
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-12.parquet
```

O projeto tambem usa o `taxi_zone_lookup.csv` da NYC TLC para enriquecer a
`dim_localizacao` com borough, zona e zona de servico:

```text
https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

Pagina oficial:

[TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

### NOAA

Dados climaticos da API CDO da NOAA.

Na V2.5, a estrategia atual usa varias estacoes NOAA dentro do recorte de NYC:

```text
NOAA varias estacoes de NYC
  -> paginacao por offset
  -> Silver NOAA com 1 linha por estacao/dia
  -> agregacao diaria de clima NYC
  -> dim_clima com 1 linha por data
```

A regra principal da modelagem e que a `fact_trips` nao deve juntar diretamente
com varias estacoes NOAA. Antes do join, o clima precisa ser consolidado para uma
linha por dia. Isso evita duplicar corridas.

Limite consciente da V2.5: o clima e diario. Portanto, 1 registro de clima
representa todas as corridas daquele dia. Mudancas de clima ao longo do dia
ficam planejadas para uma futura V3 com granularidade horaria.

Modelagem proposta:

[v2/docs/star_schema_v2.5.excalidraw](v2/docs/star_schema_v2.5.excalidraw)

Preview em PNG:

[v2/docs/star_schema_v2.5.excalidraw.png](v2/docs/star_schema_v2.5.excalidraw.png)

## Arquitetura

A arquitetura segue o modelo Medallion:

```text
raw
  -> bronze
  -> silver
  -> gold
```

Na V2 local:

```text
v2/data/raw/...
v2/data/delta/bronze/...
v2/data/delta/silver/...
v2/data/delta/gold/...
```

No Azure depois, os dados ficam no ADLS. Nos notebooks Databricks, a opcao
recomendada para os downloaders Python e usar caminhos de filesystem apoiados em
ADLS, como Volumes:

```text
/Volumes/<catalog>/<schema>/<volume>/raw/...
/Volumes/<catalog>/<schema>/<volume>/delta/bronze/...
/Volumes/<catalog>/<schema>/<volume>/delta/silver/...
/Volumes/<catalog>/<schema>/<volume>/delta/gold/...
```

Para leituras/escritas Spark tambem e possivel adaptar para caminhos cloud do
ADLS. A logica PySpark deve ser reaproveitada. O que muda na cloud e
principalmente configuracao, secrets, orquestracao e infraestrutura.

## Estrutura

```text
nyc-taxi-lakehouse/
  v1/
    bronze/
    silver/
    gold/
    pipeline/
    data/
    docs/

  v2/
    config/
      paths.py
      sources.py
      spark.py

    pipelines/
      ingestion/
      bronze/
      silver/
      quality/
      gold/

    databricks/
      notebooks/

    notebooks/
    docs/
    data/
      raw/      # ignorado no Git
      delta/    # ignorado no Git

  pyproject.toml
  README.md
```

## Como Rodar a V2 Localmente

Localmente, use a V2 principalmente para validar logica, samples e qualidade dos
dados. A execucao completa da TLC 2025 deve acontecer no Databricks, porque o
volume de dados e alto.

Runbook completo com todos os comandos:

[v2/docs/01_runbook_execucao.md](v2/docs/01_runbook_execucao.md)

Instalar dependencias:

```bash
poetry install
```

Para usar a NOAA localmente, crie `.env` a partir de `.env.example` e preencha:

```text
NOAA_TOKEN=seu_token_aqui
```

Fluxo recomendado para validar localmente ate a Silver:

```bash
poetry run ingest-nyc-tlc --start-month 1 --end-month 1
poetry run ingest-taxi-zone-lookup
poetry run ingest-noaa-weather \
  --year 2025 \
  --locationid CITY:US360019
poetry run bronze-nyc-tlc --skip-count
poetry run bronze-taxi-zone-lookup --skip-count
poetry run bronze-noaa-weather --skip-count
poetry run silver-nyc-tlc --skip-count
poetry run silver-taxi-zone-lookup
poetry run silver-noaa-weather
```

Para gerar Gold localmente, prefira o fluxo dev com sample mostrado na secao
`Teste Local Leve` ou use o runbook completo.

Validacoes principais, depois que as respectivas camadas existirem:

```bash
poetry run validate-silver-nyc-tlc --expected-days 365
poetry run validate-silver-taxi-zone-lookup
poetry run validate-silver-noaa-weather
poetry run validate-gold-star-schema
poetry run validate-gold-daily-weather-demand
```

Testes automatizados:

```bash
poetry run python -m unittest discover -s tests -p 'test_*.py' -t .
```

Visualizar a relacao clima x demanda:

```bash
poetry run jupyter lab
```

Abrir:

```text
v2/notebooks/04_eda_weather_demand.ipynb
```

## Teste Local Leve

A Silver completa da TLC pode ser pesada localmente. Para validar sem travar a maquina:

```bash
poetry run run-v2-dev-sample
```

Esse comando roda o fluxo dev de ponta a ponta com amostra de janeiro. Ele
reaproveita os scripts oficiais das camadas.

O equivalente manual e:

```bash
poetry run bronze-nyc-tlc \
  --input v2/data/raw/nyc_tlc/yellow/2025/yellow_tripdata_2025-01.parquet \
  --output v2/data/delta/dev/bronze/nyc_tlc/yellow_sample/2025_01 \
  --limit 100000 \
  --skip-count

poetry run silver-nyc-tlc \
  --input v2/data/delta/dev/bronze/nyc_tlc/yellow_sample/2025_01 \
  --output v2/data/delta/dev/silver/nyc_tlc/yellow_sample/2025_01 \
  --skip-count

poetry run gold-star-schema \
  --tlc-input v2/data/delta/dev/silver/nyc_tlc/yellow_sample/2025_01 \
  --taxi-zone-lookup-input v2/data/delta/silver/nyc_tlc/taxi_zone_lookup \
  --output v2/data/delta/dev/gold/star_schema/2025_01 \
  --skip-count

poetry run gold-daily-weather-demand \
  --tlc-input v2/data/delta/dev/silver/nyc_tlc/yellow_sample/2025_01 \
  --output v2/data/delta/dev/gold/daily_weather_demand/2025_01 \
  --skip-count
```

## Validacoes Atuais da V2

TLC 2025:

```text
dias_2025 = 365
```

NOAA 2025:

```text
escopo = CITY:US360019
paginas = 76
downloaded_results = 75991
expected_count = 75991
dias distintos = 365
estacoes distintas = 124
periodo = 2025-01-01 ate 2025-12-31
```

Silver NOAA V2.5:

```text
linhas = 33074
grao = 1 linha por estacao/data
```

Clima consolidado para Gold:

```text
linhas = 365
grao = 1 linha por data
min_estacoes_dia = 75
max_estacoes_dia = 108
media_estacoes_dia = 90.61
dias_clima_incompleto = 0
```

Gold diaria dev:

```text
linhas = 365
dias_sem_clima = 0
```

Gold Star Schema dev:

```text
dim_data = 365
dim_clima = 365
dim_localizacao = 265
fact_trips = 97060
fact_trips.clima_id_nulo = 0
fact_trips.data_id_nulo = 0
fact_trips.localizacao_partida_id_nulo = 0
fact_trips.localizacao_chegada_id_nulo = 0
```

## Principais Melhorias da V2

- Separacao clara entre V1 preservada e V2 refatorada.
- Projeto local-first com Poetry.
- Ingestao NYC TLC 2025 automatizada.
- Ingestao NOAA com paginacao por `offset`.
- Fluxo V2.5 para varias estacoes NOAA de NYC.
- Data Quality TLC, Taxi Zone Lookup e NOAA entre Bronze e Silver, com quarentena e metricas.
- Fonte Taxi Zone Lookup com Bronze, Silver e Data Quality proprios.
- Bronze e Silver em Delta Lake para TLC, Taxi Zone Lookup e NOAA.
- Silver TLC com colunas temporais, duracao, distancia em km, pagamento, flags e categorias.
- Silver NOAA com clima diario em uma linha por estacao/data.
- Gold consolida a NOAA para uma linha de clima NYC por data.
- V2.5 documenta o limite de clima diario e deixa V3 horaria como evolucao.
- Gold diaria usando calendario completo de 2025 como base.
- Gold dimensional com `dim_data`, `dim_clima`, `dim_localizacao` enriquecida e `fact_trips`.
- Wrappers Databricks para Ingestion, Bronze, Silver, Gold e validadores.

## Problemas da V1 Que a V2 Resolve

Na V1, a `dim_clima` ficou com apenas 38 registros, causando grande quantidade de `clima_id` nulo na fato.

Na V2:

- a NOAA usa paginacao;
- a cobertura climatica tem 365 dias;
- a V2.5 consolida varias estacoes antes de ligar clima com corridas;
- a Gold diaria usa calendario completo;
- a Gold dimensional liga `fact_trips` com `dim_clima` por data;
- o join entre clima e demanda ocorre por `data`;
- dias incompletos ficam marcados por flag.

## Proximos Passos

- Recriar infraestrutura Azure seguindo `v2/docs/03_plano_azure_databricks.md`.
- Executar Silver e Gold completas no Databricks.
- Validar a `dim_localizacao` enriquecida na execucao full do Databricks.
- Criar camada ML usando a Gold diaria.
- Adicionar CI/CD com GitHub Actions para deploy de notebooks/scripts no Databricks.

## Observacoes Importantes

Os dados em `v2/data/raw` e `v2/data/delta` nao sao versionados no Git. Para continuar em outra maquina, sera necessario rebaixar os dados ou copiar essas pastas manualmente.

Tokens e credenciais nao devem ser commitados. Localmente use variavel de ambiente. No Azure, use Key Vault e Secret Scope do Databricks.
