# Historico da Refatoracao V2

Arquivo criado para permitir retomar o projeto em outra maquina sem depender do historico da conversa.

Nenhum token, senha ou segredo foi registrado aqui.

## Objetivo Original

Refatorar o projeto `nyc-taxi-lakehouse` da V1 para uma V2.

Contexto:

- A V1 foi feita em Azure, Azure Data Factory, Databricks, PySpark e Delta Lake.
- A infraestrutura Azure foi perdida apos expirar o trial.
- A reconstrucao comecou localmente para validar codigo e logica antes de voltar ao Azure.
- A V1 trabalhava com dados de 2024.
- A V2 passou a trabalhar com dados de 2025.

Pergunta central do projeto:

```text
As condicoes climaticas afetam a demanda por taxi em Nova York?
```

## Diagnostico da V1

A V1 esta preservada em `v1/`.

Pontos identificados:

- Os scripts da V1 usam `2024`.
- A Gold 2024 foi publicada no Kaggle.
- A `fact_trips_final` ficou com cerca de 40 milhoes de registros.
- A `dim_clima` ficou com apenas 38 dias.
- Isso gerou grande volume de `clima_id` nulo na fato.

Causa principal do problema de clima:

- A NOAA foi chamada para uma regiao/cidade com varias estacoes.
- A API tinha limite de 1000 registros por chamada.
- Sem paginacao por `offset`, cada chamada retornava apenas uma parte dos dados.
- O resultado final cobriu poucos dias por mes.

Decisao para a V2:

- Usar paginacao na API NOAA.
- Comecar com uma unica estacao: Central Park.
- Manter a logica Bronze, Silver e Gold.
- Escrever codigo PySpark reutilizavel localmente e depois no Databricks.

## Organizacao da V2

Foi decidido manter:

```text
v1/  # projeto original
v2/  # refatoracao local-first
```

Nao usar `src/` por enquanto, para evitar confusao.

Estrutura principal da V2:

```text
v2/
  config/
  pipelines/
    ingestion/
    bronze/
    silver/
    gold/
  notebooks/
  docs/
  data/
    raw/
    delta/
```

Significado das pastas de dados:

- `v2/data/raw`: arquivos brutos baixados das fontes.
- `v2/data/delta/bronze`: dados brutos convertidos para Delta.
- `v2/data/delta/silver`: dados limpos e enriquecidos.
- `v2/data/delta/gold`: dados analiticos prontos para perguntas de negocio.
- `v2/data/delta/dev`: saidas leves de teste local.

As pastas `v2/data/raw` e `v2/data/delta` estao no `.gitignore`.

## Ambiente Local

Foi instalado e configurado Poetry.

Dependencias principais:

- `pyspark==4.1.1`
- `delta-spark==4.2.0`
- `jupyterlab`
- `ipykernel`

Comandos registrados no Poetry:

```bash
poetry run ingest-nyc-tlc
poetry run ingest-noaa-weather
poetry run bronze-nyc-tlc
poetry run silver-nyc-tlc
poetry run bronze-noaa-weather
poetry run silver-noaa-weather
poetry run gold-daily-weather-demand
poetry run gold-star-schema
```

## NYC TLC 2025

Fonte:

```text
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet
```

Meses:

```text
01 ate 12
```

Script:

```text
v2/pipelines/ingestion/download_nyc_tlc.py
```

Status:

- Os 12 arquivos Parquet de 2025 foram baixados localmente.
- A Bronze TLC foi criada em Delta.
- Foi validado que existem 365 dias de 2025.
- Existem algumas datas sujas fora de 2025 no bruto, o que e esperado em dado real e deve ser tratado na Silver/Gold.

Checagem obtida:

```text
dias_distintos=369
data_min=2007-12-05
data_max=2025-12-31
dias_2025=365
```

## Bronze NYC TLC

Script:

```text
v2/pipelines/bronze/bronze_nyc_tlc.py
```

Responsabilidade:

- Ler Parquets brutos.
- Fazer casts simples em colunas que vieram com tipo instavel.
- Salvar Delta Bronze.

Saida oficial:

```text
v2/data/delta/bronze/nyc_tlc/yellow/2025
```

## Silver NYC TLC

Script:

```text
v2/pipelines/silver/silver_nyc_tlc.py
```

Melhorias feitas:

- Renomeacao de colunas para PT-BR.
- Filtro de colunas criticas.
- Tratamento de nulos numericos nao criticos.
- Tratamento de categorias nulas.
- Filtro de valores impossiveis.
- Remocao de duplicatas de negocio.
- Criacao de colunas temporais.
- Criacao de metricas de duracao, distancia em km, valor por km e velocidade media.
- Criacao de categorias e flags semanticas.

Exemplos de colunas criadas:

```text
data_viagem
ano
mes
dia_mes
hora_partida
dia_semana_nome
fim_de_semana
periodo_dia
horario_pico
duracao_minutos
distancia_km
valor_por_km
velocidade_media_kmh
tipo_pagamento_desc
tipo_tarifa_desc
registro_suspeito
```

Observacao:

A Silver TLC completa e pesada para rodar localmente. Foi criado um fluxo dev com amostra de janeiro para validar a logica sem travar a maquina.

## NOAA 2025

Decisao:

Usar uma unica estacao meteorologica para simplificar a pergunta de negocio.

Estacao:

```text
Central Park
GHCND:USW00094728
```

Script de ingestao:

```text
v2/pipelines/ingestion/download_noaa_weather.py
```

Token:

- Deve ficar em `NOAA_TOKEN`.
- Nao deve ser escrito no codigo.
- Localmente pode ficar em `.env`, que esta ignorado pelo Git.
- No Azure, deve ir para Key Vault/Secret Scope.

Comando usado:

```bash
poetry run ingest-noaa-weather \
  --year 2025 \
  --stationid GHCND:USW00094728
```

Resultado da ingestao:

```text
expected_count=1824
downloaded_results=1824
pages=2
```

Cobertura validada:

```text
rows=1824
distinct_dates=365
min_date=2025-01-01
max_date=2025-12-31
```

Por tipo de dado:

```text
PRCP=365
TMAX=365
TMIN=365
SNOW=365
SNWD=364
```

Unico ponto incompleto:

```text
2025-09-17 sem SNWD
```

## Bronze NOAA

Script:

```text
v2/pipelines/bronze/bronze_noaa_weather.py
```

Responsabilidade:

- Ler JSONs brutos paginados da NOAA.
- Preservar `metadata` e `results`.
- Adicionar `arquivo_origem`.
- Adicionar `data_processamento_bronze`.
- Salvar Delta Bronze.

Saida:

```text
v2/data/delta/bronze/noaa/ghcnd/2025
```

## Silver NOAA

Script:

```text
v2/pipelines/silver/silver_noaa_weather.py
```

Responsabilidade:

- Explodir o array `results`.
- Padronizar campos.
- Remover registros sem campos obrigatorios.
- Gerar uma linha diaria por data e estacao.
- Criar colunas de chuva, temperatura, neve e flags.

Saida:

```text
v2/data/delta/silver/noaa/ghcnd/2025
```

Validacao:

```text
linhas=365
data_min=2025-01-01
data_max=2025-12-31
registros_incompletos=1
```

## Gold Diaria

Script:

```text
v2/pipelines/gold/gold_daily_weather_demand.py
```

Ideia:

Usar um calendario completo de 2025 como base para alinhar todos os dias do ano.

Grao:

```text
1 linha por dia
```

Composicao:

```text
calendario 2025
  + demanda diaria da TLC
  + clima diario da NOAA
```

Saida oficial:

```text
v2/data/delta/gold/daily_weather_demand/2025
```

Teste dev executado com amostra da TLC:

```bash
poetry run gold-daily-weather-demand \
  --tlc-input v2/data/delta/dev/silver/nyc_tlc/yellow_sample/2025_01 \
  --output v2/data/delta/dev/gold/daily_weather_demand/2025_01
```

Resultado:

```text
linhas=365
data_min=2025-01-01
data_max=2025-12-31
dias_sem_clima=0
dias_sem_corridas=363
dias_incompletos=1
```

`dias_sem_corridas=363` ocorreu porque o teste usou apenas amostra da TLC, nao a Silver completa.

## Como Retomar em Outra Maquina

1. Clonar o repositorio e entrar na branch da V2.
2. Rodar `poetry install`.
3. Rebaixar os dados ou copiar `v2/data/raw` e `v2/data/delta` da maquina atual.
4. Se for rebaixar NOAA, configurar `NOAA_TOKEN` no terminal.
5. Rodar os comandos da V2 pelo Poetry.

Comandos principais:

```bash
poetry install
poetry run ingest-nyc-tlc
poetry run ingest-noaa-weather --year 2025 --stationid GHCND:USW00094728
poetry run bronze-nyc-tlc --skip-count
poetry run bronze-noaa-weather --skip-count
poetry run silver-noaa-weather
poetry run gold-daily-weather-demand --dry-run
poetry run gold-star-schema --dry-run
```

Para testar sem processar tudo:

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

poetry run gold-daily-weather-demand \
  --tlc-input v2/data/delta/dev/silver/nyc_tlc/yellow_sample/2025_01 \
  --output v2/data/delta/dev/gold/daily_weather_demand/2025_01

poetry run gold-star-schema \
  --tlc-input v2/data/delta/dev/silver/nyc_tlc/yellow_sample/2025_01 \
  --output v2/data/delta/dev/gold/star_schema/2025_01 \
  --skip-count
```

## Ligacao Com Azure Depois

A ideia nao e reescrever tudo do zero no Azure.

O que deve mudar:

- caminhos locais para caminhos `abfss://`;
- token NOAA para Key Vault/Secret Scope;
- execucao local para Databricks Jobs/Workflows ou ADF;
- possivel uso de Unity Catalog;
- otimizacoes Delta como `OPTIMIZE` e `ZORDER`.

O que deve ser reaproveitado:

- funcoes PySpark de Bronze, Silver e Gold;
- regras de limpeza;
- paginacao NOAA;
- logica de calendario completo;
- logica de alinhamento por data.

## Pendencias

- Criar dicionario de dados.
- Criar notebook de inspecao para NOAA.
- Executar Silver TLC completa no Databricks.
- Executar Gold completa no Databricks.
- Enriquecer `dim_localizacao` com taxi zone lookup.
- Planejar dataset analitico para ML.
- Planejar pipelines incrementais.
- Planejar particionamento Bronze.
- Implementar infraestrutura Azure novamente.
- Configurar CI/CD.
