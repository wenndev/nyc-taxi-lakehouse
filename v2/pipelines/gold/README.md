# Gold V2

A Gold V2 junta dados tratados da Silver em tabelas analiticas.

## Daily Weather Demand

Esta tabela usa um calendario completo do ano como base e alinha:

- demanda diaria da NYC TLC;
- clima diario da NOAA;
- flags para dias sem corrida ou sem clima.

Entrada:

```text
v2/data/delta/silver/nyc_tlc/yellow/2025
v2/data/delta/silver/noaa/ghcnd/2025
```

Saida:

```text
v2/data/delta/gold/daily_weather_demand/2025
```

Comando:

```bash
poetry run gold-daily-weather-demand --dry-run
poetry run gold-daily-weather-demand
```

Teste local com amostra da TLC:

```bash
poetry run gold-daily-weather-demand \
  --tlc-input v2/data/delta/dev/silver/nyc_tlc/yellow_sample/2025_01 \
  --output v2/data/delta/dev/gold/daily_weather_demand/2025_01
```

Responsabilidade desta etapa:

- garantir 365 linhas para 2025;
- evitar `clima_id` nulo causado por falta de cobertura da NOAA;
- deixar uma base simples para responder se clima afeta demanda por taxi.

## Star Schema

Modelo dimensional da V2, inspirado na Gold da V1.

Tabelas:

```text
dim_data
dim_clima
dim_localizacao
fact_trips
```

Entrada:

```text
v2/data/delta/silver/nyc_tlc/yellow/2025
v2/data/delta/silver/noaa/ghcnd/2025
```

Saida:

```text
v2/data/delta/gold/star_schema/2025
```

Comando:

```bash
poetry run gold-star-schema --dry-run
poetry run gold-star-schema
```

Teste local com amostra da TLC:

```bash
poetry run gold-star-schema \
  --tlc-input v2/data/delta/dev/silver/nyc_tlc/yellow_sample/2025_01 \
  --output v2/data/delta/dev/gold/star_schema/2025_01
```

Na V2, a `fact_trips` inclui colunas que faltavam na V1:

```text
data_hora_partida
data_hora_chegada
duracao_minutos
qtd_passageiros
tipo_pagamento
tipo_pagamento_desc
```

Chaves usadas no modelo:

```text
data_id = data no formato yyyyMMdd
clima_id = data do registro climatico no formato yyyyMMdd
localizacao_id = ID oficial de zona da NYC TLC
```

Essa escolha evita criar IDs com janela global no Spark e deixa as tabelas mais
estaveis para execucao local e Databricks.
