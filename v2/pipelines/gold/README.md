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
v2/data/delta/silver/noaa/ghcnd_nyc/2025
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
- consolidar a NOAA para uma linha de clima NYC por data;
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
v2/data/delta/silver/noaa/ghcnd_nyc/2025
v2/data/delta/silver/nyc_tlc/taxi_zone_lookup
```

Saida:

```text
v2/data/delta/gold/star_schema/2025
```

Comando:

```bash
poetry run gold-star-schema --dry-run
poetry run gold-star-schema \
  --taxi-zone-lookup-input v2/data/delta/silver/nyc_tlc/taxi_zone_lookup
```

Validacao automatica depois da Gold:

```bash
poetry run validate-gold-star-schema --dry-run
poetry run validate-gold-star-schema
```

Esse check falha se encontrar:

- `dim_data` ou `dim_clima` diferente de 365 linhas para 2025;
- `dim_localizacao` diferente de 265 zonas oficiais;
- chaves duplicadas nas dimensoes;
- FKs nulas ou orfas na `fact_trips`;
- dias com clima incompleto na `dim_clima`.

Teste local com amostra da TLC:

```bash
poetry run gold-star-schema \
  --tlc-input v2/data/delta/dev/silver/nyc_tlc/yellow_sample/2025_01 \
  --taxi-zone-lookup-input v2/data/delta/silver/nyc_tlc/taxi_zone_lookup \
  --output v2/data/delta/dev/gold/star_schema/2025_01
```

Na V2, a `dim_localizacao` usa o Taxi Zone Lookup oficial para trazer:

```text
borough
zona
zona_servico
localizacao_sem_lookup
```

A `fact_trips` inclui colunas que faltavam na V1:

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

Regra importante: a `fact_trips` nao junta diretamente com varias estacoes NOAA.
A `dim_clima` e criada depois da consolidacao diaria, mantendo 1 `clima_id` por
data e evitando duplicacao de corridas.
