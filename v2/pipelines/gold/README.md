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
