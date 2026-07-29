# Silver V2

A Silver V2 vai ler a Bronze Delta e aplicar as regras de limpeza e padronizacao.

## NYC TLC

Entrada:

```text
v2/data/delta/bronze/nyc_tlc/yellow/2025
```

Saida planejada:

```text
v2/data/delta/silver/nyc_tlc/yellow/2025
```

## Ordem de tratamento

1. Renomear colunas para PT-BR.
2. Filtrar colunas criticas nulas ou invalidas.
3. Tratar nulos numericos nao criticos.
4. Filtrar valores impossiveis.
5. Tratar nulos categoricos.
6. Criar colunas derivadas para analise temporal.
7. Criar descricoes e flags semanticas.
8. Remover duplicatas de negocio.
9. Salvar Delta Silver.

Implementacao atual:

```text
Bronze Delta
  -> renomeacao de colunas
  -> filtro de colunas criticas
  -> tratamento de nulos numericos
  -> filtro de valores impossiveis
  -> tratamento de nulos categoricos
  -> criacao de colunas derivadas
  -> criacao de descricoes e flags semanticas
  -> remocao de duplicatas de negocio
  -> Silver Delta
```

Colunas derivadas criadas:

```text
data_viagem
ano
mes
mes_nome
dia_mes
hora_partida
dia_semana_num
dia_semana_nome
fim_de_semana
periodo_dia
horario_pico
duracao_minutos
distancia_km
valor_por_km
velocidade_media_kmh
```

Colunas semanticas criadas:

```text
tipo_pagamento_desc
tipo_tarifa_desc
categoria_distancia
categoria_duracao
categoria_valor_total
viagem_com_passageiro
viagem_sem_passageiro
valor_tarifa_zero
qtd_passageiros_suspeita
viagem_distancia_zero
viagem_distancia_alta
viagem_duracao_zero
viagem_duracao_alta
viagem_valor_alto
velocidade_media_alta
registro_suspeito
```

Regra de nulos:

```text
Colunas criticas nulas removem o registro.
Colunas numericas nao criticas nulas viram 0 e podem ser marcadas como suspeitas.
```

Comando:

```bash
poetry run silver-nyc-tlc --dry-run
poetry run silver-nyc-tlc --skip-count
```

Teste local com menos dados:

```bash
poetry run silver-nyc-tlc --start-date 2025-01-01 --end-date 2025-02-01 --limit 100000 --skip-count
```

Esse comando processa apenas uma amostra de janeiro de 2025. Use ele primeiro para
validar a regra da Silver sem forcar a maquina local a processar o ano inteiro.

## NOAA Weather

Entrada:

```text
v2/data/delta/bronze/noaa/ghcnd/2025
```

Saida:

```text
v2/data/delta/silver/noaa/ghcnd/2025
```

Ordem de tratamento:

1. Explodir o array `results` vindo da API NOAA.
2. Padronizar campos para `data_clima`, `id_estacao`, `tipo_dado` e `valor`.
3. Remover registros sem campos obrigatorios.
4. Gerar uma linha diaria por estacao.
5. Criar colunas de chuva, temperatura, neve e flags para analise.

Comando:

```bash
poetry run silver-noaa-weather --dry-run
poetry run silver-noaa-weather
```

Teste ainda mais leve, criando uma Bronze de amostra antes:

```bash
poetry run bronze-nyc-tlc --input v2/data/raw/nyc_tlc/yellow/2025/yellow_tripdata_2025-01.parquet --output v2/data/delta/dev/bronze/nyc_tlc/yellow_sample/2025_01 --limit 100000 --skip-count
poetry run silver-nyc-tlc --input v2/data/delta/dev/bronze/nyc_tlc/yellow_sample/2025_01 --output v2/data/delta/dev/silver/nyc_tlc/yellow_sample/2025_01 --skip-count
```
