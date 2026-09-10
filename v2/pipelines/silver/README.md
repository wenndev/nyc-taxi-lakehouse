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
2. Executar Data Quality no grao corrida.
3. Enviar registros invalidos para quarentena.
4. Enviar metricas para monitoring.
5. Publicar somente registros validos na Silver.
6. Tratar nulos numericos nao criticos.
7. Tratar nulos categoricos.
8. Criar colunas derivadas para analise temporal.
9. Criar descricoes e flags semanticas.
10. Remover duplicatas de negocio por seguranca.
11. Salvar Delta Silver.

Implementacao atual:

```text
Bronze Delta
  -> renomeacao de colunas
  -> Data Quality TLC
  -> valid_records
  -> invalid_records em quarentena
  -> metricas em monitoring
  -> tratamento de nulos numericos
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
Colunas criticas nulas mandam o registro para quarentena.
Colunas numericas nao criticas nulas viram 0 e podem ser marcadas como suspeitas.
```

Saidas de Data Quality:

```text
v2/data/delta/quarantine/nyc_tlc/yellow/2025
v2/data/delta/monitoring/quality/nyc_tlc/yellow/2025
```

Comando:

```bash
poetry run silver-nyc-tlc --dry-run
poetry run silver-nyc-tlc --skip-count
poetry run validate-silver-nyc-tlc --dry-run
poetry run validate-silver-nyc-tlc --expected-days 365
```

Desativar Data Quality apenas para debug:

```bash
poetry run silver-nyc-tlc --skip-quality
```

Teste local com menos dados:

```bash
poetry run silver-nyc-tlc --start-date 2025-01-01 --end-date 2025-02-01 --limit 100000 --skip-count
```

Esse comando processa apenas uma amostra de janeiro de 2025. Use ele primeiro para
validar a regra da Silver sem forcar a maquina local a processar o ano inteiro.

## Taxi Zone Lookup

Entrada:

```text
v2/data/delta/bronze/nyc_tlc/taxi_zone_lookup
```

Saida:

```text
v2/data/delta/silver/nyc_tlc/taxi_zone_lookup
```

Ordem de tratamento:

1. Renomear `LocationID`, `Borough`, `Zone` e `service_zone`.
2. Padronizar strings e cast de `location_id`.
3. Executar Data Quality.
4. Enviar registros invalidos para quarentena.
5. Publicar somente registros validos na Silver.

Saidas de Data Quality:

```text
v2/data/delta/quarantine/nyc_tlc/taxi_zone_lookup
v2/data/delta/monitoring/quality/nyc_tlc/taxi_zone_lookup
```

Comando:

```bash
poetry run silver-taxi-zone-lookup --dry-run
poetry run silver-taxi-zone-lookup
poetry run validate-silver-taxi-zone-lookup --dry-run
poetry run validate-silver-taxi-zone-lookup
```

## NOAA Weather

A Silver exige `unidades_noaa=metric` em todas as paginas Bronze antes de
explodir os resultados. Unidade ausente, nula, desconhecida ou `standard`
bloqueia a publicacao, mesmo com `--skip-quality`. A coluna de controle nao
precisa ser adicionada a configuracao das observacoes DQ: ela e validada antes
da normalizacao, que preserva o schema de observacao existente.

A API CDO ja escala/converte a resposta quando `units=metric` e informado;
nao aplicamos uma segunda divisao por dez ou conversao de temperatura.
Referencia: [parametro units da API NOAA](https://www.ncei.noaa.gov/cdo-web/webservices/v2#data).

Bronze antiga sem `unidades_noaa` deve ser reconstruida a partir do RAW e seu
manifesto metrico. Nao preencha a coluna manualmente assumindo a unidade e nao
refaca o download se os JSONs e o manifesto correto ja estiverem disponiveis:

```bash
poetry run bronze-noaa-weather --year 2025 --mode overwrite --skip-count
poetry run silver-noaa-weather --year 2025 --mode overwrite --skip-count
poetry run validate-silver-noaa-weather --year 2025
```

Esses comandos substituem as tabelas NOAA nos destinos configurados. Confira
os caminhos com `--dry-run` antes. A migracao dos dados locais nao e realizada
automaticamente pelo commit.

Entrada:

```text
v2/data/delta/bronze/noaa/ghcnd_nyc/2025
```

Saida:

```text
v2/data/delta/silver/noaa/ghcnd_nyc/2025
```

Ordem de tratamento:

1. Explodir o array `results` vindo da API NOAA.
2. Padronizar campos para `data_clima`, `id_estacao`, `tipo_dado` e `valor`.
3. Executar Data Quality no grao observacao NOAA.
4. Enviar registros invalidos para quarentena.
5. Enviar metricas para monitoring.
6. Publicar somente registros validos na Silver.
7. Gerar uma linha diaria por estacao.
8. Criar colunas de chuva, temperatura, neve e flags para analise.

Saidas de Data Quality:

```text
v2/data/delta/quarantine/noaa/ghcnd_nyc/2025
v2/data/delta/monitoring/quality/noaa/ghcnd_nyc/2025
```

Comando:

```bash
poetry run silver-noaa-weather --dry-run
poetry run silver-noaa-weather
poetry run validate-silver-noaa-weather --dry-run
poetry run validate-silver-noaa-weather
```

Desativar Data Quality apenas para debug:

```bash
poetry run silver-noaa-weather --skip-quality
```

Teste ainda mais leve, criando uma Bronze de amostra antes:

```bash
poetry run bronze-nyc-tlc --input v2/data/raw/nyc_tlc/yellow/2025/yellow_tripdata_2025-01.parquet --output v2/data/delta/dev/bronze/nyc_tlc/yellow_sample/2025_01 --limit 100000 --skip-count
poetry run silver-nyc-tlc --input v2/data/delta/dev/bronze/nyc_tlc/yellow_sample/2025_01 --output v2/data/delta/dev/silver/nyc_tlc/yellow_sample/2025_01 --skip-count
poetry run validate-silver-nyc-tlc --input v2/data/delta/dev/silver/nyc_tlc/yellow_sample/2025_01 --expected-days 2
```

## Validadores Pos-Silver

Os validadores Pos-Silver conferem a tabela ja publicada. Eles nao substituem o
Data Quality da Silver; eles verificam se a entrega final esta pronta para a Gold.

Ordem recomendada:

```text
Bronze
  -> Silver com Data Quality
  -> Validate Silver
  -> Gold
  -> Validate Gold
```

O validador da TLC confere:

- colunas obrigatorias da Silver final;
- viagens apenas dentro do ano esperado;
- locais entre 1 e 265;
- valores e duracao validos;
- duplicatas de negocio removidas.

O validador da NOAA confere:

- 365 dias para o ano completo;
- 1 linha por estacao/dia;
- minimo esperado de estacoes;
- datas dentro do ano;
- metricas climaticas em faixas plausiveis.

O validador do Taxi Zone Lookup confere:

- 265 zonas oficiais;
- `location_id` unico;
- `borough`, `zona` e `zona_servico` preenchidos.
