# Data Quality V2

Camada modular de qualidade para validar dados antes da publicacao na Silver.

Implementacoes atuais:

- NOAA Weather.
- NYC TLC Yellow Taxi.
- NYC TLC Taxi Zone Lookup.

## Onde Entra

```text
Bronze Delta
  -> padronizacao inicial da Silver
  -> Data Quality
      -> valid_records
      -> invalid_records em quarentena
      -> metricas em monitoring
  -> Silver Delta
```

A Bronze continua preservada. A Silver so recebe registros validos. Em caso de
`FAIL`, as metricas e a quarentena sao gravadas, mas a Silver nao e publicada.

## Contrato Principal

```python
result = validate_noaa_data(
    df=noaa_normalized_df,
    config=NOAAQualityConfig(),
    pipeline_run_id=run_id,
)

result = validate_tlc_data(
    df=tlc_renamed_df,
    config=TLCQualityConfig.for_year(2025),
    pipeline_run_id=run_id,
)

result = validate_taxi_zone_lookup_data(
    df=taxi_zone_lookup_df,
    config=TaxiZoneLookupQualityConfig(),
    pipeline_run_id=run_id,
)
```

Retorno:

```text
result.valid_records
result.invalid_records
result.metrics
result.status
```

## Graos Validados

NOAA recebe o DataFrame normalizado pela Silver, antes da agregacao:

```text
1 linha = 1 observacao NOAA por data/estacao/tipo_dado
```

Colunas esperadas:

```text
data_clima
id_estacao
tipo_dado
valor
atributos
arquivo_origem
data_processamento_bronze
```

NYC TLC recebe o DataFrame com colunas ja renomeadas para PT-BR, antes das
colunas derivadas:

```text
1 linha = 1 corrida de taxi
```

Colunas criticas da TLC:

```text
id_vendedor
data_hora_partida
data_hora_chegada
id_local_partida
id_local_chegada
valor_total
```

Taxi Zone Lookup recebe a referencia oficial ja padronizada pela Silver:

```text
1 linha = 1 location_id da NYC TLC
```

Colunas esperadas:

```text
location_id
borough
zona
zona_servico
```

## Regras Atuais

NOAA:

- DataFrame vazio.
- Colunas esperadas ausentes.
- Tipos incompatíveis.
- Registros completamente vazios.
- Nulos em campos obrigatorios.
- Data invalida ou futura.
- Estacao ausente.
- Tipo de dado fora de `PRCP`, `TMAX`, `TMIN`, `SNOW`, `SNWD`.
- Valor climatico fora dos limites configurados.
- Chave composta duplicada: `data_clima`, `id_estacao`, `tipo_dado`.

Latitude e longitude nao foram implementadas porque essas colunas nao existem no
schema real usado pela NOAA no projeto.

NYC TLC:

- DataFrame vazio.
- Colunas esperadas ausentes.
- Tipos incompativeis.
- Registros completamente vazios.
- Nulos em campos criticos.
- Vendedor invalido.
- Local de partida ou chegada fora do range da Taxi Zone Lookup.
- Chegada anterior a partida.
- Data futura.
- Corrida fora do ano configurado.
- Valores fora de limites plausiveis: passageiros, distancia, tarifa, total e
  duracao.
- Duplicata pela chave de negocio:
  `id_vendedor`, `data_hora_partida`, `id_local_partida`, `id_local_chegada`,
  `valor_total`.

Taxi Zone Lookup:

- DataFrame vazio.
- Colunas esperadas ausentes.
- Tipos incompativeis.
- Registros completamente vazios.
- Nulos ou strings vazias em campos obrigatorios.
- `location_id` fora do range oficial da TLC.
- Duplicata por `location_id`.
- Qualquer regra critica bloqueia a publicacao da Silver Lookup.

## Saidas Locais

Quarentena:

```text
v2/data/delta/quarantine/noaa/ghcnd_nyc/2025
v2/data/delta/quarantine/nyc_tlc/yellow/2025
v2/data/delta/quarantine/nyc_tlc/taxi_zone_lookup
```

Metricas:

```text
v2/data/delta/monitoring/quality/noaa/ghcnd_nyc/2025
v2/data/delta/monitoring/quality/nyc_tlc/yellow/2025
v2/data/delta/monitoring/quality/nyc_tlc/taxi_zone_lookup
```

Esses caminhos sao Delta locais e estao ignorados no Git.

## Status

Padrao configurado:

```text
PASS    >= 99%
WARNING >= 95% e < 99%
FAIL    < 95%
```

Schema incompativel e DataFrame vazio causam `FAIL`. Regras linha a linha entram
na porcentagem de qualidade. Na NOAA, algumas regras tambem sao criticas. Na TLC,
os registros invalidos sao removidos da Silver e auditados na quarentena; o
pipeline so bloqueia se a qualidade ficar abaixo do limite de `FAIL`.

## Validacao Atual

Execucao local com NOAA V2.5:

```text
pipeline_status = PASS
total_records = 75991
valid_records = 75991
invalid_records = 0
quality_percentage = 100.0
quarantine_rows = 0
```

Execucao local com amostra dev da TLC:

```text
pipeline_status = WARNING
total_records = 100000
valid_records = 97060
invalid_records = 2940
quality_percentage = 97.06
```

Esse `WARNING` e aceitavel na amostra porque os registros invalidos foram
isolados em quarentena e a qualidade ficou acima do limite minimo de publicacao.

## Testes

```bash
poetry run python -m unittest tests.v2.pipelines.quality.test_noaa_validator
poetry run python -m unittest tests.v2.pipelines.quality.test_tlc_validator
poetry run python -m unittest tests.v2.pipelines.quality.test_taxi_zone_lookup_validator
poetry run python -m unittest discover -s tests -p 'test_*.py' -t .
```

Os testes usam pequenos DataFrames Spark locais e nao acessam API externa nem Azure.
