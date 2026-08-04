# Data Quality V2

Camada modular de qualidade para validar dados antes da publicacao na Silver.

Primeira implementacao: NOAA Weather.

## Onde Entra

```text
Bronze NOAA Delta
  -> explode results
  -> Data Quality NOAA
      -> valid_records
      -> invalid_records em quarentena
      -> metricas em monitoring
  -> Silver NOAA Delta
```

A Bronze continua preservada. A Silver so recebe registros validos.

## Contrato Principal

```python
result = validate_noaa_data(
    df=noaa_normalized_df,
    config=NOAAQualityConfig(),
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

## Grao Validado

O validador NOAA recebe o DataFrame normalizado pela Silver, antes da agregacao:

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

## Regras Atuais

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

## Saidas Locais

Quarentena:

```text
v2/data/delta/quarantine/noaa/ghcnd_nyc/2025
```

Metricas:

```text
v2/data/delta/monitoring/quality/noaa/ghcnd_nyc/2025
```

Esses caminhos sao Delta locais e estao ignorados no Git.

## Status

Padrao configurado:

```text
PASS    >= 99%
WARNING >= 95% e < 99%
FAIL    < 95%
```

Erros criticos, como schema incompatível, DataFrame vazio e datas futuras,
podem causar `FAIL`. Em caso de `FAIL`, a Silver NOAA nao e publicada.

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

## Testes

```bash
poetry run python -m unittest tests.v2.pipelines.quality.test_noaa_validator
```

Os testes usam pequenos DataFrames Spark locais e nao acessam API NOAA nem Azure.
