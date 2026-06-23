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
3. Filtrar valores impossiveis.
4. Tratar nulos numericos.
5. Tratar nulos categoricos.
6. Criar colunas derivadas para analise temporal.
7. Remover duplicatas de negocio.
8. Salvar Delta Silver.

Implementacao atual:

```text
Bronze Delta
  -> renomeacao de colunas
  -> filtro de colunas criticas
  -> filtro de valores impossiveis
  -> tratamento de nulos numericos
  -> tratamento de nulos categoricos
  -> criacao de colunas derivadas
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

Teste ainda mais leve, criando uma Bronze de amostra antes:

```bash
poetry run bronze-nyc-tlc --input v2/data/raw/nyc_tlc/yellow/2025/yellow_tripdata_2025-01.parquet --output v2/data/delta/dev/bronze/nyc_tlc/yellow_sample/2025_01 --limit 100000 --skip-count
poetry run silver-nyc-tlc --input v2/data/delta/dev/bronze/nyc_tlc/yellow_sample/2025_01 --output v2/data/delta/dev/silver/nyc_tlc/yellow_sample/2025_01 --skip-count
```
