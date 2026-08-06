# Documentacao Da V2

Esta pasta guarda a documentacao da refatoracao V2 / V2.5 do projeto.

## Ordem Recomendada De Leitura

1. `00_visao_geral.md`
2. `01_runbook_execucao.md`
3. `02_dicionario_dados.md`
4. `03_plano_azure_databricks.md`
5. `04_orquestracao_adf_databricks.md`
6. `05_referencias_tecnicas.md`
7. `06_historico_refatoracao.md`

## Arquivos

### `00_visao_geral.md`

Documento principal para entender o projeto do zero.

Explica:

```text
contexto da V1
problema da NOAA
objetivo da V2
por que existe V2.5
como clima e taxi se conectam
limite de clima diario
ideia futura de V3
```

### `01_runbook_execucao.md`

Manual operacional.

Explica:

```text
ordem de execucao local
ordem de execucao dev com sample
comandos de Bronze, Silver e Gold
validadores
parametros ADF
troubleshooting
```

### `02_dicionario_dados.md`

Dicionario das tabelas e campos.

Explica:

```text
Raw
Bronze
Silver
Gold
Quarantine
Monitoring
```

### `03_plano_azure_databricks.md`

Roteiro para recriar a execucao no Azure.

Explica:

```text
recursos Azure
caminhos cloud
secret scope
ordem dos notebooks Databricks
parametros por etapa
validacoes depois da execucao
```

### `04_orquestracao_adf_databricks.md`

Plano especifico de orquestracao.

Explica:

```text
ADF chama Databricks
paginacao NOAA fica no codigo
jobs esperados
ordem recomendada
```

### `05_referencias_tecnicas.md`

Notas praticas sobre Databricks, Spark, Delta e Azure.

### `06_historico_refatoracao.md`

Registro historico das decisoes tomadas durante a refatoracao.

## Diagramas

```text
star_schema_v2.5.excalidraw
star_schema_v2.5.excalidraw.png
arquitetura_v1.jpg
```

`arquitetura_v1.jpg` foi mantida como referencia historica/comparativa da V1.
