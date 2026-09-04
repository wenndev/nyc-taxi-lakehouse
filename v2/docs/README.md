# Documentacao Da V2

Esta pasta guarda a documentacao da refatoracao V2 / V2.5 do projeto.

## Como Usar Esta Pasta

Aqui ficam os documentos longos da V2. Esta pasta nao substitui os READMEs das
camadas em `v2/pipelines/`; aqueles explicam os scripts por etapa, incluindo o
fluxo dev local.

Para estudar o projeto inteiro, comece pelo `README.md` da raiz e depois pelo
`v2/README.md`.

## Ordem De Leitura Recomendada

A numeracao dos arquivos ajuda a organizar a pasta, mas a melhor ordem para
estudar o projeto nao precisa seguir exatamente a ordem numerica.

1. `00_visao_geral.md` - historia, problema da V1 e solucao V2/V2.5.
2. `02_dicionario_dados.md` - campos e regras de negocio.
3. `01_runbook_execucao.md` - como executar e validar.
4. `03_plano_azure_databricks.md` - plano de infraestrutura cloud.
5. `04_orquestracao_adf_databricks.md` - ADF chamando Databricks.
6. `05_referencias_tecnicas.md` - notas praticas de Spark, Delta e Azure.
7. `07_confiabilidade_dataops.md` - recuperacao, SLOs e maturidade operacional.
8. `06_historico_refatoracao.md` - diario tecnico da refatoracao.

Resumo da logica:

```text
1. Entender a historia e a modelagem.
2. Entender tabelas e campos.
3. Executar local/dev.
4. Planejar Azure/Databricks.
5. Entender confiabilidade/DataOps.
6. Consultar referencias e historico quando precisar.
```

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

Leia depois de entender a estrutura. O runbook e para rodar o projeto, nao para
ser o primeiro contato com a logica.

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

### `07_confiabilidade_dataops.md`

Guia de confiabilidade da V2.

Explica:

```text
retry/backoff/jitter
idempotencia
deduplicacao
quarantine
metricas
SLOs
recuperacao de falhas
o que fica para Azure
```

### `06_historico_refatoracao.md`

Registro historico das decisoes tomadas durante a refatoracao.

## Diagramas

```text
star_schema_v2.5.excalidraw
star_schema_v2.5.excalidraw.png
arquitetura_v1.jpg
```

`arquitetura_v1.jpg` foi mantida como referencia historica/comparativa da V1.
