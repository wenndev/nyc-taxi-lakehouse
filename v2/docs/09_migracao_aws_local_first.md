# Migracao AWS Local-First

Este documento registra a ideia da conversa sobre levar a V2 para AWS sem
abandonar a execucao local.

Nenhum token, senha, access key ou secret key deve ser salvo neste arquivo ou no
Git.

## Ponto De Retomada - 10/09/2026

Branch de trabalho: `v2-refactor-ml`. Consulte tambem
[a auditoria e suas atualizacoes](10_auditoria_pre_aws.md).

Concluido no codigo nesta rodada:

1. C1: uma coluna extra tolerada nao reduz FAIL para WARNING na Data Quality.
2. C2: resume NOAA identifica a consulta; manifesto registra falhas e overwrite
   limpa as paginas antigas do lote. Testes usam respostas simuladas.
3. C3: Star Schema rejeita append antes de ler/gravar; overwrite e replaceWhere
   mensal foram mantidos e testados em Delta temporario.
4. C4: Bronze valida units=metric no manifesto e registra unidades_noaa; Silver
   bloqueia unidades diferentes ou ausentes, sem conversao silenciosa.

Ultima verificacao desta base: 117 testes aprovados em 161,518 segundos,
executando `poetry run python -B -m unittest discover -v`.

Dados observados nesta maquina: RAW TLC com 48.722.602 registros; RAW NOAA com
76 paginas, 75.991 observacoes e 365 dias de 2025. A Gold dev validada possui
97.060 corridas de dois dias. Silver TLC anual e Gold anual completas ainda nao
foram validadas nesta maquina. Os testes novos usam dados temporarios: as
tabelas reais nao foram migradas pelo codigo nem pelo commit.

Antes de executar novamente a Silver NOAA com a Bronze antiga, siga os comandos
de [migracao do contrato de unidades](../pipelines/silver/README.md#noaa-weather).
O RAW metrico existente pode ser reaproveitado; nao baixar o ano novamente
so para adicionar o metadado na Bronze.

AWS: Budget e bucket criados; prefixos `raw/` e `delta/bronze`, `silver`, `gold`,
`quarantine`, `monitoring` listados no CloudShell. Nenhum job Glue ou Step
Functions foi executado nesta rodada. A ultima identidade STS compartilhada
na conversa era root. Antes de novas operacoes, preparar identidade de trabalho
com menor privilegio e MFA; nao criar chaves de acesso root.

O proximo ajuste local prioritario e C5: proteger backfill contra entradas
parciais/vazias e limites incoerentes. Tambem faltam o gate de completude do
manifesto na Bronze, reconciliacao/cobertura anual TLC, IO S3 dos downloaders e
prova de runtime/permissoes Glue. Nao considerar AWS pronta apenas porque os
paths aceitam `s3://`.

Na outra maquina:

1. Obter esta branch apos o push e conferir `git status` e `git log -1`.
2. Ler este ponto de retomada e as atualizacoes C1-C4 da auditoria.
3. Preparar Python/Java e instalar as dependencias com Poetry conforme runbook.
4. Recriar `.env` localmente apenas quando precisar ingerir dados. Nunca
   copiar tokens para a documentacao ou para o Git.
5. Rodar `poetry run python -m unittest discover -v`. Os testes nao exigem
   conta AWS nem token NOAA; os testes Spark precisam de Java e sockets locais.
6. Conferir quais dados estao disponiveis antes do runner dev: RAW, Delta,
   `.venv`, `.env` e logs em `/tmp` nao acompanham o clone do repositorio.

Pedido sugerido para o agente:

> Leia v2/docs/09_migracao_aws_local_first.md e as atualizacoes de
> v2/docs/10_auditoria_pre_aws.md. C1-C4 foram implementados; confirme o Git e
> os testes. Nao reescreva o core nem crie infraestrutura automaticamente.
> Vamos revisar primeiro C5 e depois fazer uma prova pequena S3/Glue,
> preservando a execucao local e controlando custo.

## Ideia Principal

A V2 nao deve ser refeita do zero na AWS.

A ideia e manter o mesmo core do projeto:

```text
ingestion
  -> raw
  -> bronze
  -> data quality
  -> silver
  -> gold
  -> analytics / ml
```

O que muda entre local e AWS e a infraestrutura fisica:

```text
Local
  -> filesystem em v2/data
  -> Spark local
  -> .env
  -> logs no terminal

AWS
  -> Amazon S3
  -> AWS Glue com Spark
  -> Secrets Manager / variaveis de ambiente
  -> CloudWatch
  -> Step Functions futuramente
```

Ou seja:

```text
mesmo codigo PySpark
        |
        v
configuracao de ambiente e storage
        |
        +-- local: v2/data/...
        |
        +-- aws: s3://<bucket>/...
```

## Por Que Fazer Assim

Essa abordagem evita dois problemas:

- reescrever tudo quando mudar de ambiente;
- misturar regra de negocio com detalhe de infraestrutura.

O projeto continua sendo estudado e testado localmente, mas fica preparado para
rodar depois em cloud.

## Estado Atual

Ja existe:

- V1 preservada como historico do projeto original em Azure.
- V2 local-first com Poetry, PySpark e Delta Lake.
- Ingestao NYC TLC 2025.
- Ingestao NOAA 2025 com paginacao por `offset`.
- Bronze, Silver, Data Quality, Quarantine, Monitoring e Gold.
- Star Schema V2.5.
- Data mart diario para clima versus demanda.
- Testes automatizados.
- Idempotencia e backfill mensal com Delta/`replaceWhere`.
- Particionamento temporal em tabelas maiores.
- `pipeline_run_id` cloud-neutral.
- Configuracao inicial para `local` e `aws/s3`.

Ja foi criado na AWS:

- AWS Budget mensal para controle de custo.
- Bucket S3 inicial do projeto.

O nome real do bucket nao deve ser hardcoded no codigo.

## Configuracao Do Bucket S3

Decisoes tomadas para o laboratorio:

```text
Tipo de bucket: proposito geral
Namespace: regional da conta
ACLs: desabilitadas
Bloqueio de acesso publico: ativado
Versionamento: desativado por enquanto
Criptografia: SSE-S3
```

Motivo:

- manter o data lake privado;
- reduzir risco de exposicao acidental;
- evitar custo extra com versionamento durante reprocessamentos Delta;
- manter simplicidade para laboratorio.

Estrutura esperada no bucket:

```text
s3://<bucket>/
  raw/
  delta/
    bronze/
    silver/
    gold/
    quarantine/
    monitoring/
```

Equivalencia com o local:

```text
v2/data/raw                         -> s3://<bucket>/raw
v2/data/delta/bronze                -> s3://<bucket>/delta/bronze
v2/data/delta/silver                -> s3://<bucket>/delta/silver
v2/data/delta/gold                  -> s3://<bucket>/delta/gold
v2/data/delta/quarantine            -> s3://<bucket>/delta/quarantine
v2/data/delta/monitoring            -> s3://<bucket>/delta/monitoring
```

## O Que Foi Preparado No Codigo

Arquivos principais:

```text
v2/config/settings.py
v2/config/paths.py
v2/platform/run_context.py
```

Mudancas:

- `RuntimeEnvironment` passou a aceitar `aws`.
- `StorageMode` passou a aceitar `s3`.
- caminhos locais continuam usando `Path`;
- caminhos S3 continuam como URI string `s3://...`;
- `join_storage_path` monta caminhos sem quebrar `s3://`;
- `pipeline_run_id` continua usando `PIPELINE_RUN_ID` como nome principal;
- IDs futuros de Step Functions e Glue tambem podem ser usados.

Exemplo de dry-run AWS sem acessar AWS de verdade:

```bash
env NYC_TAXI_ENV=aws \
  NYC_TAXI_STORAGE_MODE=s3 \
  NYC_TAXI_RAW_ROOT=s3://example-lakehouse/raw \
  NYC_TAXI_DELTA_ROOT=s3://example-lakehouse/delta \
  poetry run bronze-nyc-tlc --year 2025 --dry-run
```

Resultado esperado:

```text
Input : s3://example-lakehouse/raw/nyc_tlc/yellow/2025
Output: s3://example-lakehouse/delta/bronze/nyc_tlc/yellow/2025
Format: parquet -> delta
```

## Como Retomar Em Casa

1. Clonar o repositorio:

```bash
git clone <url-do-repositorio>
cd nyc-taxi-lakehouse
git checkout v2-refactor-ml
```

2. Instalar dependencias:

```bash
poetry install
```

3. Criar `.env` local com o token NOAA:

```text
NOAA_TOKEN=<seu-token-noaa>
```

Nao commitar `.env`.

4. Validar configuracao local:

```bash
poetry run run-v2-dev-sample --dry-run
```

5. Validar a preparacao AWS/S3:

```bash
env NYC_TAXI_ENV=aws \
  NYC_TAXI_STORAGE_MODE=s3 \
  NYC_TAXI_RAW_ROOT=s3://<bucket>/raw \
  NYC_TAXI_DELTA_ROOT=s3://<bucket>/delta \
  poetry run bronze-nyc-tlc --year 2025 --dry-run
```

6. Rodar testes principais:

```bash
poetry run python -m unittest
```

Se o Spark local falhar por rede/socket no ambiente do Codex, rodar o mesmo
comando no terminal normal.

## Proximo Passo AWS

Ainda nao estamos rodando Glue nem Step Functions.

Antes desta fase cloud, concluir os bloqueios locais e a identidade de trabalho
descritos no ponto de retomada acima. As fases abaixo sao planejamento, nao
recursos ja implementados.

O proximo passo incremental e:

```text
Fase 2 - RAW no S3
```

Objetivo da Fase 2:

- decidir se os dados raw serao enviados por upload ou ingeridos direto no S3;
- preparar a ingestao para escrever no S3 sem quebrar a escrita local;
- manter paginacao, retry, backoff, jitter, manifest e resume da NOAA;
- validar que `raw/` no S3 recebe os arquivos esperados.

Depois:

```text
Fase 3 - Glue/Spark
  -> rodar Bronze, Silver e Gold em AWS Glue usando o mesmo core PySpark

Fase 4 - Secrets/IAM
  -> NOAA_TOKEN via Secrets Manager ou variavel injetada

Fase 5 - Observabilidade
  -> logs estruturados no CloudWatch

Fase 6 - Step Functions
  -> orquestrar ingestion, bronze, quality/silver e gold
```

## O Que Nao Fazer Agora

Evitar por enquanto:

- recriar todo o projeto do zero;
- colocar nome real do bucket hardcoded;
- salvar access key ou secret key no Git;
- mover paginacao NOAA para Step Functions;
- trocar Delta Lake por Parquet puro;
- adicionar Lambda, EMR, Airflow, Kubernetes ou Kafka sem necessidade real.

## Conceitos Que O Projeto Demonstra

Esta evolucao local-first para AWS ajuda a demonstrar:

- Medallion Architecture;
- Data Lakehouse;
- PySpark;
- Delta Lake;
- Data Quality;
- Quarantine;
- particionamento;
- idempotencia;
- backfill;
- paginacao de API;
- retry/backoff/jitter;
- observabilidade;
- controle de custo;
- separacao entre regra de negocio e infraestrutura.

## Checklist Antes De Comitar

Antes de commitar a etapa AWS local-first:

```bash
git status
git diff --check
poetry run run-v2-dev-sample --dry-run
poetry run python -m unittest
```

Conferir tambem:

- `.env` nao aparece no `git status`;
- nome real do bucket nao foi hardcoded sem necessidade;
- nenhum segredo foi adicionado nos arquivos;
- testes novos de `settings`, `paths` e `run_context` passaram.
