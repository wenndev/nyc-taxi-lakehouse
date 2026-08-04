# Referencias Databricks, Spark E Azure

Resumo pratico das docs oficiais para orientar a execucao cloud da V2.

Este arquivo nao substitui a documentacao oficial. Ele registra as decisoes que
fazem sentido para este projeto.

## Databricks

### Codigo No Workspace

Usar Git folders, antigo Databricks Repos, para conectar o workspace ao GitHub e
preservar a estrutura do repositorio:

```text
nyc-taxi-lakehouse/
  v2/
    pipelines/
    databricks/
```

Os notebooks em `v2/databricks/notebooks` importam `v2.pipelines`, entao o
diretorio `v2` precisa existir no ambiente do job.

## Unity Catalog E Volumes

Para a primeira execucao cloud, preferir Unity Catalog Volumes para arquivos raw
e arquivos de suporte:

```text
/Volumes/<catalog>/<schema>/<volume>/raw/...
/Volumes/<catalog>/<schema>/<volume>/delta/...
```

Motivo:

- caminho POSIX funciona bem com Python puro;
- e melhor para governanca do que DBFS legado;
- evita passar `abfss://` diretamente para scripts de ingestion Python.

## Secrets

Token NOAA:

```text
Azure Key Vault
  -> Databricks Secret Scope
  -> dbutils.secrets.get(scope="kv-lakehouse", key="noaa-token")
```

Cuidados:

- nao passar token como parametro visivel do ADF;
- nao imprimir token;
- nao salvar token em notebook;
- dar permissao minima no Secret Scope.

## ADF Chamando Databricks

Usar Databricks Notebook Activity no ADF.

Cada atividade deve passar `baseParameters` para o notebook:

```text
year
input/output
mode
dry_run
skip_count
```

Os notebooks retornam JSON com `dbutils.notebook.exit(...)`, permitindo o ADF
consumir `runOutput`.

## Spark

### Local

A configuracao local em `v2/config/spark.py` e propositalmente conservadora:

```text
master local[1]
driver.memory 1g
shuffle.partitions 16
snapshotPartitions 4
AQE ligado
```

Ela serve para testar logica sem travar a maquina.

### Databricks

No Databricks, o cluster controla paralelismo, memoria e shuffle. Os wrappers
Databricks usam o `spark` global do cluster e nao chamam `create_spark`.

Regras praticas:

- manter `skip_count=true` em tabelas grandes;
- evitar `count()` no caminho principal do pipeline;
- usar AQE ligado;
- deixar dimensoes pequenas serem broadcastadas automaticamente;
- observar skew por `data_id` e `localizacao_id` se a Gold completa ficar lenta.

## Delta Lake

### Primeira Execucao

Na primeira execucao full:

```text
1. escrever Bronze/Silver/Gold
2. validar contagens e chaves
3. so depois otimizar layout
```

Nao vale otimizar antes de existir dado full.

### OPTIMIZE, ZORDER E Liquid Clustering

Prioridade recomendada:

```text
1. Se usar Unity Catalog managed tables: habilitar predictive optimization.
2. Se criar tabelas novas modernas: considerar liquid clustering.
3. Se usar Delta por path simples: usar OPTIMIZE e, quando fizer sentido, ZORDER.
```

Para o projeto:

```text
Gold diaria:
  filtros comuns: data, mes, chuva, temperatura

Gold star fact_trips:
  filtros comuns: data_id, localizacao_partida_id, clima_id
```

Exemplo se a tabela for Delta por path:

```sql
OPTIMIZE delta.`/Volumes/<catalog>/<schema>/<volume>/delta/gold/star_schema/2025/fact_trips`
ZORDER BY (data_id, localizacao_partida_id);
```

Se a tabela for managed table com liquid clustering, nao usar `ZORDER`.

## Aplicacoes Diretas Na V2

Melhorias que fazem sentido antes ou durante a ida para cloud:

```text
1. Validar dim_localizacao enriquecida com Taxi Zone Lookup na Gold full.
2. Criar notebook de validacao NOAA.
3. Criar notebook de validacao Gold full.
4. Criar script/notebook de manutencao Delta para Databricks.
5. Depois da execucao full, decidir entre predictive optimization, liquid clustering ou OPTIMIZE/ZORDER.
```

## Fontes Oficiais Consultadas

```text
Databricks Delta best practices:
https://docs.databricks.com/aws/en/delta/best-practices

Databricks OPTIMIZE:
https://docs.databricks.com/aws/en/delta/optimize

Databricks liquid clustering:
https://docs.databricks.com/aws/en/delta/clustering

Databricks predictive optimization:
https://docs.databricks.com/aws/en/optimizations/predictive-optimization

Azure Databricks Unity Catalog volumes:
https://learn.microsoft.com/en-us/azure/databricks/volumes/

Azure Databricks secrets:
https://learn.microsoft.com/en-us/azure/databricks/security/secrets/

Databricks Git folders:
https://docs.databricks.com/aws/en/repos/git-folders-concepts

ADF Databricks Notebook Activity:
https://learn.microsoft.com/en-us/azure/data-factory/transform-data-databricks-notebook

Apache Spark SQL performance tuning:
https://spark.apache.org/docs/latest/sql-performance-tuning
```
