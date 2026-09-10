# Auditoria tecnica do NYC Taxi Lakehouse

Data: 2026-09-10. Branch: `v2-refactor-ml`. HEAD: `bb25591`.

## Atualizacao apos a auditoria: C1 corrigido

Em 2026-09-10, foi corrigida a precedencia de status em
`v2/pipelines/quality/validators.py`, funcao `determine_status`. Um aviso de
schema tolerado agora so muda `PASS` para `WARNING`; qualidade abaixo do limite
minimo continua retornando `FAIL`. Os limites configurados e os bloqueios por
falha critica ou entrada vazia foram preservados.

Foram adicionados testes unitarios de limites e prioridade para as tres fontes,
incluindo thresholds personalizados, e dois testes com DataFrames Spark TLC.
Os testes unitarios reproduziram o erro antes da correcao e passaram depois.
Validacao final: `poetry run python -B -m unittest discover -v` executou
**84 testes em 154,818 segundos, todos aprovados**. Spark precisou executar
fora do sandbox para abrir sockets locais. Persistem ResourceWarnings de
sockets, sem falhas de teste. Log local: `/tmp/nyc-taxi-dq-c1-tests.log`.
O README de quality explica o comportamento. Nenhuma carga real foi
reprocessada, nenhum recurso AWS foi criado e nenhum commit foi realizado.

## Atualizacao apos a auditoria: C2 corrigido

Em 2026-09-10, o downloader NOAA passou a comparar a identidade completa da
consulta e a paginacao com `_manifest.json` antes de reutilizar paginas.
Consulta divergente ou paginas sem manifesto falham sem modificar o lote.
O manifesto agora e salvo antes da primeira request e finalizado como
`success`, `incomplete` ou `failed`. `--overwrite` invalida o manifesto antigo
antes de limpar somente as paginas NOAA, inclusive protegendo contra uma
interrupcao durante essa limpeza.

Paginas com metadados inconsistentes, total variavel ou arquivos fora da
sequencia impedem sucesso. Resposta `{}` nao e mais publicada como um download
completo, tratando tambem esse caso do achado I1. A Bronze continua sem ler o
manifesto automaticamente: esse gate ainda e uma pendencia.

Foram acrescentados 17 testes sem API real para consultas diferentes, retomada
apos falha, manifestos legados, overwrite mais curto, limpeza interrompida,
paginas invalidas, ausencia de token em logs/manifesto e contagens incompletas.
O lote local foi conferido somente por leitura: manifesto compativel, 76
paginas, 75.991 observacoes. Nenhum dado real foi alterado ou baixado novamente.

Validacao final: `poetry run python -B -m unittest discover -v` executou
**101 testes em 226,228 segundos, todos aprovados**. Os 23 testes de ingestao
NOAA usam respostas simuladas, sem chamar a API ou AWS. Log local:
`/tmp/nyc-taxi-noaa-resume-tests.log`. O Spark precisou abrir sockets fora do
sandbox; os ResourceWarnings existentes nao causaram falhas. Nenhum commit
foi realizado nesta etapa.

Limites: um escritor por destino; sem checksum, lock ou snapshot imutavel da
API. `--overwrite` nao preserva uma versao anterior do RAW. O guia de ingestao
documenta o uso de outro `--output` para preservar o lote existente.

## Atualizacao apos a auditoria: C3 corrigido

Em 2026-09-10, o Star Schema passou a aceitar somente `mode=overwrite`.
O bloqueio fica tanto em `run_gold_star_schema` (antes das leituras) quanto em
`write_gold_tables` (antes de qualquer escrita). A CLI e o wrapper Databricks
tambem rejeitam append em dry run. Nao foi implementado MERGE nem uma
conversao silenciosa de append para overwrite.

O overwrite completo e o replaceWhere mensal da fato foram preservados. O
helper Delta compartilhado e o append de metricas nao foram alterados. Os
novos testes verificam rejeicao antecipada, argumentos de escrita em paths
locais/S3 simulados e escrita Delta real em diretorio temporario, com
reexecucao completa e substituicao repetida de marco. Esse teste de escrita
usa tabelas sinteticas pequenas, nao certifica a modelagem anual completa.

Validacao final: `poetry run python -B -m unittest discover -v` executou
**106 testes em 280,785 segundos, todos aprovados**. Os novos testes de contrato
falharam antes da correcao e passaram depois. O teste Delta real confirmou
reexecucao sem duplicacao e preservacao de janeiro/fevereiro ao substituir
marco duas vezes. Tambem passaram o dry run da CLI e uma verificacao local do
wrapper com widgets simulados; nao houve execucao em Databricks. Log local:
`/tmp/nyc-taxi-gold-c3-tests.log`. Nenhum commit foi realizado.

As dimensoes continuam sendo reconstruidas por inteiro durante o backfill:
as protecoes contra fontes parciais e publicacao parcial (C5) seguem pendentes.
Esta mudanca tambem nao altera os modos da Gold diaria nem elimina duplicatas
que ja estejam presentes nos DataFrames de entrada. Nenhuma tabela do projeto
foi reprocessada e nenhum recurso AWS foi utilizado.

## Atualizacao apos a auditoria: C4 corrigido

Em 2026-09-10, a Bronze NOAA passou a exigir `_manifest.json` do endpoint CDO
com exatamente uma declaracao `units=metric`, antes de ler/gravar as paginas.
A unidade e persistida na coluna `unidades_noaa`. A Silver valida esse metadado
em todas as paginas, antes de explodir os resultados; unidade ausente, nula,
nao textual, desconhecida ou `standard` causa falha mesmo com `--skip-quality`.
Os schemas de observacao DQ e da Silver permanecem iguais.

A API CDO escala/converte conforme o parametro units; o pipeline nao aplica
uma segunda conversao aos valores metricos. Referencia oficial:
[NOAA CDO: parametro units](https://www.ncei.noaa.gov/cdo-web/webservices/v2#data).
O downloader continua permitindo standard no RAW, mas esse lote e bloqueado
na Bronze do pipeline metrico. Conversao de lotes standard e uma evolucao
futura, nao uma inferencia a partir da faixa de valores.

Os testes detectaram que Spark JSON ignora `_manifest.json` por seu prefixo.
A leitura foi ajustada para o filesystem Hadoop da sessao Spark, fechando o
stream em finally e usando o parser JSON padrao. Isso evita depender de
`Path.open` local para futuros paths S3; o teste em Glue continua pendente.
O contrato atual usa Spark classico, que ja e o modelo local/Glue planejado,
nao Spark Connect.

Foram adicionados 11 testes, incluindo bloqueio antes da escrita, manifesto
legado metrico, unidades mistas e fluxo RAW -> Bronze Delta -> Silver Delta
temporario com verificacao dos valores em mm/C sem reescala. Os 11 testes
focados passaram em 32,953 segundos apos corrigir a leitura do manifesto.

Validacao final da base C1-C4: `poetry run python -B -m unittest discover -v`
executou **117 testes em 161,518 segundos, todos aprovados**. Log local:
`/tmp/nyc-taxi-c4-final-tests.log`. O log e temporario; este resultado registrado
acompanha o repositorio. Persistem ResourceWarnings de sockets do Spark, sem
falhas de teste. O teste de manifesto, Bronze e Silver foi local, nao no Glue.

Este checkpoint reune codigo, testes, guias e a arquitetura Azure historica
que ainda estava sem versionamento. Para continuar em outra maquina, fazer
push da branch apos o commit e seguir o ponto de retomada do documento 09.
Nao enviar `.env`, RAW/Delta ou logs temporarios no Git.

Migracao necessaria: a Bronze NOAA existente nesta maquina ainda nao contem
`unidades_noaa`. Antes de uma nova execucao Silver, reconstruir Bronze a partir
dos JSONs e manifesto metricos existentes, depois Silver, conforme
[guia da Silver](../pipelines/silver/README.md#noaa-weather). Nenhuma tabela
real foi alterada nesta rodada; commit nao migra dados automaticamente.

Limite importante: a Bronze agora le o manifesto para unidades, mas ainda nao
implementa o gate de status/completude nem verifica a correspondencia de
todas as paginas com o manifesto. Isso permanece pendente do achado I1.

As secoes abaixo preservam o retrato anterior as correcoes. C1-C4 estao
tratados; os demais achados continuam pendentes. O proximo item local
prioritario e C5: limites de backfill, entradas vazias/parciais e preservacao
das dimensoes. Depois, seguir a prova pequena S3/Glue do plano de retomada.

Escopo: codigo atual, testes existentes, notebooks, configuracoes, documentacao, historico Git local e dados disponiveis. Auditoria sem refatoracao, instalacao de dependencias, deploy ou alteracao das tabelas do projeto. Os experimentos de escrita foram isolados em diretorios temporarios. Este relatorio foi registrado em `v2/docs/10_auditoria_pre_aws.md` para acompanhar a retomada em outro computador.

Convencao: os caminhos de codigo abaixo sao relativos a `/home/delldev/projetos/nyc-taxi-lakehouse`. Numeros depois de `:` indicam a linha inicial relevante. Observacoes sobre AWS usam documentacao oficial consultada nesta auditoria; nao representam testes em uma conta AWS. "NAO CONFIRMADO" distingue ausencia de evidencia de uma funcionalidade comprovada.

## A. Resumo executivo

A V2 e um pipeline batch local funcional, com transformacoes PySpark reutilizaveis, armazenamento Delta, Data Quality executavel, quarentena, validadores, duas saidas Gold e runner dev. A organizacao atual e suficiente para evoluir: nao ha motivo para reconstruir o projeto ou adicionar frameworks arquiteturais.

A suite passou: **79 testes, 79 aprovados, 0 falhas, 0 ignorados**, em 156,704 segundos. O runner dev tambem passou em diretorio temporario. Esses resultados validam os cenarios cobertos, mas nao garantem recuperacao de toda falha, idempotencia de qualquer modo de escrita ou prontidao imediata para AWS.

O lote NOAA local realmente cobre 365 dias de 2025. Os 12 Parquets TLC possuem 48.722.602 registros, com 365 dias de partidas em 2025 e 29 partidas fora do ano. A Silver TLC completa e a Gold completa de 2025 **nao estao presentes** nos caminhos oficiais desta maquina. A Gold dev tem 97.060 corridas de apenas dois dias.

Principais riscos encontrados: status de DQ pode liberar qualidade insuficiente diante de coluna extra; resume NOAA pode reutilizar dados de outra consulta; `append` quebra unicidade das dimensoes; backfill requer limites explicitos para evitar substituicoes destrutivas; ausencia de demanda pode virar zero analitico; storage S3 ainda nao esta implementado nos downloaders.

Veredito: boa base de engenharia para aprendizado e portfolio, com comportamentos reais e testados. Ainda precisa de uma rodada pequena de correcoes de confiabilidade e de um contrato de runtime antes de publicar dados na AWS como entrega validada.

## B. Arquitetura encontrada

### Estrutura real

```text
nyc-taxi-lakehouse/
  pyproject.toml, poetry.lock     pacote v2, dependencias e 17 comandos CLI
  README.md                     apresentacao e ponto de entrada da documentacao
  v1/
    config_adls.py              configuracao historica Azure
    bronze/, silver/, gold/    notebooks Databricks exportados como .py
    pipeline/                  sequencia historica de %run
    data/                      exportacoes locais; dimensoes CSV versionadas
    docs/                      imagens do projeto original
  v2/
    config/                    ambientes, caminhos, fontes e SparkSession
    platform/                  escrita Delta, particoes, contexto e logs
    pipelines/
      ingestion/               download Python NOAA, TLC e lookup
      bronze/                  leitura RAW -> Delta
      quality/                 regras, resultados, excecoes e persistencia DQ
      silver/                  normalizacao, DQ, enriquecimento e validadores
      gold/                    Star Schema, mart diario e validadores
      dev/                     runner local de amostra TLC com dependencias prontas
    notebooks/                 inspecao e EDA no Jupyter
    databricks/notebooks/       16 wrappers opcionais para a plataforma Azure
    docs/                      explicacao, dicionario, runbook e planos cloud
    data/raw/                  arquivos locais nao versionados
    data/delta/                tabelas locais oficiais e dev nao versionadas
  tests/v2/                    testes Python e testes com Spark
```

Nao foi encontrado workflow `.github/workflows`, infraestrutura Terraform/CloudFormation, definicao executavel Step Functions ou job Glue. Os arquivos de plano ADF nao sao um pipeline ADF implantavel. A V1 nao e importada pelo core V2.

### Fluxo implementado

```text
TLC Parquet --- download Python ---> RAW TLC ---> Bronze Delta TLC ----+
                                                                    |
TLC lookup CSV - download Python --> RAW lookup -> Bronze Delta ------+
                                                                    v
NOAA API --- loop limit/offset ---> RAW JSON ---> Bronze Delta NOAA   normalizacao
                                                      |             + DQ
                                                      v               |
                                                explode/normalizar    +--> invalidos: quarantine
                                                      |               +--> metricas: monitoring
                                                      +--> DQ         +--> validos: Silver TLC/lookup
                                                            |
                                         invalidos ----------+--> quarantine
                                         metricas -----------+--> monitoring
                                         validos ------------+--> Silver NOAA estacao/dia

Silver TLC + Silver lookup + Silver NOAA
                |
                +--> Gold Star Schema
                |       dim_data, dim_clima, dim_localizacao, fact_trips
                |
                +--> Gold daily_weather_demand
                        calendario + demanda diaria Silver + clima consolidado

Validadores pos-Silver/pos-Gold: comandos separados; o runner dev os encadeia.
Jupyter: inspecao das tabelas e EDA com correlacoes simples.
```

**Diferenca importante em relacao ao desenho:** o mart diario nao le `fact_trips`. Os dois pipelines Gold leem Silver independentemente e compartilham `build_consolidated_daily_weather`. A igualdade entre soma do mart e contagem da fato precisa ser reconciliada; nao ha dependencia de execucao da fato para construir o mart.

DQ nao e um job externo obrigatorio: os `run_silver_*` normalizam o input, chamam o modulo quality e publicam apenas o DataFrame considerado valido quando o status permite.

### Contrato de execucao

`pyproject.toml:23` registra 17 entrypoints. Os `main()` usam argparse, resolvem caminhos e criam/encerram Spark quando necessario. As funcoes `run_*` recebem a SparkSession por argumento. Isso permite reaproveitar o processamento em outro executor sem copiar a logica de negocio.

Os comandos de validacao devolvem 0 para PASS e 1 para FAIL. As transformacoes normalmente devolvem 0 ao terminar e propagam excecoes, resultando em falha do processo. NOAA incompleta devolve 1. Nem todos os erros de configuracao/inicializacao sao registrados em JSON.

## C. Fluxo TLC

### Ingestao

Arquivo: `v2/pipelines/ingestion/download_nyc_tlc.py`, funcoes `main`, `download_files`, `download_file`.

Entrada: URL publica montada por `v2/config/sources.py`, ano e intervalo de meses. Padrao: janeiro a dezembro de 2025. Saida: `raw/nyc_tlc/yellow/2025/yellow_tripdata_2025-MM.parquet`.

O arquivo e baixado para `.parquet.part`; somente depois e renomeado para o destino. Falha remove o temporario e propaga o erro; HTTPError/URLError sao convertidos em retorno 1 no loop. Arquivo existente e pulado, salvo com `--overwrite`.

Pontos bons: preserva os bytes Parquet, URL deterministica, meses validados, escrita temporaria e retomada por arquivo.

Limites: nao configura timeout explicitamente, nao tem retry/backoff/jitter, checksum, validacao de footer Parquet, manifest ou run ID. `urlretrieve` pode detectar transferencia curta quando existe Content-Length, mas isso nao equivale a validar integridade do Parquet. Um arquivo final preexistente e aceito apenas por existir. Uma falha posterior preserva meses baixados antes. `--dry-run` cria o diretorio de destino, mesmo sem baixar.

### Bronze

Arquivo: `v2/pipelines/bronze/bronze_nyc_tlc.py:24`, `run_bronze_nyc_tlc`.

Le Parquet, opcionalmente aplica `limit`, converte passenger_count, RatecodeID e payment_type em integer e grava Delta. Mantem datas fora de 2025: isso e compativel com preservar a fonte na Bronze.

Nao adiciona arquivo de origem, data de ingestao ou run ID. Nao deduplica nem particiona fisicamente. A tipagem vem do Parquet e de tres casts; isso nao resolve automaticamente incompatibilidades futuras entre schemas mensais. Cast de valor fracionario para inteiro pode perder informacao antes da DQ; a fonte RAW continua disponivel.

### Silver

Arquivo: `v2/pipelines/silver/silver_nyc_tlc.py:95`, `run_silver_nyc_tlc`.

Sequencia real:

1. Le Bronze Delta e renomeia 20 colunas para portugues.
2. Aplica intervalo de partida e limite de linhas, quando informados.
3. Valida schema, campos obrigatorios, chaves repetidas e regras de dominio.
4. Grava invalidos em quarantine e metricas em monitoring quando ambos os caminhos sao fornecidos.
5. Interrompe em FAIL; PASS/WARNING seguem com validos.
6. Substitui nulos de determinadas colunas numericas por zero e flag textual por DESCONHECIDO.
7. Calcula data, ano, mes, dia, hora, nomes, fim de semana, periodo, pico, duracao, km, velocidade e valor/km.
8. Adiciona descricoes de pagamento/tarifa, categorias e flags de suspeita.
9. Aplica `dropDuplicates` pela chave configurada.
10. Escreve Delta particionado por `ano, mes`, com replaceWhere opcional.

`main()` fornece `TLCQualityConfig.for_year(year)`; o wrapper Databricks tambem. A chamada direta de `run_silver_nyc_tlc()` usa configuracao sem limite anual quando nao recebe `quality_config`. O runner dev restringe datas antes da DQ, em vez de fornecer esse contrato anual.

`duracao_minutos` usa `timestampdiff(SECOND, ...)/60`, arredondada a duas casas; milhas sao mantidas e km calculado separadamente. As datas atuais usam timestamp_ntz; nao existe conversao geografica de fuso. Preservar essa semantica no runtime cloud e um criterio de teste.

### Gold

Star Schema: `gold_star_schema.py`, filtra TLC por ano e associa as dimensoes por data e IDs de zona. Mart diario: `gold_daily_weather_demand.py`, agrupa Silver por data, conta corridas, soma valores e calcula medias.

O alvo atual representa **corridas de taxi amarelo aceitas pela Silver**, nao toda a demanda por transporte, pedidos nao atendidos ou todos os tipos de taxi de NYC.

### Taxi Zone Lookup

`download_taxi_zone_lookup.py` baixa CSV com a mesma estrategia de temporario e skip do TLC. `bronze_taxi_zone_lookup.py` usa header/inferSchema e grava Delta. `silver_taxi_zone_lookup.py` renomeia, converte LocationID, aplica trim nos textos e DQ. A referencia alimenta `dim_localizacao` e e pequena: nao e particionada.

Normalizacao do lookup usa select das quatro colunas antes da DQ: coluna ausente pode gerar AnalysisException antes do relatorio de qualidade, e colunas extras da origem sao descartadas nesse select.

## D. Fluxo NOAA

### Request e paginacao

Arquivo: `v2/pipelines/ingestion/download_noaa_weather.py`.

`build_base_params:246` inclui dataset, datas, units e parametros repetidos por tipo/estacao/localidade. `build_url:274` usa urlencode. Defaults: GHCND, CITY:US360019, PRCP/TMAX/TMIN/SNOW/SNWD, units metric, limit 1000, offset inicial 1.

`request_json:372` coloca o token no header e usa timeout de 60 segundos. `resolve_token:207` aceita CLI, ambiente e `.env`, nessa ordem. O token nao e incluido na URL nem no manifest.

`download_pages:279` avanca offset de limit em limit e termina quando nao ha resultados, chega ao count, ou recebe pagina curta. Ao final compara quantidade baixada com count. Para uma resposta estavel, com metadados corretos e iniciando em 1, o algoritmo percorre as paginas corretamente. O lote local comprova esse caminho feliz.

Isso nao e garantia incondicional de completude: count igual nao comprova unicidade, identidade da consulta, estabilidade da origem ou cobertura temporal. O count e atualizado a cada pagina; o protocolo nao valida offset/limit devolvidos nem estabilidade de count.

A API documenta limite por pagina e quotas por token; a AWS nao remove essas restricoes. O intervalo padrao de 0,25 s ajuda no consumo sequencial, mas nao coordena multiplos processos com o mesmo token. Fonte: [NOAA CDO API](https://www.ncei.noaa.gov/cdo-web/webservices/v2).

### Retry e falhas

Retry captura HTTPError, URLError e TimeoutError. **Backoff linear**, `sleep_seconds * tentativa`; jitter uniforme opcional, desligado por padrao. `max_retries=3` significa tres tentativas totais, nao tres repeticoes adicionais.

Nao diferencia 429, 4xx permanentes e 5xx. Nao respeita Retry-After e espera inclusive apos a ultima tentativa. JSON invalido, resposta de tipo inesperado e algumas falhas de leitura nao recebem essa politica de retry. Datas sao validadas individualmente, mas nao a ordem inicio/fim ou o comprimento da janela GHCND.

### RAW e resume

Saida: `raw/noaa/ghcnd_nyc/2025/page_NNNNNN_offset_NNNNNNNNN.json`, mais `_manifest.json`. JSON e serializado novamente, preservando o conteudo da resposta, mas nao seus bytes originais.

Escrita usa `.json.part` e replace local. Arquivo preexistente e relido sem request. Nao ha fingerprint dos parametros, validacao contra manifest antigo ou diretoria por execucao. `--overwrite` substitui paginas baixadas, mas nao limpa paginas extras antigas. O nome do temporario tambem nao isola escritores concorrentes.

Manifest registra parametros, politica de retry e contagens; nao registra run ID, horario, checksum ou estado intermediario de falha. So e atualizado ao terminar o loop; uma excecao pode deixar o manifest anterior aparentando sucesso. O manifest local atual e antigo: tem contagens corretas, mas ainda nao inclui status/download_complete. O codigo atual ja escreve esses campos em novas execucoes.

### Bronze e Silver

`bronze_noaa_weather.py:49` le apenas `page_*.json` com multiLine, mantendo metadata/results e adicionando arquivo_origem/data_processamento_bronze. O grão Bronze e **pagina**, nao observacao: 76 linhas locais representam 75.991 observacoes. Nao ha particionamento Bronze ou gate de manifest.

`silver_noaa_weather.py:98` explode results, transforma data/value e passa observacoes normalizadas para DQ. O grao de validacao e data/estacao/tipo. `build_daily_weather:123` agrupa data/estacao e usa max condicional para os cinco tipos; com a DQ habilitada, os grupos duplicados ja foram excluidos do conjunto valido.

O resultado e uma linha por estacao/dia. Calcula media dos extremos de temperatura, amplitude, chuva/neve, categorias e flag de incompletude. Nulos climaticos sao preservados. `teve_neve` tambem considera neve acumulada no solo, portanto nao significa exclusivamente "nevou hoje".

Campos arquivo_origem, atributos e data_processamento_bronze nao sobrevivem ao agrupamento Silver. Valores com falhas de schema no envelope podem falhar na normalizacao antes da DQ; `explode` descarta arrays nulos/vazios sem contabilizar uma pagina rejeitada.

### Cobertura comprovada com dados locais

| Medida | Resultado desta auditoria |
|---|---:|
| Paginas RAW NYC | 76 |
| Observacoes RAW | 75.991 |
| Count informado nas paginas | 75.991 |
| Dias esperados em 2025 | 365 |
| Dias encontrados | 365 |
| Dias ausentes | 0 |
| Cobertura de datas | 100% |
| Estacoes distintas no lote | 124 |
| Pares estacao/dia | 33.074 |
| Duplicatas excedentes data/estacao/tipo | 0 |
| Pares estacao/dia com os cinco tipos | 3.387 |
| PRCP: observacoes / dias | 32.632 / 365 |
| TMAX: observacoes / dias | 5.457 / 365 |
| TMIN: observacoes / dias | 5.460 / 365 |
| SNOW: observacoes / dias | 22.337 / 365 |
| SNWD: observacoes / dias | 10.105 / 365 |
| Estacoes observadas por dia | 75 a 108 |
| Estacoes com TMIN e TMAX no mesmo dia | 14 a 15 |
| Percentual completo segundo campo Gold | 6,98% a 12,35% |
| Dias consolidados incompletos segundo regra Gold | 0 |

Cobertura de datas = dias distintos validos do periodo / dias esperados. Para obter datas ausentes, gerar calendario do ano e fazer left_anti com as datas da fonte, preferencialmente repetindo por variavel climatica. A auditoria aplicou a diferenca de conjuntos aos JSONs e agregacoes Spark a Silver.

**365 dias nao significa 124 estacoes completas todos os dias.** `cobertura_estacoes_pct` calcula estacoes completas / estacoes observadas naquele dia. Nao mede cobertura geografica ou percentual de todas as estacoes possiveis. Nao foi encontrado inventario geografico independente para certificar "todas as estacoes de NYC": **NAO CONFIRMADO**.

Cinco observacoes possuem o segundo campo de attributes preenchido: quatro PRCP com L e uma TMIN com I. A DQ atual nao interpreta atributos. Os indicadores de qualidade GHCND incluem esses codigos; a politica de uso desses registros deve ser definida e testada, em vez de confiar apenas em faixas numericas. Fonte: [documentacao GHCND](https://www.ncei.noaa.gov/pub/data/ghcn/daily/readme.txt).

### Consolidacao climatica e cardinalidade

`weather_consolidation.py:11` agrupa **somente por data_clima** antes de qualquer join com viagens. Conta estacoes, calcula medias/maximos e flags. Assim, produz no maximo uma linha por data, mesmo quando a entrada possui varias estacoes.

O campo `temp_media_c` e a media das medias `(TMAX+TMIN)/2` disponiveis, nao uma media horaria observada. PRCP medio e a media entre estacoes com valor; nao e soma espacial da chuva em NYC. TMAX/TMIN podem usar conjuntos diferentes de estacoes. Os resultados sao um indicador diario do conjunto observado, nao o clima exato de cada corrida.

## E. Pontos fortes

1. Funcoes PySpark recebem SparkSession e caminhos: core reutilizavel e wrappers finos de infraestrutura.
2. Schemas, limites e thresholds DQ explicitos em dataclasses; regras testadas com Spark real.
3. Separacao de validos e invalidos preserva o registro normalizado rejeitado e adiciona motivos/run ID.
4. Calendario construido explicitamente e chaves de data deterministicas, sem Window global para gerar IDs.
5. NOAA consolidada antes do join impede multiplicacao por estacoes no fluxo padrao.
6. Dimensao de localizacao enriquecida, reutilizada para partida e chegada; chaves estaveis baseadas no location_id.
7. Escrita Delta centralizada e protecao contra replaceWhere com append.
8. Particao mensal e predicado de substituicao centralizados; o teste isolado confirmou rerun sem duplicar.
9. Runner dev reutiliza os pipelines e interrompe quando os validadores falham.
10. Documentacao e historico amplos; V1 preservada e independente da V2.

### Modelo dimensional real

| Tabela | Grao | Chave logica | Origem |
|---|---|---|---|
| dim_data | dia do ano solicitado | data_id = yyyyMMdd | calendario gerado |
| dim_clima | dia com observacao consolidada | clima_id = yyyyMMdd | Silver NOAA agregada |
| dim_localizacao | location_id | localizacao_id = location_id | IDs usados + lookup |
| fact_trips | registro de corrida aceito da Silver | sem PK de corrida materializada | Silver TLC + dimensoes |
| daily_weather_demand | dia do calendario | data | Silver TLC + NOAA agregada |

```text
dim_data ----------- 1:N ---------+
dim_clima ---------- 1:N ---------+--> fact_trips
dim_localizacao ---- 1:N partida -+
                   - 1:N chegada-+

Silver TLC agregada por dia + clima diario + calendario
                       -> daily_weather_demand
```

As PK/FK sao conceitos logicos, nao constraints fisicas declaradas no Delta. A fact nao tem `id_corrida` nem id_vendedor na saida. Nao foi implementado SCD2, MERGE ou historico dimensional de negocio.

No caminho `overwrite`, os builders usados por `run_gold_star_schema` geram dimensoes com chaves unicas. O lookup e deduplicado antes do join, e o join DQ com contagens de duplicatas tem uma linha por chave. Nao encontrei explosao por estacoes nesse caminho.

A V1 preservada **tambem ja agrega clima por data em `v1/gold/clima_taxi.py:81`**. A alegacao de que a versao final da V1 ainda fazia join direto com varias estacoes nao e sustentada pelo codigo atual. Uma ocorrencia anterior pode ter existido, mas e NAO CONFIRMADO por essa versao preservada.

Os CSVs V1 confirmam 38 linhas em dim_clima, 366 em dim_data e 263 em dim_localizacao. A documentacao que fala em 365 dias esperados para 2024 precisa de correcao: o CSV preservado tem o calendario bissexto. A causa exata da ingestao incompleta V1 nao e reconstituivel integralmente porque o consumo ADF original nao esta exportado como codigo executavel no repositorio.

## F. Problemas CRITICOS

Critico significa risco concreto em um caminho aceito, nao afirmacao de que o lote atual esteja inteiro corrompido. Os gatilhos abaixo delimitam cada problema.

### C1. Coluna extra pode rebaixar FAIL para WARNING

Evidencia: `v2/pipelines/quality/validators.py:795`, `determine_status`.

Depois dos erros estruturais criticos, a funcao retorna WARNING para qualquer erro de schema restante **antes** de aplicar o limite minimo de qualidade. Colunas extras sao toleradas por padrao. Assim, um lote TLC com coluna extra e 0% de registros validos pode retornar WARNING.

Reproduzido nesta auditoria: config TLC padrao, total 100, quality_percentage 0, unexpected_columns com uma coluna -> WARNING. `run_silver_nyc_tlc:136` bloqueia apenas FAIL; nesse cenario pode publicar vazio/substituir a Silver anterior.

Correcao futura: combinar severidades, sem permitir que aviso de schema enfraqueca um FAIL por qualidade. Teste de regressao deve cobrir coluna extra + qualidade abaixo de 95%.

### C2. Resume NOAA nao identifica a consulta; overwrite pode deixar paginas antigas

Evidencia: `download_noaa_weather.py:296`, `:300`, `:345`; leitura Bronze em `bronze_noaa_weather.py:56`.

Gatilho: usar o mesmo output para outra janela, estacao, conjunto de tipos ou unidades. Arquivos existentes sao aceitos pelo nome, e o manifest final recebe os parametros novos.

Reproduzido: pagina com dado de janeiro preexistente; invocacao com parametro de fevereiro; zero requests, exit 0 e manifest de fevereiro apontando para dado de janeiro.

Em overwrite, se a consulta nova tiver menos paginas, os arquivos excedentes antigos continuam no diretorio e entram no glob Bronze. O count do download atual nao detecta isso.

Correcao futura: identidade da consulta no manifest, compatibilidade obrigatoria no resume e publicacao de lote com lista precisa de arquivos. Preservar a simplicidade do downloader; nao transferir paginação para Step Functions.

### C3. `append` da Gold duplica dimensoes e quebra o Star Schema

Evidencia: `gold_star_schema.py:280`, `write_gold_tables`, e CLI em `:337`.

O mesmo mode e passado para dimensoes e fato. Mesmo em cargas mensais disjuntas da TLC, `append` adiciona novamente todo o calendario e clima anual. Um rerun tambem duplica a fato. O validador de dimensoes pode detectar depois, mas o escritor nao impede a publicacao.

Os joins internos do builder usam dimensoes novas, unicas. O risco de multiplicacao ocorre nos joins dos consumidores com as dimensoes persistidas duplicadas. Duas linhas da mesma chave em duas dimensoes podem multiplicar uma corrida por quatro no consumo BI.

Correcao futura: restringir modos por tabela; dimensoes devem manter chaves unicas. Para correcoes mensais da fato, usar overwrite com replaceWhere validado. Nao apresentar append como idempotente.

### C4. Contrato de unidades NOAA permite dados imperiais rotulados como metricos

Evidencia: `download_noaa_weather.py:40` aceita standard; `silver_noaa_weather.py:107` e `:123` apenas convertem numericamente e renomeiam como mm/C. Bronze nao valida units do manifest.

Gatilho: `--units standard` seguido do pipeline Silver existente. Temperaturas ou precipitacoes plausiveis podem atravessar a DQ com unidade errada. Os RAW auditados sao metric: nao ha evidencia dessa contaminacao no lote atual.

Correcao futura mais simples para este projeto: exigir metric no contrato da fonte, verificando manifest antes da transformacao. A API realmente muda a escala/unidade conforme esse parametro. Fonte: [NOAA CDO API](https://www.ncei.noaa.gov/cdo-web/webservices/v2).

### C5. Reprocessamento pode substituir mais dados que o recorte recebido

Evidencias: `silver_nyc_tlc.py:111` e `:153`; `gold_star_schema.py:56`, `:280`; `gold_daily_weather_demand.py:41`.

Ha tres limites que precisam virar guardas de contrato:

1. `--start-date/--end-date` com overwrite e sem replace_month substitui a tabela inteira por um recorte. Em output dev isolado isso e intencional; em tabela anual e perda logica dos outros meses.
2. Um replace_month com intervalo de entrada incompativel pode terminar sem linhas para o mes solicitado. O helper Delta aceita vazio; testado em /tmp, replaceWhere com DataFrame vazio removeu marco e preservou janeiro/fevereiro. Remover um mes pode ser legitimo, mas exige decisao explicita, nao sucesso acidental.
3. Backfill da fato sempre sobrescreve as dimensoes inteiras. Se noaa_input contem somente marco, dim_clima anual passa a conter somente marco, enquanto fatos dos outros meses permanecem. Isso cria orfaos fora do mes reprocessado.

Correcao futura: validar janela, ano base, completude da fonte necessaria e politica de substituicao vazia. Preservar o uso anual das dimensoes durante backfill. Nao ha transacao unica abrangendo as quatro tabelas Gold.

### Bloqueadores exclusivos da execucao AWS

Os downloaders ainda precisam de filesystem local; `s3://` por configuracao chega como string a `.mkdir()` e falha. `--output s3://...` vira Path e perde uma barra, passando a `s3:/...`. Isso foi reproduzido sem acessar AWS. E bloqueador para ingestao direta no S3, nao para continuar localmente.

O pacote/runtime Glue tambem precisa ser escolhido e validado antes do deploy. Detalhes na secao K. Nao existe motivo comprovado para descartar PySpark ou Delta.

## G. Problemas IMPORTANTES

### I1. Completude de transporte NOAA ainda pode ser falso sucesso

`download_noaa_weather.py:315` considera count ausente como zero e results ausente como lista vazia. Reproduzido: resposta JSON `{}` -> exit 0, download_complete true. Pode ser ausencia de dados, mas nao comprova um lote valido para publicar. Bronze nao consulta o manifest.

Tambem faltam verificacoes de count consistente entre paginas, estrutura de results, campos dos registros e fingerprint. `initial_offset > 1` compara apenas o sufixo baixado com count total; nao e uma retomada completa validada. Um JSON local corrompido e relido sem recuperacao automatica.

### I2. Quarentena nao acompanha o recorte mensal

`silver_nyc_tlc.py:128`, `silver_noaa_weather.py:65` e `quality/storage.py:14`: quarantine recebe mode overwrite do pipeline, sem replaceWhere nem particionamento por janela/run. Ao reprocessar marco, substitui o snapshot de rejeicoes do caminho anual por rejeicoes de marco. Monitoring faz append, mas nao conserva os detalhes rejeitados de cada execucao no snapshot corrente.

Historico Delta pode permitir recuperar versoes enquanto os arquivos forem retidos, mas isso nao equivale a um contrato de historico de quarentena. Definir armazenamento por execucao ou substituicao mensal coerente e necessario para auditoria operacional.

### I3. Falta distinguir ausencia de dados de demanda zero

`gold_daily_weather_demand.py:121` preenche qtd_corridas nula com zero. `registro_alinhamento_incompleto` considera clima, nao cobertura TLC. O validador aceita por padrao apenas um dia com demanda e uma corrida no ano.

Evidencia local: mart dev com 365 linhas, 97.060 corridas em dois dias, 363 dias com zero e zero dias sem clima. Correto como artefato dev identificado, mas nao como evidencia anual para ML.

O notebook avisa que usa dev e filtra dias sem corridas nesse modo, o que e positivo. Antes de analise full, exigir cobertura de ingestao e uma flag distinta para dia nao processado. Comparar contagem Silver, fato e soma do mart.

### I4. DQ nao cobre todas as falhas do schema de origem

NOAA normaliza/explode e lookup seleciona/casta antes de validar. Coluna ausente no payload ou conversao invalida pode falhar antes de gravar metricas/quarentena. Colunas novas dentro de resultado NOAA sao perdidas no select.

`fail_on_schema_error=False` nao fornece uma adaptacao completa: os validadores continuam a referenciar colunas esperadas. Nao deve ser tratado como modo tolerante a qualquer schema faltante.

### I5. Politica de duplicidade altera o conceito de demanda

`quality/validators.py:321` marca **todas** as linhas de uma chave repetida como invalidas. A Silver nao preserva automaticamente uma representante. Os testes explicitamente confirmam essa politica.

A chave TLC e vendedor + partida + local partida + local chegada + total; vendedor nao identifica um veiculo. Corridas distintas podem coincidir nesses campos. Na amostra, duas linhas foram classificadas como duplicadas; a legitimidade delas nao foi investigada: NAO CONFIRMADO.

Definir duplicata exata versus conflito de chave, contabilizar o efeito e preservar rastreabilidade antes de mudar a regra. O dropDuplicates seguinte e redundante no caminho DQ padrao.

### I6. Imputacao e limites analiticos precisam ficar explicitos

`fill_numeric_nulls` converte passageiros/distancia/tarifa ausentes em zero sem flag de imputacao. Assim, "desconhecido" vira "sem passageiro" ou "distancia zero", afetando medias e flags.

As regras removem total nao positivo e valores acima dos limites; isso pode excluir registros de viagens com ajustes financeiros. Nao e automaticamente errado, mas o alvo deve ser descrito como viagens aceitas. Avaliar o volume de rejeicoes por data antes de concluir relacao clima/demanda.

Falta validar TMIN <= TMAX, finitude dos valores numericos relevantes e qualidade dos atributos NOAA. Nos dados RAW auditados nao houve TMIN maior que TMAX nos pares completos.

### I7. Logs e lineage sao parciais

RunContext existe e usa PIPELINE_RUN_ID primeiro; descobre aliases AWS/Azure e gera ID automaticamente. Porem ingestion, Bronze e Gold nao o utilizam diretamente. Os manifests nao contem run ID. Os wrappers Silver recebem um ID, mas nao usam automaticamente o mesmo mecanismo de descoberta do main local.

Ha JSON de inicio/sucesso/falha nos mains Silver e no runner. Muitas mensagens continuam print; nao ha duracao, contagem de retries consolidada, reconciliation por etapa ou ID em cada log. Monitoring DQ guarda contagens, mas nao os paths, janela, batch_id e versoes Delta de entrada/saida.

NOAA Bronze possui lineage de arquivo; TLC Bronze e lookup nao. A agregacao Silver NOAA perde os metadados de origem. Nao ha lineage end-to-end de registro ou integracao de catalogo.

### I8. Validacao ocorre depois da publicacao

Os validadores `validate_silver_*` e `validate_gold_*` sao separados do escritor. O runner para depois de detectar problema, mas a tabela ja foi gravada. Nao ha promocao de lote validado nem marcador unico de publicacao Gold.

Gold valida contagens/PK/FK e flags, mas nao reconcilia quantidade da fato com Silver, nao testa unicidade de corrida e nao valida todas as medidas. `count_true_values` interpreta flag nula como false: sozinho nao prova completude.

### I9. Estado fisico NOAA ainda anterior ao particionamento

O codigo Silver NOAA escreve `ano, mes`, mas o Delta local `silver/noaa/ghcnd_nyc/2025` ainda tem `partitionColumns=[]` no log. A Silver dev TLC e a fato dev ja estao particionadas.

A primeira execucao com novo particionamento deve ser planejada; nao assumir que uma tabela nao particionada aceitara um backfill com especificacao diferente. Para AWS, criar a tabela corretamente desde a primeira carga. Nao apagar dados locais nesta auditoria.

### I10. Cobertura de testes concentrada nos validadores

Os testes NOAA nao exercitam `download_pages` ou `request_json`; testam igualdade de contagens, formula de espera e manifest. Nao existe teste persistido de resume, 429/Retry-After, falha no meio do download ou mistura de consultas.

O teste do writer Delta usa FakeWriter. O comportamento real de replaceWhere foi testado nesta auditoria, mas nao esta protegido na suite. Nao ha teste persistido de backfill das quatro tabelas Gold, de append dimensional ou do status DQ combinado com schema drift.

### I11. Documentacao diverge do estado atual

README principal ainda apresenta Azure Databricks como proximo destino principal, enquanto `09_migracao_aws_local_first.md` aponta para AWS. Isso confunde a retomada.

O README possui exemplo manual de Silver dev sem override de quarantine/metrics; os defaults apontam para caminhos oficiais e podem substituir a quarentena oficial por uma amostra. O runner dev fornece caminhos isolados corretamente.

Dois arquivos referenciados pelo README existem apenas como untracked: `v2/docs/08_arquitetura_azure_v2.md` e `v2/docs/arquitetura_azure_v2.excalidraw`. Os links funcionam nesta maquina, mas nao necessariamente num clone. Nao foram adicionados ou removidos durante a auditoria.

## H. MELHORIAS

### Engenharia de software

A divisao config/platform/pipelines e apropriada. Dataclasses e type hints ajudam; nao ha necessidade de repositories/providers/factories. Helpers comuns para paths, particoes e Delta ja resolvem duplicacao real.

Dividas de manutencao observadas:

- `quality/validators.py` tem 857 linhas, com fluxos e agregadores muito semelhantes nas tres fontes. Extrair apenas a montagem realmente comum facilitaria corrigir C1 sem triplicar comportamento.
- `silver_nyc_tlc.py` tem 555 linhas, incluindo classificacoes e CLI. O tamanho e compreensivel, mas nomes/regras poderiam ser agrupados quando houver alteracao funcional.
- Resolucao de output e normalizacao DBFS sao duplicadas nos tres downloaders.
- `is_local_path`/`delta_exists` aparecem em varios arquivos; a funcao central ja existe.
- Calendario, nomes de dias, categorias de chuva/temperatura sao duplicados entre Silver e Gold.
- `paths.py` carrega settings no import; mudar variaveis depois do import nao atualiza suas constantes.
- `storage_mode` nao valida combinacao de ambiente/URI; aws+s3 sem roots ainda pode usar defaults locais. A configuracao representa intencao, nao um backend ativo.
- Spark factory ignora ImportError de Delta silenciosamente; em local pode adiar erro de dependencia ate a escrita.
- Pandas existe nesta venv, mas nao e dependencia direta; matplotlib e pyarrow estao ausentes. Notebook Silver tem toPandas sem tratamento de dependencia; grafico EDA captura ImportError e pula.

Nao ha UDF Python no core. `except Exception` dos mains Silver registra e relanca; dos downloaders limpa temporario e relanca. Esses usos nao sao engolir erro. O notebook de correlacao registra apenas o tipo do erro e segue, aceitavel em EDA exploratoria com status explicito, mas nao como gate de producao.

### Spark: custo real versus melhoria futura

| Operacao | Avaliacao |
|---|---|
| select/withColumn/renames | majoritariamente projecoes; nao sao shuffles por si mesmas |
| DQ groupBy + join de duplicatas | shuffle necessario para politica atual; recalculado em multiplas actions |
| metricas DQ, escrita quarantine e escrita Silver | repetem o plano sem persist; ponto mensuravel em lote grande |
| dropDuplicates TLC depois da DQ | redundante no caminho padrao; manter somente quando necessario |
| count e checks pos-camada | varias leituras separadas; agregar checks escalares juntos pode reduzir custo |
| collect no core | traz agregacao de uma linha; nao encontrei collect da fato inteira |
| toPandas em notebooks V2 | amostra limitada ou mart diario; nao e coleta do ano inteiro da fato |
| Window global | nao existe no core V2 atual; existia na geracao de IDs V1 |
| repartition/coalesce de escrita | nao ha controle explicito; nao recomendar numero arbitrario sem medir |
| joins de dimensoes pequenas | bons candidatos a broadcast/AQE; confirmar plano antes de forcar |
| orderBy no mart | custo pequeno com 365 linhas; nao e gargalo prioritario |
| profile local[1]/1g/16 shuffle partitions | conservador para a maquina; nao serve como dimensionamento cloud |

Cache indiscriminado nao e a recomendacao: primeiro reduzir actions redundantes, selecionar colunas e medir. Se persistir um plano caro, escolher storage level e garantir unpersist.

### Particionamento real

| Tabela | Codigo atual | Snapshot local auditado | Avaliacao |
|---|---|---|---|
| Bronze TLC | sem partitionBy | sem particoes | pasta anual nao equivale a particao Delta |
| Bronze NOAA | sem partitionBy | sem particoes | poucas paginas; aceitavel |
| Bronze lookup | sem partitionBy | sem particoes | correto para referencia pequena |
| Silver TLC | ano, mes | dev: ano, mes; full ausente | adequado ao backfill mensal |
| Silver NOAA | ano, mes | NYC atual ainda sem particoes | nao necessario por volume, util para recorte mensal consistente |
| Silver lookup | sem partitionBy | sem particoes | adequado |
| dim_data, dim_clima, dim_localizacao | sem partitionBy | sem particoes | adequado |
| fact_trips | ano, mes | dev: ano, mes | adequado |
| daily_weather_demand | ano, mes | dev: 12 arquivos/particoes para 365 linhas | volume pequeno; beneficio operacional, nao de performance |
| quarantine | sem partitionBy | sem particoes | precisa politica de historico/recorte, nao particionamento indiscriminado |
| monitoring quality | sem partitionBy | sem particoes | aceitavel com poucas execucoes |

`mes` e inteiro: `mes=1` e correto, sem obrigatoriedade de `mes=01`. Filtros `ano/mes` favorecem pruning; Bronze sem particao mensal exige leitura/stats de mais arquivos. NOAA usa F.year(data_clima), embora tenha ano/mes: nao assumir pruning por diretorio sem conferir o plano. Gold filtra mes depois de montar planos; otimizar pushdown apenas com evidencia do explain/execucao.

### Capacidades Delta de verdade

Implementado: leitura por path, append, overwrite, overwriteSchema, mergeSchema nas metricas, partitionBy e replaceWhere. O log local comprova versoes e metadata; os dados TLC/fato usam reader protocol 3, writer 7 e timestampNtz. A auditoria reconstruiu snapshots a partir de add/remove, nao contou todos os Parquets antigos.

Nao implementado: MERGE, txnAppId/txnVersion, OPTIMIZE, ZORDER, liquid clustering, VACUUM, RESTORE, time travel no pipeline, checkpoint de streaming ou transacao entre tabelas. `_delta_log` nao e checkpoint de retomada da ingestao HTTP. Um diretorio `_staged_commits` sozinho nao comprova orquestracao transacional.

Idempotencia atual significa igualdade logica do conjunto publicado com mesmo input e overwrite/replaceWhere. Cada rerun ainda cria nova versao Delta, timestamps e registros de monitoring. Append nao tem idempotencia de rerun nem dedup contra a tabela destino.

## I. FUTURO

Depois de corrigir os contratos e validar execucao: catalogo, Athena, consultas BI, analise estatistica, ML com separacao temporal de treino/teste e comparacao com baseline de calendario.

Para a pergunta do projeto, correlacao exploratoria nao demonstra causalidade. Controlar calendario, sazonalidade e cobertura/qualidade das fontes. Media climatica diaria tem no maximo 365 observacoes temporais em 2025; repetir o clima em milhoes de corridas nao cria milhoes de observacoes climaticas independentes.

Clima horario, mapeamento estacao-zona e ponderacao espacial podem ser evolucao posterior. Nao ha evidencia que justifique agora Kafka, Kubernetes, Redis, microservicos, Airflow/MWAA, EMR, streaming ou nova ferramenta DQ.

OPTIMIZE/ZORDER podem ser avaliados apos medir tamanho de arquivos e filtros reais; nao sao requisitos para aprovar o processamento correto. SLOs e alertas estao documentados como metas; nao ha monitoramento automatizado de cumprimento na V2.

## J. Testes e verificacoes

### Suite existente

Comando executado: `poetry run python -B -m unittest discover -v`.

| Grupo | Quantidade | O que cobre |
|---|---:|---|
| config | 7 | defaults, env invalidos, paths locais e preservacao S3 |
| platform | 26 | run ID, particoes/calendario, opcoes FakeWriter |
| ingestion NOAA | 6 | comparacao de count, calculo jitter e manifest |
| quality | 28 | validos/invalidos, schema, nulos, ranges, duplicatas e thresholds |
| validadores Silver | 6 | fixtures boas/ruins para as tres fontes |
| Gold | 6 | dim_localizacao e validadores de Star Schema/mart |
| Total | 79 | todos aprovados |

Classificacao: 39 testes sem Spark; 40 com Spark local e DataFrames sinteticos. Nao sao 79 testes de integracao com AWS, e nao ha teste E2E autonomo registrado no unittest.

Na primeira tentativa, o sandbox impediu sockets da JVM: 39 testes executados e nove erros de setUpClass. Repeticao com permissao de sockets: 79 aprovados em 156,704 s, sem falhas/ignorados. Houve ResourceWarning de sockets e avisos de Spark/Hadoop; nao foram tratados como validacao de cloud nem ocultados como falhas de regra de negocio.

Arquivos de evidencia: `/tmp/nyc-taxi-audit-tests.log` e `/tmp/nyc-taxi-audit-tests-unsandboxed.log`.

### Experimentos adicionais, sem editar a suite

| Verificacao | Resultado |
|---|---|
| 12 Parquets RAW TLC, projecao de data de partida | 48.722.602 linhas; 365 dias em 2025; 29 fora; zero partida nula |
| JSONs RAW NOAA | 75.991 linhas; 365 dias; 124 estacoes; zero duplicata composta |
| Silver NOAA consolidada com codigo atual | 365 dias; zero dia incompleto pela regra atual |
| Silver e fato dev existentes | 97.060 linhas cada; dois dias |
| Mart dev existente | 365 linhas; soma 97.060 corridas; 363 dias zero |
| overwrite + replaceWhere marco duas vezes, Delta real em /tmp | janeiro/fevereiro preservados; marco substituido sem duplicar |
| replaceWhere com DataFrame vazio, Delta em /tmp | removeu marco; janeiro/fevereiro preservados |
| downloader NOAA com payload vazio simulado | falso sucesso reproduzido |
| resume NOAA com parametros diferentes | reutilizacao incorreta reproduzida |
| resolve_output_dir com URI S3 | conversao incorreta em Path reproduzida |
| DQ 0% + coluna inesperada | WARNING reproduzido |
| runner dev com outputs em /tmp | PASS |

O runner usou RAW TLC local e Silvers NOAA/lookup existentes; nao baixou novas fontes nem refez Bronze/Silver NOAA e lookup. Portanto e E2E do recorte TLC e Gold com dependencias preconstruidas, nao um teste de todas as fontes desde HTTP. Esse limite e do runner atual, nao uma falha da execucao.

Resultado DQ dessa execucao: 100.000 linhas Bronze sample, 99.979 apos filtro temporal, 97.060 validas, 2.919 invalidas, 97,08% -> WARNING. Entre as rejeicoes, duas linhas marcadas duplicadas e 2.917 com regra de faixa. As 21 linhas anteriores ao gate nao aparecem nas metricas DQ: sao exclusoes de escopo, que merecem uma metrica separada.

Os testes extras nao foram adicionados ao projeto e nao substituem regressao permanente. Log: `/tmp/nyc-taxi-audit-spark.log`.

### O que nao foi certificado

Execucao Silver/Gold full TLC; comportamento no Glue; upload real S3; permissoes IAM; chamadas ao Secrets Manager; ingestao HTTP real nesta auditoria; latencia/custo cloud; concorrencia de escritores; recuperacao de um crash entre as quatro escritas Gold: **NAO CONFIRMADO**.

## K. AWS readiness

### Avaliacao

| Componente | Estado | Trabalho necessario |
|---|---|---|
| S3 em paths | PEQUENA ADAPTACAO | representacao URI existe; validar combinacoes/root e existencia remota |
| Ingestao direta S3 | BLOQUEADOR atual | substituir operacoes Path por escrita/listagem de objetos ou fazer upload verificado do RAW local |
| Glue/Spark | ADAPTACAO MODERADA | escolher runtime, empacotar core, criar entrada do job e evitar bootstrap pip local |
| Delta | ADAPTACAO MODERADA | manter formato; configurar runtime/S3, testar schemas/protocolo e backfill |
| IAM | ADAPTACAO MODERADA | roles por executor e menor privilegio; nenhuma policy executavel encontrada |
| Secrets | PEQUENA ADAPTACAO | env ja funciona; buscar segredo na fronteira do executor e nao logar |
| CloudWatch | ADAPTACAO MODERADA | capturar stdout/stderr e padronizar contexto/metrica nas etapas restantes |
| Step Functions | ADAPTACAO MODERADA | contratos de argumentos/saida, sequencia, falhas e run ID; nao implementado |
| Catalog/Athena | FUTURO | registrar tabelas Delta e testar consulta/protocolo sem acoplar transformacoes |

### Runtime: nao escolher por memoria ou por nome generico "Glue"

O projeto fixa Python >=3.11,<3.12, PySpark 4.1.1 e delta-spark 4.2.0. A documentacao atual lista Glue 6.0 com Spark 4.1.1, Delta 4.2.0 e Python 3.13; Glue 5.1 usa Spark 3.5.6, Delta 3.3.2 e Python 3.11. Fonte: [versoes do AWS Glue](https://docs.aws.amazon.com/glue/latest/dg/release-notes.html).

Conclusao de engenharia: Glue 6.0 e candidato por alinhar Spark/Delta, mas o metadata Python do pacote atual rejeita instalacao em 3.13. Nao basta enviar o wheel existente. Disponibilidade na conta/regiao e execucao efetiva: NAO CONFIRMADO. A pagina especifica de Delta ainda lista versoes ate 5.1, por isso registrar a versao escolhida e confirmar na prova de conceito.

Glue 5.1 evita a diferenca de Python, mas exige validar Spark/Delta anteriores. Nao instalar os JARs Spark 4/Scala 2.13 dentro de runtime Spark 3/Scala 2.12. Nenhum downgrade/upgrade e proposto nesta auditoria sem essa decisao.

`create_spark` ja deixa master indefinido em aws, o que e bom. Entretanto sempre aplica memoria/shuffle locais e tenta `configure_spark_with_delta_pip`. Um wrapper gerenciado pode receber a sessao do runtime e chamar `run_*`, preservando o bootstrap local para desenvolvimento.

A AWS documenta leitura/escrita Delta por paths S3 e habilitacao com `--datalake-formats delta` mais configuracao de extensao/catalogo/log store. Isso confirma viabilidade, nao a validade automatica das configuracoes atuais. Fonte: [Delta no AWS Glue](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-delta-lake.html).

### Empacotamento e invocacao

Separar dependencias fornecidas pelo runtime das bibliotecas da aplicacao. Poetry continua local; nao e necessario executar Poetry dentro do Glue. No core, nao ha dependencia de boto3 hoje; nao instalar o SDK como efeito colateral do import local.

Os mains argparse nao conhecem parametros gerenciados como JOB_NAME e demais argumentos do Glue. Um entrypoint fino deve traduzir os argumentos conhecidos, fornecer Spark e invocar os run_*; nao espalhar if aws em regras de negocio.

Contrato minimo recomendado para cada job: fonte/etapa, ano/janela, paths de entrada/saida, modo permitido, run ID, paths de DQ e versao do codigo. Saida: sucesso/falha, contagens, duracao e referencias a tabelas/manifest. Falha precisa propagar para o executor; status DQ e status de escrita devem ser distinguiveis.

### Responsabilidades propostas

```text
LOCAL                         CORE V2                         AWS
filesystem --------> configuracao + persistencia <-------- S3
SparkSession local -> transformacoes/Data Quality <------- Glue/Spark
.env/env ----------> token injetado no downloader <------- Secrets/IAM
terminal ----------> eventos por execucao <--------------- CloudWatch
runner manual -----> funcoes/contratos de etapas <-------- Step Functions
```

Para a primeira prova AWS, enviar uma amostra RAW existente e executar um pequeno job Spark e suficiente. Ingestao Python gerenciada deve ser escolhida depois de medir duracao/tamanho e validar acesso HTTP. Nao ha requisito que obrigue Lambda, Glue Python Shell ou Glue Spark a executar o downloader.

### IAM e segredos

Permissoes conceituais: executor de ingestao le o segredo NOAA especifico e escreve/lista o prefixo RAW correspondente; jobs Spark leem inputs e escrevem somente camadas/quarantine/monitoring autorizadas; publicam logs no grupo definido. Orquestrador inicia/consulta jobs e passa run ID. A identidade de deploy recebe iam:PassRole apenas para roles aprovadas.

S3 ListBucket deve ser restringido por prefixo; GetObject/PutObject aos objetos necessarios. DeleteObject so quando a operacao o exigir. KMS apenas se SSE-KMS for escolhido, com chave especifica. Nenhuma necessidade de dar segredo NOAA a todo job Silver/Gold.

Nao foram criadas policies, usuarios, access keys ou recursos cloud nesta auditoria. A saida CloudShell compartilhada na conversa era root; preparar identidade de trabalho antes das execucoes cloud continua pendente de confirmacao, fora do codigo auditado.

### Catalogo e Athena

O core atual independe do catalogo, o que deve permanecer. Futuramente registrar localizacao/schema no Glue Data Catalog, preferencialmente com foco Gold. Athena engine 3 suporta leitura Delta via Glue Catalog; compatibilidade depende do protocolo/features da tabela. As tabelas locais TLC/fato declaram timestampNtz e reader 3, feature listada como suportada para SELECT; criacao/sincronizacao de metadata tem cuidados adicionais. Nao e necessario converter para Parquet simples. Fonte: [Delta no Athena](https://docs.aws.amazon.com/athena/latest/ug/delta-lake-tables.html).

### Custo e concorrencia

Nao foi calculado custo cloud porque workers, duracao e runtime nao foram definidos. Limitar concorrencia, timeout e retries; comecar por amostra; registrar consumo por execucao. Evitar escritores simultaneos na mesma tabela/lote ate haver um contrato validado. Um Budget nao substitui controle de execucoes.

## L. Conceitos de Engenharia de Dados presentes

| Conceito | Implementado? | Onde | Maturidade observada |
|---|---|---|---|
| Ingestao | sim, local | download_*.py | funcional; TLC simples; S3 pendente |
| Paginacao | sim | NOAA download_pages | lote feliz validado; resume precisa correcao |
| Retry | parcial | NOAA request_json | sem classificacao HTTP; TLC ausente |
| Backoff | sim, linear | calculate_retry_wait_seconds | basico |
| Jitter | sim, opcional | downloader NOAA | default 0 |
| RAW | sim | data/raw e downloaders | reprocessavel; sem imutabilidade por lote |
| Bronze | sim | pipelines/bronze | Delta; metadados assimetricos |
| Silver | sim | pipelines/silver | transformacoes/DQ reais; full TLC pendente |
| Gold | sim | pipelines/gold | dimensional e diaria; validada em dev |
| Data Quality | sim | quality/config + validators | boa base; defeito de precedencia |
| Quarantine | sim | quality/storage | snapshot; backfill/historico incompleto |
| Data Contract | parcial | expected_schema/config/validadores | regras em codigo; sem contrato versionado de lote/origem |
| Spark | sim | DataFrames e SparkSession | real, local, sem UDFs Python |
| Delta Lake | sim | platform/delta | escrita real; nao ha MERGE/OPTIMIZE |
| Particionamento | sim, parcial | platform/partitions e escritores | ano/mes; snapshots NOAA antigos |
| Idempotencia | condicional | overwrite/replaceWhere | nao inclui append nem todos os efeitos de auditoria |
| Backfill | sim, limitado | replace_month + replaceWhere | helper comprovado; contratos de publicacao pendentes |
| Observabilidade | parcial | logging/run_context + quality metrics | sem cobertura uniforme nem duracao |
| Lineage | parcial | arquivo_origem NOAA Bronze | nao atravessa todas as camadas |
| Orquestracao | local/dev | run_dev_sample; V1 %run | sem Step Functions/ADF executavel V2 |
| Testes | sim | tests/v2 | 79 passam; gaps de integracao importantes |
| Seguranca | parcial | token env/header, .gitignore | sem credencial real identificada; IAM ainda nao comprovado |

## M. Divida tecnica real

Prioridade de correcao: C1 status DQ; C2 identidade dos lotes NOAA; C3 modos Gold; C4 unidades; C5 contratos de backfill. Em seguida, cobrir cenarios com testes permanentes e corrigir historico de quarantine/observabilidade.

### Arquivo por arquivo: pontos que demandam adaptacao

| Arquivo | Problema / acoplamento | Acao futura justificada |
|---|---|---|
| pyproject.toml / poetry.lock | Python 3.11 e Spark/Delta fixos | contrato de runtime apos escolha Glue; manter ambiente local reproduzivel |
| config/settings.py | modos nao validam roots; perfil Spark local reaplicado | validar ambiente/storage e separar parametros gerenciados necessarios |
| config/paths.py | settings congelados no import | documentar inicializacao ou passar config explicitamente onde necessario |
| config/spark.py | bootstrap pip/Delta e perfil local | usar sessao gerenciada no wrapper; falhar claramente em local sem Delta |
| config/sources.py | URLs/IDs centralizados | manter; unidade/dataset devem ser contrato validado |
| platform/delta.py | remote exists retorna true; vazio permitido | existencia real quando necessario; politica de substituicao vazia no caller |
| platform/partitions.py | replace_year requer mes; nao e substituicao anual | validar coerencia com ano/input; preservar funcoes atuais |
| platform/run_context.py | pronto, mas nao integrado a todas as etapas | integrar nas fronteiras de execucao; aliases Azure podem continuar |
| platform/logging.py | payload flexivel sem esquema minimo uniforme | padronizar campos comuns e redacao de segredos nas fronteiras |
| ingestion/download_noaa_weather.py | Path local, resume/query, units, payload e retry | corrigir confiabilidade antes de adicionar escrita S3 |
| ingestion/download_nyc_tlc.py | Path local, sem retry/integridade final | transporte resiliente e validacao por arquivo |
| ingestion/download_taxi_zone_lookup.py | mesmo problema de filesystem/download | compartilhar apenas mecanismo realmente comum |
| bronze/bronze_noaa_weather.py | glob nao vinculado a manifest | consumir lote verificado; proteger schema do envelope |
| bronze/bronze_nyc_tlc.py | falta origem/run ID; casts antes da DQ | metadados minimos e teste de schema mensal |
| bronze/bronze_taxi_zone_lookup.py | inferSchema e falta metadados | validar contrato da fonte e origem |
| quality/config.py | contrato parcial; thresholds sem validacao | explicitar politica de schema, duplicata e unidades |
| quality/models.py | metricas sem janela/origens | evoluir somente campos necessarios a auditoria |
| quality/validators.py | status C1, regras duplicadas, atributos ignorados | correcao funcional e regressao; reduzir duplicacao com cautela |
| quality/storage.py | overwrite da quarantine inteira | persistencia por execucao ou recorte coerente |
| quality/exceptions.py | hierarquia simples adequada | manter |
| silver/silver_nyc_tlc.py | filtros antes DQ, imputacao, backfill sem guardas | contratos de janela e contagem de exclusoes |
| silver/silver_noaa_weather.py | normalizacao antes gate, flags/unidades perdidas | validar envelope/atributos e alinhar storage mensal |
| silver/silver_taxi_zone_lookup.py | select/cast antes gate | erro de schema controlado e auditavel |
| silver/validate_silver_common.py | actions repetidas, exists remoto presumido | consolidar checks e reutilizar helper central |
| silver/validate_silver_nyc_tlc.py | cobertura anual opcional | gate full separado do dev e consistencia de datas |
| silver/validate_silver_noaa_weather.py | completude por data, nao por variavel/estacao | checks meteorologicos adicionais; recorte mensal explicito |
| silver/validate_silver_taxi_zone_lookup.py | checks solidos para 265 IDs | manter regras; otimizar actions somente apos medir |
| gold/weather_consolidation.py | atributos nao chegam; indicador de cobertura ambiguo | manter grao; documentar denominadores e politica de flags |
| gold/gold_star_schema.py | append dimensional; dimensoes full no backfill | politica por tabela e guarda de fontes completas |
| gold/gold_daily_weather_demand.py | faltante vira zero | flag de cobertura TLC e reconciliacao |
| gold/validate_gold_star_schema.py | nao reconcilia com Silver; tipos/medidas parciais | invariantes de cardinalidade e publicacao |
| gold/validate_gold_daily_weather_demand.py | gate anual fraco para demanda | perfil full com cobertura e total reconciliados |
| dev/run_dev_sample.py | deps NOAA/lookup prontas; paths locais relativos | manter como runner local; fixture completa offline futura |
| notebooks/01_inspect_bronze_nyc_tlc.ipynb | Path e .exists locais | manter local; nao e entrypoint Glue |
| notebooks/02_inspect_silver_nyc_tlc.ipynb | Path, preferencia automatica dev, pandas | explicitar selecao e dependencias de inspecao |
| notebooks/03_inspect_gold_star_schema.ipynb | mesmos paths locais; checks repetidos | manter inspecao e preferir validadores compartilhados |
| notebooks/04_eda_weather_demand.ipynb | EDA dev, sem modelo; grafico opcional | manter limites visiveis; analise full depois |
| tests/v2/... | cobertura de paths nao exercita IO S3; Delta mock | adicionar regressao funcional, sem conta AWS obrigatoria |
| README.md e docs | Azure como proximo destino e links untracked | atualizar narrativa/retomada em etapa documental autorizada |

### Acoplamentos Azure/Databricks: classificacao

V1 historico, manter: `v1/config_adls.py`, os dois arquivos Bronze, os dois Silver, `v1/gold/clima_taxi.py` e `v1/pipeline/pipeline_orquestracao.py`. Usam spark/display/dbutils/%run e caminhos Azure, mas nao interferem no core V2.

V2 compatibilidade opcional, manter identificada: todos os 16 wrappers abaixo dependem de dbutils/widgets/notebook.exit e da sessao Databricks. Eles invocam funcoes do core; nao sao os scripts que devem ser copiados como jobs Glue.

```text
v2/databricks/notebooks/ingest_noaa_weather.py
v2/databricks/notebooks/ingest_nyc_tlc.py
v2/databricks/notebooks/ingest_taxi_zone_lookup.py
v2/databricks/notebooks/bronze_noaa_weather.py
v2/databricks/notebooks/bronze_nyc_tlc.py
v2/databricks/notebooks/bronze_taxi_zone_lookup.py
v2/databricks/notebooks/silver_noaa_weather.py
v2/databricks/notebooks/silver_nyc_tlc.py
v2/databricks/notebooks/silver_taxi_zone_lookup.py
v2/databricks/notebooks/gold_star_schema.py
v2/databricks/notebooks/gold_daily_weather_demand.py
v2/databricks/notebooks/validate_silver_noaa_weather.py
v2/databricks/notebooks/validate_silver_nyc_tlc.py
v2/databricks/notebooks/validate_silver_taxi_zone_lookup.py
v2/databricks/notebooks/validate_gold_star_schema.py
v2/databricks/notebooks/validate_gold_daily_weather_demand.py
```

V2 a generalizar: resolve_output_dir/normalize_databricks_path nos tres downloaders, com mensagens abfss/Volumes/DBFS. O problema real e IO local, nao a simples presenca desses nomes.

V2 compatibilidade que nao precisa remover: enums AZURE/DATABRICKS/ADLS em settings e aliases ADF/DATABRICKS no RunContext. PIPELINE_RUN_ID ja tem precedencia. `spark.databricks.delta.snapshotPartitions` e configuracao da implementacao Delta; o prefixo do nome nao implica dependencia do servico Databricks.

Documentacao a contextualizar: README principal, v2/README, docs/README, 00_visao_geral, 01_runbook, 03_plano_azure_databricks, 04_orquestracao_adf_databricks, 05_referencias_tecnicas, 07_confiabilidade_dataops, 08_arquitetura_azure_v2 e databricks/README. Preservar os planos Azure como historicos/opcionais e tornar 09_migracao_aws_local_first a trilha de continuidade atual.

### Seguranca auditada

Nenhum segredo real foi identificado pelos padroes inspecionados nos arquivos atuais e em 387 blobs de texto do historico Git local. Uma atribuicao account_key em `v1/config_adls.py:6` e placeholder, nao foi tratada como credencial real. Isso e uma busca heuristica, nao certificado de ausencia de qualquer segredo em qualquer formato, imagem ou historico remoto.

`.env`, `.key`, `.secret`, dados RAW/Delta e .venv estao ignorados. Nao ha `.env` versionado na arvore atual. `.env.local` e `.env.*` nao recebem a mesma protecao. Valores de `.env` nao foram exibidos nem utilizados para requests.

O parametro `--token` e suportado e nao e logado pelo downloader, mas pode aparecer no historico do shell/argumentos do processo. Preferir env/segredo injetado. Headers de autenticacao nao sao incluidos no manifest.

### O que nao precisa ser refeito

Manter a V1; Medallion; PySpark; Delta; separacao de business logic; grao diario de clima; calendario deterministico; reutilizacao da dim_localizacao; regras e resultados DQ existentes que estao corretos; Jupyter local; Poetry local; testes existentes; wrappers Azure como compatibilidade opcional.

O problema nao e "codigo em funcoes". Funcoes que recebem Spark e paths sao justamente o que permite levar o mesmo processamento para outro executor.

## N. Plano recomendado

### Antes da AWS

1. Corrigir C1 e adicionar teste combinando coluna extra com qualidade insuficiente. Mudanca pequena, sem cloud.
2. Blindar identidade/completude de lote NOAA e units metric; testar duas paginas, resume correto/incorreto, pagina invalida e falha parcial com mocks.
3. Restringir modos Gold e contratos de backfill; testar janeiro/fevereiro/marco, rerun, mes vazio e dimensoes completas. Alinhar quarantine ao modelo de auditoria escolhido.
4. Definir gate full de cobertura TLC e reconciliacao, separado do dev. Deixar explicito que o mart dev nao serve para resposta estatistica anual.
5. Atualizar um ponto de retomada documental e resolver os dois links para arquivos untracked, sem apagar o historico Azure.

### AWS fase 1: prova pequena de storage/runtime

1. Confirmar identidade de trabalho e role limitada; bucket/regiao por configuracao.
2. Escolher e registrar runtime Glue, incluindo a diferenca de Python; validar disponibilidade na conta.
3. Enviar uma amostra RAW ja verificada para S3, sem rebaixar o ano inteiro so para testar infraestrutura.
4. Executar Bronze pequena usando o core e sessao gerenciada. Confirmar Delta e leitura posterior.

### AWS fase 2: pipeline e operacao

1. Implementar persistencia RAW S3 ou upload verificado como fronteira pequena; preservar cliente HTTP/paginacao.
2. Executar DQ/Silver/Gold em amostra, depois um mes completo, com logs/run ID e gates.
3. Validar backfill e falha parcial no ambiente gerenciado antes dos 12 meses.
4. Escolher executor da ingestao Python e injetar o segredo; sem exigir AWS no desenvolvimento local.
5. Encadear jobs aprovados via Step Functions. Definir limites de concorrencia, timeout, retries e custo.

### Depois

Carga anual validada, Glue Catalog/Athena, visualizacao BI e analise estatistica. ML vem depois de conhecer o alvo e confirmar qualidade/cobertura. Melhorias espaciais/horarias ficam como evolucao deliberada.

**A primeira implementacao recomendada e somente C1: corrigir precedencia do status DQ e adicionar regressao.** Nenhuma correcao foi implementada nesta auditoria.

## O. Veredito

Notas subjetivas da implementacao observada, nao certificacao de capacidade individual nem de producao:

| Area | Nota / 10 | Justificativa |
|---|---:|---|
| Engenharia de Dados | 7,5 | pipeline real, multiplas fontes, modelagem e qualidade; contratos analiticos incompletos |
| Software Engineering | 7,0 | funcoes reutilizaveis, configuracao e testes; duplicacao e fronteiras ainda parciais |
| Arquitetura | 8,0 | local-first simples e modular; preserva core sem excesso de tecnologias |
| Confiabilidade | 5,5 | fluxo feliz funciona; resume, append e substituicoes precisam protecoes |
| Data Quality | 6,5 | regras extensas e quarantine; bug de severidade e falta qualidade nativa NOAA |
| Testes | 6,5 | 79 aprovados; maior cobertura em validadores que em recuperacao/IO |
| Observabilidade | 5,0 | JSON/run ID/metricas presentes, mas sem cobertura uniforme ou duracao |
| Cloud readiness | 4,5 | config S3 preparada; IO AWS, runtime, roles e execucao ainda nao comprovados |

Como portfolio, o repositorio demonstra competencias de **junior forte, com varios elementos de nivel pleno**: o candidato trabalhou com volume real, modelagem, PySpark, Delta, qualidade e testes. A maturidade de producao ainda nao foi demonstrada, especialmente recuperacao, publicacao consistente e validacao cloud. A capacidade de explicar essas decisoes e limites sera tao importante quanto os arquivos.

Tres melhorias de maior impacto:

1. Fechar falhas de confiabilidade com testes que reproduzam C1/C2 e modos de escrita/backfill.
2. Certificar a entrega analitica: cobertura TLC real, reconciliacao Silver/fato/mart e politica climatica documentada.
3. Fazer uma prova AWS pequena e reproduzivel com o mesmo core, runtime explicitado, S3, IAM minimo e run ID nos logs.

### Ponto de retomada

Nenhum arquivo de codigo ou arquivo previamente versionado foi alterado. Antes da auditoria existiam dois arquivos Azure untracked: `v2/docs/08_arquitetura_azure_v2.md` e `v2/docs/arquitetura_azure_v2.excalidraw`; eles foram preservados. Esta entrega acrescenta somente este relatorio. Nenhum commit/push foi realizado. Os logs detalhados em `/tmp` nao acompanham o Git; os resultados e limites foram registrados aqui. Os dados RAW/Delta e o `.env` tambem nao acompanham o clone.

AWS, conforme o estado compartilhado pelo usuario: Budget e bucket criados, prefixos raw/delta e camadas criados; ainda nao houve execucao Glue ou ingestao S3 comprovada aqui. Continuar da identidade de trabalho e da prova de runtime/storage depois das correcoes prioritarias, sem reconstruir o projeto.

### Como continuar em outro computador

Depois de revisar e commitar este relatorio, fazer push da branch `v2-refactor-ml`. Na outra maquina, obter essa branch e pedir ao agente para ler primeiro este arquivo, depois `09_migracao_aws_local_first.md`, o README da raiz e o runbook. Nao presumir que os dados locais, dependencias ou credenciais estarao presentes: conferir o ambiente e `git status` antes de executar.

Pedido de retomada sugerido:

> Leia v2/docs/10_auditoria_pre_aws.md, incluindo as atualizacoes C1-C4 no inicio, e v2/docs/09_migracao_aws_local_first.md. Preserve o core local-first. C1-C4 foram corrigidos com testes; confirme o Git e as dependencias. A Bronze NOAA antiga precisa ser reconstruida do RAW metrico antes de rodar novamente a Silver. O proximo item recomendado e C5: revisar backfill e entradas parciais, propor uma correcao pequena e alinhar antes das outras fases. AWS tem bucket e prefixos criados, mas nenhum job Glue validado. Nao criar infraestrutura nem executar cargas anuais automaticamente.

Antes de operar AWS, confirmar uma identidade de trabalho com menor privilegio. A ultima saida de STS compartilhada na conversa identificava root; nao foi consultada novamente nesta auditoria. Nao criar chaves de acesso root nem inserir segredos em commits.
