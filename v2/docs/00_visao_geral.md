# Entendimento Do Projeto NYC Taxi Lakehouse V2 / V2.5

Este documento e a porta de entrada conceitual da V2. Ele explica o projeto do
comeco: por que a V2 existe, qual problema da V1 ela resolve, como as camadas
Bronze/Silver/Gold funcionam, como a NOAA se conecta com as corridas de taxi e o
que fica planejado para Azure, Power BI, ML e uma futura V3.

## 1. Pergunta Do Projeto

A pergunta principal do projeto e:

```text
As condicoes climaticas afetam a demanda por taxi em Nova York?
```

Para responder isso, o projeto junta:

```text
corridas de taxi da NYC TLC
dados climaticos da NOAA
calendario
localizacao das zonas de taxi
```

O resultado final precisa servir para:

```text
Power BI
analise exploratoria
camada ML futura
```

## 2. O Que Era A V1

A V1 foi a primeira versao do projeto.

Caracteristicas:

```text
ano analisado = 2024
cloud = Azure
orquestracao = Azure Data Factory
processamento = Databricks + PySpark
armazenamento = Delta Lake
modelo final = Star Schema na Gold
```

A V1 foi importante porque criou a primeira versao funcional do lakehouse e da
Gold publicada no Kaggle. Ela tambem revelou problemas reais que precisavam ser
melhorados em uma segunda versao.

## 3. Principal Problema Da V1

O maior problema identificado foi na NOAA.

Na V1, a `dim_clima` ficou com poucos dias:

```text
dim_clima ~= 38 registros
ano esperado = 365 dias
```

Com isso, a `fact_trips` ficou com muitos registros sem clima associado:

```text
clima_id nulo em grande parte da fact
```

Isso aconteceu porque a API da NOAA retorna dados paginados. Se o pipeline baixa
somente a primeira pagina, ele nao cobre o ano inteiro.

Problema pratico:

```text
NOAA incompleta
  -> dim_clima incompleta
  -> fact_trips com clima_id nulo
  -> analise clima x demanda fica fraca
```

## 4. Por Que Criar A V2

A infraestrutura Azure da V1 foi perdida apos o trial expirar. Em vez de
reconstruir direto na cloud, a decisao foi criar uma V2 local-first.

Objetivo da V2:

```text
melhorar o codigo
validar a logica localmente
resolver o problema da NOAA
preparar tudo para rodar depois no Azure Databricks
```

A V2 nao muda a ideia principal do projeto. Ela melhora a implementacao.

Continua sendo:

```text
raw -> bronze -> silver -> gold
```

Mas agora com:

```text
codigo PySpark mais reaproveitavel
Poetry para ambiente
Data Quality
validadores Silver e Gold
runbook de execucao
wrappers Databricks
plano Azure/ADF
```

## 5. Por Que V2.5

O repositorio continua tendo uma pasta `v2`.

O nome V2.5 representa uma evolucao de modelagem dentro da V2.

V2 inicial:

```text
refatorar localmente
rodar PySpark local
baixar TLC 2025
baixar NOAA
criar Bronze/Silver/Gold
```

V2.5:

```text
usar varias estacoes NOAA de NYC
corrigir paginacao por offset
consolidar clima diario de NYC
manter dim_clima com 1 linha por dia
evitar duplicar corridas na fact
```

Entao:

```text
V2 = estrutura/refatoracao do projeto
V2.5 = modelagem climatica atual dentro da V2
```

Nao precisa criar uma pasta `v2.5`.

## 6. Fontes De Dados

### NYC TLC Yellow Taxi

Fonte das corridas de taxi amarelo de Nova York.

Na V2, o ano usado e 2025.

Padrao dos arquivos:

```text
yellow_tripdata_2025-01.parquet
yellow_tripdata_2025-02.parquet
...
yellow_tripdata_2025-12.parquet
```

Cada linha representa uma corrida.

Campos importantes:

```text
data/hora de partida
data/hora de chegada
passageiros
distancia
local de partida
local de chegada
tipo de pagamento
valor total
gorjeta
```

### Taxi Zone Lookup

Tabela oficial da NYC TLC para traduzir IDs de zona.

Ela transforma:

```text
PULocationID / DOLocationID
```

em:

```text
borough
zona
zona_servico
```

Essa fonte alimenta a `dim_localizacao`.

### NOAA GHCND

Fonte climatica usada na V2.5.

Parametros principais:

```text
datasetid = GHCND
locationid = CITY:US360019
ano = 2025
datatypeid = PRCP, TMAX, TMIN, SNOW, SNWD
units = metric
```

O ponto principal:

```text
GHCND e uma fonte diaria.
Ela nao modela mudancas hora a hora.
```

Por isso a V2.5 trabalha com clima diario.

## 7. Como A NOAA Foi Corrigida

A correcao principal foi implementar paginacao por `offset`.

Exemplo conceitual:

```text
limit = 1000
offset = 1
offset = 1001
offset = 2001
...
```

O download continua ate:

```text
downloaded_results == expected_count
```

Resultado observado na V2.5:

```text
paginas baixadas = 76
registros raw NOAA = 75991
periodo = 2025-01-01 ate 2025-12-31
```

Isso resolve o problema da V1, onde a NOAA tinha ficado incompleta.

## 8. Granularidade Dos Dados

Granularidade significa: o que uma linha representa.

Na V2.5:

```text
TLC raw/silver = 1 linha por corrida
NOAA Silver = 1 linha por estacao/dia
dim_clima Gold = 1 linha por dia
fact_trips Gold = 1 linha por corrida
daily_weather_demand Gold = 1 linha por dia
```

Essa diferenca de granularidade e o ponto mais importante para entender a juncao
entre clima e taxi.

## 9. Como O Clima Se Conecta Com As Corridas

A conexao nao e feita por estacao.

A conexao e feita por data.

Fluxo:

```text
varias estacoes NOAA de NYC
  -> Silver NOAA com 1 linha por estacao/dia
  -> consolidacao diaria
  -> dim_clima com 1 linha por data
  -> fact_trips junta por data da corrida
```

Exemplo:

```text
2025-01-01 tem 100.000 corridas
2025-01-01 tem varias estacoes NOAA com dados climaticos
```

O projeto faz:

```text
varias estacoes NOAA de 2025-01-01
  -> 1 clima consolidado para 2025-01-01
```

Depois:

```text
todas as corridas de 2025-01-01 recebem clima_id = 20250101
```

Ou seja:

```text
1 registro de clima diario representa todas as corridas daquele dia.
```

Isso nao significa que o projeto escolhe uma corrida do dia. Todas as corridas
continuam existindo na `fact_trips`.

O que e unico por dia e o clima consolidado.

## 10. Por Que Nao Juntar Corrida Com Estacao Diretamente

Se a `fact_trips` juntasse direto com a NOAA Silver, haveria duplicacao.

Exemplo:

```text
100.000 corridas no dia
100 estacoes NOAA no mesmo dia
```

Um join direto por data poderia gerar:

```text
100.000 x 100 = 10.000.000 linhas
```

Isso distorce a demanda.

Por isso a regra da V2.5 e:

```text
primeiro consolidar clima
depois juntar com corridas
```

## 11. O Que A Consolidacao Do Clima Faz

A `dim_clima` da Gold resume as varias estacoes de NYC em um registro diario.

Exemplos de colunas:

```text
qtd_estacoes
qtd_estacoes_completas
cobertura_estacoes_pct
precipitacao_media_mm
precipitacao_max_mm
temp_max_media_c
temp_min_media_c
temp_media_c
neve_media_mm
neve_max_mm
teve_chuva
teve_neve
categoria_chuva
categoria_temperatura
registro_clima_incompleto
```

Resultado esperado:

```text
dim_clima = 365 linhas em 2025
1 linha por data
```

## 12. Limite Consciente Da V2.5

O clima muda ao longo do dia.

Exemplo:

```text
08:00 sem chuva
15:00 chuva forte
22:00 frio
```

A V2.5 nao captura essas mudancas dentro do dia.

Ela trabalha com:

```text
clima diario
demanda diaria
join por data
```

Entao, se choveu em algum momento do dia, o dia pode ser classificado como dia
com chuva. Todas as corridas daquele dia recebem o mesmo `clima_id`.

Isso e aceitavel para a pergunta da V2:

```text
Dias com chuva/neve/frio/calor tiveram demanda diferente?
```

Nao e suficiente para responder:

```text
Quando comecou a chover as 15h, a demanda mudou depois?
```

Essa segunda pergunta fica para uma V3.

## 13. Camadas Do Lakehouse

### Raw

Guarda os arquivos como chegaram.

Exemplos:

```text
Parquets da NYC TLC
JSONs paginados da NOAA
CSV Taxi Zone Lookup
```

### Bronze

Converte raw para Delta Lake, mantendo o dado quase bruto.

Objetivo:

```text
padronizar leitura
preservar origem
criar base Delta para Silver
```

### Silver

Limpa, padroniza e aplica regras de qualidade.

Na Silver TLC:

```text
renomeia colunas para PT-BR
filtra ano 2025
calcula duracao
calcula periodo do dia
marca horario de pico
marca registro suspeito
aplica Data Quality
separa registros invalidos na quarantine
```

Na Silver NOAA:

```text
explode JSON da API
organiza tipo de dado por estacao/dia
calcula temperatura media
classifica chuva/temperatura
marca clima incompleto
aplica Data Quality
```

Na Silver Taxi Zone Lookup:

```text
padroniza colunas
valida 265 zonas
remove duplicidade
serve de referencia para dim_localizacao
```

### Gold

Entrega tabelas finais para consumo analitico.

Tabelas principais:

```text
dim_data
dim_clima
dim_localizacao
fact_trips
daily_weather_demand
```

## 14. Gold Star Schema

O Star Schema e a modelagem dimensional.

Tabelas:

```text
dim_data
dim_clima
dim_localizacao
fact_trips
```

A `fact_trips` representa as corridas.

Ela se conecta com:

```text
dim_data pela data da corrida
dim_clima pelo clima consolidado da data
dim_localizacao pela zona de partida
dim_localizacao pela zona de chegada
```

Essa Gold e boa para:

```text
Power BI
analises por localizacao
analises por pagamento
analises por periodo do dia
analises por clima
```

## 15. Gold Daily Weather Demand

A `daily_weather_demand` e uma tabela diaria.

Grau:

```text
1 linha por data
```

Ela junta:

```text
demanda diaria de taxi
clima diario consolidado
calendario
```

Essa tabela e a base mais direta para EDA/ML da pergunta principal.

Exemplos de perguntas:

```text
dias com chuva tem mais corridas?
dias com neve reduzem demanda?
temperatura tem correlacao com quantidade de corridas?
fim de semana muda a demanda?
```

## 16. Data Quality

Data Quality entra principalmente na Silver.

Objetivo:

```text
validar schema
validar campos obrigatorios
validar faixas plausiveis
isolar registros invalidos
impedir publicacao quando a qualidade for critica
```

Saidas:

```text
Silver valida
quarantine com registros invalidos
monitoring com metricas de qualidade
```

Depois da Silver e da Gold tambem existem validadores de contrato.

Exemplos:

```text
Silver NOAA deve ter 365 dias
Silver NOAA deve ter estacoes suficientes
Taxi Zone Lookup deve ter 265 zonas
Gold dim_clima deve ter 365 linhas
fact_trips nao deve ter clima_id nulo
fact_trips nao deve ter chaves orfas
daily_weather_demand deve ter 365 linhas
```

## 17. Execucao Local

Localmente, o projeto usa:

```text
Python 3.11
Poetry
PySpark
Delta Lake
Jupyter
```

Comando oficial de testes:

```bash
poetry run python -m unittest discover -s tests -p 'test_*.py' -t .
```

Como a TLC completa e pesada, o local serve principalmente para:

```text
testar logica
rodar samples/dev
validar NOAA
validar Taxi Zone Lookup
validar Gold com amostra
```

A execucao completa da TLC deve acontecer no Databricks.

## 18. Ligacao Com Azure Depois

A V2 foi escrita para ser reaproveitada no Databricks.

Regra:

```text
v2/pipelines = logica PySpark
v2/databricks/notebooks = wrappers para cloud
ADF = orquestracao
```

No Azure:

```text
ADF chama notebooks Databricks
notebooks recebem parametros
notebooks chamam v2/pipelines
dados ficam em ADLS/Volumes
token NOAA fica em Key Vault/Secret Scope
```

A paginacao NOAA fica no codigo Python/Databricks, nao no ADF.

## 19. Power BI E ML

Power BI deve consumir principalmente:

```text
Gold Star Schema
```

ML deve consumir principalmente:

```text
Gold daily_weather_demand
```

Motivo:

```text
para responder clima x demanda, o alvo natural e qtd_corridas diaria.
```

Features candidatas:

```text
precipitacao_media_mm
precipitacao_max_mm
temp_media_c
teve_chuva
teve_neve
categoria_chuva
categoria_temperatura
fim_de_semana
mes
dia_semana_num
```

ML real deve usar a Gold completa no Databricks, nao apenas a amostra local.

## 20. Evolucao Futura V3

A V3 faria sentido depois da V2 rodar completa na cloud.

Ideia da V3:

```text
clima por hora ou faixa horaria
demanda por hora
join por data + hora
possivel clima por zona/borough
estacao NOAA mais proxima da zona de taxi
```

Pergunta que a V3 poderia responder:

```text
Quando o clima muda durante o dia, a demanda muda depois?
```

Isso e mais complexo porque muda a granularidade e pode exigir outra fonte
climatica alem da GHCND diaria.

## 21. Estado Atual Validado

Resultados observados localmente:

```text
NOAA raw = 75991 registros
NOAA raw = 76 paginas
Silver NOAA = 33074 linhas
Silver NOAA = 365 dias
Silver NOAA = 124 estacoes
Taxi Zone Lookup = 265 zonas
Silver TLC dev = 97060 corridas validas
dim_data = 365
dim_clima = 365
dim_localizacao = 265
fact_trips dev = 97060
daily_weather_demand dev = 365 linhas
```

Testes automatizados:

```text
Ran 43 tests
OK
```

## 22. Como Ler O Projeto

Ordem recomendada:

```text
1. v2/docs/00_visao_geral.md
2. v2/README.md
3. v2/docs/01_runbook_execucao.md
4. v2/docs/02_dicionario_dados.md
5. v2/docs/03_plano_azure_databricks.md
6. v2/databricks/README.md
```

Ordem para ler o codigo:

```text
1. v2/config/sources.py
2. v2/config/paths.py
3. v2/pipelines/ingestion/
4. v2/pipelines/bronze/
5. v2/pipelines/silver/
6. v2/pipelines/quality/
7. v2/pipelines/gold/weather_consolidation.py
8. v2/pipelines/gold/gold_star_schema.py
9. v2/pipelines/gold/gold_daily_weather_demand.py
10. v2/databricks/notebooks/
```

Frase principal para lembrar:

```text
A V2.5 usa varias estacoes NOAA para construir 1 clima diario consolidado de NYC,
e todas as corridas daquele dia recebem esse mesmo clima_id.
```
