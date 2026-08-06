# Dicionario De Dados V2

Este documento explica, em linguagem simples, o que cada tabela e coluna
representa na V2/V2.5 do projeto.

Objetivo: ajudar a estudar o pipeline, explicar o projeto no GitHub/LinkedIn e
preparar o uso em Power BI, EDA e ML.

Observacao: os tipos abaixo sao os tipos logicos esperados. Em alguns ambientes
Spark, `timestamp` pode aparecer como `timestamp_ntz`.

## Fluxo Geral

```text
Raw
  -> Bronze
  -> Data Quality
  -> Silver
  -> Gold
```

- Raw: arquivos baixados sem conversao.
- Bronze: dados brutos salvos em Delta Lake.
- Data Quality: separa registros validos, invalidos, metricas e status.
- Silver: dados limpos, padronizados e enriquecidos.
- Gold: tabelas finais para BI, EDA e ML.

## Raw

Raw nao e tabela analitica. E a area onde ficam os arquivos originais.

| Fonte | Caminho local | Formato | Descricao |
|---|---|---|---|
| NYC TLC Yellow Taxi | `v2/data/raw/nyc_tlc/yellow/2025` | Parquet | Arquivos mensais de corridas de taxi amarelo em 2025. |
| NYC TLC Taxi Zone Lookup | `v2/data/raw/nyc_tlc/taxi_zone_lookup/taxi_zone_lookup.csv` | CSV | Referencia oficial de zonas da NYC TLC. |
| NOAA GHCND NYC | `v2/data/raw/noaa/ghcnd_nyc/2025` | JSON | Paginas da API NOAA baixadas com paginacao por offset. |

## Bronze NYC TLC

Tabela Delta criada a partir dos Parquets originais da NYC TLC.

Grao:

```text
1 linha = 1 corrida bruta da NYC TLC
```

Regra principal:

```text
Preservar o dado original. Transformacoes importantes entram na Silver.
```

Principais colunas de entrada:

| Coluna original | Tipo logico | Descricao |
|---|---:|---|
| `VendorID` | integer | Identificador do fornecedor que registrou a corrida. |
| `tpep_pickup_datetime` | timestamp | Data e hora de inicio da corrida. |
| `tpep_dropoff_datetime` | timestamp | Data e hora de fim da corrida. |
| `passenger_count` | integer | Quantidade de passageiros informada. |
| `trip_distance` | double | Distancia da corrida em milhas. |
| `RatecodeID` | integer | Codigo da tarifa aplicada. |
| `store_and_fwd_flag` | string | Indica se o registro foi armazenado e enviado depois. |
| `PULocationID` | integer | ID da zona de partida. |
| `DOLocationID` | integer | ID da zona de chegada. |
| `payment_type` | integer | Codigo do tipo de pagamento. |
| `fare_amount` | double | Valor base da tarifa. |
| `extra` | double | Taxas extras. |
| `mta_tax` | double | Taxa fixa MTA. |
| `tip_amount` | double | Gorjeta. |
| `tolls_amount` | double | Pedagios. |
| `improvement_surcharge` | double | Sobretaxa de melhoria. |
| `total_amount` | double | Valor total da corrida. |
| `congestion_surcharge` | double | Sobretaxa de congestionamento. |
| `Airport_fee` | double | Taxa de aeroporto. |
| `cbd_congestion_fee` | double | Taxa de congestionamento CBD. |

## Bronze Taxi Zone Lookup

Tabela Delta criada a partir do CSV oficial de zonas da NYC TLC.

Grao:

```text
1 linha = 1 location_id oficial da NYC TLC
```

Regra principal:

```text
Preservar a referencia original. Padronizacao e Data Quality entram na Silver.
```

Colunas de entrada:

| Coluna original | Tipo logico | Descricao |
|---|---:|---|
| `LocationID` | integer | ID oficial da zona TLC. |
| `Borough` | string | Borough da zona. |
| `Zone` | string | Nome da zona. |
| `service_zone` | string | Zona de servico usada pela TLC. |

## Bronze NOAA

Tabela Delta criada a partir das paginas JSON da API NOAA.

Grao:

```text
1 linha = 1 pagina JSON baixada da API NOAA
```

Principais colunas:

| Coluna | Tipo logico | Descricao |
|---|---:|---|
| `metadata` | struct | Metadados da resposta da API, incluindo contagem esperada. |
| `results` | array | Lista de observacoes climaticas retornadas na pagina. |
| `arquivo_origem` | string | Nome do JSON de origem. |
| `data_processamento_bronze` | timestamp | Momento em que a pagina foi carregada para Bronze. |

## Silver NYC TLC

Tabela tratada das corridas de taxi.

Grao:

```text
1 linha = 1 corrida valida da NYC TLC
```

Regras principais:

- Colunas sao renomeadas para PT-BR.
- Data Quality roda antes de publicar a Silver.
- Registros invalidos vao para quarentena.
- Nulos numericos nao criticos viram `0`.
- Sao criadas colunas temporais, metricas derivadas, categorias e flags.

| Coluna | Tipo logico | Descricao | Origem/regra |
|---|---:|---|---|
| `id_vendedor` | integer | Identificador do fornecedor da corrida. | `VendorID`. |
| `data_hora_partida` | timestamp | Data e hora de inicio da corrida. | `tpep_pickup_datetime`. |
| `data_hora_chegada` | timestamp | Data e hora de fim da corrida. | `tpep_dropoff_datetime`. |
| `qtd_passageiros` | integer | Quantidade de passageiros. | `passenger_count`; nulo vira `0`. |
| `distancia_milhas` | double | Distancia em milhas. | `trip_distance`; nulo vira `0`. |
| `id_tarifa` | integer | Codigo da tarifa. | `RatecodeID`. |
| `flag_armazenado_e_enviado` | string | Registro armazenado e enviado depois. | Nulo vira `DESCONHECIDO`. |
| `id_local_partida` | integer | Zona de partida da corrida. | `PULocationID`. |
| `id_local_chegada` | integer | Zona de chegada da corrida. | `DOLocationID`. |
| `tipo_pagamento` | integer | Codigo do tipo de pagamento. | `payment_type`. |
| `valor_tarifa` | double | Valor base da tarifa. | `fare_amount`; nulo vira `0`. |
| `taxa_extra` | double | Taxas extras. | `extra`; nulo vira `0`. |
| `taxa_mta_fixa` | double | Taxa fixa MTA. | `mta_tax`; nulo vira `0`. |
| `gorjeta` | double | Valor da gorjeta. | `tip_amount`; nulo vira `0`. |
| `valor_pedagios` | double | Valor de pedagios. | `tolls_amount`; nulo vira `0`. |
| `sobretaxa_melhoria` | double | Sobretaxa de melhoria. | `improvement_surcharge`; nulo vira `0`. |
| `valor_total` | double | Valor total da corrida. | `total_amount`. |
| `sobretaxa_transito` | double | Sobretaxa de congestionamento/transito. | `congestion_surcharge`; nulo vira `0`. |
| `taxa_aeroporto` | double | Taxa de aeroporto. | `Airport_fee`; nulo vira `0`. |
| `taxa_congestionamento_cbd` | double | Taxa de congestionamento CBD. | `cbd_congestion_fee`; nulo vira `0`. |
| `data_viagem` | date | Data da corrida. | Derivada de `data_hora_partida`. |
| `ano` | integer | Ano da corrida. | Derivado de `data_hora_partida`. |
| `mes` | integer | Mes da corrida. | Derivado de `data_hora_partida`. |
| `mes_nome` | string | Nome do mes em PT-BR. | Ex: `janeiro`, `fevereiro`. |
| `dia_mes` | integer | Dia do mes. | Derivado de `data_hora_partida`. |
| `hora_partida` | integer | Hora de inicio da corrida. | 0 a 23. |
| `dia_semana_num` | integer | Dia da semana em numero Spark. | 1=domingo, 7=sabado. |
| `dia_semana_nome` | string | Dia da semana em PT-BR. | Ex: `segunda`, `terca`. |
| `fim_de_semana` | boolean | Indica sabado ou domingo. | `true` quando dia 1 ou 7. |
| `periodo_dia` | string | Periodo da partida. | `madrugada`, `manha`, `tarde`, `noite`. |
| `horario_pico` | boolean | Indica horario de pico em dia util. | 7-9h ou 16-19h, segunda a sexta. |
| `duracao_minutos` | decimal/double | Duracao da corrida em minutos. | Chegada - partida. |
| `distancia_km` | double | Distancia convertida para km. | `distancia_milhas * 1.60934`. |
| `valor_por_km` | double | Valor total dividido por km. | Calculado quando `distancia_km > 0`. |
| `velocidade_media_kmh` | double | Velocidade media da corrida. | `distancia_km / duracao_horas`. |
| `tipo_pagamento_desc` | string | Descricao do pagamento. | Ex: `cartao_credito`, `dinheiro`. |
| `tipo_tarifa_desc` | string | Descricao da tarifa. | Ex: `tarifa_padrao`, `jfk`, `newark`. |
| `categoria_distancia` | string | Faixa da distancia. | `zero`, `curta`, `media`, `longa`, `muito_longa`. |
| `categoria_duracao` | string | Faixa da duracao. | `zero`, `curta`, `media`, `longa`, `muito_longa`. |
| `categoria_valor_total` | string | Faixa do valor total. | `baixo`, `medio`, `alto`, `muito_alto`. |
| `viagem_com_passageiro` | boolean | Indica passageiro informado. | `qtd_passageiros > 0`. |
| `viagem_sem_passageiro` | boolean | Indica ausencia de passageiro informado. | `qtd_passageiros == 0`. |
| `valor_tarifa_zero` | boolean | Indica tarifa base igual a zero. | Possivel registro suspeito. |
| `qtd_passageiros_suspeita` | boolean | Indica quantidade improvavel de passageiros. | `< 0` ou `> 6`. |
| `viagem_distancia_zero` | boolean | Indica distancia igual a zero. | Possivel registro suspeito. |
| `viagem_distancia_alta` | boolean | Indica distancia muito alta. | `distancia_milhas > 100`. |
| `viagem_duracao_zero` | boolean | Indica duracao zero ou negativa. | `duracao_minutos <= 0`. |
| `viagem_duracao_alta` | boolean | Indica corrida muito longa. | `duracao_minutos > 180`. |
| `viagem_valor_alto` | boolean | Indica valor total muito alto. | `valor_total > 500`. |
| `velocidade_media_alta` | boolean | Indica velocidade media alta. | `velocidade_media_kmh > 120`. |
| `registro_suspeito` | boolean | Consolidado de flags suspeitas. | `true` se qualquer flag suspeita for verdadeira. |

## Silver Taxi Zone Lookup

Tabela tratada da referencia oficial de zonas da NYC TLC.

Grao:

```text
1 linha = 1 location_id valido
```

Regras principais:

- Colunas sao renomeadas para nomes padronizados.
- Strings sao aparadas com `trim`.
- `location_id` e convertido para inteiro.
- Data Quality bloqueia a publicacao se houver duplicidade, nulos ou ID fora do range.
- Essa tabela alimenta a `dim_localizacao` da Gold.

| Coluna | Tipo logico | Descricao | Origem/regra |
|---|---:|---|---|
| `location_id` | integer | ID oficial da zona TLC. | `LocationID`. |
| `borough` | string | Borough da zona. | `Borough`. |
| `zona` | string | Nome da zona. | `Zone`. |
| `zona_servico` | string | Zona de servico. | `service_zone`. |

## Silver NOAA

Tabela tratada do clima por estacao e dia.

Grao:

```text
1 linha = 1 estacao NOAA em 1 data
```

Regras principais:

- Explode o array `results` da Bronze.
- Valida observacoes NOAA antes da agregacao.
- Transforma tipos climaticos em colunas.
- Cria flags de chuva, neve e categorias.

| Coluna | Tipo logico | Descricao | Origem/regra |
|---|---:|---|---|
| `data_clima` | date | Data da observacao climatica. | Campo `date` da NOAA. |
| `id_estacao` | string | Identificador da estacao NOAA. | Campo `station`. |
| `precipitacao_mm` | double | Precipitacao diaria em mm. | Tipo NOAA `PRCP`. |
| `temp_max_c` | double | Temperatura maxima em Celsius. | Tipo NOAA `TMAX`. |
| `temp_min_c` | double | Temperatura minima em Celsius. | Tipo NOAA `TMIN`. |
| `neve_mm` | double | Neve diaria em mm. | Tipo NOAA `SNOW`. |
| `neve_acumulada_mm` | double | Neve acumulada no solo em mm. | Tipo NOAA `SNWD`. |
| `qtd_tipos_dado` | integer | Quantidade de tipos climaticos presentes. | Conta tipos disponiveis no dia/estacao. |
| `ano` | integer | Ano da data climatica. | Derivado de `data_clima`. |
| `mes` | integer | Mes da data climatica. | Derivado de `data_clima`. |
| `dia_mes` | integer | Dia do mes. | Derivado de `data_clima`. |
| `temp_media_c` | double | Temperatura media diaria. | Media entre maxima e minima. |
| `amplitude_termica_c` | double | Diferenca entre maxima e minima. | `temp_max_c - temp_min_c`. |
| `teve_chuva` | boolean | Indica chuva no dia/estacao. | `precipitacao_mm > 0`. |
| `teve_neve` | boolean | Indica neve no dia/estacao. | `neve_mm > 0` ou `neve_acumulada_mm > 0`. |
| `categoria_chuva` | string | Faixa de chuva. | `sem_chuva`, `chuva_leve`, `chuva_moderada`, `chuva_forte`. |
| `categoria_temperatura` | string | Faixa de temperatura. | `muito_frio`, `frio`, `ameno`, `quente`, `muito_quente`. |
| `registro_clima_incompleto` | boolean | Indica clima incompleto. | `true` quando falta chuva, temperatura ou neve. |

## Gold: `dim_data`

Dimensao de calendario.

Grao:

```text
1 linha = 1 data do ano
```

| Coluna | Tipo logico | Descricao |
|---|---:|---|
| `data_id` | integer | Chave da data no formato `yyyyMMdd`. |
| `data` | date | Data calendario. |
| `ano` | integer | Ano. |
| `mes` | integer | Mes. |
| `dia_mes` | integer | Dia do mes. |
| `dia_semana_num` | integer | Dia da semana em numero Spark. |
| `dia_semana_nome` | string | Nome do dia da semana. |
| `fim_de_semana` | boolean | Indica sabado ou domingo. |

## Gold: `dim_clima`

Dimensao climatica consolidada de NYC.

Grao:

```text
1 linha = 1 clima consolidado por data
```

Regra importante:

```text
A fact_trips nao junta direto com varias estacoes NOAA.
Antes, a NOAA e consolidada para 1 linha por data.
Isso evita duplicar corridas.
```

Limite da V2.5:

```text
O clima e diario. Todas as corridas da mesma data recebem o mesmo clima_id.
Mudancas de clima ao longo do dia ficam para uma futura V3 horaria.
```

| Coluna | Tipo logico | Descricao |
|---|---:|---|
| `clima_id` | integer | Chave do clima, no formato `yyyyMMdd`. |
| `data` | date | Data do clima. |
| `fonte_clima` | string | Fonte do dado climatico, atualmente `NOAA_GHCND`. |
| `escopo_clima` | string | Escopo consolidado, atualmente `NYC_consolidado`. |
| `qtd_estacoes` | integer | Quantidade de estacoes NOAA no dia. |
| `qtd_estacoes_completas` | integer | Estacoes com registro climatico completo. |
| `cobertura_estacoes_pct` | double | Percentual de cobertura das estacoes. |
| `qtd_estacoes_com_precipitacao` | integer | Estacoes com dado de precipitacao. |
| `qtd_estacoes_com_temperatura` | integer | Estacoes com temperatura maxima e minima. |
| `precipitacao_media_mm` | double | Media da precipitacao entre estacoes. |
| `precipitacao_max_mm` | double | Maior precipitacao observada entre estacoes. |
| `temp_max_media_c` | double | Media das temperaturas maximas. |
| `temp_min_media_c` | double | Media das temperaturas minimas. |
| `temp_media_c` | double | Temperatura media consolidada. |
| `amplitude_termica_c` | double | Diferenca entre max media e min media. |
| `neve_media_mm` | double | Media de neve entre estacoes. |
| `neve_max_mm` | double | Maior neve observada entre estacoes. |
| `neve_acumulada_media_mm` | double | Media de neve acumulada. |
| `neve_acumulada_max_mm` | double | Maior neve acumulada observada. |
| `teve_chuva` | boolean | Indica se houve chuva em NYC no dia. |
| `teve_neve` | boolean | Indica se houve neve em NYC no dia. |
| `categoria_chuva` | string | Categoria consolidada de chuva. |
| `categoria_temperatura` | string | Categoria consolidada de temperatura. |
| `registro_clima_incompleto` | boolean | Indica clima consolidado incompleto. |

## Gold: `dim_localizacao`

Dimensao de localizacao enriquecida com o Taxi Zone Lookup oficial da NYC TLC.

Grao:

```text
1 linha = 1 location_id da NYC TLC
```

| Coluna | Tipo logico | Descricao |
|---|---:|---|
| `localizacao_id` | integer | Chave da dimensao, igual ao `location_id`. |
| `location_id` | integer | ID da zona TLC. |
| `borough` | string | Borough da zona. Ex: `Manhattan`, `Queens`, `Brooklyn`. |
| `zona` | string | Nome da zona TLC. Ex: `Midtown Center`. |
| `zona_servico` | string | Zona de servico. Ex: `Yellow Zone`, `Boro Zone`, `EWR`. |
| `localizacao_sem_lookup` | boolean | Indica que o ID apareceu na TLC, mas nao foi encontrado no lookup. |

## Gold: `fact_trips`

Fato das corridas.

Grao:

```text
1 linha = 1 corrida valida da Silver TLC
```

| Coluna | Tipo logico | Descricao |
|---|---:|---|
| `data_id` | integer | FK para `dim_data`. |
| `localizacao_partida_id` | integer | FK para `dim_localizacao` no papel de partida. |
| `localizacao_chegada_id` | integer | FK para `dim_localizacao` no papel de chegada. |
| `clima_id` | integer | FK para `dim_clima`, usando a data da corrida. |
| `data_hora_partida` | timestamp | Data e hora de inicio da corrida. |
| `data_hora_chegada` | timestamp | Data e hora de fim da corrida. |
| `duracao_minutos` | decimal/double | Duracao da corrida. |
| `qtd_passageiros` | integer | Quantidade de passageiros. |
| `tipo_pagamento` | integer | Codigo do tipo de pagamento. |
| `tipo_pagamento_desc` | string | Descricao do pagamento. |
| `distancia_milhas` | double | Distancia em milhas. |
| `distancia_km` | double | Distancia em km. |
| `valor_total` | double | Valor total da corrida. |
| `gorjeta` | double | Valor da gorjeta. |
| `id_tarifa` | integer | Codigo da tarifa. |
| `tipo_tarifa_desc` | string | Descricao da tarifa. |
| `periodo_dia` | string | Periodo do dia da partida. |
| `horario_pico` | boolean | Indica horario de pico. |
| `fim_de_semana` | boolean | Indica fim de semana. |
| `registro_suspeito` | boolean | Indica possivel registro estranho, mas ainda analitico. |

## Gold: `daily_weather_demand`

Tabela diaria para EDA e ML.

Grao:

```text
1 linha = 1 dia
```

Uso principal:

```text
Responder se condicoes climaticas afetam a demanda por taxi em Nova York.
```

Limite:

```text
A analise desta tabela e diaria. Ela nao mede mudancas de demanda dentro do dia
apos uma mudanca climatica especifica.
```

| Coluna | Tipo logico | Descricao |
|---|---:|---|
| `data` | date | Data do calendario. |
| `ano` | integer | Ano. |
| `mes` | integer | Mes. |
| `dia_mes` | integer | Dia do mes. |
| `dia_semana_num` | integer | Dia da semana. |
| `fim_de_semana` | boolean | Indica sabado ou domingo. |
| `qtd_corridas` | long | Quantidade diaria de corridas. |
| `valor_total_corridas` | double | Soma diaria dos valores das corridas. |
| `valor_medio_corrida` | double | Valor medio diario por corrida. |
| `distancia_media_km` | double | Distancia media diaria em km. |
| `duracao_media_minutos` | double | Duracao media diaria. |
| `media_passageiros` | double | Media diaria de passageiros. |
| `qtd_registros_suspeitos` | long | Quantidade diaria de registros suspeitos. |
| `fonte_clima` | string | Fonte climatica. |
| `escopo_clima` | string | Escopo climatico consolidado. |
| `qtd_estacoes` | integer | Estacoes usadas na consolidacao do dia. |
| `cobertura_estacoes_pct` | double | Cobertura climatica do dia. |
| `precipitacao_media_mm` | double | Chuva media do dia. |
| `precipitacao_max_mm` | double | Chuva maxima do dia. |
| `temp_media_c` | double | Temperatura media do dia. |
| `temp_max_media_c` | double | Temperatura maxima media. |
| `temp_min_media_c` | double | Temperatura minima media. |
| `neve_media_mm` | double | Neve media do dia. |
| `neve_max_mm` | double | Neve maxima do dia. |
| `teve_chuva` | boolean | Indica chuva no dia. |
| `teve_neve` | boolean | Indica neve no dia. |
| `categoria_chuva` | string | Categoria de chuva. |
| `categoria_temperatura` | string | Categoria de temperatura. |
| `sem_corridas` | boolean | Indica dia sem corridas na base. |
| `sem_clima` | boolean | Indica dia sem clima associado. |
| `registro_alinhamento_incompleto` | boolean | Indica problema de alinhamento entre demanda e clima. |

## Quarantine

Tabelas de registros invalidos geradas pela Data Quality.

Caminhos:

```text
v2/data/delta/quarantine/nyc_tlc/yellow/2025
v2/data/delta/quarantine/noaa/ghcnd_nyc/2025
```

Regra:

```text
Preserva as colunas originais do registro invalido e adiciona colunas de auditoria.
```

Colunas de auditoria:

| Coluna | Tipo logico | Descricao |
|---|---:|---|
| `dq_error_code` | string | Primeiro codigo de erro encontrado no registro. |
| `dq_error_message` | string | Lista textual das regras quebradas. |
| `dq_failed_rules` | array<string> | Todas as regras quebradas pelo registro. |
| `dq_validation_timestamp` | timestamp | Momento da validacao. |
| `dq_pipeline_run_id` | string | ID da execucao do pipeline. |
| `dq_dataset_name` | string | Nome do dataset validado. |

## Monitoring Quality

Tabela de metricas da execucao de Data Quality.

Caminhos:

```text
v2/data/delta/monitoring/quality/nyc_tlc/yellow/2025
v2/data/delta/monitoring/quality/noaa/ghcnd_nyc/2025
```

Grao:

```text
1 linha = 1 execucao de Data Quality
```

| Coluna | Tipo logico | Descricao |
|---|---:|---|
| `pipeline_run_id` | string | ID da execucao. No Azure pode vir do ADF. |
| `dataset_name` | string | Dataset validado. |
| `execution_timestamp` | timestamp | Momento da execucao. |
| `total_records` | long | Total de registros avaliados. |
| `valid_records` | long | Registros validos. |
| `invalid_records` | long | Registros invalidos. |
| `quality_percentage` | double | Percentual de registros validos. |
| `duplicate_count` | long | Registros marcados como duplicados. |
| `null_error_count` | long | Registros com nulos em campos obrigatorios. |
| `schema_error_count` | long | Problemas estruturais de schema. |
| `range_error_count` | long | Registros fora dos limites configurados. |
| `future_date_count` | long | Registros com data futura. |
| `pipeline_status` | string | `PASS`, `WARNING` ou `FAIL`. |
| `missing_columns` | array<string> | Colunas esperadas ausentes. |
| `unexpected_columns` | array<string> | Colunas nao esperadas. |
| `incompatible_types` | array<string> | Colunas com tipo incompativel. |

## Regras De Status

Padrao atual:

```text
PASS    >= 99% de qualidade
WARNING >= 95% e < 99%
FAIL    < 95%
```

Na pratica:

- `PASS`: pode seguir.
- `WARNING`: pode seguir, mas precisa olhar a quarantine.
- `FAIL`: a Silver nao deve ser publicada.
