# Historico de Versoes do Experimento

Este documento descreve o que foi feito em cada versao operacional do projeto. Como o repositorio ainda nao usa tags formais de release, cada versao abaixo corresponde a um commit relevante no historico.

Use este arquivo para entender a evolucao tecnica do experimento, justificar mudancas metodologicas e recuperar rapidamente o motivo de cada correcao.

---

## Linha do tempo resumida

| Versao | Commit | Tema principal |
|---|---|---|
| v0.1 | `563e4b4` | Importacao inicial da estrutura completa |
| v0.2 | `7417957` | Reorganizacao do projeto na raiz do repositorio |
| v0.3 | `02f384b` | Preservacao da pasta `jars/` no Git |
| v0.4 | `ef94196` | Correcao de instrucao no README |
| v0.5 | `0317d2a` | Ajuste do Kafka em modo KRaft e melhorias de plots |
| v0.6 | `1fc924a` | Inclusao inicial do dataset via Git LFS |
| v0.7 | `80900ce` | Remocao do dataset do Git e suporte a dataset externo |
| v0.8 | `e2effb3` | Correcao do download de JARs Spark/Kafka |
| v0.9 | `a969e94` | Simplificacao das instrucoes de dataset |
| v0.10 | `f673150` | Correcao do throughput stream zerado por falta de `lz4` |
| v0.11 | `091e8b5` | Controle de uso de disco e registro de problemas |

---

## v0.1 - Estrutura inicial completa

**Commit:** `563e4b4`  
**Mensagem:** `melhorias aplicadas e refatoração completa`

### O que foi feito

- Criada a estrutura base do experimento Batch vs Stream.
- Adicionados jobs Spark para batch e streaming:
  - `jobs/batch_job.py`
  - `jobs/stream_job.py`
- Adicionado produtor Kafka:
  - `producer/taxi_stream_producer.py`
- Adicionado `docker-compose.yml` com Kafka e Spark.
- Criados scripts de orquestracao e analise:
  - `scripts/run_full_experiment.py`
  - `scripts/consolidate_results.py`
  - `scripts/generate_plots.py`
  - `scripts/generate_report.py`
  - `scripts/container_monitor.py`
  - `scripts/create_samples.py`
  - `scripts/capture_environment.py`
- Adicionado README com desenho experimental, cenarios, metricas e instrucoes.

### Impacto

Esta versao estabeleceu a base reproduzivel do estudo: execucao automatizada, coleta de metricas, consolidacao estatistica e geracao de relatorios.

### Observacoes

O conteudo entrou inicialmente dentro de uma subpasta chamada `stream-batch-experiment-main/`, o que foi reorganizado na versao seguinte.

---

## v0.2 - Reorganizacao do projeto na raiz

**Commit:** `7417957`  
**Mensagem:** `refactor: otimização de recursos e melhorias metodológicas`

### O que foi feito

- Movidos os arquivos da subpasta `stream-batch-experiment-main/` para a raiz do repositorio.
- Mantida a mesma estrutura funcional, agora diretamente acessivel na raiz:
  - `README.md`
  - `docker-compose.yml`
  - `jobs/`
  - `producer/`
  - `scripts/`
  - `requirements.txt`

### Impacto

Simplificou a execucao dos comandos e eliminou a necessidade de entrar em uma pasta intermediaria antes de rodar o experimento.

### Precaucao

Depois desta versao, todos os comandos devem ser executados a partir da raiz:

```bash
cd ~/stream-batch-experiment
```

---

## v0.3 - Preservacao da pasta de JARs

**Commit:** `02f384b`  
**Mensagem:** `chore: adiciona jars/.gitignore para manter pasta no repositório`

### O que foi feito

- Adicionado `jars/.gitignore`.
- A pasta `jars/` passou a existir no Git mesmo sem versionar os arquivos `.jar` baixados.

### Impacto

Facilitou o fluxo de pre-download dos conectores Spark/Kafka, porque scripts e usuarios podem assumir que a pasta `jars/` existe.

### Precaucao

Os JARs continuam nao sendo versionados. Antes de rodar streaming em ambiente novo:

```bash
bash scripts/download_jars.sh
```

---

## v0.4 - Correcao de instrucao no README

**Commit:** `ef94196`  
**Mensagem:** `Fix typo in execution instructions in README`

### O que foi feito

- Corrigido um erro textual nas instrucoes de execucao.

### Impacto

Reduziu ambiguidade para quem segue o README ao preparar ou executar o experimento.

---

## v0.5 - Kafka KRaft e ajustes operacionais

**Commit:** `0317d2a`  
**Mensagem:** `Fix Kafka KRaft listener and controller configuration`

### O que foi feito

- Ajustada a configuracao do Kafka em modo KRaft no `docker-compose.yml`.
- Corrigidas configuracoes de listeners, controller e broker.
- Removida dependencia operacional de Zookeeper.
- Ajustado comportamento relacionado aos plots em `scripts/generate_plots.py`.
- Removidos parametros obsoletos ou conflitantes no runner.

### Impacto

O Kafka passou a subir de forma mais confiavel com a imagem `confluentinc/cp-kafka:7.7.0`, usando KRaft nativo.

### Precaucao

Se Kafka nao responder:

```bash
docker compose logs kafka
docker compose ps
```

Validar tambem:

```bash
docker exec kafka kafka-broker-api-versions --bootstrap-server kafka:9092
```

---

## v0.6 - Inclusao inicial do dataset via Git LFS

**Commit:** `1fc924a`  
**Mensagem:** `Add dataset with Git LFS`

### O que foi feito

- Adicionado `.gitattributes`.
- Incluidos ponteiros Git LFS para arquivos Parquet do NYC Taxi:
  - `yellow_tripdata_2023-01.parquet`
  - `yellow_tripdata_2023-02.parquet`
  - `yellow_tripdata_2023-03.parquet`

### Impacto

Tentou tornar o dataset disponivel pelo proprio repositorio sem armazenar arquivos grandes diretamente no Git tradicional.

### Problema identificado depois

Mesmo com Git LFS, manter dataset grande atrelado ao repositorio complicava clone, push, armazenamento e reproducibilidade em maquinas diferentes.

Esse desenho foi substituido na versao seguinte.

---

## v0.7 - Dataset externo e configuravel

**Commit:** `80900ce`  
**Mensagem:** `Configure external NYC Taxi dataset setup`

### O que foi feito

- Removidos os arquivos Parquet do controle direto do repositorio.
- Adicionado `scripts/download_dataset.sh`.
- Atualizado `.gitignore` para ignorar dados locais.
- Atualizado README com instrucoes para baixar dataset real.
- Ajustado `docker-compose.yml` para montar `DATA_ROOT`.
- Ajustado `scripts/run_full_experiment.py` para resolver caminhos de dados a partir de `DATA_ROOT`.
- Ajustados scripts de criacao e teste de amostras.

### Impacto

O repositorio ficou mais leve e mais adequado para reproducibilidade: codigo no Git, dados baixados localmente.

### Como usar

Baixar dataset:

```bash
bash scripts/download_dataset.sh
```

Usar outro diretorio de dados:

```bash
DATA_ROOT=/caminho/para/dados python scripts/run_full_experiment.py
```

### Precaucao

Confirmar antes da execucao:

```bash
du -h -d 2 data
find data/raw/nyc_taxi -name '*.parquet' | head
```

---

## v0.8 - Correcao do download de JARs

**Commit:** `e2effb3`  
**Mensagem:** `Fix Spark Kafka JAR download script`

### O que foi feito

- Corrigido `scripts/download_jars.sh`.
- Adicionado uso de cache Ivy externo temporario.
- Melhorado o processo de resolucao do pacote:
  - `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1`
- Copiados os JARs resolvidos para `./jars`.

### Impacto

Reduziu falhas no pre-download dos conectores Spark/Kafka e tornou a preparacao do ambiente mais previsivel.

### Como validar

```bash
bash scripts/download_jars.sh
ls jars/*.jar
```

---

## v0.9 - Simplificacao das instrucoes de dataset

**Commit:** `a969e94`  
**Mensagem:** `Simplify dataset instructions in README`

### O que foi feito

- Simplificadas as instrucoes no README sobre dataset.
- Removidas instrucoes redundantes ou confusas.

### Impacto

Deixou o onboarding mais direto: baixar dataset com script, criar amostras automaticamente pelo runner e manter dados fora do Git.

---

## v0.10 - Correcao do throughput stream zerado

**Commit:** `f673150`  
**Mensagem:** `Corrige compressao lz4 no producer stream`

### O que foi feito

- Adicionado `lz4` ao `requirements.txt`.
- Alterado `producer/taxi_stream_producer.py` para aceitar:

```bash
--compression lz4
--compression none
```

- Adicionada mensagem de erro clara quando `lz4` nao estiver disponivel.

### Problema corrigido

O throughput stream aparecia zerado porque o produtor Kafka falhava antes de enviar mensagens:

```text
AssertionError: Libraries for lz4 compression codec not found
```

Com `producer_events_sent=0`, o Spark processava zero linhas e o throughput calculado era `0.0`.

### Impacto

O streaming voltou a produzir eventos quando as dependencias estao instaladas corretamente.

### Como validar

```bash
source .venv/bin/activate
python -m pip install -r requirements.txt
python scripts/run_full_experiment.py --skip-batch --stream-repetitions 1 --stream-duration-sec 10
tail -n 5 results/raw/stream_runs.csv
```

Campos esperados:

- `producer_events_sent > 0`
- `stream_total_input_rows > 0`
- `stream_throughput_rows_per_sec > 0`

---

## v0.11 - Controle de disco e registro operacional

**Commit:** `091e8b5`  
**Mensagem:** `Documenta problemas e limita uso de disco`

### O que foi feito

- Adicionado `docs/registro_problemas_experimento.md`.
- Atualizado README para apontar para o registro de problemas.
- Configurado Spark worker para limpar dados antigos:

```yaml
SPARK_WORKER_OPTS: >-
  -Dspark.worker.cleanup.enabled=true
  -Dspark.worker.cleanup.interval=60
  -Dspark.worker.cleanup.appDataTtl=120
```

- Adicionada limpeza explicita em `scripts/run_full_experiment.py` para remover:

```text
/opt/spark/work/app-*
```

- Removido uso de `--packages` no streaming, usando apenas `--jars` locais.

### Problema corrigido

Durante campanhas longas, o container `spark-worker` acumulava dezenas ou centenas de GB na camada gravavel Docker.

Exemplo observado:

```text
stream-batch-experiment-spark-worker-1: 39.4GB
```

### Impacto

Reduz fortemente o risco de travamento por falta de disco durante campanhas longas.

### Como validar

Antes e durante a campanha:

```bash
docker system df
docker ps -a --size
df -h .
```

Se algum container antigo estiver inchado:

```bash
docker compose down
```

---

## Convencao recomendada para proximas versoes

Para facilitar a escrita cientifica e a reproducibilidade, recomenda-se criar tags para proximas versoes estaveis:

```bash
git tag -a v0.12 -m "Descricao curta da versao"
git push origin v0.12
```

Sugestao de criterios:

- `v0.x`: ajustes experimentais, infraestrutura e documentacao.
- `v1.0`: primeira versao estavel usada para coleta final dos resultados.
- `v1.x`: correcoes reprodutiveis depois da coleta final, sem alterar metodologia central.

---

## Relacao com outros documentos

- Registro de incidentes e correcoes operacionais: [`registro_problemas_experimento.md`](registro_problemas_experimento.md)
- Guia principal de execucao: [`../README.md`](../README.md)

