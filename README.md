# Experimento Batch vs Stream com Spark + Kafka (NYC Taxi)

## 1) Objetivo
Este projeto compara processamento **batch** e **stream** com o mesmo dominio (corridas de taxi), coletando metricas de:
- desempenho (tempo, throughput),
- latencia (media e distribuicao por evento),
- uso de recursos (CPU e memoria),
- metadados de execucao (cenario, repeticao, run_id).

O desenho foi feito para funcionar localmente e ser escalado depois com mudanca de parametros.

## 2) Tecnologias
- Docker / Docker Compose
- Apache Spark (master + worker)
- Apache Kafka (KRaft)
- Python (PySpark, pandas, kafka-python)
- PowerShell (automacao e coleta de recursos)

## 3) Estrutura principal
```text
stream-batch-experiment
├── data
│   ├── raw
│   │   └── nyc_taxi
│   └── samples
│       ├── 200mb
│       ├── 1gb
│       └── 3gb
├── jobs
│   ├── batch_taxi_job.py
│   └── stream_taxi_job.py
├── producer
│   └── taxi_stream_producer.py
├── scripts
│   ├── test_read_dataset.py
│   ├── create_samples.py
│   ├── collect_docker_stats.ps1
│   ├── run_batch_experiment.ps1
│   └── run_stream_experiment.ps1
├── results
│   ├── batch_results.csv
│   ├── stream_results.csv
│   ├── latency_results.csv
│   └── resource_usage.csv
└── docker-compose.yml
```

## 4) Pre-requisitos
- Docker Desktop ativo
- Python 3 com ambiente virtual (`venv`)
- Dependencias Python instaladas:
```powershell
venv\Scripts\python.exe -m pip install -r requirements.txt
```

## 5) Subir infraestrutura
```powershell
docker compose up -d
docker compose ps
```

Servicos esperados:
- `spark-master`
- `spark-worker`
- `stream-batch-experiment-kafka-1`
- `stream-batch-experiment-zookeeper-1`

## 6) ETAPA A - Dataset bruto NYC Taxi
Crie pasta e baixe os 3 meses:
```powershell
mkdir data\raw\nyc_taxi -Force
cd data\raw\nyc_taxi
curl -L -O https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet
curl -L -O https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-02.parquet
curl -L -O https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-03.parquet
cd ..\..\..
```

## 7) ETAPA B - Validar leitura
```powershell
venv\Scripts\python.exe scripts\test_read_dataset.py
```

Se mostrar `Total rows` e amostra de linhas, leitura OK.

## 8) ETAPA C - Gerar amostras experimentais
```powershell
venv\Scripts\python.exe scripts\create_samples.py
```

Saidas:
- `data/samples/200mb`
- `data/samples/1gb`
- `data/samples/3gb`

## 9) ETAPA D - Rodar experimento batch (automatico)
Script:
- 1 warm-up por cenario (opcional)
- 5 repeticoes validas por cenario
- cenarios B1/B2/B3
- coleta de recursos em paralelo

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_batch_experiment.ps1
```

Parametros uteis:
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_batch_experiment.ps1 -ValidRuns 5 -IncludeWarmup
```

## 10) ETAPA E - Rodar experimento stream (automatico)
Script:
- garante topico `taxi-topic`
- sobe job stream em background
- inicia producer na taxa do cenario
- coleta recursos em paralelo
- 1 warm-up + 5 repeticoes validas por cenario S1/S2/S3

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_stream_experiment.ps1
```

Parametros uteis:
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_stream_experiment.ps1 -ValidRuns 5 -Duration 60 -IncludeWarmup
```

## 11) Cenarios oficiais
### Batch
- B1: `data/samples/200mb`
- B2: `data/samples/1gb`
- B3: `data/samples/3gb`

### Stream
- S1: 200 eventos/s
- S2: 500 eventos/s
- S3: 1000 eventos/s

Total recomendado:
- 3 cenarios batch x (1 warm-up + 5 validas) = 18 execucoes
- 3 cenarios stream x (1 warm-up + 5 validas) = 18 execucoes
- **Total geral = 36 execucoes**

## 12) Arquivos de resultado
### `results/batch_results.csv`
Colunas:
- `run_id`
- `scenario`
- `data_path`
- `rows`
- `execution_time_s`
- `throughput_rps`

### `results/stream_results.csv`
Colunas:
- `run_id`
- `scenario`
- `batch_id`
- `records`
- `avg_latency_s`

### `results/latency_results.csv`
Colunas:
- `run_id`
- `scenario`
- `batch_id`
- `latency_s` (1 linha por evento do microbatch)

### `results/resource_usage.csv`
Colunas:
- `run_id`
- `timestamp`
- `container`
- `cpu_percent`
- `mem_usage`
- `mem_percent`

## 13) Como interpretar depois
Com os CSVs, calcule:
- media, mediana, desvio padrao
- percentis de latencia (P50, P95, P99)
- throughput por cenario
- comparacao batch vs stream
- comportamento de CPU/memoria por carga

## 14) Execucao manual (opcional)
### Batch manual (exemplo B1)
```powershell
docker exec `
  -e RUN_ID=B1_r1 `
  -e SCENARIO=B1 `
  -e DATA_PATH=/opt/data/samples/200mb `
  -e RESULTS_FILE=/opt/results/batch_results.csv `
  spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --conf spark.executor.memory=1g `
  --conf spark.driver.memory=512m `
  --conf spark.sql.shuffle.partitions=8 `
  /opt/jobs/batch_taxi_job.py
```

### Stream manual (exemplo S1)
1. Inicie stream:
```powershell
docker exec -it `
  -e RUN_ID=S1_r1 `
  -e SCENARIO=S1 `
  -e RESULTS_FILE=/opt/results/stream_results.csv `
  -e LATENCY_FILE=/opt/results/latency_results.csv `
  spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --conf spark.executor.memory=1g `
  --conf spark.driver.memory=512m `
  --conf spark.sql.shuffle.partitions=8 `
  --conf spark.jars.ivy=/tmp/.ivy `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 `
  /opt/jobs/stream_taxi_job.py
```

2. Em outro terminal, rode producer:
```powershell
$env:DATA_PATH="data/samples/200mb"
$env:RATE="200"
$env:KAFKA_BROKER="localhost:29092"
$env:KAFKA_TOPIC="taxi-topic"
venv\Scripts\python.exe producer\taxi_stream_producer.py
```

## 15) Escalar para infraestrutura maior
Ao migrar de maquina, mantenha scripts/metodologia e ajuste apenas:
- `spark.executor.memory`
- `spark.executor.instances`
- `spark.sql.shuffle.partitions`
- volume de dados
- taxa de eventos
- duracao dos cenarios
- numero de repeticoes

Isso preserva comparabilidade metodologica.

## 16) Checklist de execucao (36 runs)
Use este checklist para acompanhar o progresso da campanha experimental.

### Preparacao
- [ ] Docker Desktop iniciado
- [ ] `docker compose up -d` executado sem erro
- [ ] Dataset bruto em `data/raw/nyc_taxi`
- [ ] Samples gerados em `data/samples/200mb`, `1gb`, `3gb`
- [ ] Dependencias Python instaladas no `venv`
- [ ] Pasta `results/` limpa para nova campanha (opcional)

### Batch (18 execucoes)
#### B1 (200mb)
- [ ] B1_warmup
- [ ] B1_r1
- [ ] B1_r2
- [ ] B1_r3
- [ ] B1_r4
- [ ] B1_r5

#### B2 (1gb)
- [ ] B2_warmup
- [ ] B2_r1
- [ ] B2_r2
- [ ] B2_r3
- [ ] B2_r4
- [ ] B2_r5

#### B3 (3gb)
- [ ] B3_warmup
- [ ] B3_r1
- [ ] B3_r2
- [ ] B3_r3
- [ ] B3_r4
- [ ] B3_r5

### Stream (18 execucoes)
#### S1 (200 eps)
- [ ] S1_warmup
- [ ] S1_r1
- [ ] S1_r2
- [ ] S1_r3
- [ ] S1_r4
- [ ] S1_r5

#### S2 (500 eps)
- [ ] S2_warmup
- [ ] S2_r1
- [ ] S2_r2
- [ ] S2_r3
- [ ] S2_r4
- [ ] S2_r5

#### S3 (1000 eps)
- [ ] S3_warmup
- [ ] S3_r1
- [ ] S3_r2
- [ ] S3_r3
- [ ] S3_r4
- [ ] S3_r5

### Pos-execucao
- [ ] `results/batch_results.csv` preenchido
- [ ] `results/stream_results.csv` preenchido
- [ ] `results/latency_results.csv` preenchido
- [ ] `results/resource_usage.csv` preenchido
- [ ] Backup dos CSVs brutos realizado
- [ ] Consolidacao/analise iniciada
