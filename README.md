# Experimento Batch vs Stream com Spark + Kafka

Este repositório está preparado para ser executado em Linux. A automação principal fica em `scripts/run_experiments.py`, e há um wrapper shell em `scripts/run_experiments.sh`.

## Objetivo
Comparar processamento batch e stream usando Spark + Kafka, coletando:
- tempo total
- throughput
- uso médio e pico de CPU/memória dos containers
- métricas agregadas do Structured Streaming

## Estrutura atual
```text
stream-batch-experiment/
├── data/
│   ├── small/input.csv
│   └── medium/input.csv
├── jobs/
│   ├── batch_job.py
│   └── stream_job.py
├── producer/
│   └── kafka_producer.py
├── scripts/
│   ├── run_experiments.py
│   ├── run_experiments.sh
│   ├── generate_datasets.py
│   ├── consolidate_results.py
│   └── test_read_dataset.py
├── results/
│   └── raw/
├── docker-compose.yml
└── requirements.txt
```

## Pré-requisitos no Linux
- Docker Engine com Compose habilitado
- Python 3.10+ com `venv`
- acesso ao daemon Docker pelo seu usuário

Exemplo de preparação:
```bash
python3 -m venv venv
source venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Se o usuário atual não conseguir rodar `docker ps`, ajuste o grupo `docker` antes de executar o experimento.

## Subir a infraestrutura
```bash
docker compose up -d
docker compose ps
```

Serviços esperados:
- `spark-master`
- `spark-worker`
- `kafka`
- `zookeeper`

## Validar o ambiente
Teste de leitura com Spark local:
```bash
source venv/bin/activate
python scripts/test_read_dataset.py
```

Gerar dataset maior a partir do dataset pequeno:
```bash
source venv/bin/activate
python scripts/generate_datasets.py
```

## Rodar o experimento completo
Pelo wrapper shell:
```bash
chmod +x scripts/run_experiments.sh
./scripts/run_experiments.sh --warmup
```

Ou direto pelo Python:
```bash
source venv/bin/activate
python scripts/run_experiments.py --warmup
```

Parâmetros úteis:
```bash
python scripts/run_experiments.py \
  --batch-repetitions 5 \
  --stream-repetitions 5 \
  --stream-duration-sec 30 \
  --stream-trigger-sec 2 \
  --stats-interval-sec 1 \
  --topic input-topic
```

## Cenários atuais
Batch:
- `small`: `data/small/input.csv`
- `medium`: `data/medium/input.csv`

Stream:
- `stream-200eps`
- `stream-500eps`
- `stream-1000eps`

## Resultados gerados
Execuções brutas:
- `results/raw/batch_runs.csv`
- `results/raw/stream_runs.csv`

Consolidação:
```bash
source venv/bin/activate
python scripts/consolidate_results.py
```

Saídas consolidadas:
- `results/summary/batch_summary.csv`
- `results/summary/stream_summary.csv`

## Observações para Linux
- O script agora detecta automaticamente `venv/bin/python`.
- Os caminhos passados para os containers são convertidos para formato POSIX antes do `docker exec`.
- O projeto não depende dos scripts `.ps1` para a execução principal em Linux.
- `localhost:29092` é usado apenas pelo producer local; dentro do container Spark, o Kafka é acessado por `kafka:9092`.
- `docker-compose.yml` fixa os nomes dos containers principais para evitar dependência do nome da pasta do projeto.

## Troubleshooting
Se `docker compose up -d` falhar:
- verifique portas em uso: `29092`, `7077`, `8080`, `8081`
- veja os logs do serviço com problema: `docker compose logs kafka` ou `docker compose logs spark-master`
- confirme acesso ao daemon Docker com `docker ps`

Se o batch ou stream falhar no `spark-submit`:
- verifique o log do Spark master: `docker logs spark-master`
- confirme volumes montados: `docker exec spark-master ls /opt/jobs` e `docker exec spark-master ls /opt/data`
- confirme que o Kafka responde: `docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list`

Se o stream não consumir mensagens:
- confirme que o producer local usa `localhost:29092`
- confirme que o job Spark no container usa `kafka:9092`
- teste a criação do tópico dentro do container Kafka antes da execução
