# Experimento Científico — Batch vs Stream com Spark + Kafka

Comparativo empírico entre processamento batch e streaming sobre o dataset NYC Taxi.
O desenho experimental controla volume, taxa de eventos, número de workers e cadência de micro-batch, coletando métricas de throughput, latência, CPU e memória de forma automatizada.

---

## Requisitos de hardware

| Recurso | Mínimo recomendado |
|---|---|
| RAM | 16 GB |
| CPU | 4 cores |
| Disco livre | 35 GB (dataset bruto + amostras + resultados) |
| SO | Linux (Ubuntu 22.04+) |

A soma dos containers em uso simultâneo com 4 workers é aproximadamente 10 GB (Kafka 1 GB + Spark master 2 GB + 4 workers × 2,5 GB), deixando margem suficiente para o SO e o processo Python.

---

## Objetivo

Responder, com base empírica reproduzível, como os paradigmas batch e stream se comportam sob diferentes volumes, taxas de entrada, triggers e níveis de paralelismo no Spark.

### Questões de pesquisa

- **RQ1** — Qual paradigma apresenta maior throughput em sistemas intensivos de dados?
- **RQ2** — Como o volume de dados afeta o desempenho batch vs stream?
- **RQ3** — Qual o impacto da taxa de eventos na latência do streaming?
- **RQ4** — Como a escalabilidade horizontal afeta os dois paradigmas?

---

## Variáveis independentes

| Variável | Valores |
|---|---|
| Paradigma | Batch, Stream |
| Volume de dados | 200 MB, 1 GB, 3 GB, 10 GB |
| Taxa de eventos | 200, 500, 1000, 2000 eventos/s |
| Número de workers Spark | 1, 2, 4 |
| Trigger de micro-batch | 1 s, 2 s, 5 s |

## Variáveis dependentes

| Métrica | Descrição |
|---|---|
| Tempo total | Tempo de execução do job |
| Latência | Tempo médio evento → processamento |
| Throughput | Linhas processadas por segundo |
| CPU média / pico | Uso médio e máximo dos containers |
| Memória média / pico | Consumo médio e máximo |
| Backpressure | Atraso do micro-batch frente ao trigger configurado |
| Processing rate | `processedRowsPerSecond` do Structured Streaming |
| Network I/O / Disk I/O | Telemetria dos containers |

---

## Cenários experimentais

### Batch

**Bloco de carga** — varia volume, workers fixo em 1:

| ID | Dataset | Workers |
|---|---|---|
| BL1 | 200 MB | 1 |
| BL2 | 1 GB | 1 |
| BL3 | 3 GB | 1 |
| BL4 | 10 GB | 1 |

**Bloco de escalabilidade** — varia workers por volume:

| ID | Dataset | Workers |
|---|---|---|
| BS1–BS3 | 200 MB | 1 / 2 / 4 |
| BS4–BS6 | 1 GB | 1 / 2 / 4 |
| BS7–BS9 | 3 GB | 1 / 2 / 4 |
| BS10–BS12 | 10 GB | 1 / 2 / 4 |

### Stream

**Bloco de carga** — varia taxa, trigger 1 s, 1 worker:

| ID | Rate | Trigger | Workers |
|---|---|---|---|
| SL1 | 200/s | 1 s | 1 |
| SL2 | 500/s | 1 s | 1 |
| SL3 | 1000/s | 1 s | 1 |
| SL4 | 2000/s | 1 s | 1 |

**Bloco de escalabilidade** — varia workers por taxa:

| ID | Rate | Workers |
|---|---|---|
| SS1–SS3 | 200/s | 1 / 2 / 4 |
| SS4–SS6 | 500/s | 1 / 2 / 4 |
| SS7–SS9 | 1000/s | 1 / 2 / 4 |
| SS10–SS12 | 2000/s | 1 / 2 / 4 |

**Bloco de sensibilidade de trigger** — varia trigger, carga e workers fixos:

| ID | Rate | Trigger | Workers |
|---|---|---|---|
| ST1 | 1000/s | 1 s | 1 |
| ST2 | 1000/s | 2 s | 1 |
| ST3 | 1000/s | 5 s | 1 |

---

## Repetições e poder estatístico

O runner usa **30 repetições por cenário** por padrão.

| Grandeza | Valor |
|---|---|
| Cenários batch | 16 |
| Cenários stream | 19 |
| Total de cenários | 35 |
| Repetições padrão | 30 |
| Total de execuções | 1050 |

Estime **12–24 h** de execução para a campanha completa, dependendo do hardware. Comece sempre com 1 repetição para validar a esteira antes de iniciar a campanha.

### Normalidade e intervalo de confiança

O `consolidate_results.py` aplica o teste de **Shapiro-Wilk** por cenário e por métrica. O campo `*_normal` nos CSVs de sumário indica se a distribuição é normal (p > 0,05):

- `True` — IC 95% paramétrico (z = 1,96) é válido.
- `False` — prefira bootstrap IC ou IC de Wilcoxon para aquele cenário.
- `null` — menos de 3 amostras ou `scipy` não instalado.

---

## Estrutura do projeto

```text
stream-batch-experiment/
├── data/
│   ├── raw/nyc_taxi/*.parquet
│   └── samples/{200mb,1gb,3gb,10gb}
├── jars/                          ← JARs pré-baixados do conector Kafka
├── jobs/
│   ├── batch_job.py
│   └── stream_job.py
├── producer/
│   └── taxi_stream_producer.py
├── scripts/
│   ├── capture_environment.py
│   ├── consolidate_results.py
│   ├── container_monitor.py
│   ├── create_samples.py
│   ├── download_jars.sh
│   ├── generate_plots.py
│   ├── generate_report.py
│   ├── run_full_experiment.py
│   └── run_experiments.py
└── results/
    ├── raw/
    ├── summary/
    ├── plots/
    ├── report.md
    └── environment.json
```

---

## Pré-requisitos

- Linux (Ubuntu 22.04+ recomendado)
- Docker Engine ≥ 24 com Compose V2
- Python 3.10+
- Acesso do usuário ao daemon Docker

```bash
sudo usermod -aG docker $USER   # logout e login após
```

---

## Instalação

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

O `requirements.txt` inclui `scipy`, necessário para o teste de Shapiro-Wilk no `consolidate_results.py`.

---

## Pré-download dos JARs (recomendado)

O conector Kafka para Spark é baixado em cada cenário stream se não estiver disponível localmente, introduzindo variância de rede entre repetições. Para eliminar essa variância:

```bash
bash scripts/download_jars.sh
```

Os JARs ficam em `./jars/` e são montados automaticamente nos containers via volume.

---

## Execução

### 1. Validação operacional (obrigatória antes de qualquer campanha)

Confirma que a esteira completa — Docker, Kafka, Spark, producer, coleta de métricas, CSVs, plots e relatório — executa sem erros. Não gera dados estatisticamente válidos.

```bash
python scripts/run_full_experiment.py \
  --batch-repetitions 1 \
  --stream-repetitions 1
```

### 2. Estudo piloto

Estima a duração total da campanha e detecta gargalos operacionais:

```bash
python scripts/run_full_experiment.py \
  --batch-repetitions 3 \
  --stream-repetitions 3
```

### 3. Campanha final

```bash
python scripts/run_full_experiment.py \
  --batch-repetitions 30 \
  --stream-repetitions 30 \
  --stream-duration-sec 30 \
  --stats-interval-sec 1
```

### Wrapper shell

```bash
./scripts/run_experiments.sh
```

### Parâmetros disponíveis

| Parâmetro | Padrão | Descrição |
|---|---|---|
| `--batch-repetitions` | 30 | Repetições por cenário batch |
| `--stream-repetitions` | 30 | Repetições por cenário stream |
| `--stream-duration-sec` | 30 | Duração de cada execução stream |
| `--stats-interval-sec` | 1.0 | Intervalo de coleta de métricas dos containers |
| `--skip-batch` | — | Pula todos os cenários batch |
| `--skip-stream` | — | Pula todos os cenários stream |
| `--topic-prefix` | taxi-topic | Prefixo dos tópicos Kafka |
| `--python` | auto | Caminho do Python (detectado automaticamente) |

---

## Fluxo automatizado

O runner executa em sequência:

1. Snapshot do ambiente em `results/environment.json`
2. Subida da infraestrutura Docker (`compose up`)
3. Geração das amostras de dados se ausentes
4. Execução dos cenários batch e stream com escalonamento automático de workers
5. Coleta de métricas dos containers em paralelo (série temporal)
6. Consolidação estatística com Shapiro-Wilk por cenário
7. Geração de plots
8. Geração de relatório markdown

O escalonamento de workers é feito por cenário e usa cache interno: `docker compose up --scale` só é chamado quando o número de workers muda entre cenários consecutivos.

---

## Artefatos gerados

| Arquivo | Conteúdo |
|---|---|
| `results/environment.json` | CPU, RAM, OS, versões Docker e Spark |
| `results/raw/batch_runs.csv` | Uma linha por repetição batch |
| `results/raw/stream_runs.csv` | Uma linha por repetição stream |
| `results/raw/container_metrics.csv` | Série temporal de CPU/memória por container |
| `results/raw/stream_microbatches.csv` | Métricas por micro-batch |
| `results/summary/batch_summary.csv` | Estatísticas por cenário batch (inclui Shapiro-Wilk) |
| `results/summary/stream_summary.csv` | Estatísticas por cenário stream (inclui Shapiro-Wilk) |
| `results/summary/container_summary.csv` | Estatísticas de recursos por container |
| `results/summary/stream_microbatch_summary.csv` | Estatísticas de micro-batch |
| `results/plots/*.png` | Throughput, latência, CPU, escalabilidade |
| `results/report.md` | Resumo textual dos resultados |

---

## Melhorias aplicadas nesta versão

### Infraestrutura
- **KRaft no lugar de Zookeeper** — o Kafka 7.7.0 roda em modo KRaft nativo, eliminando o container de Zookeeper e economizando ~512 MB de RAM e uma dependência de serviço.
- **JARs pré-baixados** — o conector `spark-sql-kafka` é baixado uma vez via `download_jars.sh` e montado como volume, eliminando variância de rede entre repetições.
- **`mem_limit` revisado para 16 GB** — Kafka 1 GB, Spark master 2 GB, workers 2,5 GB cada.
- **Healthcheck no Kafka** — o compose aguarda o broker responder antes de liberar dependentes.
- **Retenção de log configurada** — `KAFKA_LOG_RETENTION_MS=3600000` evita acúmulo de dados entre repetições longas.

### Jobs
- **`maxOffsetsPerTrigger` dinâmico** — calculado a partir da taxa real do cenário (`rate_eps × trigger_sec × 2`), evitando sub-consumo em cargas baixas e pico de memória em cargas altas.
- **Scan único no batch** — `count` e `avg` calculados no mesmo `groupBy`, eliminando dois scans do dataset.
- **`foreachBatch` com cache** — o micro-batch é materializado uma vez, estatísticas de latência e agregação de negócio reutilizam o cache; `unpersist()` libera memória ao final.

### Runner
- **Escalonamento idempotente** — `docker compose up --scale` só é chamado quando o número de workers muda, reduzindo overhead entre repetições consecutivas do mesmo cenário.
- **CSV em batch** — `_write_csv` abre e fecha o arquivo uma vez por conjunto de linhas, não uma vez por linha.
- **Log com timestamp** — cada execução imprime `[HH:MM:SS][modo] ID | parâmetros`, facilitando estimativa de tempo restante.

### Análise estatística
- **Shapiro-Wilk por cenário** — `consolidate_results.py` testa normalidade de cada distribuição de métrica. O campo `*_normal` sinaliza se o IC 95% paramétrico é válido; caso contrário, use bootstrap IC.

---

## Melhorias sugeridas para trabalhos futuros

- **Prometheus + Grafana** — visualização de métricas em tempo real durante a coleta via Docker Compose.
- **Coleta de heap JVM** — via Spark REST API `/api/v1/applications` para separar memória de aplicação de memória de SO.
- **Bloco de latência fim-a-fim no batch** — simular chegada incremental de arquivos para tornar a comparação com stream metodologicamente mais simétrica.
- **Trigger contínuo** — testar `trigger(continuous="1 second")` disponível desde Spark 2.3 para avaliar latência sub-segundo.
- **Terceiro paradigma** — incluir Flink ou Kafka Streams para benchmark triangular.
- **Teste de Kruskal-Wallis** — complementar análise visual de escalabilidade com teste estatístico entre grupos de workers.

---

## Troubleshooting

**`docker compose up` falha**
```bash
ss -tlnp | grep -E '9092|29092|7077|8084'
docker compose logs kafka
docker compose logs spark-master
```

**Kafka demora a responder**
- O healthcheck aguarda até 100 s; verifique `docker compose ps` para ver o status.
- Em máquinas lentas, aumente `retries` no healthcheck do `docker-compose.yml`.

**Stream falha ao carregar o conector Kafka**
- Execute `bash scripts/download_jars.sh` para pré-baixar os JARs.
- O runner usa `--jars` com os JARs locais e `--packages` como fallback.

**Workers não sobem ao escalar**
- Confirme que `docker-compose.yml` não tem `container_name` fixo para `spark-worker`.
- Verifique: `docker ps --filter label=com.docker.compose.service=spark-worker`

**`scipy` não encontrado no consolidate_results**
```bash
pip install scipy
```
Sem `scipy`, o Shapiro-Wilk é omitido mas os demais sumários são gerados normalmente.

**Producer falha ao ler parquet**
```bash
pip show pyarrow
```

