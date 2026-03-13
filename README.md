# Experimento Cientifico Batch vs Stream com Spark + Kafka

Este repositório executa um experimento comparativo entre processamento batch e streaming sobre o dataset real de NYC Taxi em `data/raw/nyc_taxi`. O fluxo principal segue um desenho experimental de sistemas computacionais, com cenários controlados, repetições, coleta automatizada de métricas, geração de plots e relatório final.

O README está alinhado com o desenho atualmente implementado no código. Se novos cenários forem adicionados, este documento deve ser atualizado junto com `scripts/run_full_experiment.py`.

## Objetivo

Responder, com base empírica reproduzível, como os paradigmas batch e stream se comportam sob diferentes volumes, taxas de entrada, triggers e níveis de paralelismo no Spark.

## Variáveis independentes

| Variável | Valores |
| --- | --- |
| Paradigma | Batch, Stream |
| Volume de dados | 200MB, 1GB, 3GB, 10GB |
| Taxa de eventos | 200, 500, 1000, 2000 eventos/s |
| Número de workers Spark | 1, 2, 4 |
| Trigger de micro-batch | 1s, 2s |

## Variáveis dependentes

| Métrica | Descrição |
| --- | --- |
| Tempo total | Tempo de execução do job |
| Latência | Tempo médio evento -> processamento |
| Throughput | Eventos ou linhas processadas por segundo |
| CPU média / pico | Uso médio e máximo dos containers |
| Memória média / pico | Consumo médio e máximo |
| Backpressure | Atraso do micro-batch frente ao trigger configurado |
| Processing rate | `processedRowsPerSecond` do Structured Streaming |
| Network I/O / Disk I/O | Telemetria dos containers |

## Cenários experimentais

### Batch

Bloco de carga:

| ID | Categoria | Dataset | Workers |
| --- | --- | --- | --- |
| BL1 | carga | 200MB | 1 |
| BL2 | carga | 1GB | 1 |
| BL3 | carga | 3GB | 1 |
| BL4 | carga | 10GB | 1 |

Bloco de escalabilidade com todas as cargas propostas:

| ID | Categoria | Dataset | Workers |
| --- | --- | --- | --- |
| BS1 | escalabilidade | 200MB | 1 |
| BS2 | escalabilidade | 200MB | 2 |
| BS3 | escalabilidade | 200MB | 4 |
| BS4 | escalabilidade | 1GB | 1 |
| BS5 | escalabilidade | 1GB | 2 |
| BS6 | escalabilidade | 1GB | 4 |
| BS7 | escalabilidade | 3GB | 1 |
| BS8 | escalabilidade | 3GB | 2 |
| BS9 | escalabilidade | 3GB | 4 |
| BS10 | escalabilidade | 10GB | 1 |
| BS11 | escalabilidade | 10GB | 2 |
| BS12 | escalabilidade | 10GB | 4 |

### Stream

Bloco de carga:

| ID | Categoria | Rate | Trigger | Workers |
| --- | --- | --- | --- | --- |
| SL1 | carga | 200/s | 1s | 1 |
| SL2 | carga | 500/s | 1s | 1 |
| SL3 | carga | 1000/s | 1s | 1 |
| SL4 | carga | 2000/s | 1s | 1 |

Bloco de escalabilidade com todas as cargas propostas:

| ID | Categoria | Rate | Trigger | Workers |
| --- | --- | --- | --- | --- |
| SS1 | escalabilidade | 200/s | 1s | 1 |
| SS2 | escalabilidade | 200/s | 1s | 2 |
| SS3 | escalabilidade | 200/s | 1s | 4 |
| SS4 | escalabilidade | 500/s | 1s | 1 |
| SS5 | escalabilidade | 500/s | 1s | 2 |
| SS6 | escalabilidade | 500/s | 1s | 4 |
| SS7 | escalabilidade | 1000/s | 1s | 1 |
| SS8 | escalabilidade | 1000/s | 1s | 2 |
| SS9 | escalabilidade | 1000/s | 1s | 4 |
| SS10 | escalabilidade | 2000/s | 1s | 1 |
| SS11 | escalabilidade | 2000/s | 1s | 2 |
| SS12 | escalabilidade | 2000/s | 1s | 4 |

Bloco de sensibilidade de trigger:

| ID | Categoria | Rate | Trigger | Workers |
| --- | --- | --- | --- | --- |
| ST1 | trigger | 1000/s | 1s | 1 |
| ST2 | trigger | 1000/s | 2s | 1 |
| ST3 | trigger | 1000/s | 5s | 1 |

## Repetições

O runner científico usa por padrão `30` repetições válidas por cenário, permitindo:

- média
- desvio padrão
- IQR
- intervalo de confiança de 95%

Com o desenho atual, isso representa:

- `16` cenários batch
- `19` cenários stream
- `35` cenários no total

Em `30` repetições por cenário, o protocolo completo chega a `1050` execuções. Para validação operacional, é recomendável começar com `1` repetição por cenário e só depois avançar para a campanha completa do experimento.

Esse design permite comparar, de forma isolada:

- o efeito do volume em batch
- o efeito da taxa de eventos em stream
- o efeito de mudar apenas a quantidade de workers mantendo a mesma carga
- o efeito de mudar apenas o trigger mantendo a mesma carga no streaming

## Estrutura principal

```text
stream-batch-experiment/
├── data/
│   ├── raw/nyc_taxi/*.parquet
│   └── samples/{200mb,1gb,3gb,10gb}
├── jobs/
│   ├── batch_job.py
│   └── stream_job.py
├── producer/
│   └── taxi_stream_producer.py
├── scripts/
│   ├── capture_environment.py
│   ├── container_monitor.py
│   ├── consolidate_results.py
│   ├── create_samples.py
│   ├── generate_plots.py
│   ├── generate_report.py
│   ├── run_full_experiment.py
│   └── run_experiments.py
└── results/
```

## Pré-requisitos

- Linux
- Docker Engine com Compose habilitado
- Python 3.10+ com `venv`
- acesso do usuário ao daemon Docker

Instalação:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

## Execução

Runner científico completo:

```bash
python scripts/run_full_experiment.py --warmup
```

Wrapper compatível:

```bash
./scripts/run_experiments.sh --warmup
```

Parâmetros úteis:

```bash
python scripts/run_full_experiment.py \
  --batch-repetitions 30 \
  --stream-repetitions 30 \
  --stream-duration-sec 30 \
  --stats-interval-sec 1
```

## Protocolo experimental

O experimento será executado em quatro fases.

### Fase 1: validação operacional

Objetivo:

- verificar se a esteira completa executa sem falhas
- confirmar geração de CSVs, plots, relatório e `environment.json`
- confirmar funcionamento de Spark, Kafka, escalonamento de workers e coleta de métricas

Execução:

```bash
python scripts/run_full_experiment.py --warmup --batch-repetitions 1 --stream-repetitions 1
```

Essa fase não será usada para conclusões estatísticas. Ela servirá apenas para validar o protocolo.

### Fase 2: estudo piloto

Objetivo:

- estimar tempo total da campanha
- identificar gargalos operacionais
- avaliar viabilidade dos cenários mais pesados
- observar a variabilidade inicial das métricas

Execução:

```bash
python scripts/run_full_experiment.py --batch-repetitions 3 --stream-repetitions 3
```

Essa fase ajuda a decidir se o ambiente suporta a campanha final completa sem alterar o design experimental.

### Fase 3: campanha experimental final

Objetivo:

- produzir os dados definitivos da dissertação
- coletar repetições suficientes para análise estatística robusta

Execução:

```bash
python scripts/run_full_experiment.py --batch-repetitions 30 --stream-repetitions 30
```

Nessa fase, o ideal é não alterar código, infraestrutura, versões de dependência ou hardware durante a coleta.

### Fase 4: análise e redação

Objetivo:

- consolidar resultados
- interpretar métricas por bloco experimental
- responder às questões de pesquisa
- incorporar gráficos e tabelas no texto final

Artefatos usados nessa fase:

- `results/raw/*.csv`
- `results/summary/*.csv`
- `results/plots/*.png`
- `results/report.md`
- `results/environment.json`

### Interpretação metodológica

- `1` repetição por cenário serve para validação operacional
- `3` a `5` repetições por cenário servem para estudo piloto
- `30` repetições por cenário servem para a campanha final do experimento


## Fluxo automatizado

O runner executa automaticamente:

1. subida da infraestrutura Docker
2. escalonamento do número de workers Spark por cenário
3. warm-up opcional
4. execução dos cenários batch e stream
5. coleta de métricas dos containers
6. coleta de métricas de micro-batches do Structured Streaming
7. consolidação estatística
8. geração de plots
9. geração de relatório markdown
10. captura do ambiente experimental em JSON

## Artefatos gerados

### Ambiente

- `results/environment.json`

### Resultados brutos

- `results/raw/batch_runs.csv`
- `results/raw/stream_runs.csv`
- `results/raw/container_metrics.csv`
- `results/raw/stream_microbatches.csv`

### Sumários

- `results/summary/batch_summary.csv`
- `results/summary/stream_summary.csv`
- `results/summary/container_summary.csv`
- `results/summary/stream_microbatch_summary.csv`

### Plots

- `results/plots/throughput_batch_vs_stream.png`
- `results/plots/latency_boxplot.png`
- `results/plots/cpu_usage_comparison.png`
- `results/plots/scalability_workers.png`
- `results/plots/scalability_workers_batch.png`
- `results/plots/scalability_workers_stream.png`

### Relatório

- `results/report.md`

## Detalhes metodológicos

- `scripts/create_samples.py` cria amostras derivadas do dataset real, incluindo `10gb`.
- A geração da amostra `10gb` é incremental para reduzir pressão de memória durante a escrita parquet.
- `scripts/container_monitor.py` coleta CPU, memória, rede e disco via Docker SDK.
- `jobs/stream_job.py` exporta métricas por micro-batch, incluindo `processedRowsPerSecond`, latência média e backpressure.
- `scripts/consolidate_results.py` calcula média, mediana, desvio padrão, IQR e IC95.
- `scripts/generate_report.py` monta um resumo.

## Questões de pesquisa

- RQ1: Qual paradigma apresenta maior throughput em sistemas intensivos de dados?
- RQ2: Como o volume de dados afeta o desempenho batch vs stream?
- RQ3: Qual o impacto da taxa de eventos na latência do streaming?
- RQ4: Como a escalabilidade horizontal afeta os dois paradigmas?

## Troubleshooting

Se `docker compose up` falhar:

- verifique portas em uso
- veja `docker compose logs kafka`
- veja `docker compose logs spark-master`

Se o producer falhar ao ler parquet:

- confirme `pyarrow` instalado no ambiente ativo

Se o stream falhar ao baixar dependências Kafka:

- o runner já usa `spark.jars.ivy=/tmp/.ivy`

Se os workers não escalarem:

- confirme que `docker-compose.yml` não fixa `container_name` para `spark-worker`
- confira `docker ps --filter label=com.docker.compose.service=spark-worker`
