# Registro de Problemas, Precaucoes e Correcoes

Este documento registra os problemas encontrados durante a execucao do experimento Batch vs Stream, os sintomas observados, a causa raiz, as precaucoes recomendadas e os comandos de correcao.

Use este arquivo como checklist antes de iniciar uma campanha longa.

---

## Checklist rapido antes da campanha

1. Ativar o ambiente Python:

```bash
source .venv/bin/activate
python -m pip install -r requirements.txt
```

2. Confirmar que os JARs do Spark/Kafka existem:

```bash
bash scripts/download_jars.sh
ls jars/*.jar
```

3. Limpar containers antigos sem apagar dados do volume Kafka:

```bash
docker compose down
```

4. Conferir uso de disco antes de iniciar:

```bash
df -h .
docker system df
du -h -d 2 data results jars
```

5. Subir a infraestrutura:

```bash
docker compose up -d
docker compose ps
```

6. Rodar primeiro uma validacao curta:

```bash
python scripts/run_full_experiment.py --skip-batch --stream-repetitions 1 --stream-duration-sec 10
```

7. Se a validacao estiver ok, rodar a campanha stream completa:

```bash
python scripts/run_full_experiment.py --skip-batch
```

---

## 1. Throughput streaming zerado

### Sintomas

- `stream_throughput_rows_per_sec` aparece como `0.0`.
- `stream_total_input_rows` aparece como `0`.
- `producer_events_sent` aparece como `0`.
- `stream_runs.csv` registra `status=error`.
- `stream_microbatches.csv` mostra `num_input_rows=0`.

### Causa raiz encontrada

O produtor Kafka falhava antes de enviar mensagens porque o `KafkaProducer` estava configurado com `compression_type="lz4"`, mas a biblioteca Python `lz4` nao estava declarada originalmente no `requirements.txt`.

Erro observado:

```text
AssertionError: Libraries for lz4 compression codec not found
```

Sem mensagens no Kafka, o Spark Structured Streaming lia micro-batches vazios. O calculo de throughput estava correto, mas recebia total de linhas igual a zero.

### Correcao aplicada

- `lz4` foi adicionado ao `requirements.txt`.
- O produtor passou a aceitar `--compression`.
- O produtor agora mostra mensagem explicita se `lz4` estiver ausente.

### Como corrigir no ambiente

```bash
source .venv/bin/activate
python -m pip install -r requirements.txt
```

Se for necessario desabilitar compressao temporariamente:

```bash
python producer/taxi_stream_producer.py \
  --data-path data/samples/200mb \
  --bootstrap-servers localhost:29092 \
  --topic taxi-topic-teste \
  --rate 200 \
  --duration 10 \
  --compression none
```

### Como confirmar que foi resolvido

Depois de uma execucao curta, verificar:

```bash
tail -n 5 results/raw/stream_runs.csv
tail -n 5 results/raw/stream_microbatches.csv
```

Os campos esperados devem ser maiores que zero:

- `producer_events_sent`
- `stream_total_input_rows`
- `stream_throughput_rows_per_sec`
- `num_input_rows`

---

## 2. Comando `pip` nao encontrado

### Sintomas

Ao rodar:

```bash
pip install -r requirements.txt
```

aparece:

```text
Command 'pip' not found
```

### Causa

O `pip` global nao esta instalado ou nao esta no `PATH`. O projeto usa ambiente virtual `.venv`, entao o caminho mais confiavel e executar `pip` via Python do proprio ambiente.

### Correcao

Usar:

```bash
/home/bruno/stream-batch-experiment/.venv/bin/python -m pip install -r requirements.txt
```

Ou:

```bash
source .venv/bin/activate
python -m pip install -r requirements.txt
```

### Precaucao

Em scripts e instrucoes, preferir `python -m pip` em vez de depender do comando `pip` global.

---

## 3. Crescimento excessivo de disco no Docker/Spark

### Sintomas

- O experimento consome 100 GB ou 200 GB antes de terminar.
- A maquina trava ou fica sem espaco.
- `docker system df` mostra containers com dezenas de GB.
- `docker ps -a --size` mostra o `spark-worker` muito grande.

Exemplo observado:

```text
Containers: 39.52GB
stream-batch-experiment-spark-worker-1: 39.4GB
```

### Causa raiz encontrada

O Spark worker acumulava diretorios temporarios em:

```text
/opt/spark/work/app-*
```

Cada execucao do Spark criava um novo diretorio de app com JARs, stdout, stderr e dados temporarios. Como esse caminho ficava dentro da camada gravavel do container, o Docker mantinha tudo acumulado.

Em campanha stream completa, isso cresce rapido porque existem muitos cenarios e repeticoes.

### Correcoes aplicadas

1. `docker-compose.yml` passou a ativar limpeza automatica do Spark worker:

```yaml
SPARK_WORKER_OPTS: >-
  -Dspark.worker.cleanup.enabled=true
  -Dspark.worker.cleanup.interval=60
  -Dspark.worker.cleanup.appDataTtl=120
```

2. `scripts/run_full_experiment.py` passou a remover diretorios `app-*` em `/opt/spark/work` ao final de cada execucao.

3. O runner deixou de usar `--packages` no streaming quando os JARs locais ja estao disponiveis, reduzindo resolucao e copia extra de dependencias.

### Correcao imediata para recuperar espaco

Se containers antigos estao parados ou inchados:

```bash
docker compose down
docker system df
```

Esse comando remove os containers do projeto e preserva volumes, porque nao usa `-v`.

### Correcao mais agressiva, usar com cuidado

Somente se tiver certeza de que nao precisa dos volumes Docker locais:

```bash
docker compose down -v
```

Isso remove tambem o volume do Kafka.

Para limpar recursos Docker nao usados fora do projeto:

```bash
docker system prune
```

Antes de usar `docker system prune`, conferir se nao ha outros projetos Docker importantes na maquina.

### Como monitorar durante a campanha

Em outro terminal:

```bash
watch -n 30 'df -h . && docker system df && docker ps -a --size'
```

Se `spark-worker` voltar a crescer muito, interromper a campanha e rodar:

```bash
docker compose down
```

---

## 4. JARs do Spark/Kafka duplicados ou baixados a cada execucao

### Sintomas

- Execucoes stream demoram para iniciar.
- Logs mostram Ivy/Maven resolvendo dependencias repetidamente.
- Diretorios temporarios do Spark acumulam copias dos JARs.

### Causa

O runner usava `--jars` e tambem `--packages`. Mesmo com JARs locais, o Spark podia resolver pacotes via Ivy e distribuir/copiar dependencias repetidas.

### Correcao aplicada

O runner agora usa apenas `--jars` com os arquivos locais em `./jars`.

### Precaucao

Antes de rodar campanha stream, garantir que os JARs existem:

```bash
bash scripts/download_jars.sh
ls jars/*.jar
```

Se algum JAR esperado estiver ausente, o streaming pode falhar ao iniciar.

---

## 5. Rejeicao no `git push`: `fetch first`

### Sintomas

Ao enviar para o GitHub:

```text
! [rejected] main -> main (fetch first)
Updates were rejected because the remote contains work that you do not have locally.
```

### Causa

O branch remoto recebeu commits que nao existiam no branch local.

### Correcao segura

```bash
git fetch origin
git rebase origin/main
git push origin main
```

### Precaucao

Nao usar `git push --force` neste projeto sem motivo claro. O rebase local e suficiente quando nao ha conflito.

---

## 6. Falha de autenticacao no GitHub por HTTPS

### Sintomas

```text
fatal: could not read Username for 'https://github.com': No such device or address
```

### Causa

O ambiente nao tinha credenciais GitHub disponiveis para o remoto HTTPS.

### Correcao

Autenticar com GitHub CLI:

```bash
gh auth login
```

Depois:

```bash
git push origin main
```

---

## 7. Permissao negada ao acessar Docker

### Sintomas

```text
permission denied while trying to connect to the docker API at unix:///var/run/docker.sock
```

### Causa

O usuario atual nao tem permissao para acessar o daemon Docker, ou a sessao ainda nao reconheceu o grupo `docker`.

### Correcao

Adicionar o usuario ao grupo:

```bash
sudo usermod -aG docker $USER
```

Depois, fazer logout/login ou reiniciar a sessao WSL.

Teste:

```bash
docker ps
```

---

## 8. Dados e amostras ocupando espaco fixo

### Observacao

O diretorio `data/samples` pode ocupar dezenas de GB. No diagnostico realizado, o projeto tinha aproximadamente:

```text
data/samples: 27GB
results: 9MB
project total: 30GB
```

Esse consumo e esperado para as amostras `200mb`, `1gb`, `3gb` e `10gb`, principalmente porque os tamanhos de diretorio podem ser maiores que os rotulos nominais dependendo do formato, particoes e replicacoes locais.

### Precaucao

Antes de rodar campanha completa, reservar espaco para:

- dataset bruto;
- amostras;
- resultados;
- imagens Docker;
- temporarios Spark;
- logs do Docker.

Recomendacao pratica: manter pelo menos 100 GB livres para campanhas longas. Com as limpezas aplicadas, o consumo temporario deve ser muito menor, mas ainda e prudente monitorar.

### Como medir

```bash
du -h -d 2 data results jars
df -h .
```

---

## 9. Kafka acumulando dados entre execucoes

### Sintomas

- Volume Docker do Kafka cresce.
- Reexecucoes leem dados antigos se o topico for reutilizado.

### Precaucoes ja existentes

O `docker-compose.yml` define:

```yaml
KAFKA_LOG_RETENTION_MS: 3600000
KAFKA_LOG_SEGMENT_BYTES: 134217728
```

O runner tambem cria topicos por `run_id`, reduzindo colisao entre repeticoes.

### Correcao se for necessario zerar Kafka

Parar e remover volume Kafka:

```bash
docker compose down -v
```

Use com cuidado: isso apaga os dados Kafka mantidos no volume Docker.

---

## 10. Campanha completa muito longa

### Observacao

Configuracao padrao:

- 16 cenarios batch;
- 19 cenarios stream;
- 30 repeticoes por cenario;
- 1050 execucoes no total.

Rodar somente stream completo:

```bash
python scripts/run_full_experiment.py --skip-batch
```

Isso executa:

```text
19 cenarios stream x 30 repeticoes = 570 execucoes
```

### Precaucao

Antes de rodar a campanha completa, sempre fazer uma execucao curta:

```bash
python scripts/run_full_experiment.py --skip-batch --stream-repetitions 1 --stream-duration-sec 10
```

Depois conferir:

```bash
tail -n 5 results/raw/stream_runs.csv
docker system df
```

So iniciar a campanha completa se:

- `status` estiver `ok`;
- `producer_events_sent` for maior que zero;
- `stream_total_input_rows` for maior que zero;
- Docker nao estiver crescendo de forma anormal.

---

## 11. Procedimento de recuperacao quando travar

Se a maquina comecar a travar ou o disco subir rapidamente:

1. Interromper o runner com `Ctrl+C`.
2. Parar containers:

```bash
docker compose down
```

3. Medir disco:

```bash
df -h .
docker system df
docker ps -a --size
```

4. Se containers ainda existirem e forem do projeto:

```bash
docker compose down
```

5. Se precisar remover volume Kafka:

```bash
docker compose down -v
```

6. Validar novamente com campanha curta antes de retomar.

---

## 12. Arquivos principais envolvidos

- `producer/taxi_stream_producer.py`: produtor Kafka e compressao.
- `jobs/stream_job.py`: calculo das metricas stream.
- `jobs/batch_job.py`: calculo das metricas batch.
- `scripts/run_full_experiment.py`: orquestracao da campanha, limpeza Spark work, coleta de metricas.
- `docker-compose.yml`: configuracao Kafka/Spark, limites de memoria e limpeza do worker.
- `scripts/download_jars.sh`: pre-download dos JARs Spark/Kafka.
- `results/raw/stream_runs.csv`: resultados brutos por execucao stream.
- `results/raw/stream_microbatches.csv`: metricas por micro-batch.
- `results/raw/batch_runs.csv`: resultados brutos batch.

---

## 13. Comandos uteis de diagnostico

Uso geral de disco:

```bash
df -h .
du -h -d 2 .
```

Uso do Docker:

```bash
docker system df
docker ps -a --size
docker compose ps -a
```

Resultados stream:

```bash
tail -n 5 results/raw/stream_runs.csv
tail -n 5 results/raw/stream_microbatches.csv
```

Logs Docker:

```bash
docker compose logs kafka
docker compose logs spark-master
docker compose logs spark-worker
```

Estado Git:

```bash
git status --short
git log --oneline --max-count=5
```

