param(
    # Numero de repeticoes validas por cenario.
    [int]$ValidRuns = 5,
    # Duracao da execucao stream (e da coleta de recursos), em segundos.
    [int]$Duration = 60,
    # Se habilitado, executa 1 warm-up por cenario.
    [switch]$IncludeWarmup = $true
)

# Cenarios stream por taxa de entrada.
$scenarios = @(
    @{ Name = "S1"; Rate = 200 },
    @{ Name = "S2"; Rate = 500 },
    @{ Name = "S3"; Rate = 1000 }
)

function Start-StreamJob {
    param(
        [string]$RunId,
        [string]$Scenario
    )

    Start-Job -ScriptBlock {
        param($rid, $scn)
        # Submete o job stream no Spark com pacote Kafka.
        docker exec `
          -e RUN_ID=$rid `
          -e SCENARIO=$scn `
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
    } -ArgumentList $RunId, $Scenario
}

# Sobe infraestrutura e garante existencia do topico.
docker compose up -d | Out-Null
docker exec stream-batch-experiment-kafka-1 kafka-topics --create --if-not-exists --topic taxi-topic --bootstrap-server localhost:29092 --partitions 1 --replication-factor 1 | Out-Null

foreach ($scenario in $scenarios) {
    if ($IncludeWarmup) {
        # Warm-up: valida pipeline e aquece JVM/cache.
        $runId = "$($scenario.Name)_warmup"
        $streamJob = Start-StreamJob -RunId $runId -Scenario $scenario.Name

        Start-Sleep -Seconds 10
        Start-Job -ScriptBlock {
            param($rid, $duration)
            powershell.exe -ExecutionPolicy Bypass -File ".\scripts\collect_docker_stats.ps1" -RunId $rid -Duration $duration
        } -ArgumentList $runId, $Duration | Out-Null

        $env:DATA_PATH = "data/samples/200mb"
        $env:RATE = [string]$scenario.Rate
        $env:KAFKA_BROKER = "localhost:29092"
        $env:KAFKA_TOPIC = "taxi-topic"
        # Gera eventos durante o cenario.
        python producer/taxi_stream_producer.py | Out-Null

        Start-Sleep -Seconds $Duration
        Stop-Job -Job $streamJob -ErrorAction SilentlyContinue | Out-Null
        Remove-Job -Job $streamJob -Force -ErrorAction SilentlyContinue | Out-Null
    }

    for ($i = 1; $i -le $ValidRuns; $i++) {
        # Repeticoes validas: usadas na analise estatistica.
        $runId = "$($scenario.Name)_r$i"
        $streamJob = Start-StreamJob -RunId $runId -Scenario $scenario.Name

        Start-Sleep -Seconds 10
        Start-Job -ScriptBlock {
            param($rid, $duration)
            powershell.exe -ExecutionPolicy Bypass -File ".\scripts\collect_docker_stats.ps1" -RunId $rid -Duration $duration
        } -ArgumentList $runId, $Duration | Out-Null

        $env:DATA_PATH = "data/samples/200mb"
        $env:RATE = [string]$scenario.Rate
        $env:KAFKA_BROKER = "localhost:29092"
        $env:KAFKA_TOPIC = "taxi-topic"
        python producer/taxi_stream_producer.py

        Start-Sleep -Seconds $Duration
        Stop-Job -Job $streamJob -ErrorAction SilentlyContinue | Out-Null
        Remove-Job -Job $streamJob -Force -ErrorAction SilentlyContinue | Out-Null
    }
}
