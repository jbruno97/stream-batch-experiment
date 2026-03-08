param(
    # Numero de repeticoes validas por cenario.
    [int]$ValidRuns = 5,
    # Se habilitado, executa 1 warm-up antes das repeticoes validas.
    [switch]$IncludeWarmup = $true
)

# Cenarios batch (volume e duracao sugerida da coleta de recursos).
$scenarios = @(
    @{ Name = "B1"; DataPath = "/opt/data/samples/200mb"; Duration = 30 },
    @{ Name = "B2"; DataPath = "/opt/data/samples/1gb"; Duration = 45 },
    @{ Name = "B3"; DataPath = "/opt/data/samples/3gb"; Duration = 60 }
)

# Sobe infraestrutura minima.
docker compose up -d | Out-Null

foreach ($scenario in $scenarios) {
    if ($IncludeWarmup) {
        # Warm-up: nao entra na analise final.
        $runId = "$($scenario.Name)_warmup"
        Start-Job -ScriptBlock {
            param($rid, $duration)
            powershell.exe -ExecutionPolicy Bypass -File ".\scripts\collect_docker_stats.ps1" -RunId $rid -Duration $duration
        } -ArgumentList $runId, $scenario.Duration | Out-Null

        docker exec `
          -e RUN_ID=$runId `
          -e SCENARIO=$($scenario.Name) `
          -e DATA_PATH=$($scenario.DataPath) `
          -e RESULTS_FILE=/opt/results/batch_results.csv `
          spark-master /opt/spark/bin/spark-submit `
          --master spark://spark-master:7077 `
          --conf spark.executor.memory=1g `
          --conf spark.driver.memory=512m `
          --conf spark.sql.shuffle.partitions=8 `
          /opt/jobs/batch_taxi_job.py | Out-Null
    }

    for ($i = 1; $i -le $ValidRuns; $i++) {
        # Repeticoes validas: entram na analise estatistica.
        $runId = "$($scenario.Name)_r$i"
        Start-Job -ScriptBlock {
            param($rid, $duration)
            powershell.exe -ExecutionPolicy Bypass -File ".\scripts\collect_docker_stats.ps1" -RunId $rid -Duration $duration
        } -ArgumentList $runId, $scenario.Duration | Out-Null

        docker exec `
          -e RUN_ID=$runId `
          -e SCENARIO=$($scenario.Name) `
          -e DATA_PATH=$($scenario.DataPath) `
          -e RESULTS_FILE=/opt/results/batch_results.csv `
          spark-master /opt/spark/bin/spark-submit `
          --master spark://spark-master:7077 `
          --conf spark.executor.memory=1g `
          --conf spark.driver.memory=512m `
          --conf spark.sql.shuffle.partitions=8 `
          /opt/jobs/batch_taxi_job.py
    }
}
