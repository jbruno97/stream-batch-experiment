param(
    # Identificador da execucao (ex.: B1_r1, S2_r3).
    [string]$RunId = "run_001",
    # Tempo total de coleta em segundos.
    [int]$Duration = 60
)

$output = "results/resource_usage.csv"

# Garante pasta de saida.
if (!(Test-Path "results")) {
    New-Item -ItemType Directory -Path "results" | Out-Null
}

# Cria cabecalho do CSV na primeira execucao.
if (!(Test-Path $output)) {
    "run_id,timestamp,container,cpu_percent,mem_usage,mem_percent" | Out-File -Encoding utf8 $output
}

# Coleta 1 amostra por segundo.
for ($i = 0; $i -lt $Duration; $i++) {
    $stats = docker stats --no-stream --format "{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}}"
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"

    foreach ($line in $stats) {
        "$RunId,$timestamp,$line" | Out-File -Append -Encoding utf8 $output
    }

    Start-Sleep -Seconds 1
}
