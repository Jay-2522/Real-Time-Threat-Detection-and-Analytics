Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")
python scripts/setup_local.py
if ($LASTEXITCODE -ne 0) { throw "Local environment setup failed" }

$baseServices = @(
  "zookeeper",
  "kafka",
  "kafka-init",
  "postgres",
  "backend",
  "frontend",
  "telemetry-producer"
)

$intelligenceServices = @(
  "ml-inference",
  "alert-engine",
  "threat-intelligence-engine"
)

Write-Host "Starting laptop-friendly platform mode..."
docker compose up --build -d $baseServices
docker compose up --build -d --no-deps $intelligenceServices
python scripts/health_check.py
