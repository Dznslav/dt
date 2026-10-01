# Automaticke nasadenie na Windows (Docker Desktop):  .\scripts\deploy.ps1
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")
if (-not (Test-Path ".env")) { Copy-Item ".env.example" ".env"; Write-Host "Vytvoreny .env z .env.example" }
docker compose up -d --build --wait
if ($LASTEXITCODE -ne 0) { throw "docker compose up zlyhal" }
docker compose ps
Write-Host ""
Write-Host "Uzol A: http://localhost:8001    Uzol B: http://localhost:8002    Uzol C: http://localhost:8003"
Write-Host "API dokumentacia: http://localhost:8001/docs"
Start-Process "http://localhost:8001"
