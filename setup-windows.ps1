# Run this script from PowerShell as Administrator for the first-time WSL setup.
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$dockerExe = 'C:\Program Files\Docker\Docker\resources\bin\docker.exe'
$dockerBin = 'C:\Program Files\Docker\Docker\resources\bin'
$env:Path = "$dockerBin;$env:Path"

Write-Host 'Enabling Windows features required by Docker Desktop...' -ForegroundColor Cyan
dism.exe /online /enable-feature /featurename:Microsoft-Windows-Subsystem-Linux /all /norestart
dism.exe /online /enable-feature /featurename:VirtualMachinePlatform /all /norestart

Write-Host 'Updating WSL...' -ForegroundColor Cyan
wsl.exe --update
wsl.exe --set-default-version 2

if (-not (Test-Path $dockerExe)) {
    throw 'Docker Desktop is not installed. Install it with: winget install --id Docker.DockerDesktop -e'
}

Write-Host 'Starting Docker Desktop...' -ForegroundColor Cyan
Start-Process 'C:\Program Files\Docker\Docker\Docker Desktop.exe'

& $dockerExe info *> $null
if ($LASTEXITCODE -ne 0) {
    throw 'Docker Desktop is still starting or its Linux engine is stopped. Wait until Docker Desktop reports Running, then run this script again.'
}

Set-Location $projectRoot
Write-Host 'Validating Docker Compose...' -ForegroundColor Cyan
& $dockerExe compose config --quiet
if ($LASTEXITCODE -ne 0) { throw 'docker-compose.yml validation failed.' }

Write-Host 'Building and starting the platform...' -ForegroundColor Cyan
& $dockerExe compose up -d --build
if ($LASTEXITCODE -ne 0) { throw 'Docker Compose failed to start the platform.' }

Write-Host ''
Write-Host 'Platform started. Check services with: docker compose ps' -ForegroundColor Green
Write-Host 'Airflow:  http://localhost:8080  (admin/admin)' -ForegroundColor Green
Write-Host 'Superset: http://localhost:8088  (admin/admin)' -ForegroundColor Green
