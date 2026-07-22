<#
.SYNOPSIS
    Deploy and manage the Dagster pipeline stack using Docker or Podman.

.DESCRIPTION
    Windows-native deployment script. Auto-detects Podman or Docker.

.EXAMPLE
    .\deploy\deploy.ps1 up        # Start the full stack
    .\deploy\deploy.ps1 down      # Stop and remove volumes
    .\deploy\deploy.ps1 push      # Push pipeline changes (restart code server)
    .\deploy\deploy.ps1 status    # Show stack health
    .\deploy\deploy.ps1 build     # Build the image only
    .\deploy\deploy.ps1 logs      # Tail code-server logs
#>

param(
    [Parameter(Position = 0)]
    [ValidateSet("up", "down", "push", "status", "build", "logs", "help")]
    [string]$Command = "help",

    [Parameter()]
    [ValidateSet("podman", "docker", "auto")]
    [string]$Engine = "auto"
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir
$ComposeFile = Join-Path $ScriptDir "docker-compose.yml"

# ── Detect container engine ──

function Find-Engine {
    if ($Engine -ne "auto") { return $Engine }

    if (Get-Command "podman" -ErrorAction SilentlyContinue) {
        return "podman"
    }
    if (Get-Command "docker" -ErrorAction SilentlyContinue) {
        return "docker"
    }

    Write-Error "Neither podman nor docker found in PATH. Install one first."
    exit 1
}

$ContainerEngine = Find-Engine
$Compose = "$ContainerEngine compose"

Write-Host "  Using engine: $ContainerEngine" -ForegroundColor DarkGray

# ── Commands ──

function Invoke-Build {
    Write-Host "`n  Building deployment image..." -ForegroundColor Cyan
    Push-Location $ProjectRoot
    try {
        & $ContainerEngine build --build-arg INSTALL_EXTRAS=deploy -t dagster-config-framework:deploy .
        if ($LASTEXITCODE -ne 0) { throw "Build failed" }
        Write-Host "  Image built successfully" -ForegroundColor Green
    }
    finally {
        Pop-Location
    }
}

function Invoke-Up {
    Invoke-Build

    Write-Host "`n  Starting production stack..." -ForegroundColor Cyan
    Invoke-Expression "$Compose -f $ComposeFile up -d"
    if ($LASTEXITCODE -ne 0) { throw "Failed to start stack" }

    $uiPort = if ($env:DAGSTER_UI_PORT) { $env:DAGSTER_UI_PORT } else { "3000" }
    $pgExpose = if ($env:DAGSTER_PG_EXPOSE) { $env:DAGSTER_PG_EXPOSE } else { "5433" }
    $dataExpose = if ($env:POSTGRES_EXPOSE) { $env:POSTGRES_EXPOSE } else { "7432" }

    Write-Host ""
    Write-Host "  Production stack running" -ForegroundColor Green
    Write-Host ""
    Write-Host "  Dagster UI        http://localhost:$uiPort"
    Write-Host "  Code server       localhost:4000 (gRPC)"
    Write-Host "  Dagster metadata  localhost:$pgExpose"
    Write-Host "  Pipeline data     localhost:$dataExpose"
    Write-Host ""
    Write-Host "  To push pipeline changes:  .\deploy\deploy.ps1 push"
    Write-Host ""
}

function Invoke-Down {
    Write-Host "`n  Stopping stack and removing volumes..." -ForegroundColor Yellow
    Invoke-Expression "$Compose -f $ComposeFile down -v"
    Write-Host "  Stack stopped" -ForegroundColor Green
}

function Invoke-Push {
    Write-Host "`n  Restarting code server to pick up changes..." -ForegroundColor Cyan
    Invoke-Expression "$Compose -f $ComposeFile restart code-server"
    if ($LASTEXITCODE -ne 0) { throw "Restart failed" }

    Write-Host ""
    Write-Host "  Code server restarted — new definitions loading" -ForegroundColor Green
    Write-Host "  Check status: .\deploy\deploy.ps1 status"
    Write-Host ""
}

function Invoke-Status {
    Invoke-Expression "$Compose -f $ComposeFile ps"
    Write-Host ""
    Invoke-Expression "$Compose -f $ComposeFile logs code-server --tail 5" 2>$null
}

function Invoke-Logs {
    Invoke-Expression "$Compose -f $ComposeFile logs -f code-server"
}

function Show-Help {
    Write-Host ""
    Write-Host "  Dagster Pipeline Deployment" -ForegroundColor Cyan
    Write-Host "  Works with Docker and Podman (auto-detected)" -ForegroundColor DarkGray
    Write-Host ""
    Write-Host "  Commands:" -ForegroundColor White
    Write-Host "    up       Start the full production stack"
    Write-Host "    down     Stop the stack and remove volumes"
    Write-Host "    push     Push pipeline changes (restart code server)"
    Write-Host "    status   Show stack health and recent logs"
    Write-Host "    build    Build the deployment image only"
    Write-Host "    logs     Tail code-server logs"
    Write-Host ""
    Write-Host "  Options:" -ForegroundColor White
    Write-Host "    -Engine podman    Force Podman (default: auto-detect)"
    Write-Host "    -Engine docker    Force Docker"
    Write-Host ""
    Write-Host "  Examples:" -ForegroundColor White
    Write-Host "    .\deploy\deploy.ps1 up"
    Write-Host "    .\deploy\deploy.ps1 up -Engine podman"
    Write-Host "    .\deploy\deploy.ps1 push"
    Write-Host ""
    Write-Host "  Environment:" -ForegroundColor White
    Write-Host "    Copy deploy\.env.example to deploy\.env to configure"
    Write-Host "    ports, passwords, and external Postgres instances."
    Write-Host ""
}

# ── Dispatch ──

switch ($Command) {
    "up"     { Invoke-Up }
    "down"   { Invoke-Down }
    "push"   { Invoke-Push }
    "status" { Invoke-Status }
    "build"  { Invoke-Build }
    "logs"   { Invoke-Logs }
    "help"   { Show-Help }
}
