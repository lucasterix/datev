# DATEV-Bridge Windows Installer
#
# ASCII only - PowerShell 5.1 has encoding issues with non-BOM UTF-8.
# All Unicode arrows / checkmarks / em-dashes have been removed.
#
# Voraussetzung: PowerShell als Administrator starten.

param(
    [switch]$SkipPython,
    [switch]$Force
)

$ErrorActionPreference = "Stop"

# --- run as admin? --------------------------------------------------------
$me = New-Object Security.Principal.WindowsPrincipal(
    [Security.Principal.WindowsIdentity]::GetCurrent()
)
if (-not $me.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    Write-Host "Bitte PowerShell als Administrator starten." -ForegroundColor Red
    exit 1
}

$InstallDir = "C:\datev-bridge"
$ScriptDir = $PSScriptRoot
if (-not $ScriptDir) {
    # When invoked via "powershell -File", $PSScriptRoot is set; fallback
    # for dot-sourced or piped runs:
    $ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
}

Write-Host ""
Write-Host "=== DATEV-Bridge Installer ===" -ForegroundColor Cyan
Write-Host ""

# --- 1. Tailscale Serve abschalten (Konflikt mit Bridge auf 8765) -------
Write-Host "[1/7] Tailscale Serve abschalten (falls aktiv)..." -ForegroundColor Cyan
# tailscale.exe schreibt "serve config does not exist" auf stderr, wenn
# nichts zu deaktivieren ist. Das ist OK - kein Abbruch.
try {
    & tailscale serve --tcp=8765 off *>&1 | Out-Null
} catch {
    # ignore - serve war eh nicht aktiv
}
$global:LASTEXITCODE = 0

# --- 2. Python pruefen / installieren -----------------------------------
Write-Host "[2/7] Python pruefen..." -ForegroundColor Cyan
$python = (Get-Command python -ErrorAction SilentlyContinue).Source
if (-not $python -and -not $SkipPython) {
    Write-Host "      Python nicht gefunden, installiere via winget..." -ForegroundColor Yellow
    try {
        & winget install --id Python.Python.3.12 `
            --accept-package-agreements `
            --accept-source-agreements `
            --silent *>&1 | Out-Null
    } catch {
        # winget kann mit Non-Zero exiten wenn Paket schon da ist - egal
    }
    $global:LASTEXITCODE = 0
    # PATH neu laden in dieser Session:
    $env:Path = `
        [System.Environment]::GetEnvironmentVariable("Path", "Machine") + ";" + `
        [System.Environment]::GetEnvironmentVariable("Path", "User")
    $python = (Get-Command python -ErrorAction SilentlyContinue).Source
}
if (-not $python) {
    Write-Host "Python nicht verfuegbar - manuell installieren und Skript erneut starten." -ForegroundColor Red
    exit 1
}
Write-Host "      Python gefunden: $python" -ForegroundColor Green

# --- 3. Dependencies ----------------------------------------------------
Write-Host "[3/7] aiohttp installieren..." -ForegroundColor Cyan
try {
    & $python -m pip install --quiet --upgrade pip *>&1 | Out-Null
} catch {}
$global:LASTEXITCODE = 0
try {
    & $python -m pip install --quiet aiohttp *>&1 | Out-Null
} catch {}
if ($LASTEXITCODE -ne 0) {
    # Falls leise nicht ging, nochmal laut versuchen damit Daniel den Fehler sieht
    & $python -m pip install aiohttp
    if ($LASTEXITCODE -ne 0) {
        Write-Host "pip install aiohttp ist fehlgeschlagen." -ForegroundColor Red
        exit 1
    }
}
$global:LASTEXITCODE = 0

# --- 4. Install-Verzeichnis + Skript kopieren ---------------------------
Write-Host "[4/7] Bridge-Skript installieren ($InstallDir)..." -ForegroundColor Cyan
New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
$src = Join-Path $ScriptDir "datev_bridge.py"
if (-not (Test-Path $src)) {
    Write-Host "datev_bridge.py nicht gefunden in $ScriptDir" -ForegroundColor Red
    exit 1
}
Copy-Item -Force $src "$InstallDir\datev_bridge.py"

# --- 5. Credentials abfragen + bridge.env schreiben ---------------------
Write-Host "[5/7] Credentials..." -ForegroundColor Cyan
$envFile = "$InstallDir\bridge.env"
if ((Test-Path $envFile) -and -not $Force) {
    Write-Host "      bestehende bridge.env vorhanden - wird wiederverwendet (mit -Force ueberschreiben)" -ForegroundColor Yellow
} else {
    $WinUser = Read-Host "  Windows-Username (Email bei MS-Konto)"
    $WinPassSec = Read-Host "  Windows-Passwort" -AsSecureString
    $WinPass = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto(
        [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($WinPassSec)
    )
    $envContent = "DATEV_BRIDGE_USER=$WinUser`r`nDATEV_BRIDGE_PASSWORD=$WinPass`r`n"
    [System.IO.File]::WriteAllText($envFile, $envContent, [System.Text.Encoding]::UTF8)

    # Restrict file rights: only SYSTEM + Administrators can read
    & icacls $envFile /inheritance:r /grant:r "SYSTEM:(R)" "Administrators:(R)" | Out-Null
    Write-Host "      Credentials gespeichert ($envFile, nur SYSTEM/Administrators lesbar)" -ForegroundColor Green
}

# --- 6. Firewall-Regel (nur Tailscale-Interface) ------------------------
Write-Host "[6/7] Firewall-Regel..." -ForegroundColor Cyan
Get-NetFirewallRule -DisplayName "datev-bridge" -ErrorAction SilentlyContinue | Remove-NetFirewallRule

$fwParams = @{
    DisplayName    = "datev-bridge"
    Direction      = "Inbound"
    Protocol       = "TCP"
    LocalPort      = 8765
    InterfaceAlias = "Tailscale"
    Action         = "Allow"
    Profile        = "Private", "Public", "Domain"
}
New-NetFirewallRule @fwParams | Out-Null

# --- 7. Scheduled Task --------------------------------------------------
Write-Host "[7/7] Auto-Start-Task..." -ForegroundColor Cyan
Unregister-ScheduledTask -TaskName "datev-bridge" -Confirm:$false -ErrorAction SilentlyContinue

# Task fuehrt python.exe direkt aus - bridge.env wird vom Skript selbst geladen
$action = New-ScheduledTaskAction `
    -Execute $python `
    -Argument "$InstallDir\datev_bridge.py" `
    -WorkingDirectory $InstallDir

$trigger = New-ScheduledTaskTrigger -AtStartup

$settingsParams = @{
    AllowStartIfOnBatteries    = $true
    DontStopIfGoingOnBatteries = $true
    StartWhenAvailable         = $true
    RestartCount               = 99
    RestartInterval            = (New-TimeSpan -Minutes 1)
    ExecutionTimeLimit         = (New-TimeSpan -Days 365)
}
$settings = New-ScheduledTaskSettingsSet @settingsParams

$principal = New-ScheduledTaskPrincipal `
    -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest

Register-ScheduledTask `
    -TaskName "datev-bridge" `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Principal $principal `
    -Description "DATEV-Bridge: HTTP-Reverse-Proxy von Tailscale zu localhost:58454" | Out-Null

# Service starten
Start-ScheduledTask -TaskName "datev-bridge"
Start-Sleep -Seconds 4

Write-Host ""
Write-Host "=== Setup abgeschlossen ===" -ForegroundColor Green
Write-Host ""

# --- Selbsttest ---------------------------------------------------------
Write-Host "Selbsttest..." -ForegroundColor Cyan

$tailscaleIP = $null
try {
    $tailscaleIP = (Get-NetIPAddress -InterfaceAlias "Tailscale" `
        -AddressFamily IPv4 -ErrorAction Stop).IPAddress
} catch {
    Write-Host "  Tailscale-Interface nicht gefunden - laeuft Tailscale?" -ForegroundColor Yellow
}

if ($tailscaleIP) {
    Write-Host "  Tailscale-IP:  $tailscaleIP" -ForegroundColor Green
    Write-Host "  Bridge-URL:    http://${tailscaleIP}:8765" -ForegroundColor Green
}
Write-Host "  Logs:          $InstallDir\bridge.log"
Write-Host "  Service:       'datev-bridge' im Aufgabenplaner"
Write-Host ""

$testUrl = "http://localhost:8765/datev/api/hr/v3/clients?reference-date=2026-04-01"
$testParams = @{
    Uri             = $testUrl
    UseBasicParsing = $true
    TimeoutSec      = 5
    ErrorAction     = "Stop"
}

$ok = $false
$failMsg = $null
$statusCode = $null
try {
    $r = Invoke-WebRequest @testParams
    $ok = $true
    $statusCode = $r.StatusCode
} catch {
    $resp = $_.Exception.Response
    if ($resp) {
        $statusCode = [int]$resp.StatusCode
    }
    $failMsg = $_.Exception.Message
}

if ($ok) {
    Write-Host "OK: Bridge laeuft, DATEV antwortet ($statusCode)" -ForegroundColor Green
} elseif ($statusCode -eq 503) {
    Write-Host "OK: Bridge laeuft, DATEV antwortet 503 - bitte LuG oeffnen + Stick einstecken" -ForegroundColor Yellow
} else {
    Write-Host "FEHLER: Bridge-Test fehlgeschlagen" -ForegroundColor Red
    Write-Host "  $failMsg"
    Write-Host ""
    Write-Host "Letzte Log-Zeilen:" -ForegroundColor Yellow
    if (Test-Path "$InstallDir\bridge.log") {
        Get-Content "$InstallDir\bridge.log" -Tail 15
    } else {
        Write-Host "  (kein Log vorhanden - Service noch nicht gestartet?)"
    }
}
