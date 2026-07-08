<#
.SYNOPSIS
Run devtools-mcp as a single shared local service (MCP over HTTP + dashboard).

One instance serves every project: point Claude Code / Cursor at
http://127.0.0.1:8000/mcp and open the dashboard at http://127.0.0.1:8765.

.EXAMPLE
.\scripts\devtools-service.ps1 start      # launch detached (idempotent)
.\scripts\devtools-service.ps1 status     # is it up? what URLs?
.\scripts\devtools-service.ps1 stop       # stop the listener
.\scripts\devtools-service.ps1 install    # also start automatically at login
#>
[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateSet('start', 'stop', 'status', 'install', 'uninstall')]
    [string]$Action = 'start',
    [int]$Port = 8000,
    [int]$DashboardPort = 8765
)

Set-StrictMode -Version Latest
$repo = Split-Path -Parent $PSScriptRoot
$shortcutPath = Join-Path ([Environment]::GetFolderPath('Startup')) 'devtools-mcp-service.lnk'

function Test-Dashboard {
    try {
        $r = Invoke-WebRequest -Uri "http://127.0.0.1:$DashboardPort/health" -UseBasicParsing -TimeoutSec 2
        return ($r.StatusCode -eq 200)
    } catch {
        return $false
    }
}

function Test-Mcp {
    try {
        $r = Invoke-WebRequest -Uri "http://127.0.0.1:$Port/mcp" -UseBasicParsing -TimeoutSec 2 -Method GET
        return ($r.StatusCode -lt 500)
    } catch {
        return $false
    }
}

function Show-Status {
    $dash = Test-Dashboard
    $mcp = Test-Mcp
    if ($dash -or $mcp) {
        Write-Host "devtools-mcp service is RUNNING"
        if ($mcp) {
            Write-Host "  MCP (streamable HTTP): http://127.0.0.1:$Port/mcp  [OK]"
        } else {
            Write-Host "  MCP (streamable HTTP): http://127.0.0.1:$Port/mcp  [NOT RESPONDING]"
        }
        if ($dash) {
            Write-Host "  Dashboard:             http://127.0.0.1:$DashboardPort  (tracker: /tracker)  [OK]"
        } else {
            Write-Host "  Dashboard:             http://127.0.0.1:$DashboardPort  [NOT RESPONDING]"
        }
    } else {
        Write-Host "devtools-mcp service is NOT running. Start it with:"
        Write-Host "  $PSCommandPath start"
    }
}

function Start-Service-Instance {
    if (Test-Dashboard) {
        Write-Host "Already running."
        Show-Status
        return
    }
    $args = @(
        'run', '--directory', $repo, 'devtools-mcp',
        '--transport', 'http', '--port', "$Port", '--dashboard-port', "$DashboardPort"
    )
    Start-Process -FilePath 'uv' -ArgumentList $args -WindowStyle Hidden
    $deadline = (Get-Date).AddSeconds(30)
    while ((Get-Date) -lt $deadline) {
        if (Test-Dashboard) { Show-Status; return }
        Start-Sleep -Milliseconds 500
    }
    Write-Error "Service did not become healthy within 30s. Run manually to see errors: uv run --directory `"$repo`" devtools-mcp --transport http"
}

function Stop-Service-Instance {
    $conns = @()
    try { $conns = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction Stop } catch {}
    if (-not $conns) {
        Write-Host "Nothing listening on port $Port."
        return
    }
    $conns | Select-Object -ExpandProperty OwningProcess -Unique | ForEach-Object {
        Write-Host "Stopping pid $_"
        Stop-Process -Id $_ -Force -Confirm:$false
    }
}

function Install-Startup {
    $shell = New-Object -ComObject WScript.Shell
    $lnk = $shell.CreateShortcut($shortcutPath)
    $lnk.TargetPath = 'powershell.exe'
    $lnk.Arguments = "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$PSCommandPath`" start -Port $Port -DashboardPort $DashboardPort"
    $lnk.WorkingDirectory = $repo
    $lnk.Description = 'devtools-mcp shared local service'
    $lnk.Save()
    Write-Host "Installed login autostart: $shortcutPath"
}

switch ($Action) {
    'start' { Start-Service-Instance }
    'stop' { Stop-Service-Instance }
    'status' { Show-Status }
    'install' { Install-Startup; Start-Service-Instance }
    'uninstall' {
        if (Test-Path $shortcutPath) {
            Remove-Item $shortcutPath -Confirm:$false
            Write-Host "Removed login autostart."
        } else {
            Write-Host "No autostart shortcut installed."
        }
    }
}
