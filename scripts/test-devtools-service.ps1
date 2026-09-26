# Exercise actual service functions with mocked Windows/HTTP boundaries.
# Runs under Windows PowerShell 5.1 or PowerShell 7; no service is started.
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$tokens = $null
$parseErrors = $null
$servicePath = Join-Path $PSScriptRoot 'devtools-service.ps1'
$ast = [System.Management.Automation.Language.Parser]::ParseFile($servicePath, [ref]$tokens, [ref]$parseErrors)
if ($parseErrors.Count) { throw "Service script does not parse: $parseErrors" }
$functions = $ast.FindAll({param($node) $node -is [System.Management.Automation.Language.FunctionDefinitionAst]}, $true)
foreach ($fn in $functions) { . ([scriptblock]::Create($fn.Extent.Text)) }
function Assert-Equal($actual, $expected, $label) {
    if ($actual -ne $expected) { throw "$label : expected $expected; got $actual" }
}
$Port = 8010
$DashboardPort = 8765
$repo = 'C:\project with spaces\ai-grind'
# A different HTTP application must not satisfy the MCP readiness check.
function Invoke-WebRequest { [pscustomobject]@{StatusCode=$script:responseCode} }
foreach ($status in @(200, 401, 404, 500, 406)) {
    $script:responseCode = $status
    Assert-Equal (Test-Mcp) ($status -eq 406) "MCP response $status"
}
function Invoke-WebRequest { throw [System.Exception]::new('connection refused') }
Assert-Equal (Test-Mcp) $false 'connection failure without Response property'
function Invoke-WebRequest {
    $failure = [System.Exception]::new('HTTP error')
    $failure | Add-Member -NotePropertyName Response -NotePropertyValue ([pscustomobject]@{StatusCode=$script:responseCode})
    throw $failure
}
foreach ($status in @(404, 406, 500)) {
    $script:responseCode = $status
    Assert-Equal (Test-Mcp) ($status -eq 406) "HTTP exception $status"
}
function Test-Dashboard { $script:dashHealthy }
function Test-Mcp { $script:mcpHealthy }
function Show-Status {}
function Start-Sleep {}
function Start-Process {
    param($FilePath, $ArgumentList, $WindowStyle, $ErrorAction)
    $script:launches++
    Assert-Equal $ArgumentList[2] ('"' + $repo + '"') 'quoted repo path'
    Assert-Equal $ArgumentList[7] '8010' 'requested MCP port'
    $script:dashHealthy = $true
    $script:mcpHealthy = $true
}
foreach ($state in @(@($true,$true,0,$false), @($true,$false,0,$true), @($false,$true,0,$true), @($false,$false,1,$false))) {
    $script:dashHealthy = $state[0]
    $script:mcpHealthy = $state[1]
    $script:launches = 0
    $threw = $false
    try { Start-Service-Instance } catch {
        if ($_.Exception.Message -notlike 'Only one service endpoint*') { throw }
        $threw = $true
    }
    Assert-Equal $threw $state[3] 'incomplete service refused'
    Assert-Equal $script:launches $state[2] 'number of spawned services'
}
Write-Output 'PASS: HTTP readiness, idempotent startup, stale port refusal, quoted paths and requested port'
