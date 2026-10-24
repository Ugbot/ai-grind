---
name: pwsh-scripting-style
description: >
  Writing robust PowerShell scripts and functions: param blocks, [CmdletBinding()],
  advanced functions, $PSScriptRoot, Set-StrictMode, comment-based help, modules,
  and Pester tests. Use when authoring a .ps1 script or reusable function, adding
  parameters/validation, making a script locate its own files, hardening against
  typos with strict mode, or setting up tests. Applies to 5.1 and 7+ with edition
  notes.
---

# PowerShell scripting style

Conventions for scripts that are safe, predictable, and testable. Same in 5.1 and
7+ unless noted.

## Script skeleton

```powershell
#Requires -Version 5.1
[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string]   $InputPath,
    [ValidateSet('Debug','Release')]  [string] $Config = 'Release',
    [ValidateRange(1,64)]             [int]    $Jobs   = 4,
    [switch] $Force
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# ... body ...
```

- `#Requires -Version 5.1` (or `7.0`) fails fast on the wrong edition.
- `[CmdletBinding()]` turns the script/function into an *advanced* one: free
  `-Verbose`, `-Debug`, `-ErrorAction`, etc., and `$PSCmdlet`.
- `param()` must be the **first executable statement** (after `#Requires` and
  comment-based help).

## Parameters & validation

```powershell
param(
    [Parameter(Mandatory, Position=0)] [string] $Name,
    [Parameter(ValueFromPipeline)]     [string[]] $Items,
    [ValidateNotNullOrEmpty()]         [string] $Out,
    [ValidatePattern('^\d{4}-\d{2}-\d{2}$')] [string] $Date,
    [ValidateScript({ Test-Path $_ })] [string] $Path,
    [switch] $DryRun
)
```

Validation attributes fail at bind time with clear messages, so prefer them over
hand-written `if` checks. Use `[switch]` for booleans (`-DryRun`, not
`-DryRun $true`).

## `Set-StrictMode` catches typos and nulls

```powershell
Set-StrictMode -Version Latest
```

Turns these into errors: referencing an **undefined variable**, accessing a
**non-existent property**, calling a function like a method. Use it in every
non-trivial script. (Note: it makes some loose patterns fail, so initialize
variables before use.)

## Locate files relative to the script with `$PSScriptRoot`

Never depend on the current directory. `$PSScriptRoot` is the folder containing the
running script.

```powershell
$config = Join-Path $PSScriptRoot 'config.json'
$module = Join-Path $PSScriptRoot 'lib\helpers.psm1'
```

## Advanced functions with pipeline support

```powershell
function Convert-Thing {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory, ValueFromPipeline)] [string[]] $Name
    )
    begin   { $count = 0 }                      # runs once
    process { foreach ($n in $Name) { $count++; $n.ToUpper() } }  # runs per pipeline item
    end     { Write-Verbose "converted $count" }                 # runs once
}

'a','b' | Convert-Thing -Verbose
```

`begin/process/end` is what lets a function stream pipeline input correctly.

## Output discipline

- **A function's output is whatever it leaves on the pipeline.** Don't `return`
  strings you meant as messages. They become the function's data.
- Use the right stream for messages:

```powershell
Write-Output $result        # DATA (the return value) — pipeline
Write-Verbose 'progress'    # shown only with -Verbose
Write-Warning 'heads up'    # warning stream
Write-Error 'bad'           # error stream (see pwsh-errors)
Write-Host 'ui text'        # console-only, NOT capturable (use sparingly)
```

Suppress unwanted output with `| Out-Null` or `$null = expr` (faster than
`| Out-Null`).

## Comment-based help

```powershell
<#
.SYNOPSIS  Build and test the project.
.PARAMETER Config  Debug or Release.
.EXAMPLE   .\build.ps1 -Config Release -Jobs 8
#>
[CmdletBinding()] param(...)
```

`Get-Help .\build.ps1` then renders it like a real cmdlet's help.

## Modules (reuse across scripts)

```powershell
# helpers.psm1
function Get-Thing { param($x) ... }
Export-ModuleMember -Function Get-Thing

# consumer
Import-Module (Join-Path $PSScriptRoot 'helpers.psm1') -Force
```

For a shipped module add a `.psd1` manifest (`New-ModuleManifest`).

## Testing with Pester

```powershell
# build.Tests.ps1  (Pester v5 syntax)
Describe 'Get-Thing' {
    It 'doubles its input' {
        Get-Thing 21 | Should -Be 42
    }
    It 'throws on null' {
        { Get-Thing $null } | Should -Throw
    }
}
# run:  Invoke-Pester .\build.Tests.ps1
```

Pester v5 ships in-box on many 7+ setups; on 5.1 `Install-Module Pester -Force`.

## Execution policy (running .ps1 at all)

If scripts are blocked: set a per-user policy (no admin):

```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
# or run one script bypassing policy:
powershell -ExecutionPolicy Bypass -File .\build.ps1
```

## Quick reference

| Need | Do |
|---|---|
| typed/validated args | `param([ValidateSet(...)][string]$X)` |
| boolean flag | `[switch] $Force` |
| catch typos/nulls | `Set-StrictMode -Version Latest` |
| find script's files | `Join-Path $PSScriptRoot ...` |
| pipeline-aware fn | `begin/process/end` + `ValueFromPipeline` |
| message vs data | `Write-Verbose`/`Write-Warning` vs `Write-Output` |
| drop output | `$null = expr` |
| require edition | `#Requires -Version 7.0` |
| run blocked script | `powershell -ExecutionPolicy Bypass -File x.ps1` |
