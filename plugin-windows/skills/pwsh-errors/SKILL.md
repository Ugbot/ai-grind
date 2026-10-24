---
name: pwsh-errors
description: >
  Error handling in PowerShell: terminating vs non-terminating errors, try/catch,
  -ErrorAction, $ErrorActionPreference, $LASTEXITCODE, and exit codes. Use when a
  try/catch isn't catching, when a cmdlet "fails" but the script keeps going, when
  -ErrorAction SilentlyContinue still makes the script report failure, when you
  need a script to exit non-zero on error, or when deciding how to make errors
  fatal. Covers both 5.1 and 7+.
---

# PowerShell error handling

The central gotcha: PowerShell has **two kinds of error**, and `try/catch` only
catches one of them by default. Behavior is the same in 5.1 and 7+ except where
noted.

## Terminating vs non-terminating

- Terminating errors stop execution and are caught by `try/catch`. Sources: `throw`,
  `.NET` exceptions, and cmdlets run with `-ErrorAction Stop`.
- Non-terminating errors write to the error stream and keep going; `try/catch`
  does **not** catch it. This is the default for most cmdlet errors
  (`Get-Item missing`, `Remove-Item` on a locked file, etc.).

```powershell
# DOES NOT catch — Get-Item raises a NON-terminating error by default:
try { Get-Item C:\nope } catch { 'caught' }       # prints the error, NOT 'caught'

# Promote to terminating so catch fires:
try { Get-Item C:\nope -ErrorAction Stop } catch { "caught: $($_.Exception.Message)" }
```

## The rule that fixes 90% of cases

**Add `-ErrorAction Stop` to any cmdlet whose failure should be fatal**, then wrap
in `try/catch`.

```powershell
try {
    Copy-Item $src $dst -ErrorAction Stop
    $json = Get-Content $cfg -Raw -ErrorAction Stop | ConvertFrom-Json
}
catch {
    Write-Error "setup failed: $($_.Exception.Message)"
    exit 1
}
```

To make a whole script treat all errors as terminating, set the preference once
at the top:

```powershell
$ErrorActionPreference = 'Stop'      # now most cmdlet errors throw and are catchable
```

`$ErrorActionPreference` values: `Continue` (default), `Stop`, `SilentlyContinue`
(suppress + continue), `Ignore` (suppress, don't even record), `Inquire`.

## `-ErrorAction SilentlyContinue` does NOT mean "ignore the failure"

It suppresses the error output, but the cmdlet still failed, and in an
automation harness the overall step can still report exit 1. To make a failure
**truly non-fatal**, promote it to terminating and swallow it:

```powershell
# Robust "best effort, ignore if it fails":
try { Remove-Item $tmp -Recurse -Force -ErrorAction Stop } catch { }
```

(Without `-ErrorAction Stop`, a non-terminating error skips the `catch` and may
still flip the step's success state.)

## The error object `$_` / `$PSItem` inside catch

```powershell
catch {
    $_.Exception.Message          # human message
    $_.Exception.GetType().FullName
    $_.ScriptStackTrace           # where it threw
    $_.InvocationInfo.PositionMessage
}
```

`$Error` is an auto-array of recent errors; `$Error[0]` is the most recent.

## `finally` and typed catches (typed catch: 5.1 and 7+)

```powershell
try {
    $fs = [System.IO.File]::OpenRead($path)
}
catch [System.IO.FileNotFoundException] { 'no such file' }
catch [System.UnauthorizedAccessException] { 'permission denied' }
catch { "other: $($_.Exception.Message)" }
finally { if ($fs) { $fs.Dispose() } }     # always runs
```

## Native exes don't raise PowerShell errors

A failing `git`, `npm`, or `docker` does not throw. It sets `$LASTEXITCODE`. Check
it explicitly (see also `pwsh-native-commands`):

```powershell
& npm ci
if ($LASTEXITCODE -ne 0) { throw "npm ci failed ($LASTEXITCODE)" }
```

In **7+** only, `&&`/`||` react to native exit codes:
```powershell
npm ci || throw 'npm ci failed'
```

## Making your script exit non-zero

A caller (CI, another script, this harness) judges your script by its exit code.

```powershell
try { ... } catch { Write-Error $_; exit 1 }
exit 0      # explicit success
```

`exit N` sets the process exit code. `throw` at top level also yields a non-zero
exit. Don't rely on the last cmdlet's success leaking out. Be explicit.

## `Write-Error` vs `throw`

- `throw 'msg'` → **terminating**, unwinds to the nearest catch / exits the script.
- `Write-Error 'msg'` → **non-terminating** by default (unless
  `$ErrorActionPreference='Stop'`); records an error but continues.

Use `throw` (or `Write-Error` + `exit 1`) when you mean "stop now".

## Quick reference

| Situation | Fix |
|---|---|
| try/catch not catching a cmdlet | add `-ErrorAction Stop` |
| make all errors fatal | `$ErrorActionPreference = 'Stop'` (top of script) |
| truly ignore a failure | `try { ... -ErrorAction Stop } catch { }` |
| detect native exe failure | `if ($LASTEXITCODE -ne 0) { throw }` |
| guarantee non-zero exit on error | `catch { Write-Error $_; exit 1 }` |
| inspect the error | `$_.Exception.Message`, `$_.ScriptStackTrace` |
