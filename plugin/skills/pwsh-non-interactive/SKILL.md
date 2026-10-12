---
name: pwsh-non-interactive
description: >
  Writing PowerShell that runs safely under automation / -NonInteractive (CI,
  agents, scheduled tasks, this harness) without hanging on a prompt. Use when a
  command might block waiting for input, when a destructive cmdlet pops a
  confirmation, when you need a command to fail fast instead of prompting, or when
  porting an interactive script to run headless. Lists the blocking cmdlets to
  avoid and their non-interactive replacements. 5.1 and 7+.
---

# PowerShell for non-interactive / automated execution

In a headless context (CI, a scheduled task, an AI agent harness, `pwsh
-NonInteractive`) any prompt is a hang, because there's no human to answer it. Write
defensively.

## Never call these in automation (they block forever)

| Blocking call | Use instead |
|---|---|
| `Read-Host` | a `param()` argument, env var, or config file |
| `Get-Credential` | `-Credential` from a stored secret / `New-Object PSCredential` from a vault |
| `$Host.UI.PromptForChoice(...)` | a `[switch]` parameter that decides up front |
| `Out-GridView` (interactive) | `Format-Table` / `Export-Csv` |
| `pause` / `cmd /c pause` | remove it |
| `Wait-Event` with no source | a bounded `Wait-Job -Timeout` |
| `-Confirm` prompts (implicit) | pass `-Confirm:$false` (see below) |

## Destructive cmdlets prompt by default, so suppress it

Cmdlets with a high `ConfirmImpact` (`Remove-Item` on read-only/hidden,
`Stop-Process`, `Clear-Content`, `Stop-Service`, many `Remove-*`) may prompt.
Always be explicit:

```powershell
Remove-Item $path -Recurse -Force -Confirm:$false
Stop-Process -Name node -Force -Confirm:$false
Stop-Service -Name w3svc -Force -Confirm:$false
```

`-Force` handles read-only/hidden/edge cases; `-Confirm:$false` defeats the
confirmation prompt. You can also globally lower the bar at the top of a script:

```powershell
$ConfirmPreference = 'None'      # never auto-prompt for confirmation
```

## Run the shell itself non-interactively

```powershell
powershell -NonInteractive -NoProfile -ExecutionPolicy Bypass -File .\task.ps1
pwsh       -NonInteractive -NoProfile -ExecutionPolicy Bypass -File .\task.ps1
```

- `-NonInteractive` makes cmdlets that would prompt error out instead of hanging
  (turning a silent hang into a visible, debuggable failure; recommended).
- `-NoProfile` skips `$PROFILE`, so machine-specific profile code can't interfere.
- `-ExecutionPolicy Bypass` runs unsigned scripts in this invocation only.

## Make errors fail the run (don't hang or silently pass)

```powershell
$ErrorActionPreference = 'Stop'      # cmdlet errors become terminating
$ProgressPreference    = 'SilentlyContinue'   # speeds up Invoke-WebRequest, avoids progress UI in logs
try { ... } catch { Write-Error $_; exit 1 }
exit 0
```

Setting `$ProgressPreference = 'SilentlyContinue'` also dramatically speeds up
`Invoke-WebRequest`/`Invoke-RestMethod` in 5.1 (the progress bar is slow and
useless in logs).

## Native commands: pass their non-interactive flags

Most tools have a "don't ask" flag. Use it:

```powershell
git ... --no-edit          # don't open an editor
npm ci --no-audit --no-fund
choco install x -y         # auto-confirm
apt-get install -y ...     # (in containers)
docker build -q ...
```

Never invoke editor-driven git modes in automation: `git rebase -i`,
`git commit` (no `-m`) and `git add -i` open `$EDITOR` and hang.

## Inputs come from parameters/env, not prompts

```powershell
param(
    [Parameter(Mandatory)] [string] $Target,        # caller MUST supply -> no Read-Host
    [string] $ApiKey = $env:API_KEY                  # fall back to env, never prompt
)
if (-not $ApiKey) { Write-Error 'API_KEY not set'; exit 1 }
```

`[Parameter(Mandatory)]` is safe in automation **only if the caller passes the
value**. A missing mandatory param will itself prompt interactively. In strict
headless flows, validate explicitly and `exit 1` rather than relying on Mandatory.

## Timeouts: never wait unbounded

```powershell
$job = Start-Job { slow-thing }
if (-not (Wait-Job $job -Timeout 120)) { Stop-Job $job; throw 'timed out' }
Receive-Job $job; Remove-Job $job

# HTTP with a timeout
Invoke-RestMethod $url -TimeoutSec 30
```

## Quick reference

| Risk | Mitigation |
|---|---|
| any prompt | `powershell -NonInteractive -NoProfile` |
| confirmation prompt | `-Confirm:$false` (or `$ConfirmPreference='None'`) |
| `Read-Host`/`Get-Credential` | `param()` / env / secret store |
| silent hang on error | `$ErrorActionPreference='Stop'` + `try/catch/exit 1` |
| editor-driven git | `--no-edit`, always `-m`, never `-i` |
| slow web cmdlets (5.1) | `$ProgressPreference='SilentlyContinue'` |
| unbounded wait | `Wait-Job -Timeout`, `-TimeoutSec` |
