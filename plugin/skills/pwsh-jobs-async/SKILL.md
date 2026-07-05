---
name: pwsh-jobs-async
description: >
  Running work in the background or in parallel in PowerShell — Start-Job,
  ThreadJob, ForEach-Object -Parallel, Start-Process, and runspaces. Use when you
  need to run commands concurrently, launch a long-running process without
  blocking, fan out work over many items, wait on / collect background results,
  or speed up an I/O-bound loop. Notes which mechanisms are 7+ only vs available
  in 5.1.
---

# PowerShell background & parallel execution

Several mechanisms, with different cost and edition support. Pick by workload.

## At a glance

| Mechanism | Isolation | Edition | Best for |
|---|---|---|---|
| `Start-Job` | separate **process** | 5.1 & 7+ | a few heavy/independent tasks; survives across pipelines |
| `Start-ThreadJob` | thread (same process) | 7+ in-box; 5.1 via `Install-Module ThreadJob` | many lightweight I/O tasks (cheaper than Start-Job) |
| `ForEach-Object -Parallel` | thread/runspace | **7+ only** | fan-out over a collection |
| `Start-Process` | new OS process | 5.1 & 7+ | launch an external app/exe, optionally fire-and-forget |
| runspaces / `RunspacePool` | threads | 5.1 & 7+ | max-control high-throughput parallelism |

## Start-Job — process-isolated background jobs (both editions)

```powershell
$j = Start-Job -ScriptBlock { Start-Sleep 3; "done at $(Get-Date -Format T)" }
# ... do other work ...
$j | Wait-Job | Receive-Job          # block, then collect output
Remove-Job $j                         # clean up

# Many jobs:
$jobs = 1..5 | ForEach-Object { Start-Job { param($n) $n*$n } -ArgumentList $_ }
$jobs | Wait-Job | Receive-Job
$jobs | Remove-Job
```

Jobs run in a **separate process** — no shared variables. Pass data via
`-ArgumentList` (and `param()` in the block) or `$using:var`. Output is buffered
until `Receive-Job`.

## Start-ThreadJob — lighter, same API (7+ in-box / 5.1 module)

```powershell
# 5.1: Install-Module ThreadJob -Scope CurrentUser   (one-time)
$jobs = $urls | ForEach-Object { Start-ThreadJob -ScriptBlock { param($u) Invoke-WebRequest $u } -ArgumentList $_ }
$jobs | Wait-Job | Receive-Job
$jobs | Remove-Job
```

Much lower startup cost than `Start-Job` because there's no new process. Ideal for
dozens of network/file calls.

## ForEach-Object -Parallel — fan-out (7+ ONLY)

```powershell
# 7+ only. -ThrottleLimit caps concurrent runspaces (default 5).
$results = $items | ForEach-Object -Parallel {
    $u = $using:baseUrl + $_           # outer vars need the $using: prefix
    Invoke-RestMethod $u
} -ThrottleLimit 8
```

Gotchas: each iteration is an isolated runspace — reference outer variables with
`$using:`; don't append to a shared `[List]` without a thread-safe collection
(`[System.Collections.Concurrent.ConcurrentBag[object]]`). **Not available in 5.1**
— use ThreadJob or a runspace pool there.

## Start-Process — launch external programs

```powershell
# Fire-and-forget (returns immediately)
Start-Process notepad.exe

# Run and WAIT, capture exit code + streams
$p = Start-Process myexe.exe -ArgumentList 'build','--release' -NoNewWindow -Wait -PassThru `
        -RedirectStandardOutput out.txt -RedirectStandardError err.txt
$p.ExitCode

# Elevated (UAC prompt)
Start-Process powershell.exe -Verb RunAs -ArgumentList '-File','setup.ps1'
```

`-Wait` blocks; `-PassThru` returns the process object; `-NoNewWindow` keeps it in
the current console.

## Runspace pool — maximum-throughput parallelism (both editions)

When you need 5.1-compatible high concurrency beyond ThreadJob:

```powershell
$pool = [runspacefactory]::CreateRunspacePool(1, 8); $pool.Open()
$tasks = foreach ($item in $items) {
    $ps = [powershell]::Create(); $ps.RunspacePool = $pool
    [void]$ps.AddScript({ param($x) $x * 2 }).AddArgument($item)
    [pscustomobject]@{ PS = $ps; Handle = $ps.BeginInvoke() }
}
$results = foreach ($t in $tasks) { $t.PS.EndInvoke($t.Handle); $t.PS.Dispose() }
$pool.Close(); $pool.Dispose()
```

More boilerplate, but the fastest and works on 5.1.

## Polling / timeouts / cleanup

```powershell
Get-Job                                            # list jobs in this session
Wait-Job $j -Timeout 30                             # bounded wait
$j.State                                            # Running / Completed / Failed
Receive-Job $j -Keep                                # read output without consuming
Get-Job | Where-Object State -eq 'Completed' | Remove-Job   # tidy up
```

**Always `Remove-Job` (or Dispose runspaces) when done** — orphaned jobs leak
memory and (for `Start-Job`) processes.

## Choosing

- 1–5 heavy independent tasks → `Start-Job`.
- Many I/O-bound calls, 7+ → `ForEach-Object -Parallel`; 5.1 → `Start-ThreadJob`.
- Launching an external app → `Start-Process`.
- CPU-bound or need 5.1 + high throughput → runspace pool.

## Quick reference

| Need | 5.1 | 7+ |
|---|---|---|
| background task | `Start-Job` | `Start-Job` / `Start-ThreadJob` |
| parallel over list | runspace pool / ThreadJob | `ForEach-Object -Parallel` |
| outer var in parallel | `-ArgumentList`+`param()` | `$using:var` |
| launch + wait + exit code | `Start-Process -Wait -PassThru` | same |
| collect results | `Wait-Job \| Receive-Job` | same |
