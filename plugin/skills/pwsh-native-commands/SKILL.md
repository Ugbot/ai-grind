---
name: pwsh-native-commands
description: >
  Invoking native executables (git, npm, docker, .exe) from PowerShell correctly.
  Use when a native command's args get mangled, when paths with spaces fail, when
  redirecting a native command's stderr makes PowerShell report failure on exit 0
  (NativeCommandError), when an exe's exit code isn't being detected, or when an
  argument starting with - / -- / @ is misparsed. Covers the call operator,
  --% stop-parsing, $LASTEXITCODE vs $?, and stdout/stderr capture.
---

# Calling native executables from PowerShell

PowerShell parses a command line before handing it to a native `.exe`. Most
"it works in cmd/bash but not here" problems come from that parsing layer.
Behavior below is the same in 5.1 and 7+ unless noted.

## Run an exe whose path has spaces — the call operator `&`

A bare quoted string is treated as data, not a command. Use `&` to invoke it.

```powershell
& "C:\Program Files\App\app.exe" arg1 arg2
$exe = "C:\tools\my tool.exe"
& $exe --flag value
```

## Stop PowerShell from parsing args — `--%`

The stop-parsing token `--%` passes everything after it verbatim (no variable
expansion, no operator interpretation). Use it for args containing `-`, `--`,
`@`, `()`, `{}`, or literal `$` that a foreign tool must see unchanged.

```powershell
git log --% --pretty=format:%H %an        # % and : reach git untouched
npm run build --% -- --env=prod           # the -- and = pass through
```

Caveat: inside `--%` you cannot use PowerShell variables — it's fully literal.

## Arguments: splatting an array is the robust way

```powershell
$cmakeArgs = @('-S', '.', '-B', 'build', '-G', 'Visual Studio 17 2022', '-A', 'x64')
& cmake @cmakeArgs        # @array splats each element as a separate, properly-quoted arg
```

This avoids quoting headaches far better than building one big string.

## Exit codes: `$LASTEXITCODE` vs `$?`

- `$LASTEXITCODE` — the **integer** exit code of the last *native exe*. This is
  what you check for git/npm/docker/etc.
- `$?` — a **boolean**: did the last *PowerShell* operation succeed? For native
  exes it's just `$LASTEXITCODE -eq 0`, but it's also affected by cmdlet errors.

```powershell
& cmake --build build
if ($LASTEXITCODE -ne 0) { Write-Error "build failed ($LASTEXITCODE)"; exit 1 }
```

In **7+** you can also use `&&`/`||` which key off the native exit code:
```powershell
cmake --build build && ctest --test-dir build
```

## The `2>&1` NativeCommandError trap (the big one)

In **Windows PowerShell 5.1**, redirecting a native exe's stderr *inside
PowerShell* wraps each stderr line in a `System.Management.Automation.ErrorRecord`
(a "NativeCommandError"). Side effects:

- The wrapped lines look like PowerShell errors, not plain text.
- `$?` becomes `$false` **even when the exe returned exit code 0** (many tools —
  git, docker, msbuild — write progress to stderr normally).

```powershell
# 5.1 — RISKY: a clean `git` run can look "failed"
$out = git status 2>&1            # stderr lines become ErrorRecords; $? may be $false

# 5.1 — SAFER: check the real exit code, don't trust $?
$out = git status 2>&1
if ($LASTEXITCODE -ne 0) { throw "git failed" }   # judge by exit code only

# Or merge streams via cmd so PowerShell never wraps them:
$out = cmd /c 'git status 2>&1'
```

In **7+** stream handling is cleaner and this rarely bites, but judging success
by `$LASTEXITCODE` (not `$?`) is still the correct habit for native commands.

## Capturing stdout and stderr separately

```powershell
# Capture stdout to a var, let stderr flow to the console
$stdout = & myexe.exe args

# Capture both, separately, via temp files (works in both editions)
$out = New-TemporaryFile; $err = New-TemporaryFile
Start-Process myexe.exe -ArgumentList 'args' -NoNewWindow -Wait `
    -RedirectStandardOutput $out -RedirectStandardError $err
$stdoutText = Get-Content $out -Raw
$stderrText = Get-Content $err -Raw
Remove-Item $out, $err
```

## Don't pipe objects into a native exe expecting text

`$obj | myexe.exe` stringifies objects with PowerShell's formatter (often
truncated/aligned). Convert explicitly first:

```powershell
$data | ConvertTo-Json | & myexe.exe --stdin
$lines | Set-Content tmp.txt -Encoding utf8; & myexe.exe tmp.txt
```

## Quick reference

| Need | Do |
|---|---|
| run exe with spaces in path | `& "C:\path with space\a.exe" args` |
| pass weird args verbatim | `tool --% --raw=$literal stuff` |
| many args, safe quoting | `& tool @argArray` |
| check native success | `if ($LASTEXITCODE -ne 0) {...}` (NOT `$?` in 5.1) |
| avoid stderr-wrapping (5.1) | judge by `$LASTEXITCODE`; or `cmd /c '... 2>&1'` |
| capture stdout+stderr apart | `Start-Process -RedirectStandard*` |
