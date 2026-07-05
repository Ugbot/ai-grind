---
name: pwsh-env-and-packages
description: >
  Environment variables, PATH, and package managers in PowerShell. Use when you
  need to read/set an env var, make an env change persist across sessions, edit
  PATH, or install/update CLI tools via winget, scoop, or choco. Covers the
  difference between session-only ($env:) and persistent
  ([Environment]::SetEnvironmentVariable) changes. 5.1 and 7+ behave the same here.
---

# PowerShell environment & package management

## Environment variables — session vs persistent

```powershell
$env:PATH                                  # read
$env:MY_VAR = 'value'                       # set FOR THIS SESSION ONLY (and children)
Remove-Item Env:\MY_VAR                     # unset for this session
Get-ChildItem Env:                          # list all env vars
```

`$env:NAME` changes vanish when the session ends. To **persist**, use the .NET API
and pick a scope:

```powershell
# Persist for the current user (no admin needed). Scope: 'User' | 'Machine' | 'Process'
[Environment]::SetEnvironmentVariable('MY_VAR', 'value', 'User')

# Persist machine-wide (requires elevation)
[Environment]::SetEnvironmentVariable('MY_VAR', 'value', 'Machine')

# Read a persisted value back
[Environment]::GetEnvironmentVariable('MY_VAR', 'User')
```

**A persisted change does NOT affect already-open sessions** — only new ones. To
use it immediately, also set `$env:MY_VAR` in the current session.

```powershell
[Environment]::SetEnvironmentVariable('MY_VAR','value','User')
$env:MY_VAR = 'value'      # so the current session sees it too
```

There is **no inline `VAR=value command` prefix** like bash. Set `$env:VAR` first,
then run the command (it inherits into children):

```powershell
$env:NODE_ENV = 'production'; npm run build
```

## PATH — append safely, persistently

```powershell
# Session only:
$env:PATH = "$env:PATH;C:\tools\bin"

# Persistent (User scope), without clobbering the existing PATH:
$old = [Environment]::GetEnvironmentVariable('Path','User')
if ($old -notlike "*C:\tools\bin*") {
    [Environment]::SetEnvironmentVariable('Path', "$old;C:\tools\bin", 'User')
}
$env:PATH += ';C:\tools\bin'      # reflect immediately in this session
```

Always read-modify-write the **same scope** (User or Machine). Reading
`$env:PATH` and writing it back to `User` would merge Machine+User into User and
corrupt it — read with `GetEnvironmentVariable(...,'User')` for the User scope.

## Special / useful built-in variables

```powershell
$env:USERPROFILE        # C:\Users\<you>   (home dir)
$env:USERNAME
$env:COMPUTERNAME
$env:TEMP               # temp dir
$env:ProgramFiles ; ${env:ProgramFiles(x86)}    # note braces for the (x86) name
$env:OS                 # 'Windows_NT'
$HOME                   # PowerShell's home (7+ cross-platform; 5.1 may be unset)
```

`${env:ProgramFiles(x86)}` needs braces because `(x86)` isn't a legal bare var
name.

## Package managers

### winget (Microsoft, in-box on Win10 21H2+/Win11)

```powershell
winget search nodejs
winget install OpenJS.NodeJS.LTS
winget upgrade --all
winget list
winget uninstall <id>
```

System-wide installs; may prompt for elevation. Best default for mainstream apps.

### scoop (per-user, no admin, dev-friendly)

```powershell
# Install scoop (per-user, no admin):
Invoke-RestMethod get.scoop.sh | Invoke-Expression
scoop install ripgrep fd jq
scoop update *           # update everything
scoop bucket add extras  # extra app catalog
```

Installs into `~\scoop`, adds to User PATH automatically. Great for CLI tooling
without UAC.

### choco (Chocolatey, system-wide, mature catalog)

```powershell
# Install requires an elevated shell; see https://chocolatey.org/install
choco install git -y     # -y auto-confirms (non-interactive)
choco upgrade all -y
choco list --local-only
```

Run in an **elevated** PowerShell; pass `-y` so it doesn't prompt.

## Choosing a package manager

- Mainstream GUI/CLI apps, zero setup → **winget**.
- Dev CLI tools, no admin, clean per-user → **scoop**.
- Servers / mature scripted provisioning → **choco** (elevated, `-y`).

## Quick reference

| Need | Command |
|---|---|
| read env var | `$env:NAME` |
| set (session) | `$env:NAME = 'v'` |
| set (persist, user) | `[Environment]::SetEnvironmentVariable('NAME','v','User')` |
| inline env for one cmd | `$env:NAME='v'; cmd` (no bash-style prefix) |
| append PATH (persist) | read `User` scope, append, write back `User` |
| install tool | `winget install <id>` / `scoop install <x>` / `choco install <x> -y` |
| list env | `Get-ChildItem Env:` |
