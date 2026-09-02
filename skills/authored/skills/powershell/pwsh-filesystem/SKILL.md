---
name: pwsh-filesystem
description: >
  Filesystem operations in PowerShell and the Unix-command equivalents
  (head/tail/which/touch/mkdir -p/rm -rf/ln -s/wc -l). Use when you need to
  list/find/copy/move/delete files, handle paths with spaces, create or remove
  directories safely, make symlinks, read first/last N lines, or translate a bash
  file command into PowerShell. Covers safety traps like Remove-Item prompts and
  New-Item -Force truncation. 5.1 and 7+.
---

# PowerShell filesystem operations

Behavior is the same in 5.1 and 7+ unless noted. Quote any path containing spaces
with double quotes; prefer `Join-Path` over string concatenation.

## Unix → PowerShell command map

| Unix | PowerShell |
|---|---|
| `ls` | `Get-ChildItem` (alias `gci`, `ls`, `dir`) |
| `ls -la` | `Get-ChildItem -Force` |
| `find . -name '*.ts'` | `Get-ChildItem -Recurse -Filter *.ts` |
| `head -n 20 f` | `Get-Content f -TotalCount 20` |
| `tail -n 20 f` | `Get-Content f -Tail 20` |
| `tail -f f` | `Get-Content f -Wait -Tail 10` |
| `wc -l f` | `(Get-Content f \| Measure-Object -Line).Lines` |
| `which tool` | `(Get-Command tool).Source` |
| `cat a b > c` | `Get-Content a, b \| Set-Content c` |
| `cp -r a b` | `Copy-Item a b -Recurse` |
| `mv a b` | `Move-Item a b` |
| `rm f` | `Remove-Item f` |
| `rm -rf d` | `Remove-Item d -Recurse -Force` |
| `mkdir -p a/b/c` | `New-Item -ItemType Directory -Force a/b/c` |
| `touch f` | see "touch" below; do NOT use `New-Item -Force` |
| `ln -s tgt lnk` | `New-Item -ItemType SymbolicLink -Path lnk -Target tgt` |
| `pwd` | `Get-Location` (alias `pwd`) |
| `realpath f` | `(Resolve-Path f).Path` |
| `basename f` | `Split-Path f -Leaf` |
| `dirname f` | `Split-Path f -Parent` |

## Safety traps (read these)

### `touch`, and never `New-Item -Force` on an existing file

`New-Item -ItemType File -Force f` **truncates** an existing file. To touch safely:

```powershell
if (-not (Test-Path $f)) { New-Item -ItemType File $f | Out-Null }
else { (Get-Item $f).LastWriteTime = Get-Date }     # update mtime only
```

### `Remove-Item` can prompt and hang automation

Deleting read-only/hidden items or non-empty dirs may prompt. In non-interactive
contexts a prompt = a hang. Be explicit:

```powershell
Remove-Item $path -Recurse -Force -Confirm:$false
```

`-Force` covers read-only/hidden; `-Confirm:$false` suppresses the confirmation.

### `mkdir -p` equivalent

`New-Item -ItemType Directory -Force a/b/c` creates the whole chain and does **not**
error if it already exists. (`-Force` on a directory is safe, unlike on a file.)

## Listing, filtering, finding

```powershell
Get-ChildItem -Recurse -File -Filter *.log              # all .log files, recursive
Get-ChildItem -Recurse -Directory                       # directories only
Get-ChildItem | Where-Object Length -gt 10MB            # files over 10 MB
Get-ChildItem -Recurse -File |
    Sort-Object LastWriteTime -Descending |
    Select-Object -First 5 FullName, LastWriteTime      # 5 most-recent files
```

`-Filter` (provider-level, fast) beats `-Include`/`Where-Object` (post-filter) for
simple name globs. Use `Where-Object` for property predicates.

## Reading & writing

```powershell
$text  = Get-Content $f -Raw                 # whole file as ONE string
$lines = Get-Content $f                       # array of lines
'line' | Add-Content $f -Encoding utf8        # append
$obj | Set-Content $f -Encoding utf8          # overwrite (always set -Encoding; see pwsh-core-idioms)
```

Always pass `-Encoding utf8` when another program reads the file. 5.1 defaults
to UTF-16-with-BOM.

## Paths: build them, don't concatenate

```powershell
$p = Join-Path $root 'src' 'main.ts'          # OS-correct separators (7+ takes multiple parts)
$p = Join-Path (Join-Path $root 'src') 'main.ts'   # 5.1: two args at a time
Test-Path $p                                   # exists?
Test-Path $p -PathType Leaf                    # exists AND is a file
Split-Path $p -Parent                          # dirname
[System.IO.Path]::ChangeExtension($p, '.bak')  # .NET helpers when needed
```

`$PSScriptRoot` is the directory of the running script. Use it to locate files
relative to the script, never the (unpredictable) current directory.

## Copy/move/rename

```powershell
Copy-Item $src $dst -Recurse -Force            # dir or file
Move-Item $src $dst -Force
Rename-Item $f 'newname.txt'
robocopy $srcDir $dstDir /MIR /NFL /NDL        # large/mirrored copies; robocopy exit codes 0-7 = success
```

`robocopy` returns 0 to 7 on success (8 and up is failure), so don't treat its non-zero
"files copied" codes as errors.

## Bulk operations

```powershell
Get-ChildItem -Recurse -Filter *.tmp | Remove-Item -Force          # delete all .tmp
Get-ChildItem *.jpeg | Rename-Item -NewName { $_.Name -replace '\.jpeg$','.jpg' }
Get-ChildItem -Recurse -File | Measure-Object Length -Sum          # total size
```

## Quick reference

| Need | Command |
|---|---|
| first/last N lines | `Get-Content f -TotalCount N` / `-Tail N` |
| recursive find | `Get-ChildItem -Recurse -Filter *.ext` |
| make dir tree | `New-Item -ItemType Directory -Force p` |
| safe delete | `Remove-Item p -Recurse -Force -Confirm:$false` |
| safe touch | `if(-not(Test-Path f)){New-Item -ItemType File f}` |
| which | `(Get-Command tool).Source` |
| symlink | `New-Item -ItemType SymbolicLink -Path l -Target t` |
| script's own dir | `$PSScriptRoot` |
