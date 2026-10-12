---
name: pwsh-core-idioms
description: >
  Core PowerShell language idioms and the Windows PowerShell 5.1 vs PowerShell 7
  split. Use whenever writing or fixing a PowerShell command/script and you hit
  "operator is not recognized", a ternary/?? that won't parse, unexpected text
  output, files that open as garbled UTF-16, or you need to know which edition a
  feature requires. Covers chaining, ternary, null-coalescing, here-strings,
  the object pipeline, comparison operators, and default encoding.
---

# PowerShell core idioms: 5.1 vs 7, side by side

Two editions exist on most Windows boxes:

- Windows PowerShell 5.1 is `powershell.exe`: ships in-box, .NET Framework,
  frozen feature set. The default in many automation harnesses.
- PowerShell 7+ is `pwsh.exe`: installed separately, .NET (Core),
  cross-platform, actively developed.

Check which you're in:

```powershell
$PSVersionTable.PSVersion          # 5.1.x = Windows PowerShell, 7.x = pwsh
$PSVersionTable.PSEdition          # Desktop = 5.1, Core = 7+
```

## Pipeline chaining (`&&` / `||`)

| | 5.1 | 7+ |
|---|---|---|
| run B if A succeeds | not available, parser error | `A && B` |
| run B regardless | `A; B` | `A; B` or `A && B` |
| run B if A fails | **not available** | `A \|\| B` |

```powershell
# 5.1 — emulate &&  (check $? = success of last command)
git pull; if ($?) { npm install }
# 5.1 — emulate ||
git pull; if (-not $?) { Write-Error "pull failed" }

# 7+
git pull && npm install
git pull || Write-Error "pull failed"
```

## Ternary / null-coalescing / null-conditional

| Feature | 5.1 | 7+ |
|---|---|---|
| ternary `a ? b : c` | **no** | `$x = $cond ? 'y' : 'n'` |
| null-coalescing `??` | **no** | `$x = $a ?? 'default'` |
| coalescing assign `??=` | **no** | `$a ??= 'default'` |
| null-conditional `?.` `?[]` | **no** | `$obj?.Prop` (7.1+) |

```powershell
# 5.1 equivalents
$x = if ($cond) { 'y' } else { 'n' }          # ternary
$x = if ($null -ne $a) { $a } else { 'default' }  # ??
if ($null -eq $a) { $a = 'default' }              # ??=
```

**Always put `$null` on the LEFT** of an equality test (`$null -eq $a`), in both
editions, because it makes `-eq` behave scalar even when `$a` is an array.

## The pipeline carries OBJECTS, not text

This is the biggest mental shift from bash. `|` passes live .NET objects.

```powershell
Get-ChildItem | Where-Object Length -gt 1MB | Sort-Object Length -Descending |
    Select-Object Name, Length          # filter/sort/project on real properties
(Get-ChildItem *.log).Count             # property access, no `wc -l`
```

Don't text-scrape what's already a property. Reach for `Select-String`/regex only
on genuinely unstructured text.

## Comparison operators are words, not symbols

`-eq -ne -lt -le -gt -ge -like -match -contains -in`. `>` is **redirection**, not
greater-than. `-eq` on an array returns the matching elements (a filter), not a
boolean.

```powershell
1 -lt 2                       # True
'foo.txt' -like '*.txt'       # True  (wildcard)
'foo123' -match '\d+'         # True  (regex; sets $matches)
1,2,3 -contains 2             # True
2 -in 1,2,3                   # True
'A' -eq 'a'                   # True  (case-INsensitive by default; use -ceq for case)
```

## Here-strings (multi-line literals)

The closing `'@` / `"@` must be at column 0 on its own line. Leading
whitespace is a parse error. Same in both editions.

```powershell
$literal = @'
no $expansion here, backticks `literal too
'@

$expanded = @"
user is $env:USERNAME, 2+2 = $(2+2)
"@
```

## Default output encoding (a real trap)

- **5.1**: `Out-File`/`>`/`Set-Content` default to **UTF-16 LE with BOM**. Tools
  expecting UTF-8 will choke. Always pass `-Encoding utf8` when other programs
  read the file.
- 7+ defaults to UTF-8 with no BOM, usually what you want.

```powershell
'data' | Out-File out.txt -Encoding utf8        # safe in both editions
```

## Variables, interpolation, escape char

```powershell
$name = 'Ben'
"Hello $name"                 # interpolates
"Path is $($obj.FullName)"    # $() for expressions/properties
'Hello $name'                 # single quotes = literal, no interpolation
"`tTabbed`nNewline"           # backtick is the escape char, NOT backslash
```

## Cmdlet naming & aliases

Cmdlets are `Verb-Noun` (`Get-ChildItem`, `New-Item`, `Remove-Item`). Aliases
(`ls`, `cd`, `cat`, `rm`) exist for interactive use. **Use full Verb-Noun in
scripts** for clarity and portability (some aliases differ across editions/OSes).

## Quick reference

| Task | 5.1 | 7+ |
|---|---|---|
| chain on success | `A; if ($?){B}` | `A && B` |
| ternary | `if(c){a}else{b}` | `c ? a : b` |
| default value | `if($null -ne $a){$a}else{$d}` | `$a ?? $d` |
| version check | `$PSVersionTable.PSVersion` | same |
| utf8 file | `... -Encoding utf8` | default utf8 |

**Rule of thumb:** if a script must run under 5.1, avoid `&&`/`\|\|`/`??`/ternary
and always specify `-Encoding utf8` on file writes.
