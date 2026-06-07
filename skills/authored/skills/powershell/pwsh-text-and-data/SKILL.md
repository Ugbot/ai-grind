---
name: pwsh-text-and-data
description: >
  Working with text and structured data in PowerShell — JSON, CSV, regex,
  Select-String (grep), string manipulation, and here-strings. Use when you need
  to parse or emit JSON/CSV, grep file contents, extract with regex, build
  multi-line strings, or when ConvertFrom-Json gives you a PSCustomObject you
  expected to be a hashtable. Covers the -AsHashtable difference between 5.1 and 7+.
---

# PowerShell text & structured data

PowerShell prefers structured objects over text scraping. Reach for regex only on
genuinely unstructured input. Notes mark 5.1 vs 7+ differences.

## JSON

```powershell
$obj  = Get-Content config.json -Raw | ConvertFrom-Json     # parse
$obj.server.port                                            # dotted access
$json = $obj | ConvertTo-Json -Depth 10                     # serialize
$json | Set-Content out.json -Encoding utf8
```

Key gotchas:

- **`ConvertFrom-Json` returns a `PSCustomObject`, not a hashtable.** Access fields
  with dot notation (`$o.name`), not `$o['name']`.
- **`-AsHashtable` exists only in 7+.** In 5.1 you must convert manually:
  ```powershell
  # 7+
  $h = Get-Content f.json -Raw | ConvertFrom-Json -AsHashtable
  # 5.1 — build a hashtable from the PSCustomObject
  $o = Get-Content f.json -Raw | ConvertFrom-Json
  $h = @{}; $o.PSObject.Properties | ForEach-Object { $h[$_.Name] = $_.Value }
  ```
- **`ConvertTo-Json` default `-Depth` is 2 (5.1) / 2 (7+)** — nested objects beyond
  that become `System.Object[]` / type names. **Always set `-Depth`** for nested data.
- 7+ adds `-Depth` auto-warnings, `-AsArray`, and nicer formatting; 5.1 output is
  more verbose/escaped.

## CSV

```powershell
$rows = Import-Csv data.csv                          # array of PSCustomObjects (header = props)
$rows | Where-Object { [int]$_.age -gt 30 } |
    Select-Object name, age |
    Export-Csv out.csv -NoTypeInformation -Encoding utf8
$rows | ConvertTo-Csv -NoTypeInformation             # to string, not file
```

- `-NoTypeInformation` drops the `#TYPE` header line. In **7+ it's the default**;
  in **5.1 you must pass it** or you get an extra junk first line.
- Use `-Delimiter "`t"` for TSV.

## Select-String — the grep of PowerShell

```powershell
Select-String -Path *.log -Pattern 'ERROR'                       # grep
Select-String -Path src\*.ts -Pattern 'TODO' -SimpleMatch        # literal, not regex
Select-String -Path . -Pattern 'foo' -Recurse                    # NOTE: -Recurse is 7+ only
Get-ChildItem -Recurse -Filter *.ts | Select-String 'foo'        # 5.1 recursive idiom
(Select-String -Path f -Pattern 'x' -Quiet)                      # boolean: any match?
Select-String -Pattern 'warn' f -Context 2,2                     # 2 lines of context each side
```

Each match is a `MatchInfo` object: `.LineNumber`, `.Line`, `.Path`, `.Matches`.
`-Recurse` on `Select-String` itself is **7+ only** — on 5.1 pipe `Get-ChildItem`
into it.

## Regex — `-match`, `-replace`, `[regex]`

```powershell
'order-1234' -match 'order-(\d+)'      # True; populates $matches
$matches[1]                            # '1234'  (capture group)

'a,b;c' -split '[,;]'                   # split on a regex -> @('a','b','c')
'foo123bar' -replace '\d+', '#'         # 'foo#bar'  (regex replace)
'A B  C' -replace '\s+', ' '            # collapse whitespace

[regex]::Matches($text, '\b\w+@\w+\.\w+\b') | ForEach-Object { $_.Value }   # all matches
```

`-match`/`-replace`/`-split` are case-insensitive by default; prefix `c`
(`-cmatch`, `-creplace`) for case-sensitive.

## String manipulation

```powershell
$s.Trim(); $s.TrimEnd(';')
$s.ToUpper(); $s.ToLower()
$s.Substring(0, 8)
$s.Replace('a', 'b')          # LITERAL replace (.NET method) — vs -replace (regex)
$s.Split(',')                 # literal split — vs -split (regex)
$s.Contains('x'); $s.StartsWith('y'); $s.EndsWith('.txt')
$s.PadLeft(10); $s -f $args   # formatting:  '{0:N2}' -f 3.14159  -> '3.14'
'{0,-20} {1,8:N0}' -f $name, $count    # aligned columns
```

Note the pairing: **`.Replace()`/`.Split()` are literal; `-replace`/`-split` are
regex.** Pick deliberately.

## Here-strings for multi-line text

Closing delimiter must be at column 0 (see `pwsh-core-idioms`).

```powershell
$report = @"
Build:   $buildId
Status:  $status
Elapsed: $($sw.Elapsed)
"@
```

## Joining & splitting collections

```powershell
$lines -join "`n"            # array -> single string
'a;b;c' -split ';'           # string -> array
($nums | Measure-Object -Sum).Sum
$items -join ', '
```

## Quick reference

| Need | Command |
|---|---|
| parse JSON | `Get-Content f -Raw \| ConvertFrom-Json` |
| emit JSON (nested) | `$o \| ConvertTo-Json -Depth 10` |
| JSON as hashtable | `... -AsHashtable` (7+); manual loop (5.1) |
| read CSV | `Import-Csv f` |
| write CSV | `... \| Export-Csv f -NoTypeInformation -Encoding utf8` |
| grep | `Select-String -Path f -Pattern p` |
| grep recursive (5.1) | `gci -Recurse \| Select-String p` |
| regex capture | `'s' -match 'p(\d+)'; $matches[1]` |
| regex replace | `$s -replace 'pat','rep'` |
| literal replace | `$s.Replace('a','b')` |
