# {{PROJECT_NAME}}: Working Agreement

<!-- Loaded into every agent turn: keep ≤ ~12,000 characters. Detail belongs in
     PROJECT_MAP.md, docs/ENGINEERING.md (or docs/STYLE.md), and area CLAUDE.md files. -->

{{ONE_PARAGRAPH_WHAT_THIS_IS}}

**Kind:** {{code | docs | mixed}} · **Primary language(s):** {{LANGS}}

## Read first

- **@PROJECT_MAP.md**: what exists, where, and *why*. Update it in the same change
  that adds/renames/removes an area.
- **Area docs**: every major directory has its own `CLAUDE.md`; it
  auto-loads when you work there. The list is in PROJECT_MAP.md → "Areas".
- **docs/ENGINEERING.md**: the full principles manual (this file is the
  quick reference). {{or docs/STYLE.md for docs projects}}
- **docs/JOURNAL.md**: history and narratives. Not a map; don't put
  current-state facts only there.

## Find things before searching

```sh
python3 .claude/tools/kb.py search "<terms>"   # BM25 over files + notes
python3 .claude/tools/kb.py symbol <name>      # definition sites
```
Grep only if the index misses. When you learn something non-obvious, record
it: `kb.py note add --topic ... --paths ... --body ...`.

## Hard rules

<!-- 2 to 7 rules that are genuinely non-negotiable here. Each: the rule, one
     line of why. Delete this comment. -->
1. {{RULE}}: {{WHY}}
2. {{RULE}}: {{WHY}}

## Principles (summary: full text in docs/ENGINEERING.md)

- **DRY** for knowledge, not for coincidentally similar lines; prefer
  duplication over the wrong abstraction (rule of three).
- **SOLID** where there are real seams: single responsibility per
  module/function, depend on interfaces at boundaries, no speculative layers.
- **KISS / YAGNI**: the simplest thing that meets today's requirement.
- **Errors are values at boundaries**; assertions are for programmer errors.
- **Tests ship with the change.** A feature without a test is not done.
- **Unfinished = explicit.** Return "not implemented" loudly; never a silent
  stub that pretends to work.

## Build & test

```sh
{{BUILD_COMMAND}}
{{TEST_COMMAND}}
{{LINT_COMMAND}}
```

## Workflow

- Branch for non-trivial work; commit only when asked.
- New area checklist: create dir → `<dir>/CLAUDE.md` from the area template →
  row in PROJECT_MAP.md → `kb.py check`.
- Correcting a wrong doc claim: fix in place, mark `[CORRECTED YYYY-MM-DD: …]`.
