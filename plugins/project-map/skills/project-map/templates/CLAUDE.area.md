# `{{AREA_PATH}}`: {{AREA_TITLE}}

STATUS: {{stub | current}}  ·  Last verified: {{YYYY-MM-DD}}

<!-- ≤ ~200 lines. Every claim verified against the code/content; say
     "unverified:" otherwise. Link to root CLAUDE.md rather than restating it. -->

## Purpose (and why it exists as its own area)

{{2 to 4 sentences: what this area owns, what it deliberately does NOT own, and
the reason the boundary is here.}}

## What lives here

- **`{{file_or_subdir}}`**: {{responsibility}}. {{why it's shaped this way, if non-obvious}}
- …

## How it connects

- **Depends on.** {{areas/libs}}: {{for what}}
- **Used by.** {{areas}}: {{through which entry point}}
- **Public surface.** {{the functions/types/pages others should use; everything else is internal}}

## Invariants (must stay true)

1. {{invariant}}: enforced by {{test/assert/review}}

## Load-bearing gotchas

<!-- Things that look wrong but are intentional, or look fine but bite.
     Name the evidence: file:line, test name, or the command you ran. -->
- {{gotcha}}: verified: {{evidence}}

## Common tasks

- **Add a {{thing}}.** {{steps, files to touch, test to add}}
- **Test this area.** `{{command}}`

## Open work

- {{item}} ({{tracker id if any}})
