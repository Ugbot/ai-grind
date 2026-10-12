# {{PROJECT_NAME}}: Map

> What exists, where, and **why it is shaped that way**. Keep ≤ ~400 lines.
> History/changelog → `docs/JOURNAL.md`. Rules → `CLAUDE.md`.
> Status values: done · in progress · stub · planned · frozen

## In one paragraph

{{WHAT_THE_SYSTEM_DOES_AND_ITS_SHAPE: the 3 to 5 big pieces and how data/control
flows between them.}}

## Layout

```
{{TREE — top two levels only, one-line comment per entry}}
```

## Areas

Each area has a `CLAUDE.md` that auto-loads when working inside it.

| Area | Path | Purpose | Why it's shaped this way | Status |
|---|---|---|---|---|
| {{name}} | [`{{path}}/`]({{path}}/CLAUDE.md) | {{one line}} | {{the design reason / constraint}} | done |

## Where does a new … go?

| New thing | Goes in | Because |
|---|---|---|
| {{e.g. HTTP endpoint}} | {{src/api/handlers/}} | {{handlers stay thin; logic lives in the domain area}} |

## Dependency direction

<!-- Which areas may depend on which. Arrows point at dependencies. A cycle
     here is a bug. -->
```
{{api → domain → storage → core}}
```

## Key concepts / glossary

| Term | Meaning here | Defined in |
|---|---|---|
| {{term}} | {{meaning}} | {{path}} |

## Entry points

- {{binary/CLI/site root}}: `{{path}}`
- Tests: `{{path}}` (`{{command}}`)

## Known gaps & traps (cross-cutting only; area-specific traps live in the area file)

- {{trap}}: verified by {{how}}
