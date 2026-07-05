---
name: tracker-acceptance
description: >
  Link tracker tasks to test outcomes: write acceptance criteria, attach
  concrete test references (file::test_name), record pass/fail after runs, and
  work with the close gate (advisory vs strict project policy). Use when
  defining "done" for a story, after running tests that verify a tracked task,
  or when a close is rejected by the strict gate.
---

# Acceptance criteria and the close gate

Every outcome a task promises should be a criterion, and every criterion
should point at the test that proves it. The tracker enforces exactly that at
close time, as hard as the project's policy says.

## Writing criteria

```
tracker_criteria(action="add", key="GRIND-7",
                 text="filter returns only tagged tasks",
                 test_ref="tests/test_tracker_tools.py::test_tasks_view_and_filters")
```

- `test_ref` convention: `path/to/test_file.py::test_name` (pytest style); use
  the analogous stable identifier for other frameworks (e.g. ctest name,
  `mvn -Dtest=Class#method`).
- A criterion without `test_ref` counts as **unlinked**; add the ref as soon as
  the test exists (`action="update"`, `criterion_id=…`).

## Recording outcomes

After running the linked test:

```
tracker_criteria(action="record", criterion_id=3, result="pass")   # or "fail"
```

This stamps `last_run_at`. Criteria are **met** only when their last recorded
result is `pass`. `tracker_criteria(action="list", key="GRIND-7")` shows
`[x]` met / `[!]` failed / `[ ]` never-run, and which have no linked test.

## The close gate

`tracker_status(key=…, status="done")` evaluates all criteria on the task:

- **advisory** (project default): closes, but the response carries warnings
  for every unmet or unlinked criterion. Treat warnings as a to-do, not noise.
- **strict** (`tracker_project(action="set_policy", key="GRIND",
  close_policy="strict")`): the close is **rejected** while any criterion is
  unmet or unlinked. `override=True` forces it through (the response still
  lists what was outstanding — say why you overrode in your commit/PR).

The gate looks only at the task being closed, not its children — close
bottom-up so the tree's state stays honest.

## Recommended rhythm

1. When a story is created, write its criteria immediately — they're the spec.
2. When the test lands, link it (`update` with `test_ref`).
3. After each relevant test run, `record` the result.
4. Close. If the gate complains, the work isn't done — finish it or
   consciously override.
