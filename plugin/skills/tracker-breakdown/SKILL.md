---
name: tracker-breakdown
description: >
  Decompose work in the devtools-mcp tracker: epics into stories, stories into
  punch-card subtasks, with auto-tag rules classifying tasks at creation. Use
  when planning a feature ("break this down", "build me a backlog", user-story
  writing), when restructuring a tree (reparent/reorder), or when setting up
  tag rules so new tasks self-classify.
---

# Breaking work down (punch cards)

The tracker models a bounded hierarchy: any task can parent subtasks via a
foreign key, up to **6 levels deep**. The conventional ladder is
`epic → story → task → subtask`, but the schema doesn't force it — `kind` is a
label, structure is the parent link.

## User-story building

1. Epic for the goal:
   `tracker_task(action="create", project="GRIND", title="Search v2", kind="epic")`
2. Stories under it (user-facing slices):
   `tracker_task(action="create", project="GRIND", title="As a user, I can filter by tag", kind="story", parent="GRIND-1")`
3. Punch-card the story into executable steps in one call:
   `tracker_task(action="breakdown", key="GRIND-2", subtasks=["schema migration", "query parser", "UI chips", "tests"])`

`breakdown` creates up to **20** children at once; the child kind defaults by
parent kind (`epic→story`, `story→task`, `task→subtask`, anything else
`→subtask`). Pass `kind=` to override.

Write acceptance criteria on each story as you go — see tracker-acceptance.

## Restructuring

- Reparent: `tracker_task(action="move", key="GRIND-9", parent="GRIND-4")` —
  moves the whole subtree; rejected if it would exceed 6 levels or create a
  cycle.
- Detach to top level: `tracker_task(action="move", key="GRIND-9", to_root=True)`
- Reorder among siblings: `tracker_task(action="move", key="GRIND-9", before="GRIND-5")`

## Auto-tag rules

Rules apply tags at **creation time** (they don't retro-tag). Conditions AND
together; any can be omitted:

```
tracker_tag(action="rule_add", tag="user-story", match_kind="story")
tracker_tag(action="rule_add", tag="performance", match_regex="(?i)\\bperf|latency\\b")
tracker_tag(action="rule_add", tag="acceptance", match_parent_kind="story")
tracker_tag(action="rule_add", tag="grind-only", match_kind="task", project="GRIND")
```

- `match_regex` searches title + description.
- `project=` scopes a rule; otherwise it's global.
- Inspect/remove: `rule_list`, `rule_remove(rule_id=…)`.
- Manual tags any time: `tracker_tag(action="add", key="GRIND-9", tag="needs-review")`.

A good default setup: `user-story` on stories, `acceptance` on children of
stories, plus domain tags (perf, security, docs) via regex rules.

## Sequencing the punch cards

When subtasks have a real order (schema before parser before UI), declare it:

```
tracker_deps(action="add", key="GRIND-5", depends_on="GRIND-4")
```

Then `tracker_deps(action="resolve", key="GRIND-2")` answers "what needs to
happen to finish this story" — ready tasks, blockers, and the parallelizable
order. Don't add edges for things the hierarchy already says (a parent is
automatically 'waiting on subtasks' while children are open).

## Checking the shape

`tracker_query(view="tree", project="GRIND")` shows the whole hierarchy;
`tracker_query(view="rollup", project="GRIND")` shows progress by kind.
