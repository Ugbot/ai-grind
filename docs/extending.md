# Extending devtools-mcp

There are two ways to add capability, and they are different jobs:

| You want to…                                              | Do this                        | Lives                     |
| --------------------------------------------------------- | ------------------------------ | ------------------------- |
| Add a **domain** (a new stateful area: tracker, recipes…) | Bolt on a `<domain>/` package  | In-tree, in this repo     |
| Ship a **plugin** (a suite/tools/page) separately         | Publish an entry-point package | Out-of-tree, its own repo |

Both are additive and both **degrade, never crash**: a broken or version-incompatible
plugin lands in a `_FAILED_*` map, surfaced by the `plugins` MCP tool
(`plugins(action="status")`), not on the server's death.

The two worked references throughout are the in-tree **`recipes`** domain and the
out-of-tree **`devtools-mcp-abap`** / **`devtools-mcp-conduct`** plugins.

---

## Part 1: bolt on a domain (in-tree)

A "domain" is a self-contained, SQLite-backed area of state with its own tools and
(optionally) its own console page. `tracker/` and `recipes/` are the templates. A
new domain `<domain>/` is six moving parts:

```
src/devtools_mcp/<domain>/
  __init__.py      # public API re-exports (open_<domain>, register_*, errors)
  db.py            # connection + pragmas + migration application
  schema.py        # versioned SQL migrations
  models.py        # dataclasses mirroring the tables (+ vocab constants)
  store.py         # domain logic: validate, upsert, record
  frames.py        # Polars frame builders (the bounded-query layer)
src/devtools_mcp/tools/<domain>_tools.py   # the action-multiplexed MCP tool
```

plus **one import line** in `tools/__init__.py` and **one workspace getter** in
`workspace.py`.

### 1a. The SQLite store conventions

Every domain store follows the same shape (see `recipes/db.py`):

- **One global DB file** under the shared data root: `data_root() / "<domain>.db"`.
  `data_root()` (`store/paths.py`) is `~/.devtools-mcp/`, overridable with
  `DEVTOOLS_MCP_DATA`. Give the store its **own** env override too, e.g.
  `DEVTOOLS_MCP_RECIPES_DB`, resolved *first* so tests (and power users) can point it
  at a temp file:

  ```python
  ENV_DB_PATH = "DEVTOOLS_MCP_<DOMAIN>_DB"

  def resolve_db_path() -> Path:
      override = os.environ.get(ENV_DB_PATH, "").strip()
      return Path(override) if override else data_root() / "<domain>.db"
  ```

- **WAL + foreign keys + busy timeout**, asserted at open:

  ```python
  self.conn = sqlite3.connect(str(path), isolation_level=None)  # autocommit; explicit txns
  self.conn.row_factory = sqlite3.Row
  self.conn.execute("PRAGMA journal_mode=WAL")     # concurrent readers + one writer
  self.conn.execute("PRAGMA foreign_keys=ON")      # ON DELETE CASCADE actually cascades
  self.conn.execute(f"PRAGMA busy_timeout={BUSY_TIMEOUT_MS}")
  ```

  WAL is what lets the dashboard read while a tool writes, and lets a background
  worker thread open its **own** connection to the same file (sqlite connections are
  thread-affine, so never hand one across threads; reopen from the path).

- **Explicit write transactions** via a `transaction()` context manager doing
  `BEGIN IMMEDIATE` / `COMMIT` / `ROLLBACK`, so concurrent server instances serialize
  writes safely.

- **Versioned migrations** in `schema.py`, applied in order inside one transaction
  each, tracked in a `schema_migrations(version, applied_at)` table. Add a table by
  appending a `(N, (…statements…))` tuple. Never edit an applied migration:

  ```python
  MIGRATIONS = ((1, MIGRATION_V1), (2, MIGRATION_V2))  # contiguous, starting at 1
  ```

  Foreign keys use `ON DELETE CASCADE` so deleting a parent row cleans up children
  (`recipe_runs` → `run_steps`).

- **Close is idempotent** and releases the WAL sidecar files.

### 1b. Frames, the bounded-query layer

Tools never dump raw rows. `frames.py` builds a typed Polars frame with a bound
(`FRAME_MAX_ROWS`), the tool slices it, and hands it to the shared formatters
(`format_dataframe`). See `recipes/frames.py`.

### 1c. The tool

`tools/<domain>_tools.py` is **action-multiplexed**: one `@mcp.tool()` with an
`action` parameter (`recipe(action="register|list|get|run|…")`), returning bounded
markdown. It pulls its DB from the app context getter and validates its inputs,
raising the domain's error type for expected failures (never asserting on user
input, because asserts are for programmer-error invariants):

```python
def _recipes(ctx: Context) -> RecipesDB:
    db = get_app_ctx(ctx).get_recipes()
    assert db is not None and db.conn is not None, "recipes db unavailable"
    return db
```

Input validation the recipes tool enforces (copy the pattern):

- keys are slugs (`KEY_RE`), kinds are slugs (`KIND_RE`), not free text;
- a recipe must have **at least one step**, and every step a non-empty command;
- `run` validates the key exists (the store's `get_recipe` raises `RecipesError`).

Wire it up:

```python
# tools/__init__.py
import devtools_mcp.tools.<domain>_tools  # noqa: F401
```

```python
# workspace.py — AppContext
def get_<domain>(self):
    if self.<domain> is None:
        from devtools_mcp.<domain> import open_<domain>
        self.<domain> = open_<domain>()
    return self.<domain>
```

and close it in `AppContext.cleanup_all()`.

### 1d. Durable external sequences: the DBOS `@workflow`/`@step` pattern

When a domain runs an **external, side-effecting sequence** (shell commands, network
calls) that must survive an interrupt, use DBOS Transact, the durable executor the
recipes runner is built on. The rule: **every external side effect lives inside a
`@DBOS.step`**, so it is checkpointed exactly once and NOT repeated when the workflow
replays completed steps on recovery.

```python
from dbos import DBOS, SetWorkflowID

@DBOS.step()                       # side effects (shell exec, DB writes) go here
def _exec_step_activity(db_path: str, run_id: int, ...) -> dict:
    rc, text = asyncio.run(run_capture(...))     # the external effect
    db = open_recipes(Path(db_path))             # reopen — a live conn can't cross
    try:                                         # the workflow arg boundary
        store.record_step(db, run_id, ...)
    finally:
        db.close()
    return {...}

@DBOS.workflow()                   # the orchestrator: pure control flow + step calls
def _recipe_workflow(db_path: str, key: str, run_id: int, ...) -> dict:
    ...
    for ordinal, step in enumerate(steps):
        outcome = _exec_step_activity(db_path, run_id, ordinal, ...)
    return {...}
```

Key gotchas, all learned the hard way (see `recipes/runner.py` + `recipes/dbos_app.py`):

- **DBOS is a process-global singleton.** `launch_dbos()` constructs + launches it once
  (idempotent), backed by its own SQLite *system database* (`dbos.db`, override
  `DEVTOOLS_MCP_DBOS_DB`), separate from your domain DB. The domain DB stays the
  human-facing model; the DBOS DB is the durability layer.
- **Reopen your own connection inside the workflow/step from a path**, never capture a
  live sqlite connection. The executor checkpoints workflow args for recovery, and a
  connection can't be serialized or cross a thread.
- **The workflow + steps are synchronous.** An async workflow binds to the event loop
  that launched DBOS and cannot be re-invoked from a later loop (fatal under
  pytest-asyncio's per-test loops). Keep the public API async and offload the sync
  workflow with `asyncio.to_thread` (`run_recipe`) or `DBOS.start_workflow` for a
  fire-and-forget console run (`start_background_run`).
- **Generate ids in the workflow, not the step** (a step may replay). The workflow id
  is deterministic (`_workflow_id`) so a crashed run resumes/forks under the same id.
- **Wrap the durable call** so a launch/step crash *finalizes the run failed* rather
  than leaving it stuck `running` and propagating. `run_recipe` and
  `start_background_run` both record a failed run (with the error captured to a raw
  log) on any DBOS error.

---

## Part 2: ship a plugin (out-of-tree)

A plugin is its own installable Python package that the host discovers through
entry-point groups, with no host edit and no fork. There are three groups, each mirroring
an in-tree loader in `registry.py` / `viz/pages.py`:

| Group                     | Adds                         | Loaded by            | When                              |
| ------------------------- | ---------------------------- | -------------------- | --------------------------------- |
| `devtools_mcp.backends`   | a tool **suite** (BackendSpec) | `load_backends()`    | at import, before `mcp` exists    |
| `devtools_mcp.mcp_tools`  | `@mcp.tool()` **tools**      | `load_tool_plugins()`| after `mcp` + in-tree tools exist |
| `devtools_mcp.viz_pages`  | a console **page** (VizPage) | `load_viz_pages()`   | when the viz server starts        |

The worked example is **`devtools-mcp-abap`** (`plugins/devtools-mcp-abap/`), which
ships a backend + a bundle of MCP tools; **`devtools-mcp-conduct`** is a second such
plugin (Conduct/SAP-specific tools) that follows the identical shape.

### 2a. Declare the entry points in `pyproject.toml`

```toml
[project]
name = "devtools-mcp-abap"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = ["httpx>=0.28.1"]
# Optional: declare the host version you need. The loader reads this Requires-Dist
# BEFORE importing you, so a too-new plugin is skipped-with-warning, never crashed.
# dependencies = ["httpx>=0.28.1", "devtools-mcp>=0.2"]

[project.entry-points."devtools_mcp.backends"]
abap = "devtools_mcp_abap.backend"          # module self-registers on import

[project.entry-points."devtools_mcp.mcp_tools"]
abap = "devtools_mcp_abap.tools_entry"      # module attaches tools on import

[project.entry-points."devtools_mcp.viz_pages"]
abap = "devtools_mcp_abap.page"             # module calls register_page on import

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/devtools_mcp_abap"]
```

Install it into the same environment as the host (`uv pip install -e .` /
`pip install devtools-mcp-abap`) and the host discovers it automatically.

### 2b. A backend (`devtools_mcp.backends`)

The entry point points at a module that calls `register_backend()` on import, exactly
like an in-tree backend. Backends load *before* the FastMCP `mcp` instance exists, so a
backend module must not touch `mcp`.

```python
# devtools_mcp_abap/backend.py
def register() -> None:
    try:
        from devtools_mcp.registry import BackendSpec, register_backend
    except ImportError:
        return  # host absent — nothing to register
    import contextlib
    with contextlib.suppress(AssertionError):        # idempotent re-registration
        register_backend(BackendSpec(
            suite="abap", tools=["adt"],
            detect=_detect, run=_run, df_builders={}, format_summary=_format_summary,
            description="SAP ABAP over ADT REST",
        ))

register()
```

### 2c. MCP tools (`devtools_mcp.mcp_tools`)

Tools attach `@mcp.tool()`s, so they load **after** `mcp` and the in-tree tools exist
(the host calls `load_tool_plugins()` at the very end of `server.py`). Convention: a
`tools_entry.py` exposing a `register()` that imports the host `mcp` and attaches each
domain's tools, each domain guarded so one broken module can't sink the rest:

```python
# devtools_mcp_abap/tools_entry.py
_DOMAINS = [("devtools_mcp_abap.tools", "_register_tools"),
            ("devtools_mcp_abap.nav", "register")]

def register() -> None:
    try:
        from devtools_mcp.server import mcp
    except ImportError:
        return
    import importlib
    for module_name, attr in _DOMAINS:
        try:
            getattr(importlib.import_module(module_name), attr)(mcp)
        except Exception:        # a domain not present/broken must not sink the rest
            continue

register()
```

### 2d. A console page (`devtools_mcp.viz_pages`)

This is the seam that used to be missing: a plugin can now add a **web-console tab**.
The entry point points at a module that calls `register_page(VizPage(...))` on import.
The in-tree reference is `viz/recipes_page.py`. The `/recipes` tab is itself a
registered page, proving the mechanism end-to-end.

A `VizPage` (from `devtools_mcp.viz.pages`) is a tab + its routes:

```python
# devtools_mcp_abap/page.py
from devtools_mcp.viz.pages import VizPage, VizResponse, register_page

def render_index() -> str:                       # GET /abap  (the tab)
    return "<h2>ABAP</h2>…"                       # any HTML string

def handle_get(rest, query):                     # GET /abap/<rest...>
    if rest == ["object"]:
        return f"<p>{query.get('name', ['?'])[0]}</p>"
    return None                                  # None -> 404

def handle_post(rest, body):                     # POST /abap/<rest...>
    if rest == ["sync"]:
        do_sync()
        return VizResponse.redirect("/abap")     # 303, POST-redirect-GET
    return VizResponse(body="<p>bad</p>", status=400)

register_page(VizPage(
    name="abap", prefix="abap", label="ABAP",
    render=render_index, get=handle_get, post=handle_post,
))
```

Contract:

- **`prefix`** is the single first path segment the page owns (`abap` → `/abap/*`). It
  may not shadow a built-in route (`tracker`, `run`, `api`, and so on); `register_page`
  asserts.
- A handler returns an HTML **`str`** (sent 200), a **`VizResponse`** (explicit
  status / `content_type` / `redirect`), or **`None`** (falls through to a 404).
- The tab appears in the nav on every page automatically, because `render.page()` appends
  `registered_tabs()` to the built-ins.
- POSTs still pass through the server's `_guard_state_change` (cross-origin / DNS-
  rebinding protection) before your handler runs, so you get the same CSRF guard the
  built-in POSTs get, for free.

### 2e. Version compatibility

A plugin may declare the host version it needs, so an incompatible plugin is
**skipped-with-warning** (into the `_FAILED_*` map) instead of importing and crashing.
Two ways, both honored by every loader:

1. **`Requires-Dist: devtools-mcp>=X`** in your `pyproject` dependencies. This is the
   **enforced, pre-import** gate: the loader reads your distribution metadata *before*
   importing your module, so a too-new plugin never runs a line of code.
2. **`__devtools_mcp_requires__ = ">=X"`** as a module attribute on the entry-point
   target. Honored best-effort *after* import as a convenience for plugins that don't
   pin via metadata.

On a mismatch the loader records `skipped: requires devtools-mcp>=X (host is Y)`.

### 2f. Observe what loaded with the `plugins` tool

Because loaders degrade silently, use the `plugins` MCP tool to see the surface:

```
plugins(action="list")     # inventory: backends, tool plugins, console pages, fail count
plugins(action="status")   # + a Health section: every failed/skipped entry + its error
```

The dashboard is the visual counterpart: a registered plugin's tab appears in
the nav.
```
