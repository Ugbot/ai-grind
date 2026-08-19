---
name: build-tools
description: >
  Drive build systems and package managers, Maven, Gradle, npm, pnpm, yarn, and
  Cargo, through the devtools-mcp backends without drowning in output. Use to
  build/test a project, inspect its dependency tree and SUBDEPENDENCIES (versions,
  depth, conflicts), synchronize/resolve/install deps, run a security audit, find
  outdated packages, or list runnable tasks/scripts. One verb vocabulary across
  every language; output is a bounded summary + a queryable Polars frame.
---

# Build systems & package managers (devtools-mcp)

Six backends, `maven`, `gradle`, `npm`, `pnpm`, `yarn`, and `cargo`, share **one
verb vocabulary**, so the same `tool` means the same thing everywhere; only the
backend implementation differs. `binary` = the **project directory** (with the
`pom.xml` / `build.gradle` / `package.json` / `Cargo.toml`). Maven/Gradle prefer a
`mvnw`/`gradlew` wrapper, else the global tool.

## The shared verbs

| tool | meaning | maven | gradle | npm/pnpm/yarn | cargo |
|---|---|---|---|---|---|
| `deps` | dependency tree + **subdependencies** | `dependency:tree` | `dependencies` | `ls`/`list --json` | `tree` |
| `sync` | resolve / install / refresh / fetch | `dependency:resolve` | `--refresh-dependencies` | `install` | `fetch` |
| `build` | compile / package | `package` | `build` | `run build` | `build` |
| `test` | run tests (→ JUnit/libtest) | `test` | `test` | `test` | `test` |
| `audit` | security advisories | OSV.dev over dep tree¹ | OSV.dev over dep tree¹ | `audit --json` | `audit` |
| `outdated` | newer versions available | `versions:…updates`² | `dependencyUpdates`³ | `outdated` | `outdated`⁴ |
| `tasks` | runnable goals/tasks/scripts | n/a | `tasks --all` | package.json scripts | n/a |
| `check` | verify without full package | `verify` | `check` | n/a | `check` |

**JVM extras** (maven + gradle only):

| tool | meaning | maven | gradle |
|---|---|---|---|
| `insight` | why is X here / who wins the version, needs `args=["<artifact>"]` | `dependency:tree -Dincludes=X` | `dependencyInsight --dependency X`, multi-module: `args=["X", ":module"]` |
| `projects` | module / reactor listing → modules frame | reactor build order | `projects` |

Gradle `deps`/`sync`/`audit` run a devtools-registered all-projects report task
(via `--init-script`), so a multi-module root yields the **full tree of every
module**. Plain `gradle dependencies` at the root would be empty.

¹ audit parses the dep tree, then batch-queries OSV.dev, so no plugin injection,
works under Gradle dependency verification; needs network at audit time.
² fully-qualified `versions-maven-plugin` goal, which runs against any pom untouched.
³ ben-manes versions plugin injected via a devtools-shipped `--init-script`,
project files untouched; first run downloads the plugin (needs network).
⁴ needs `cargo install cargo-outdated`; yarn `outdated` is classic-only (berry
lacks the command).

```
devtools_run(suite="npm",   tool="deps", binary="C:/code/app")          # JS dep tree
devtools_run(suite="cargo", tool="deps", binary="C:/code/crate")        # Rust dep tree
devtools_run(suite="maven", tool="deps", binary="C:/code/svc")          # Java dep tree
```

## Seeing subdependencies

`deps` returns the **full transitive tree** as a frame with
`group, artifact, version, requested, resolved, scope, depth, conflict, omitted`
(`function` = the package coord). `depth == 1` is direct; deeper rows are
transitive subdependencies.

```
devtools_run(suite="npm", tool="deps", binary="C:/code/app")   # e.g. 2140 nodes, depth 11
devtools_analyze(run_id="...", function_pattern="react")       # everything pulling react
devtools_analyze(run_id="...", sort_by="depth")                # deepest transitive deps
devtools_analyze(run_id="...")                                  # then filter conflict == true
```

The summary already reports node/distinct/direct counts, max depth, and version
conflicts (`requested → resolved`).

## Common jobs

- **"What's pulling in package X / why this version?"** → `deps`, filter to X;
  `requested` vs `resolved` shows who won (Maven `(omitted for conflict with …)`,
  Gradle/npm `requested → resolved`). On JVM projects, `insight` answers it
  directly: `devtools_run(suite="gradle", tool="insight", args=["guava"], ...)`.
- **"Re-sync / install dependencies"** → `sync`.
- **"Any vulnerabilities?"** → `audit` (every backend) → severity-ranked
  frame; `devtools_analyze(run_id, kind_pattern="critical|high")`.
- **"What can I run?"** → `tasks` (Gradle tasks / npm scripts). Any Gradle task
  or Maven goal runs via the passthrough: `tool="build", args=[":module:anyTask"]`.
- **Build/test failures** → `build`/`test`; the summary carries the error lines /
  failing `class.method`, and the full log is on disk (`devtools_raw` or the
  [[devtools-visualizer]] terminal).

## Notes

- `audit`/`outdated` exit non-zero "by design" when they find something, and the
  backends treat them as informational, not failures.
- Multi-module / workspaces: pass `-pl :module` (Maven), `:module:task` (Gradle),
  or workspace flags via `extra_args`.
- Per-tool detail + accumulated project quirks live in the **live skills**
  `gradle-commands`, `maven-commands`, `cargo-commands`, `npm-commands`,
  `pnpm-commands`, `yarn-commands`. Keep their verb tables in sync when verbs
  change here.
- Overall workflow + the no-token-flood principle: [[devtools-mcp-usage]].
