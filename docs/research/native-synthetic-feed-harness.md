# Local native synthetic-feed callable

`devtools_market_feed` controls the native `chukonu_market_feed` executable.
Python launches processes and retains bounded telemetry; generation, pacing,
book reconstruction and measured counters run in native code. This is a local
developer harness, explicitly synthetic/unpersisted/native-local. It is not an
ordinary Gestalt job, a production scheduler, a live venue handler or durable
source/output publication.

Set `DEVTOOLS_MARKET_FEED_BINARY` to the exact absolute executable path before
the first callable use. The path and server budgets resolve once for the manager. Requests cannot
choose another executable, shell, environment or arbitrary CLI arguments.
Processes launch with `asyncio.create_subprocess_exec` and managed stdin.

```json
{"action":"start","config":{"symbols":48,"depth":8,"seed":17,"rate":1000,"batch":256,"burst":256,"seconds":60,"memory_mib":256}}
```

The result returns opaque `run_id` and `session_id`. Subsequent calls:

```json
{"action":"control","run_id":"<returned32hexID>","command":"ramp 100000 5000"}
{"action":"control","run_id":"<returned32hexID>","command":"book 3"}
{"action":"status","run_id":"<returned32hexID>"}
{"action":"stop","run_id":"<returned32hexID>"}
{"action":"list"}
```

Controls are `rate N` (0 pauses), `ramp TARGET MS` (positive target), `burst N`, `pause`, `resume`,
`unlimited`, `book ID`, `status`, `stop`. Rate means **messages per second**,
not exchange-event rate or achieved throughput. Unlimited removes pacing, not
memory/lifetime caps. Status reads cached native counters; control `status`
requests a native refresh. Native command receipts echo the entire command
and a monotonically increasing revision, including refusals. Acknowledgement
precedes refreshed status/book output: poll the cached payload until its revision
is at least the command receipt revision before treating it as fresh. A command timeout
means it may have applied: retain its exact pending identity until receipt or
process exit; new controls refuse rather than blindly resend it.

| Input | Harness bound | Default |
|---|---|---|
| symbols | uint32 positive, active IDs 1..count; native memory admission | 16 |
| depth | 1..(UINT32MAX-1)/2; native memory admission | 8 |
| seed | full uint64 | 1 |
| rate | 0..INT64MAX messages/s | 1000 |
| batch | 1..UINT32MAX messages; native memory admission | 256 |
| burst | 1..UINT32MAX messages | 256 |
| seconds | 1..server max_seconds, always finite | 60 |
| max_messages | 0..INT64MAX;0 uses the finite seconds cap | 0 |
| memory_mib | 1..server max_memory_mib, native feed arena budget | 256 |
| unlimited | boolean | false |

`DEVTOOLS_MARKET_FEED_MAX_MEMORY_MIB` defaults to 512 and can be explicitly
set up to the native SIZE_MAX/MiB representation limit. `DEVTOOLS_MARKET_FEED_MAX_SECONDS` defaults to 3600 and can be
set to 1..86400 (native CLI time representation). Effective server limits appear
in run/list responses. Inputs refuse above them rather than silently clamping.
For example 100000 symbols/full-u64 seeds reach native combined-memory admission;
there is no unrelated 4096-symbol cap. Ramp duration is bounded by the named
server lifetime ceiling; use pause/rate 0 instead of unsupported ramp-to-zero.
Native preparation also validates event-complete finite
horizons and actual storage requirements; accepted Python input is not a
promise that any combination fits. Family/contract definitions remain synthetic,
not an exchange security master or calibrated prices/options.

At most four producers run concurrently. Sixteen run summaries are retained,
evicting only exited runs. Per run retain the latest status/book/final, feed
definition, eight small command receipts and the last 4096 stderr bytes. NDJSON
lines are capped at 65536 bytes and must use schema_version 1 and recognized
record types. Excess old command/definition diagnostics are dropped with a
counter, not mistaken for data loss. Repeated malformed output, oversized lines,
wrong command identity/revision or native fatal errors immediately trigger
bounded terminate/kill. Rejected stdout drains in fixed chunks during teardown
so a full pipe cannot prevent reaping. Cancellation during creation keeps an
owned shielded creation task and reaps any child it establishes.
No per-event IPC is used. Native target/effective/achieved counters are exposed
as received; the wrapper does not calculate replacement market statistics.

Startup waits at most 5s for initial native status after process creation. Controls wait 2s; stop/EOF asks graceful
shutdown, then bounded terminate/kill. A watchdog also enforces the finite run
ceiling with startup allowance. The established MCP lifespan shields bounded producer cleanup from server
task-group cancellation and stops owned producers before context teardown. Final status distinguishes finished from
failed/forced shutdown and includes the real exit code. None is a durable ACK.

Sources: [configuration](../../src/devtools_mcp/market_feed/config.py),
[manager](../../src/devtools_mcp/market_feed/manager.py),
[callable](../../src/devtools_mcp/tools/market_feed_tools.py),
[fault/lifecycle tests](../../tests/test_market_feed.py),
[opt-in native acceptance](../../tests/test_market_feed_native.py).

On 2026-10-14, 18 tests passed in 6.54s (16 fake-process/fault cases and two
real-native cases). Ruff check/format (eight files) and whitespace checks passed.
The native acceptance uses the registered MCP callable over the SDK's in-memory
client/server transport and established server lifespan. Unrelated dashboard,
profiler discovery and DBOS startup are disabled in that test fixture. It covers:

- 48 symbols, full-u64 seed, paused startup, rate/ramp/unlimited controls,
  revision-bound status/book responses and graceful stop.
- 100,000 paused symbols admitted with a 512 MiB feed arena budget, including
  a selected symbol's book response; 100,000 symbols at depth 64 refused with
  a 1 MiB budget. A uint32-max batch also reaches native preparation and is
  refused under a 512 MiB budget.
- Fake-process faults cover bounded malformed/flooded output, immediate kill of
  TERM-ignoring producers, command uncertainty, process-creation cancellation,
  active/retained caps, and both normal and cancelled MCP lifespan cleanup.

Repeat after building the native executable (the tests never build it):

```sh
DEVTOOLS_MARKET_FEED_BINARY=/Users/bengamble/Gestalt2/build/release/extern/chukonu/src/market/chukonu_market_feed \
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest \
  tests/test_market_feed.py tests/test_market_feed_native.py -q -p no:cacheprovider
```

Without the binary environment variable, native acceptance skips explicitly.
These are correctness and lifecycle checks, not throughput measurements or
proof of persistent platform job submission.
