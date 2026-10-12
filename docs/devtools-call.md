# MCP batch client

`devtools_mcp.client.call_batch` is a maintained async callable using the official
MCP SDK. `devtools-call` is its CLI. Before reinstalling console scripts, run
`.venv/bin/python -m devtools_mcp.client` from this checkout.

The default transport is streamable HTTP. URL precedence is `--url`, then
`DEVTOOLS_MCP_URL`, then `http://127.0.0.1:8010/mcp` (the configured shared local
service). The client never spawns a server in HTTP mode, probes another port,
or automatically switches to stdio. Start/manage the shared service separately.

A request file is a JSON array of objects with exactly `tool` and `arguments`:

```json
[{"tool":"tracker_project","arguments":{"action":"list"}}]
```

```sh
.venv/bin/python -m devtools_mcp.client requests.json results.json \
  --call-timeout 35 --batch-timeout 55
# Optional bounded tool-schema artifact:
devtools-call requests.json results.json --inventory schemas.json
# Explicit server launch using argv, with no shell parsing:
devtools-call requests.json results.json \
  --stdio-argv '["devtools-mcp","--transport","stdio","--no-dashboard"]'
```

```python
from pathlib import Path
from devtools_mcp.client import ClientConfig, ClientLimits, ToolRequest, call_batch

receipt = await call_batch(
    [ToolRequest("tracker_project", {"action": "list"})],
    Path("results.json"),
    config=ClientConfig(limits=ClientLimits(call_timeout=35, batch_timeout=55)),
)
```

Invalid inputs are rejected before connecting. The defaults and maximum supported
bounds are 128 calls, 1 MiB encoded request input, 8 MiB per result/inventory,
32 MiB receipts, 16 inventory pages, 2048 inventoried tools, JSON argument depth
32, and 65536 argument nodes. Timeouts must be finite, positive, and at most
86400 seconds. Callable limits may lower these caps. The batch timeout covers
connection, initialization, inventory, calls, and transport cleanup; per-call
timeouts cover initialization, inventory pages, and tool calls. Synchronous JSON
serialization and file fsync are not preemptible by asyncio timeouts.

These are validation/encoded-output bounds, not OS hard memory limits. The SDK
receives and parses responses before result bounds can be checked; serializers
and atomic publication also allocate memory. The client uses the public typed
`ClientSession.send_request` boundary for `tools/call`, preserving SDK MCP
protocol parsing and protocol errors. It deliberately omits per-tool
`outputSchema` validation and automatic schema lookup: the SDK's higher-level
`call_tool` can receive a mutation response, then discard it while performing
schema lookup/validation. The bridge transports that response as evidence;
callers decide domain/schema acceptance. Receipts explicitly record
`output_schema_validation: "not_performed"`. Schemas are fetched only when an
inventory artifact is explicitly requested, before the batch's tool calls.

Receipts contain schema version, batch status, and ordered call records with
supplied arguments and the complete SDK JSON result, including `content`,
`structuredContent`, metadata/extensions, and `isError`. The client publishes an
initial receipt, an `in_flight`/`outcome: unknown` record before sending each call,
and full completed results immediately afterward, using the shared atomic JSON
writer (same-directory temporary file, flush/fsync, atomic replacement, POSIX
parent-directory fsync). An `in_flight` receipt can survive a hard process kill.
On timeout, interruption, or transport failure, earlier completed results remain;
the unfinished call is marked failed/interrupted with outcome unknown. Publication
failure raises an error; the last successfully published receipt remains, except
that directory fsync/verification failure after replacement can leave the newer
receipt present. Windows lacks the POSIX parent-directory durability guarantee.

Calls stop on `isError`. The CLI returns nonzero for MCP errors, timeouts,
transport problems, input/result/inventory bounds, and receipt failures. Domain
failure text or structured fields may accompany `isError: false`; these are
preserved and **callers must inspect them**. A successful batch means the MCP
calls completed without protocol errors or `isError`, not that every domain
operation succeeded or its declared output schema was satisfied. An oversized result is rejected without truncation; its full payload
is not persisted, that call stays outcome unknown, and the batch fails. Receipt
capacity is checked with reserved failure space before accepting a completed
result, preserving previous results if later output exceeds the cap.

The callable raises `BatchError` with `kind` and in-memory `receipt` on execution
failure; invalid input raises `ValueError`, and cancellation propagates
`asyncio.CancelledError` after attempting a partial receipt. No retries, reconnect
replays, or automatic resumptions occur. An unknown outcome may have produced
side effects; reconcile it before deciding whether to run it again. Treat a
receipt with running/failed/interrupted status as incomplete, even when earlier
calls completed. Existing result files are rejected before connecting: a new batch claims its
receipt path without clobbering earlier evidence. Use a new path for each attempt.
Only a receipt created by the current batch is subsequently replaced. Optional
inventory output replaces its explicitly selected artifact path.

Only bounded summaries appear on stdout, and exception messages, environment,
endpoint URLs, and stdio argv are excluded from receipts. Stdio server stderr is
suppressed; manage server diagnostics separately. Request arguments, results,
and optional schemas are explicitly supplied/returned data and may contain
secrets; store these artifacts with appropriate access controls. Parent output
directories must already exist. This client is a tool-call bridge, not a Python
script registry or process supervisor.
