Rename a symbol across the llm-station codebase using the daemon's AST-aware rename tool.

Arguments format: `old_name new_name` (e.g. `ned-rename FooBar BazBar`)

Parse the arguments: first word is old_name, second word is new_name.
Old name: (first word of $ARGUMENTS)
New name: (second word of $ARGUMENTS)

Step 1 — Dry run to see what would change:
```bash
timeout 30 llm-station --run rename_symbol "old_name=<OLD>" "new_name=<NEW>" "dry_run=true" 2>&1
```

Show the full diff preview and ask me to confirm before proceeding.

Step 2 — Only if I confirm, apply the rename:
```bash
timeout 30 llm-station --run rename_symbol "old_name=<OLD>" "new_name=<NEW>" "dry_run=false" 2>&1
```

After applying: rebuild with `cd build && ninja llm-station 2>&1 | grep -E "error:|warning:" | head -20` to verify no breakage.

Note: Quote each `key=value` param as a single shell argument.
