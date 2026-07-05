---
name: llm-station-patterns
description: Search for and instantiate design patterns and reusable code templates. Includes Gang of Four patterns (Observer, Strategy, Factory, etc.) and previously crystallized action templates. Always check before writing new code.
user-invocable: false
---

# LLM Station Pattern Library & Code Reuse

**Always check for existing implementations before writing new code.** LLM Station searches across three sources:
1. Existing code already in the workspace
2. Previously successful action templates
3. Built-in Gang of Four design patterns

## Search for patterns and existing implementations
```bash
!`echo $LLMSTATION_BIN` run crystal_search query="$ARGUMENTS"
```
This returns matches ranked by relevance, showing whether it's existing code, a template, or a design pattern.

## Execute a pattern template
```bash
!`echo $LLMSTATION_BIN` run crystal_execute template_id=ID params='{"class_name":"Foo","namespace":"Bar"}'
```

## Dry run (preview without executing)
```bash
!`echo $LLMSTATION_BIN` run crystal_execute template_id=ID params='{"class_name":"Foo"}' dry_run=true
```

## List available templates
```bash
!`echo $LLMSTATION_BIN` run crystal_manage action=list
```

## Check system status
```bash
!`echo $LLMSTATION_BIN` run crystal_manage action=status
```

## Available built-in patterns
- **observer_pattern** — Subject/listener with typed events
- **strategy_pattern** — Swappable algorithms via interface
- **factory_pattern** — Object creation with type registration
- **builder_pattern** — Step-by-step construction with fluent API
- **singleton_pattern** — Thread-safe lazy initialization
- **command_pattern** — Encapsulated requests with undo/redo
- **adapter_pattern** — Interface conversion wrapper
- **decorator_pattern** — Dynamic behavior extension
- **repository_pattern** — Abstract data access (CRUD)
- **service_pattern** — Business logic with dependency injection

**When to use:** Before writing any new class or pattern. If you need an observer, factory, repository, etc., check here first. The pattern will be instantiated with your specific class names, namespaces, and parameters.
