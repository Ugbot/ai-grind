Build the LLM Station project using ninja.

Run the build and report results:

```
cd /Users/bengamble/llm-station/build-ned && ninja llm-station-mcp 2>&1
```

- If the build succeeds: report how many files were compiled and confirm the binary exists.
- If the build fails: show the full error output and diagnose the root cause.

After a successful build, optionally run the test suite:
```
cd /Users/bengamble/llm-station/build-ned && ninja test_dynamic_tools test_permissions_hooks test_mode_system test_config_resolver test_async_runtime 2>&1 | tail -5
```

Then run the tests:
```
for t in test_async_runtime test_dynamic_tools test_permissions_hooks test_mode_system test_config_resolver; do
  echo "=== $t ===" && /Users/bengamble/llm-station/build-ned/tests/$t --gtest_brief=1 2>&1 | tail -3
done
```

NEVER run `cmake` unless CMakeLists.txt has structurally changed (new source files added).
NEVER recreate the build directory.
Always use ninja, not make.
