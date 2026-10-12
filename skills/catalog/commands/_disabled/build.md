Build the LLM Station project.

Run the build and report results (replace `build` with your configured build
directory if different — e.g. `build/windows-ninja` on Windows/Ninja):

```
cmake --build build --target llm-station
```

- If the build succeeds: report how many files were compiled and confirm the binary exists.
- If the build fails: show the full error output and diagnose the root cause.

After a successful build, optionally run the test suite (portable, any generator):
```
ctest --test-dir build --output-on-failure -R "test_dynamic_tools|test_permissions_hooks|test_mode_system|test_config_resolver|test_async_runtime"
```

NEVER recreate the build directory. Re-run `cmake` to configure only when
CMakeLists.txt structurally changed (new source files added).
