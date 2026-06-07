"""Tests for Maven/Gradle parsers + shared JUnit parsing and analysis."""

from __future__ import annotations

from devtools_mcp.build.analysis import available_tasks_df, deps_df
from devtools_mcp.build.analysis import tests_df as build_tests_df
from devtools_mcp.build.models import BuildResult
from devtools_mcp.build.parsers import parse_junit_text
from devtools_mcp.gradle.parsers import parse_gradle_build, parse_gradle_deps, parse_gradle_tasks
from devtools_mcp.maven.parsers import parse_maven_build, parse_maven_resolve, parse_maven_tree


class TestMavenTree:
    SAMPLE = """\
[INFO] com.example:app:jar:1.0.0
[INFO] +- org.springframework:spring-core:jar:5.3.0:compile
[INFO] |  \\- org.springframework:spring-jcl:jar:5.3.0:compile
[INFO] +- com.google.guava:guava:jar:30.0-jre:compile (omitted for conflict with 31.0-jre)
[INFO] \\- junit:junit:jar:4.13:test
[INFO]    \\- org.hamcrest:hamcrest-core:jar:1.3:test
"""

    def test_parses_tree_with_depth(self):
        deps = parse_maven_tree(self.SAMPLE)
        by = {d.artifact: d for d in deps}
        assert by["app"].depth == 0
        assert by["spring-core"].depth == 1
        assert by["spring-jcl"].depth == 2
        assert by["hamcrest-core"].depth == 2

    def test_scope_and_coords(self):
        deps = {d.artifact: d for d in parse_maven_tree(self.SAMPLE)}
        assert deps["junit"].scope == "test"
        assert deps["spring-core"].group == "org.springframework"
        assert deps["spring-core"].version == "5.3.0"

    def test_conflict_resolution(self):
        guava = next(d for d in parse_maven_tree(self.SAMPLE) if d.artifact == "guava")
        assert guava.conflict
        assert guava.resolved == "31.0-jre"

    def test_deps_df_aliases(self):
        df = deps_df(BuildResult(run_id="r", tool="tree", binary="x", dependencies=parse_maven_tree(self.SAMPLE)))
        assert "function" in df.columns
        assert df.filter(df["conflict"]).height == 1


class TestMavenBuild:
    SAMPLE = """\
[INFO] Reactor Summary for app 1.0:
[INFO]
[INFO] app-core ........................................... SUCCESS [  2.345 s]
[INFO] app-web ............................................ FAILURE [  0.123 s]
[INFO] app-cli ............................................ SKIPPED
[INFO] ------------------------------------------------------------------------
[INFO] BUILD FAILURE
[ERROR] Failed to execute goal on project app-web: compilation failure
[ERROR] To see the full stack trace, re-run with -e
"""

    def test_modules_and_status(self):
        success, modules, failures = parse_maven_build(self.SAMPLE)
        assert not success
        assert len(modules) == 3
        assert {m.status for m in modules} == {"SUCCESS", "FAILURE", "SKIPPED"}

    def test_failures_filtered(self):
        _s, _m, failures = parse_maven_build(self.SAMPLE)
        assert any("compilation failure" in f for f in failures)
        assert not any("To see the full" in f for f in failures)  # noise dropped

    def test_resolve_flat(self):
        text = "[INFO]    org.slf4j:slf4j-api:jar:1.7.30:compile\n[INFO]    junit:junit:jar:4.13:test\n"
        deps = parse_maven_resolve(text)
        assert len(deps) == 2
        assert all(d.depth == 1 for d in deps)


class TestGradleDeps:
    SAMPLE = """\
compileClasspath - Compile classpath for source set 'main'.
+--- org.springframework:spring-core:5.3.0
|    \\--- org.springframework:spring-jcl:5.3.0
+--- com.google.guava:guava:30.0-jre -> 31.0-jre
\\--- project :shared

testRuntimeClasspath - Runtime classpath of source set 'test'.
\\--- junit:junit:4.13 (*)

(*) - dependencies omitted (listed previously)
"""

    def test_depth_and_scope(self):
        deps = parse_gradle_deps(self.SAMPLE)
        core = next(d for d in deps if d.artifact == "spring-core")
        jcl = next(d for d in deps if d.artifact == "spring-jcl")
        assert core.depth == 1 and jcl.depth == 2
        assert core.scope == "compileClasspath"

    def test_conflict_arrow(self):
        guava = next(d for d in parse_gradle_deps(self.SAMPLE) if d.artifact == "guava")
        assert guava.conflict
        assert guava.requested == "30.0-jre"
        assert guava.resolved == "31.0-jre"

    def test_project_and_omitted(self):
        deps = parse_gradle_deps(self.SAMPLE)
        assert any(d.artifact.startswith("project ") for d in deps)
        junit = next(d for d in deps if d.artifact == "junit")
        assert junit.omitted


class TestGradleBuildAndTasks:
    BUILD = """\
> Task :compileJava
> Task :processResources NO-SOURCE
> Task :classes UP-TO-DATE
> Task :test FAILED

BUILD FAILED in 3s

* What went wrong:
Execution failed for task ':test'.
> There were failing tests. See the report.

* Try:
> Run with --stacktrace
"""

    def test_task_outcomes(self):
        success, tasks, failures = parse_gradle_build(self.BUILD)
        assert not success
        outcomes = {t.name: t.outcome for t in tasks}
        assert outcomes[":compileJava"] == "EXECUTED"
        assert outcomes[":classes"] == "UP-TO-DATE"
        assert outcomes[":test"] == "FAILED"

    def test_what_went_wrong(self):
        _s, _t, failures = parse_gradle_build(self.BUILD)
        assert any("Execution failed for task" in f for f in failures)
        assert not any(f.startswith("* Try") for f in failures)

    TASKS = """\
Build tasks
-----------
assemble - Assembles the outputs of this project.
build - Assembles and tests this project.

Help tasks
----------
help - Displays a help message.
"""

    def test_tasks_listing(self):
        tasks = parse_gradle_tasks(self.TASKS)
        names = {t.name: t for t in tasks}
        assert names["build"].group == "Build tasks"
        assert "Assembles and tests" in names["build"].description
        assert names["help"].group == "Help tasks"

    def test_available_tasks_df(self):
        df = available_tasks_df(BuildResult(run_id="r", tool="tasks", binary="x",
                                            available_tasks=parse_gradle_tasks(self.TASKS)))
        assert "function" in df.columns and df.height == 3


class TestJUnit:
    XML = b"""<?xml version="1.0"?>
<testsuite name="com.example.MyTest" tests="3" failures="1" errors="0" skipped="1">
  <testcase classname="com.example.MyTest" name="testOk" time="0.012"/>
  <testcase classname="com.example.MyTest" name="testBad" time="0.003">
    <failure message="expected true">AssertionError</failure>
  </testcase>
  <testcase classname="com.example.MyTest" name="testSkip" time="0">
    <skipped/>
  </testcase>
</testsuite>"""

    def test_parses_statuses(self):
        cases = parse_junit_text(self.XML)
        by = {c.name: c for c in cases}
        assert by["testOk"].status == "passed"
        assert by["testBad"].status == "failed"
        assert by["testBad"].message == "expected true"
        assert by["testSkip"].status == "skipped"

    def test_tests_df(self):
        df = build_tests_df(BuildResult(run_id="r", tool="test", binary="x", tests=parse_junit_text(self.XML)))
        assert "kind" in df.columns
        assert df.filter(df["kind"] == "failed").height == 1

    def test_bad_xml(self):
        assert parse_junit_text(b"not xml") == []
