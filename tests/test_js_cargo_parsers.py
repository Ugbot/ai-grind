"""Tests for JS package-manager and Cargo parsers."""

from __future__ import annotations

import json

from devtools_mcp.build.analysis import deps_df, vulns_df
from devtools_mcp.build.jsdeps import (
    parse_npm_audit,
    parse_npm_ls,
    parse_npm_outdated,
    parse_pnpm_list,
    parse_yarn_audit,
    parse_yarn_list,
)
from devtools_mcp.build.models import BuildResult
from devtools_mcp.cargo.parsers import parse_cargo_audit, parse_cargo_build, parse_cargo_test, parse_cargo_tree


class TestNpmLs:
    TREE = json.dumps({
        "name": "app", "version": "1.0.0",
        "dependencies": {
            "express": {"version": "4.18.0", "dependencies": {
                "accepts": {"version": "1.3.8"},
                "body-parser": {"version": "1.20.0", "dependencies": {"bytes": {"version": "3.1.2"}}},
            }},
            "lodash": {"version": "4.17.0", "invalid": "true"},
        },
    })

    def test_subdependencies_depth(self):
        deps = parse_npm_ls(self.TREE)
        by = {d.artifact: d for d in deps}
        assert by["express"].depth == 1
        assert by["accepts"].depth == 2
        assert by["bytes"].depth == 3  # transitive subdependency

    def test_versions_and_invalid(self):
        by = {d.artifact: d for d in parse_npm_ls(self.TREE)}
        assert by["express"].version == "4.18.0"
        assert by["lodash"].conflict  # marked invalid

    def test_deps_df(self):
        df = deps_df(BuildResult(run_id="r", tool="deps", binary="x", dependencies=parse_npm_ls(self.TREE)))
        assert "function" in df.columns
        assert df.height == 5
        assert df["depth"].max() == 3

    def test_bad_json(self):
        assert parse_npm_ls("nope") == []


class TestPnpmList:
    OUT = json.dumps([{
        "name": "app", "version": "1.0.0",
        "dependencies": {"react": {"version": "18.2.0", "dependencies": {"loose-envify": {"version": "1.4.0"}}}},
        "devDependencies": {"vitest": {"version": "1.0.0"}},
    }])

    def test_prod_and_dev_scopes(self):
        deps = parse_pnpm_list(self.OUT)
        by = {d.artifact: d for d in deps}
        assert by["react"].scope == "prod"
        assert by["vitest"].scope == "dev"
        assert by["loose-envify"].depth == 2


class TestYarnList:
    NDJSON = (
        '{"type":"info","data":"x"}\n'
        '{"type":"tree","data":{"type":"list","trees":['
        '{"name":"express@4.18.0","children":[{"name":"accepts@1.3.8","children":[]}]},'
        '{"name":"@scope/pkg@2.0.0","children":[]}'
        ']}}\n'
    )

    def test_parses_tree(self):
        deps = parse_yarn_list(self.NDJSON)
        by = {d.artifact: d for d in deps}
        assert by["express"].version == "4.18.0"
        assert by["accepts"].depth == 2

    def test_scoped_package(self):
        by = {d.artifact: d for d in parse_yarn_list(self.NDJSON)}
        assert "@scope/pkg" in by
        assert by["@scope/pkg"].version == "2.0.0"


class TestAudit:
    NPM_V7 = json.dumps({"vulnerabilities": {
        "lodash": {"name": "lodash", "severity": "high", "range": "<4.17.21",
                   "via": [{"title": "Prototype Pollution", "url": "https://x", "severity": "high"}],
                   "fixAvailable": True},
    }})
    NPM_V6 = json.dumps({"advisories": {
        "1065": {"module_name": "minimist", "severity": "moderate", "title": "ReDoS",
                 "url": "https://y", "vulnerable_versions": "<1.2.6", "patched_versions": ">=1.2.6"},
    }})
    YARN = '{"type":"auditAdvisory","data":{"advisory":{"module_name":"axios","severity":"critical","title":"SSRF","url":"https://z","vulnerable_versions":"<0.21.1","patched_versions":">=0.21.1"}}}\n'

    def test_npm_v7(self):
        v = parse_npm_audit(self.NPM_V7)
        assert v[0].name == "lodash" and v[0].severity == "high" and v[0].fix_available

    def test_npm_v6_advisories(self):
        v = parse_npm_audit(self.NPM_V6)
        assert v[0].name == "minimist" and v[0].severity == "moderate"

    def test_yarn_ndjson(self):
        v = parse_yarn_audit(self.YARN)
        assert v[0].name == "axios" and v[0].severity == "critical"

    def test_vulns_df_sorted_by_severity(self):
        v = parse_npm_audit(self.NPM_V7) + parse_npm_audit(self.NPM_V6)
        df = vulns_df(BuildResult(run_id="r", tool="audit", binary="x", vulnerabilities=v))
        assert df["severity"][0] == "high"  # ranked above moderate
        assert "kind" in df.columns


class TestOutdated:
    OUT = json.dumps({"lodash": {"current": "4.17.0", "wanted": "4.17.21", "latest": "4.17.21"}})

    def test_current_vs_latest(self):
        deps = parse_npm_outdated(self.OUT)
        assert deps[0].version == "4.17.0"
        assert deps[0].resolved == "4.17.21"
        assert deps[0].conflict


class TestCargo:
    TREE = """\
myapp v0.1.0 (/home/u/myapp)
├── rand v0.8.5
│   ├── rand_core v0.6.4
│   └── rand_chacha v0.3.1
└── serde v1.0.193
    └── serde_derive v1.0.193
"""

    def test_tree_depth(self):
        deps = parse_cargo_tree(self.TREE)
        by = {d.artifact: d for d in deps}
        assert by["myapp"].depth == 0
        assert by["rand"].depth == 1
        assert by["rand_core"].depth == 2
        assert by["serde_derive"].depth == 2

    def test_tree_versions(self):
        by = {d.artifact: d for d in parse_cargo_tree(self.TREE)}
        assert by["serde"].version == "1.0.193"

    TEST_OUT = """\
running 3 tests
test tests::it_adds ... ok
test tests::it_subtracts ... FAILED
test tests::ignored_one ... ignored

test result: FAILED. 1 passed; 1 failed; 1 ignored
"""

    def test_test_parsing(self):
        cases, success = parse_cargo_test(self.TEST_OUT)
        by = {c.name: c.status for c in cases}
        assert by["tests::it_adds"] == "passed"
        assert by["tests::it_subtracts"] == "failed"
        assert by["tests::ignored_one"] == "skipped"
        assert not success

    def test_build_errors(self):
        text = "   Compiling app\nerror[E0382]: borrow of moved value\nerror: could not compile `app`\n"
        success, failures = parse_cargo_build(text)
        assert not success
        assert any("borrow of moved value" in f for f in failures)

    def test_audit_json(self):
        data = json.dumps({"vulnerabilities": {"list": [
            {"advisory": {"id": "RUSTSEC-2021-0001", "title": "X", "url": "https://r", "severity": "high"},
             "package": {"name": "time", "version": "0.1.0"}, "versions": {"patched": [">=0.2.23"]}},
        ]}})
        v = parse_cargo_audit(data)
        assert v[0].name == "time" and v[0].fix_available
