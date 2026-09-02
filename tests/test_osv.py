"""Tests for the OSV.dev client, canned responses via the injectable fetch, no network."""

from __future__ import annotations

import json
import urllib.error

from devtools_mcp.build.analysis import vulns_df
from devtools_mcp.build.models import BuildResult, Dependency
from devtools_mcp.build.osv import DETAIL_CAP, bucket_severity, dedupe_packages, query_osv

GUAVA = Dependency(group="com.google.guava", artifact="guava", version="19.0", resolved="19.0", depth=1)
JUNIT = Dependency(group="junit", artifact="junit", version="4.11", resolved="4.11", depth=2)


def make_fetch(batch_response: dict, details: dict[str, dict]):
    """A canned fetch: POST -> querybatch response, GET -> per-id detail."""
    calls: list[str] = []

    def fetch(url: str, payload: bytes | None, timeout: float) -> object:
        calls.append(url)
        if payload is not None:
            return batch_response
        return details.get(url.rsplit("/", 1)[-1], {})

    fetch.calls = calls
    return fetch


class TestDedupe:
    def test_unique_by_name_version(self):
        pairs = dedupe_packages([GUAVA, GUAVA, JUNIT])
        assert pairs == [("com.google.guava:guava", "19.0"), ("junit:junit", "4.11")]

    def test_skips_omitted_versionless_and_projects(self):
        skipped = [
            Dependency(artifact="guava", group="g", version="1.0", omitted=True),
            Dependency(artifact="no-version"),
            Dependency(artifact="project :app"),
        ]
        assert dedupe_packages(skipped) == []

    def test_resolved_wins_over_requested(self):
        d = Dependency(group="g", artifact="a", version="1.0", resolved="2.0")
        assert dedupe_packages([d]) == [("g:a", "2.0")]


class TestSeverity:
    def test_database_specific(self):
        assert bucket_severity({"database_specific": {"severity": "CRITICAL"}}) == "critical"
        assert bucket_severity({"database_specific": {"severity": "MEDIUM"}}) == "moderate"

    def test_numeric_score_buckets(self):
        assert bucket_severity({"severity": [{"score": "9.8"}]}) == "critical"
        assert bucket_severity({"severity": [{"score": "7.5"}]}) == "high"
        assert bucket_severity({"severity": [{"score": "5.0"}]}) == "moderate"
        assert bucket_severity({"severity": [{"score": "2.0"}]}) == "low"

    def test_vector_string_is_unknown(self):
        assert bucket_severity({"severity": [{"score": "CVSS:3.1/AV:N/AC:L"}]}) == "unknown"
        assert bucket_severity({}) == "unknown"


class TestQuery:
    BATCH = {
        "results": [
            {"vulns": [{"id": "GHSA-guava-1", "modified": "2024-01-01"}]},
            {},  # junit clean
        ]
    }
    DETAILS = {
        "GHSA-guava-1": {
            "summary": "Guava deserialization issue",
            "database_specific": {"severity": "HIGH"},
            "affected": [{"ranges": [{"events": [{"introduced": "0"}, {"fixed": "24.1.1"}]}]}],
        }
    }

    def test_maps_advisories(self):
        vulns, errors = query_osv([GUAVA, JUNIT], fetch=make_fetch(self.BATCH, self.DETAILS))
        assert errors == []
        assert len(vulns) == 1
        v = vulns[0]
        assert v.name == "com.google.guava:guava"
        assert v.severity == "high"
        assert v.vulnerable_range == "GHSA-guava-1"
        assert v.url.endswith("/GHSA-guava-1")
        assert v.fix_available
        assert v.title == "Guava deserialization issue"

    def test_vulns_df_sorts_worst_first(self):
        vulns, _ = query_osv([GUAVA, JUNIT], fetch=make_fetch(self.BATCH, self.DETAILS))
        df = vulns_df(BuildResult(run_id="r", tool="audit", binary="x", vulnerabilities=vulns))
        assert df["severity"][0] == "high"

    def test_empty_deps_no_fetch(self):
        fetch = make_fetch({}, {})
        vulns, errors = query_osv([], fetch=fetch)
        assert vulns == [] and errors == [] and fetch.calls == []

    def test_network_failure_single_error(self):
        def broken(url: str, payload: bytes | None, timeout: float) -> object:
            raise urllib.error.URLError("no route to host")

        vulns, errors = query_osv([GUAVA], fetch=broken)
        assert vulns == []
        assert len(errors) == 1 and "OSV query failed" in errors[0]

    def test_detail_cap(self):
        many_ids = [{"id": f"GHSA-{i}"} for i in range(DETAIL_CAP + 10)]
        batch = {"results": [{"vulns": many_ids}]}
        fetch = make_fetch(batch, {f"GHSA-{i}": {"summary": f"s{i}"} for i in range(DETAIL_CAP + 10)})
        vulns, errors = query_osv([GUAVA], fetch=fetch)
        assert errors == []
        assert len(vulns) == DETAIL_CAP + 10
        detail_gets = [c for c in fetch.calls if "/vulns/" in c]
        assert len(detail_gets) == DETAIL_CAP
        capped = [v for v in vulns if v.title == "details not fetched (capped)"]
        assert len(capped) == 10 and all(v.severity == "unknown" for v in capped)

    def test_batch_chunking(self):
        deps = [Dependency(group="g", artifact=f"a{i}", version="1.0") for i in range(1500)]
        empty = {"results": [{} for _ in range(1000)]}

        def fetch(url: str, payload: bytes | None, timeout: float) -> object:
            assert payload is not None
            n = len(json.loads(payload)["queries"])
            assert n <= 1000
            fetch.batches.append(n)
            return empty

        fetch.batches = []
        vulns, errors = query_osv(deps, fetch=fetch)
        assert errors == [] and vulns == []
        assert fetch.batches == [1000, 500]
