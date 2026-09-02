"""The skill-router live skill: frontmatter parsing, index build, rebuild."""

from __future__ import annotations

import pathlib

from devtools_mcp.skilldocs import router
from devtools_mcp.skilldocs.store import SkillDocStore

# -- frontmatter / goap parsing ----------------------------------------------


def test_read_frontmatter_single_line():
    assert router.read_frontmatter("---\nname: a\ndescription: hi there\n---\nbody") == ("a", "hi there")


def test_read_frontmatter_folded():
    doc = "---\nname: a\ndescription: >\n  line one\n  line two\n---\nx"
    assert router.read_frontmatter(doc) == ("a", "line one line two")


def test_read_frontmatter_quoted():
    assert router.read_frontmatter('---\nname: a\ndescription: "quoted"\n---\n') == ("a", "quoted")


def test_read_frontmatter_invalid():
    assert router.read_frontmatter("no frontmatter") is None
    assert router.read_frontmatter("---\ndescription: no name\n---\n") is None


def test_parse_goap_block():
    doc = 'intro\n```goap\n{"preconditions": {"a": true}, "effects": {"b": true}}\n```\nrest'
    assert router.parse_goap(doc) == {"preconditions": {"a": True}, "effects": {"b": True}}


def test_parse_goap_absent_or_bad():
    assert router.parse_goap("no block") is None
    assert router.parse_goap("```goap\nnot json\n```") is None


# -- index build --------------------------------------------------------------


def test_build_index_markers_and_grouping():
    entries = [
        router.Entry("b-skill", "does b", "cat2", modes=["low", "high"]),
        router.Entry("a-skill", "does a", "cat1", goap={"effects": {"x": True}}),
    ]
    index = router.build_index(entries, "high")
    assert index.startswith(router.INDEX_START)
    assert index.rstrip().endswith(router.INDEX_END)
    assert "active power mode: **high**" in index
    assert "### cat1" in index and "### cat2" in index
    assert "power: low/high" in index and "goap" in index


# -- rebuild ------------------------------------------------------------------


def _seed_library(tmp_path: pathlib.Path) -> pathlib.Path:
    root = tmp_path / "skills"
    cat = root / "catalog" / "skills" / "profiling" / "demo-cat"
    cat.mkdir(parents=True)
    (cat / "SKILL.md").write_text("---\nname: demo-cat\ndescription: a catalog demo\n---\nbody\n", encoding="utf-8")
    exp = root / "catalog" / "skills" / "experimental" / "narrative" / "demo-exp"
    exp.mkdir(parents=True)
    (exp / "SKILL.md").write_text("---\nname: demo-exp\ndescription: a sidelined skill\n---\nbody\n", encoding="utf-8")
    meta = root / "authored" / "skills" / "meta"
    meta.mkdir(parents=True)
    (meta / "skill-router.rules.md").write_text("# Skill Router\n\nSEEDED RULES.\n", encoding="utf-8")
    return root


def test_rebuild_creates_then_preserves_rules(tmp_path, monkeypatch):
    root = _seed_library(tmp_path)
    monkeypatch.setattr(router, "find_skills_root", lambda: root)
    store = SkillDocStore()
    try:
        path = router.rebuild(store)
        assert path is not None
        body = pathlib.Path(path).read_text(encoding="utf-8")
        assert "SEEDED RULES." in body  # rules seeded from the repo template
        assert "demo-cat" in body  # catalog skill indexed
        assert "demo-exp" not in body  # experimental/ category is sidelined from the index
        assert router.INDEX_START in body and router.INDEX_END in body

        # a live edit to the rules region must survive a rebuild
        store.patch(router.ROUTER_NAME, "SEEDED RULES.", "SEEDED RULES.\n\nLIVE EDIT.")
        router.rebuild(store)
        after = pathlib.Path(path).read_text(encoding="utf-8")
        assert "LIVE EDIT." in after
    finally:
        store.close()


def test_rebuild_indexes_live_skills(tmp_path, monkeypatch):
    monkeypatch.setattr(router, "find_skills_root", lambda: None)  # live-only
    store = SkillDocStore()
    try:
        store.create("live-demo", "---\nname: live-demo\ndescription: a live one\n---\nbody\n")
        router.rebuild(store)
        body = router.build_index(router.collect_skills(store), "high")
        assert "live-demo" in body
        assert router.ROUTER_NAME not in body  # router excludes itself
    finally:
        store.close()


def test_disabled_skills_pruned_from_index(tmp_path, monkeypatch):
    """Disabling a skill, static or live, removes it from the router index."""
    from devtools_mcp.skilldocs.control import SkillControl

    root = _seed_library(tmp_path)
    monkeypatch.setattr(router, "find_skills_root", lambda: root)
    store = SkillDocStore()
    try:
        store.create("live-demo", "---\nname: live-demo\ndescription: a live one\n---\nbody\n")
        control = SkillControl(conn=store.conn)
        control.set_disabled("demo-cat", True)  # static catalog skill
        control.set_disabled("live-demo", True)  # live skill
        names = {e.name for e in router.collect_skills(store)}
        assert "demo-cat" not in names
        assert "live-demo" not in names

        control.set_disabled("demo-cat", False)
        names = {e.name for e in router.collect_skills(store)}
        assert "demo-cat" in names  # re-enable restores it
    finally:
        store.close()
