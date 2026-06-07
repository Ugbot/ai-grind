"""Tests for the shared flame-graph engine (fold -> tree -> text/svg)."""

from __future__ import annotations

import random

from devtools_mcp.flamegraph import build_call_tree, emit_folded, focus, parse_folded, render_svg
from devtools_mcp.flamegraph.render_text import render_text_tree, top_table
from devtools_mcp.flamegraph.tree import MAX_DEPTH, function_stats, top_dispatchers, top_leaves
from devtools_mcp.models import StackSample


def _random_folded(n_stacks: int = 30) -> str:
    funcs = ["main", "run", "parse", "alloc", "memcpy", "hash", "compare", "flush", "read", "write"]
    lines = []
    for _ in range(n_stacks):
        depth = random.randint(1, 8)
        frames = ["main"] + [random.choice(funcs) for _ in range(depth)]
        lines.append(";".join(frames) + f" {random.randint(1, 500)}")
    return "\n".join(lines)


class TestFold:
    def test_roundtrip_preserves_weights(self):
        text = _random_folded()
        samples = parse_folded(text)
        assert samples
        total = sum(s.weight for s in samples)
        reparsed = parse_folded(emit_folded(samples))
        assert sum(s.weight for s in reparsed) == total

    def test_skips_malformed_lines(self):
        text = "good;stack 10\nno_count_here\n# comment\nbad;stack notanumber\n"
        samples = parse_folded(text)
        assert len(samples) == 1
        assert samples[0].weight == 10

    def test_empty(self):
        assert parse_folded("") == []


class TestTree:
    def test_root_total_equals_sum_of_weights(self):
        samples = parse_folded(_random_folded())
        total = sum(s.weight for s in samples)
        tree = build_call_tree(samples)
        assert tree.total_weight == total

    def test_exclusive_sums_to_total(self):
        samples = [
            StackSample(frames=["a", "b"], weight=10),
            StackSample(frames=["a", "c"], weight=5),
            StackSample(frames=["a"], weight=2),
        ]
        tree = build_call_tree(samples)
        stats = function_stats(tree)
        exclusive_total = sum(exc for exc, _ in stats.values())
        assert exclusive_total == tree.total_weight == 17
        # 'a' inclusive is the whole tree; b and c are leaves.
        assert stats["a"][1] == 17
        assert stats["b"] == (10, 10)
        assert stats["c"] == (5, 5)

    def test_recursion_not_double_counted_inclusive(self):
        # a -> a -> a ; inclusive for 'a' must be 9, not 9+9+9.
        tree = build_call_tree([StackSample(frames=["a", "a", "a"], weight=9)])
        stats = function_stats(tree)
        assert stats["a"][1] == 9
        assert stats["a"][0] == 9  # exclusive at the deepest leaf

    def test_depth_bounded(self):
        deep = StackSample(frames=[f"f{i}" for i in range(MAX_DEPTH + 50)], weight=1)
        tree = build_call_tree([deep])
        # Walk to find max depth actually built.
        from devtools_mcp.flamegraph.tree import walk

        max_seen = max(d for _, d in walk(tree))
        assert max_seen <= MAX_DEPTH

    def test_top_leaves_and_dispatchers(self):
        samples = [
            StackSample(frames=["main", "work", "memcpy"], weight=100),
            StackSample(frames=["main", "work"], weight=5),
        ]
        tree = build_call_tree(samples)
        stats = function_stats(tree)
        leaves = top_leaves(stats, 5)
        assert leaves[0][0] == "memcpy"  # hottest leaf
        disp = top_dispatchers(stats, 5)
        assert disp[0][0] in ("main", "work")  # time mostly in callees


class TestFocus:
    def test_reroots_at_frame(self):
        samples = [
            StackSample(frames=["main", "work", "memcpy"], weight=100),
            StackSample(frames=["main", "work", "hash"], weight=40),
            StackSample(frames=["main", "idle"], weight=10),
        ]
        tree = build_call_tree(samples)
        sub = focus(tree, "work")
        assert sub is not None
        assert sub.total_weight == 140  # only what ran under work
        assert set(sub.children) == {"memcpy", "hash"}

    def test_missing_frame_returns_none(self):
        tree = build_call_tree([StackSample(frames=["a", "b"], weight=1)])
        assert focus(tree, "nope") is None

    def test_merges_multiple_occurrences(self):
        # 'lock' appears under two different parents — focus merges both subtrees.
        samples = [
            StackSample(frames=["a", "lock", "spin"], weight=5),
            StackSample(frames=["b", "lock", "spin"], weight=7),
        ]
        sub = focus(build_call_tree(samples), "lock")
        assert sub.total_weight == 12
        assert sub.children["spin"].total_weight == 12

    def test_svg_href_links_when_focusable(self):
        tree = build_call_tree([StackSample(frames=["a", "b"], weight=3)])
        svg = render_svg(tree, href_base="/flame/x")
        assert "<a " in svg and "focus=" in svg


class TestRenderText:
    def test_bounded_and_has_root(self):
        tree = build_call_tree(parse_folded(_random_folded(100)))
        out = render_text_tree(tree, min_pct=0.1, max_depth=32)
        assert "%" in out
        assert len(out.splitlines()) <= 301  # MAX_NODES + truncation note

    def test_empty_tree(self):
        tree = build_call_tree([])
        assert "no samples" in render_text_tree(tree)

    def test_top_table_markdown(self):
        tree = build_call_tree([StackSample(frames=["a", "b"], weight=10)])
        table = top_table(tree, 5)
        assert table.startswith("| Exc%")
        assert "b" in table


class TestRenderSvg:
    def test_well_formed_svg(self):
        tree = build_call_tree(parse_folded(_random_folded(50)))
        svg = render_svg(tree, title="test")
        assert svg.startswith("<svg")
        assert svg.rstrip().endswith("</svg>")
        assert "<rect" in svg

    def test_empty_svg(self):
        svg = render_svg(build_call_tree([]), title="empty")
        assert svg.startswith("<svg")
        assert "empty" in svg

    def test_escapes_special_chars(self):
        tree = build_call_tree([StackSample(frames=["a<b>&c", "leaf"], weight=10)])
        svg = render_svg(tree)
        assert "<b>" not in svg.replace("<svg", "").replace("</svg>", "")
        assert "&amp;" in svg or "&lt;" in svg
