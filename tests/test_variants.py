"""Power-variant rendering for dynamic skills."""

from __future__ import annotations

import pytest

from devtools_mcp.skilldocs import variants

DOC = (
    "shared A\n"
    "<!-- power:high -->\n"
    "HIGH ONLY\n"
    "<!-- /power -->\n"
    "<!-- power:low -->\n"
    "LOW ONLY\n"
    "<!-- /power -->\n"
    "shared B\n"
)


def test_render_selects_active_mode():
    assert variants.render(DOC, "high") == "shared A\nHIGH ONLY\nshared B\n"
    assert variants.render(DOC, "low") == "shared A\nLOW ONLY\nshared B\n"


def test_render_unknown_mode_drops_all_labelled_blocks():
    assert variants.render(DOC, "medium") == "shared A\nshared B\n"


def test_no_markers_is_byte_identical_passthrough():
    plain = "---\nname: x\ndescription: y\n---\n\nbody with no markers\n"
    assert variants.render(plain, "low") == plain
    assert variants.render(plain, "high") == plain


def test_frontmatter_untouched():
    doc = "---\nname: s\ndescription: d\n---\n<!-- power:low -->\nL\n<!-- /power -->\n"
    assert variants.render(doc, "low").startswith("---\nname: s\ndescription: d\n---\n")


def test_detect_modes_and_has_variants():
    assert variants.detect_modes(DOC) == ["high", "low"]
    assert variants.has_variants(DOC)
    assert not variants.has_variants("no markers")
    assert variants.detect_modes("no markers") == []


def test_nested_blocks_rejected():
    bad = "<!-- power:high -->\n<!-- power:low -->\nx\n<!-- /power -->\n<!-- /power -->\n"
    with pytest.raises(AssertionError):
        variants.render(bad, "high")


def test_unbalanced_close_rejected():
    with pytest.raises(AssertionError):
        variants.render("<!-- /power -->\n", "high")


def test_unclosed_block_rejected():
    with pytest.raises(AssertionError):
        variants.render("<!-- power:high -->\nx\n", "high")


def test_block_count_bounded():
    many = "".join(f"<!-- power:high -->\n{i}\n<!-- /power -->\n" for i in range(variants.VARIANT_BLOCKS_MAX + 1))
    with pytest.raises(AssertionError):
        variants.render(many, "high")


def test_render_never_grows():
    assert len(variants.render(DOC, "high")) <= len(DOC)
