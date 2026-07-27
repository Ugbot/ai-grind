"""Tests for the out-of-tree console-page registry (viz/pages.py).

Covers the registry API (register/get/iter, collision guards), the built-in
/recipes page migrated onto it, and — the headline — a registered
out-of-tree-STYLE page that renders and routes (GET root, GET subpath, POST
redirect) through the live viz HTTP server exactly as a plugin's page would.
"""

from __future__ import annotations

import urllib.error
import urllib.request

import pytest

from devtools_mcp.viz import pages
from devtools_mcp.viz.pages import VizPage, VizResponse, get_page, register_page, unregister_page
from devtools_mcp.viz.server import VizServer
from devtools_mcp.workspace import AppContext


class TestRegistry:
    def test_recipes_page_registered_by_loader(self):
        pages.load_viz_pages()
        page = get_page("recipes")
        assert page is not None and page.name == "recipes" and page.label == "Recipes"
        # It appears in the nav tab list.
        assert ("recipes", "/recipes", "Recipes") in pages.registered_tabs()

    def test_builtin_prefix_rejected(self):
        with pytest.raises(AssertionError, match="shadows a built-in"):
            register_page(VizPage(name="x", prefix="tracker", label="X"))

    def test_multi_segment_prefix_rejected(self):
        with pytest.raises(AssertionError, match="single segment"):
            register_page(VizPage(name="x", prefix="a/b", label="X"))

    def test_duplicate_prefix_rejected(self):
        register_page(VizPage(name="dup_a", prefix="dupprefix", label="A"))
        try:
            with pytest.raises(AssertionError, match="already claimed"):
                register_page(VizPage(name="dup_b", prefix="dupprefix", label="B"))
        finally:
            unregister_page("dup_a")

    def test_idempotent_identical_registration(self):
        page = VizPage(name="idem", prefix="idemprefix", label="Idem")
        register_page(page)
        register_page(page)  # identical re-register is a no-op, not an error
        try:
            assert get_page("idemprefix") is page
        finally:
            unregister_page("idem")


@pytest.fixture
def demo_page():
    """A fully out-of-tree-style page: its own tab, GET root/subpath, POST."""

    def render() -> str:
        from devtools_mcp.viz import render as r

        return r.page("demo", "<h2>Demo tab</h2>", active="demo")

    def handle_get(rest: list[str], query: dict[str, list[str]]) -> VizResponse | str | None:
        if rest == ["item"]:
            name = (query.get("name") or ["?"])[0]
            return f"<p>item {name}</p>"
        return None  # -> 404

    def handle_post(rest: list[str], body: str) -> VizResponse | None:
        if rest == ["do"]:
            return VizResponse.redirect("/demo")
        return VizResponse(body="<p>bad</p>", status=400)

    page = VizPage(name="demo", prefix="demo", label="Demo", render=render, get=handle_get, post=handle_post)
    register_page(page)
    yield page
    unregister_page("demo")


@pytest.fixture
def served():
    app = AppContext()
    ws = app.create_workspace("default")
    app.default_workspace_id = ws.workspace_id
    srv = VizServer(app)
    url = srv.start(port=0)
    yield url
    srv.stop()


def _get(url: str) -> str:
    return urllib.request.urlopen(url, timeout=5).read().decode()


class TestLiveRouting:
    def test_registered_tab_in_nav(self, demo_page, served):
        # The demo tab is discovered at server start and shows in every page's nav.
        body = _get(served + "/")
        assert ">Demo<" in body and "/demo" in body

    def test_render_root(self, demo_page, served):
        body = _get(served + "/demo")
        assert "Demo tab" in body

    def test_get_subpath(self, demo_page, served):
        body = _get(served + "/demo/item?name=widget")
        assert "item widget" in body

    def test_get_subpath_404(self, demo_page, served):
        with pytest.raises(urllib.error.HTTPError) as exc:
            _get(served + "/demo/missing")
        assert exc.value.code == 404

    def test_post_redirects(self, demo_page, served):
        # A registered POST goes through _guard_state_change then the page handler.
        resp = urllib.request.urlopen(urllib.request.Request(served + "/demo/do", data=b"", method="POST"), timeout=5)
        # urllib follows the 303 to /demo (GET) and lands on the rendered page.
        assert "Demo tab" in resp.read().decode()

    def test_unregistered_prefix_404(self, served):
        with pytest.raises(urllib.error.HTTPError) as exc:
            _get(served + "/nope")
        assert exc.value.code == 404
