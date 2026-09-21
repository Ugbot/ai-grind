"""Out-of-tree console-page registry for the visualization terminal.

The built-in tabs (runs / tracker / collab / …) are hard-wired into
viz/render.py (the ``page()`` nav) and viz/server.py (the ``do_GET``/``do_POST``
dispatch). That is fine for in-tree pages but leaves no seam for a plugin to add
its OWN console page. This module is that seam: a plugin registers a
:class:`VizPage` (a tab plus its GET/POST routes) and the viz server renders the
tab and dispatches its routes without any edit to viz/.

Loading mirrors ``registry.py``: :func:`load_viz_pages` imports the in-tree page
modules from ``_VIZ_PAGE_MODULES`` and then the ``devtools_mcp.viz_pages``
entry-point group, exactly like ``load_backends`` handles ``devtools_mcp.backends``.
A plugin's entry point points at a module that calls :func:`register_page` on
import. A version-incompatible plugin is skipped-with-warning (into
``_FAILED_VIZ_PAGES``) rather than crashing, the same degrade-never-crash
contract the backend/tool loaders honor.

Handlers stay decoupled from the HTTP layer: a GET/POST handler returns either an
HTML ``str`` (sent 200) or a :class:`VizResponse` (explicit status / redirect /
content type), or ``None`` to fall through to a standard 404. The viz server
translates that into an HTTP reply and keeps ``_guard_state_change`` on POSTs.
"""

from __future__ import annotations

import importlib
import importlib.metadata
from collections.abc import Callable
from dataclasses import dataclass

from devtools_mcp.registry import _attr_incompat_reason, host_incompat_reason

MAX_VIZ_PAGES: int = 64

# Path prefixes owned by the built-in viz routes, a registered page may not
# claim one of these (it would shadow a core route). Keep in sync with
# viz/server.py's do_GET/do_POST first-segment branches.
_BUILTIN_PREFIXES: frozenset[str] = frozenset(
    {
        "",
        "health",
        "search",
        "compare",
        "correlate",
        "tools",
        "run",
        "flame",
        "raw",
        "tracker",
        "collab",
        "skills",
        "graph",
        "station",
        "api",
    }
)


@dataclass(frozen=True)
class VizResponse:
    """A registered handler's reply, translated to HTTP by the viz server.

    body: response payload (utf-8) written when there is no redirect
    status: HTTP status code
    content_type, Content-Type header for a body reply
    location, when set, the server sends a 303 redirect here (body ignored)
    """

    body: str = ""
    status: int = 200
    content_type: str = "text/html; charset=utf-8"
    location: str | None = None

    @classmethod
    def redirect(cls, location: str) -> VizResponse:
        """A 303 See Other to `location` (the POST-redirect-GET pattern)."""
        assert location, "redirect needs a location"
        return cls(status=303, location=location)


# A GET subpath handler: (rest_parts, query) -> reply. `rest` is the path
# segments AFTER the page prefix; `query` is the parsed query string.
GetHandler = Callable[[list[str], dict[str, list[str]]], "VizResponse | str | None"]
# A POST handler: (rest_parts, raw_body) -> reply.
PostHandler = Callable[[list[str], str], "VizResponse | str | None"]


@dataclass(frozen=True)
class VizPage:
    """A console page contributed by a plugin (or an in-tree module).

    name: unique registry key
    prefix: first path segment the page owns (e.g. "recipes" for /recipes/*)
    label: tab label shown in the nav
    render: GET at exactly /{prefix} (no trailing segments) -> HTML str
    get: GET at /{prefix}/<rest...>; None means "no subpaths"
    post: POST at /{prefix}/<rest...>; None means "read-only page"
    href: tab link target; defaults to /{prefix}
    """

    name: str
    prefix: str
    label: str
    render: Callable[[], str] | None = None
    get: GetHandler | None = None
    post: PostHandler | None = None
    href: str = ""

    def tab(self) -> tuple[str, str, str]:
        """(active-key, href, label) for the nav bar in render.page()."""
        return (self.prefix, self.href or f"/{self.prefix}", self.label)


_PAGES: dict[str, VizPage] = {}


def register_page(page: VizPage) -> None:
    """Register a console page. Idempotent per import; asserts on real conflicts."""
    assert page.name, "viz page name must not be empty"
    assert page.prefix, f"viz page {page.name!r} must set a path prefix"
    assert page.label, f"viz page {page.name!r} must set a tab label"
    assert "/" not in page.prefix, f"viz page prefix must be a single segment: {page.prefix!r}"
    assert page.prefix not in _BUILTIN_PREFIXES, f"viz page prefix {page.prefix!r} shadows a built-in route"
    if page.name in _PAGES:
        # Same page re-registered (module re-imported): allow if identical.
        assert _PAGES[page.name] == page, f"duplicate viz page registration: {page.name!r}"
        return
    existing = get_page(page.prefix)
    assert existing is None, f"viz page prefix {page.prefix!r} already claimed by {existing.name!r}"
    assert len(_PAGES) < MAX_VIZ_PAGES, f"viz page registry full ({MAX_VIZ_PAGES})"
    _PAGES[page.name] = page


def iter_pages() -> list[VizPage]:
    """All registered pages, in registration order (stable nav ordering)."""
    return list(_PAGES.values())


def get_page(prefix: str) -> VizPage | None:
    """The page owning `prefix` (a first path segment), or None."""
    for page in _PAGES.values():
        if page.prefix == prefix:
            return page
    return None


def registered_tabs() -> list[tuple[str, str, str]]:
    """(active-key, href, label) tuples for every registered page, for the nav."""
    return [page.tab() for page in _PAGES.values()]


# --- Loading ----------------------------------------------------------------
# In-tree page modules that register on import (the reference is `recipes_page`).
_VIZ_PAGE_MODULES: tuple[str, ...] = ("devtools_mcp.viz.recipes_page",)

_FAILED_VIZ_PAGES: dict[str, str] = {}
_LOADED_VIZ_PAGES: set[str] = set()


def load_viz_pages() -> None:
    """Import in-tree page modules + the 'devtools_mcp.viz_pages' entry points.

    Idempotent (import caching + register_page's idempotency). A broken or
    version-incompatible page module degrades into _FAILED_VIZ_PAGES, never
    crashes the viz server.
    """
    assert len(_VIZ_PAGE_MODULES) <= MAX_VIZ_PAGES, "viz page manifest exceeds bound"
    for module_name in _VIZ_PAGE_MODULES:
        try:
            importlib.import_module(module_name)
        except Exception as exc:  # noqa: BLE001  # degrade, don't die
            _FAILED_VIZ_PAGES[module_name] = f"{type(exc).__name__}: {exc}"
    for ep in importlib.metadata.entry_points(group="devtools_mcp.viz_pages"):
        key = f"viz:{ep.name}"
        if key in _FAILED_VIZ_PAGES or key in _LOADED_VIZ_PAGES:
            continue
        reason = host_incompat_reason(ep)
        if reason:
            _FAILED_VIZ_PAGES[key] = f"skipped: {reason}"
            continue
        try:
            loaded = ep.load()
        except Exception as exc:  # noqa: BLE001
            _FAILED_VIZ_PAGES[key] = f"{type(exc).__name__}: {exc}"
            continue
        attr_reason = _attr_incompat_reason(loaded)
        if attr_reason:
            _FAILED_VIZ_PAGES[key] = f"skipped: {attr_reason}"
            continue
        _LOADED_VIZ_PAGES.add(key)


def failed_viz_pages() -> dict[str, str]:
    """Page modules/entry-points that failed to load, key -> one-line error."""
    return dict(_FAILED_VIZ_PAGES)


def unregister_page(name: str) -> None:
    """Remove a page by name (test hygiene for pages registered ad hoc)."""
    _PAGES.pop(name, None)


__all__ = [
    "VizPage",
    "VizResponse",
    "register_page",
    "iter_pages",
    "get_page",
    "registered_tabs",
    "load_viz_pages",
    "failed_viz_pages",
    "unregister_page",
]
