"""Code-graph viz: load a native knowledge-graph.json export, render SVG, serve /graph."""

from __future__ import annotations

import json

from devtools_mcp.codegraph import load_graph, render_graph_svg

SAMPLE = {
    "version": "1",
    "kind": "codebase",
    "project": "demo",
    "nodes": [
        {
            "id": "file:a.py:a.py",
            "type": "file",
            "name": "a.py",
            "filePath": "a.py",
            "lineRange": [0, 0],
            "complexity": 0,
            "summary": "",
            "tags": [],
        },
        {
            "id": "function:a.py:main",
            "type": "function",
            "name": "main",
            "filePath": "a.py",
            "lineRange": [1, 10],
            "complexity": 3,
            "summary": "def main()",
            "tags": ["exported"],
        },
        {
            "id": "function:a.py:helper",
            "type": "function",
            "name": "helper",
            "filePath": "a.py",
            "lineRange": [12, 20],
            "complexity": 1,
            "summary": "",
            "tags": [],
        },
        {
            "id": "class:a.py:Widget",
            "type": "class",
            "name": "Widget",
            "filePath": "a.py",
            "lineRange": [22, 40],
            "complexity": 0,
            "summary": "",
            "tags": [],
        },
    ],
    "edges": [
        {"source": s, "target": t, "type": r, "direction": "forward", "weight": w}
        for s, t, r, w in [
            ("file:a.py:a.py", "function:a.py:main", "contains", 1.0),
            ("file:a.py:a.py", "function:a.py:helper", "contains", 1.0),
            ("function:a.py:main", "function:a.py:helper", "calls", 1.0),
            ("class:a.py:Widget", "function:a.py:helper", "calls", 0.8),
        ]
    ],
}


def test_load_indexes_nodes_and_adjacency():
    g = load_graph(SAMPLE)
    assert len(g.nodes) == 4 and len(g.edges) == 4
    assert len(g.out_adj["function:a.py:main"]) == 1  # main -> helper
    assert len(g.in_adj["function:a.py:helper"]) == 3  # file, main, Widget
    assert g.degree("function:a.py:helper") == 3
    assert g.top_node() == "function:a.py:helper"  # highest degree


def test_load_accepts_json_string():
    g = load_graph(json.dumps(SAMPLE))
    assert len(g.nodes) == 4


def test_ego_neighbourhood():
    g = load_graph(SAMPLE)
    placement, edges = g.ego("function:a.py:main", hops=1)
    assert placement["function:a.py:main"] == 0
    assert placement["function:a.py:helper"] == 1  # dependency (out)
    assert placement["file:a.py:a.py"] == -1  # dependent (in, contains)
    # only edges among included nodes; Widget->helper excluded (Widget not in ego)
    assert all(e["source"] in placement and e["target"] in placement for e in edges)
    assert not any(e["source"] == "class:a.py:Widget" for e in edges)


def test_render_svg_click_to_focus():
    g = load_graph(SAMPLE)
    svg = render_graph_svg(g, focus="function:a.py:main")
    assert svg.startswith("<svg") and svg.endswith("</svg>")
    assert "xlink:href" in svg and "focus=" in svg  # every node re-roots the view
    assert "main" in svg and "helper" in svg
    assert "function%3Aa.py%3Ahelper" in svg  # focus links are url-encoded ids


def test_render_empty_graph():
    g = load_graph({"nodes": [], "edges": []})
    out = render_graph_svg(g)
    assert "Empty graph" in out


def test_graph_route(tmp_path):
    import json as _json
    import urllib.request

    from devtools_mcp.viz.server import VizServer

    path = tmp_path / "kg.json"
    path.write_text(_json.dumps(SAMPLE), encoding="utf-8")
    srv = VizServer(None)
    url = srv.start(port=0)
    try:
        body = urllib.request.urlopen(f"{url}/graph?src={path}").read().decode()
        assert "<svg" in body and "Focus:" in body
        # no source -> instructions, not a crash
        empty = urllib.request.urlopen(f"{url}/graph").read().decode()
        assert "No graph loaded" in empty
    finally:
        srv.stop()
