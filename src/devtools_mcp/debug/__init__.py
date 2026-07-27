"""Unified debugging layer: one interface, many implementations.

The `DebugSession` ABC (session.py) is protocol-agnostic. The dominant
implementation family is DAP (dap_session.py + adapters/); non-DAP
implementations (e.g. SAP ADT REST) implement the same surface.
"""
