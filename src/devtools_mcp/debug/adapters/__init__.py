"""Debug adapter registry. Each adapter module registers an AdapterSpec at
import time, mirroring how backends register in the tool registry."""

from __future__ import annotations

# Import order = registration order. A broken adapter module should never
# take the others down — keep these imports side-effect-only registrations.
from devtools_mcp.debug.adapters import debugpy as _debugpy  # noqa: F401,E402
from devtools_mcp.debug.adapters import java_debug as _java_debug  # noqa: F401,E402
from devtools_mcp.debug.adapters import js_debug as _js_debug  # noqa: F401,E402
from devtools_mcp.debug.adapters import kotlin as _kotlin  # noqa: F401,E402
from devtools_mcp.debug.adapters import lldb_dap as _lldb_dap  # noqa: F401,E402
from devtools_mcp.debug.adapters.base import (
    AdapterSpec,
    SessionProvider,
    failed_adapter_plugins,
    get_adapter,
    get_session_provider,
    list_adapters,
    list_session_providers,
    load_adapter_plugins,
    register_adapter,
    register_session_provider,
    resolve_adapter,
    resolve_session_provider,
)

# Out-of-tree adapters (e.g. the ABAP plugin) register via the
# 'devtools_mcp.debug_adapters' entry-point group. Load them after the
# in-tree adapters so a broken plugin can never shadow a core adapter.
load_adapter_plugins()

__all__ = [
    "AdapterSpec",
    "SessionProvider",
    "failed_adapter_plugins",
    "get_adapter",
    "get_session_provider",
    "list_adapters",
    "list_session_providers",
    "load_adapter_plugins",
    "register_adapter",
    "register_session_provider",
    "resolve_adapter",
    "resolve_session_provider",
]
