"""AdapterSpec: how to spawn/connect one debug adapter and shape its configs.

An adapter = transport + config templates + detection. The session layer
(DapSession) is adapter-agnostic; everything adapter-specific lives in a
spec registered here.
"""

from __future__ import annotations

import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from devtools_mcp.debug.models import AttachConfig, LaunchConfig
from devtools_mcp.debug.protocol import DapTransport
from devtools_mcp.registry import InstalledTool, InstallSpec

if TYPE_CHECKING:
    from devtools_mcp.debug.session import DebugSession, DebugSessionManager, SnapshotSink

# Bounds.
MAX_ADAPTERS = 16

# Native executable magic numbers for sniffing (ELF, Mach-O 64 LE/BE + fat, PE).
_NATIVE_MAGICS = (b"\x7fELF", b"\xcf\xfa\xed\xfe", b"\xfe\xed\xfa\xcf", b"\xca\xfe\xba\xbe", b"MZ")


@dataclass(frozen=True)
class AdapterQuirks:
    """Behavioral differences the session layer must know about."""

    multi_session: bool = False  # spawns children via startDebugging (js-debug)
    needs_run_in_terminal: bool = False  # launches the debuggee via reverse request
    supports_attach_pid: bool = False
    supports_attach_socket: bool = False


@dataclass(frozen=True)
class AdapterSpec:
    """One debug adapter registration."""

    name: str  # "debugpy", "lldb-dap", "js-debug", "java", "kotlin"
    languages: tuple[str, ...]  # ("python",)
    # async (config) -> started transport. Receives the Launch/Attach config
    # because some adapters embed config in how they're spawned.
    transport: Callable[[LaunchConfig | AttachConfig], Awaitable[DapTransport]]
    launch_template: Callable[[LaunchConfig], dict]
    attach_template: Callable[[AttachConfig], dict]
    detect: Callable[[], Awaitable[InstalledTool]]
    # program path (or "") -> confidence 0..100 that this adapter handles it.
    sniff: Callable[[str], int]
    install: InstallSpec | None = None
    quirks: AdapterQuirks = field(default_factory=AdapterQuirks)
    description: str = ""


_ADAPTERS: dict[str, AdapterSpec] = {}


def register_adapter(spec: AdapterSpec) -> None:
    assert spec.name, "adapter name must not be empty"
    assert spec.languages, f"adapter {spec.name!r} must declare at least one language"
    assert spec.name not in _ADAPTERS, f"duplicate adapter registration: {spec.name!r}"
    assert len(_ADAPTERS) < MAX_ADAPTERS, f"adapter registry full ({MAX_ADAPTERS})"
    _ADAPTERS[spec.name] = spec


def get_adapter(name: str) -> AdapterSpec:
    if name not in _ADAPTERS:
        raise KeyError(f"Unknown adapter '{name}'. Available: {sorted(_ADAPTERS)}")
    return _ADAPTERS[name]


_FAILED_ADAPTER_PLUGINS: dict[str, str] = {}  # entry-point name -> one-line error


def load_adapter_plugins() -> None:
    """Load out-of-tree debug adapters from the 'devtools_mcp.debug_adapters'
    entry-point group. A plugin exposes a module that calls register_adapter()
    on import — exactly like the in-tree adapters. Mirrors the backend
    registry's load_backends(): a broken plugin degrades to an entry in
    _FAILED_ADAPTER_PLUGINS, never crashes the server. Idempotent.
    """
    import importlib.metadata

    for ep in importlib.metadata.entry_points(group="devtools_mcp.debug_adapters"):
        if ep.name in _FAILED_ADAPTER_PLUGINS or any(ep.name == s.name for s in _ADAPTERS.values()):
            continue
        try:
            ep.load()
        except Exception as exc:  # noqa: BLE001 — degrade to unavailable, don't die
            _FAILED_ADAPTER_PLUGINS[ep.name] = f"{type(exc).__name__}: {exc}"


def failed_adapter_plugins() -> dict[str, str]:
    return dict(_FAILED_ADAPTER_PLUGINS)


# --- Non-DAP session factories -------------------------------------------
# Most adapters are DAP: the host wraps their AdapterSpec in a DapSession.
# Some implementations (e.g. SAP ADT) speak their own protocol and provide a
# DebugSession directly. They register a factory here instead of an
# AdapterSpec. resolve_session() below tries factories first, then DAP.

# factory(session_id, manager, snapshot_sink) -> DebugSession
SessionFactory = Callable[[str, "DebugSessionManager", "SnapshotSink | None"], "DebugSession"]


@dataclass(frozen=True)
class SessionProvider:
    """A non-DAP debugger implementation: builds a DebugSession directly."""

    name: str
    languages: tuple[str, ...]
    factory: SessionFactory
    sniff: Callable[[str], int] = lambda _program: 0
    description: str = ""


_SESSION_PROVIDERS: dict[str, SessionProvider] = {}


def register_session_provider(provider: SessionProvider) -> None:
    """Register a non-DAP DebugSession provider (e.g. the ABAP plugin)."""
    assert provider.name, "session provider name must not be empty"
    assert provider.languages, f"provider {provider.name!r} must declare a language"
    assert provider.name not in _SESSION_PROVIDERS, f"duplicate session provider: {provider.name!r}"
    assert provider.name not in _ADAPTERS, f"name {provider.name!r} already a DAP adapter"
    assert len(_SESSION_PROVIDERS) < MAX_ADAPTERS, f"session-provider registry full ({MAX_ADAPTERS})"
    _SESSION_PROVIDERS[provider.name] = provider


def get_session_provider(name: str) -> SessionProvider | None:
    return _SESSION_PROVIDERS.get(name)


def list_session_providers() -> list[SessionProvider]:
    return list(_SESSION_PROVIDERS.values())


def resolve_session_provider(program: str = "", language: str = "", adapter: str = "") -> SessionProvider | None:
    """Match a non-DAP provider by explicit name > language > sniff.
    Returns None so the caller can fall back to DAP adapter resolution."""
    if adapter:
        return _SESSION_PROVIDERS.get(adapter)
    if language:
        matches = [p for p in _SESSION_PROVIDERS.values() if language.lower() in p.languages]
        return matches[0] if len(matches) == 1 else None
    if program:
        scored = sorted(
            ((p.sniff(program), p) for p in _SESSION_PROVIDERS.values()),
            key=lambda pair: pair[0],
            reverse=True,
        )
        if scored and scored[0][0] > 0:
            return scored[0][1]
    return None


def list_adapters() -> list[AdapterSpec]:
    return list(_ADAPTERS.values())


def resolve_adapter(program: str = "", language: str = "", adapter: str = "") -> AdapterSpec:
    """Pick the adapter: explicit name > language > sniff. Ambiguity or a
    miss fails loud with the candidate list."""
    if adapter:
        return get_adapter(adapter)
    if language:
        matches = [spec for spec in _ADAPTERS.values() if language.lower() in spec.languages]
        if len(matches) == 1:
            return matches[0]
        if matches:
            names = ", ".join(sorted(spec.name for spec in matches))
            raise KeyError(f"Multiple adapters handle language '{language}': {names}. Pass adapter= explicitly.")
        raise KeyError(
            f"No adapter for language '{language}'. "
            f"Known: {sorted({lang for spec in _ADAPTERS.values() for lang in spec.languages})}"
        )
    if program:
        scored = [(spec.sniff(program), spec) for spec in _ADAPTERS.values()]
        scored = [(score, spec) for score, spec in scored if score > 0]
        if scored:
            scored.sort(key=lambda pair: pair[0], reverse=True)
            best_score = scored[0][0]
            best = [spec for score, spec in scored if score == best_score]
            if len(best) == 1:
                return best[0]
            names = ", ".join(sorted(spec.name for spec in best))
            raise KeyError(f"Ambiguous adapter for '{program}' ({names}). Pass adapter= or language= explicitly.")
    raise KeyError(
        f"Cannot determine debug adapter for program={program!r}. "
        f"Pass language= or adapter= (available: {sorted(_ADAPTERS)})."
    )


def is_native_binary(program: str) -> bool:
    """True if the file starts with a known native-executable magic number."""
    try:
        with open(program, "rb") as fh:
            head = fh.read(4)
    except OSError:
        return False
    return any(head.startswith(magic) for magic in _NATIVE_MAGICS)


def find_project_file(start: str, names: tuple[str, ...], max_depth: int = 3) -> str:
    """Walk up from `start` looking for a project marker file. Bounded."""
    directory = start if os.path.isdir(start) else os.path.dirname(os.path.abspath(start))
    for _ in range(max_depth):
        for name in names:
            candidate = os.path.join(directory, name)
            if os.path.exists(candidate):
                return candidate
        parent = os.path.dirname(directory)
        if parent == directory:
            break
        directory = parent
    return ""
