"""Guard model-supplied sync URLs against the worst SSRF targets.

The peer-sync features (`skill_live` sync, `tracker_sync`) deliberately reach
other machines — LAN hosts and localhost are legitimate peers — so this is a
targeted guard, not a blanket private-IP block (which would disable the feature).
It rejects the cloud-metadata / link-local range and other addresses that can
never be a real peer, so a prompt-injected agent cannot point a sync at
169.254.169.254 or a multicast/reserved address. RFC-1918 LAN peers remain
reachable by design; that residual is documented, not accidental.
"""

from __future__ import annotations

import ipaddress
import socket
from urllib.parse import urlparse

_ALLOWED_SCHEMES = ("http", "https")


class SsrfError(Exception):
    """A sync URL was rejected as unsafe (expected/reportable, not a bug)."""


def _disallowed(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    """True for addresses that can never be a legitimate sync peer."""
    return ip.is_link_local or ip.is_multicast or ip.is_reserved or ip.is_unspecified


def check_sync_url(url: str) -> None:
    """Raise SsrfError if `url` is not a safe http(s) sync target.

    Resolves every A/AAAA record so a hostname that resolves to a link-local /
    metadata address is caught, not just literal-IP URLs.
    """
    assert isinstance(url, str), "url must be str"
    parsed = urlparse(url.strip())
    if parsed.scheme not in _ALLOWED_SCHEMES:
        raise SsrfError(f"scheme {parsed.scheme or '(none)'!r} not allowed — use http or https")
    host = parsed.hostname
    if not host:
        raise SsrfError("URL has no host")
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    try:
        infos = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
    except socket.gaierror as exc:
        raise SsrfError(f"cannot resolve host {host!r}: {exc}") from exc
    for info in infos:
        ip = ipaddress.ip_address(info[4][0])
        if _disallowed(ip):
            raise SsrfError(f"host {host!r} resolves to a disallowed address ({ip})")
