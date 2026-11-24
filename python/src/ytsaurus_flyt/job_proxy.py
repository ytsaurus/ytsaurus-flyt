"""The YT HTTP proxy as the jobs of an application cluster must reach it.

The client may talk to YT through a port-forward or a tunnel (``localhost:<port>``), which the
TaskManagers' discovery helper inside the cluster cannot use; then the cluster's own proxy hosts
(``GET /hosts``) are handed to the jobs instead.
"""

from __future__ import annotations

import json
import urllib.parse
import urllib.request
from typing import List

from yt.wrapper import YtClient

_LOOPBACK_HOSTS = {"localhost", "127.0.0.1", "::1"}


def is_loopback_proxy(proxy_url: str) -> bool:
    """True when the client reaches YT through localhost (a port-forward or tunnel), which jobs cannot."""
    raw = proxy_url if "://" in proxy_url else "http://" + proxy_url
    host = (urllib.parse.urlsplit(raw).hostname or "").lower()
    return host in _LOOPBACK_HOSTS or host.endswith(".localhost")


def discover_job_proxies(yt_client: YtClient, proxy_url: str) -> List[str]:
    """Proxy hosts as the cluster names them (``GET /hosts``), reachable from inside jobs.

    This is what heavy-command discovery in the ``yt`` client uses; on Kind it yields
    ``hp-0.http-proxies.default.svc.cluster.local`` instead of the port-forwarded ``localhost``.
    """
    base = proxy_url if "://" in proxy_url else "http://" + proxy_url
    headers = {}
    token = None
    try:
        token = yt_client.config.get("token")  # type: ignore[union-attr]
    except Exception:  # noqa: BLE001 - config shape differs between client versions
        token = None
    if token:
        headers["Authorization"] = "OAuth " + str(token)
    req = urllib.request.Request(base.rstrip("/") + "/hosts", headers=headers)
    with urllib.request.urlopen(req, timeout=30) as resp:
        hosts = json.loads(resp.read().decode("utf-8"))
    return [str(h) for h in hosts if h]


def _with_scheme(proxy: str, scheme: str) -> str:
    return proxy if "://" in proxy else f"{scheme}://{proxy}"


def job_yt_proxy(yt_client: YtClient, proxy_url: str, configured: str = "") -> str:
    """``FLYT_YT_PROXY`` for the jobs: the configured ``job_yt_proxy``, else the client proxy, unless that
    is a loopback address, in which case the cluster's own proxy hosts (comma-separated, tried in order).

    Proxies without a scheme get the client's one: not every cluster serves HTTPS (Kind does not), but
    the jobs can use whatever the client already talks to.
    """
    scheme = urllib.parse.urlsplit(proxy_url).scheme if "://" in proxy_url else "http"
    if configured:
        return ",".join(_with_scheme(p.strip(), scheme) for p in configured.split(",") if p.strip())
    if not is_loopback_proxy(proxy_url):
        return _with_scheme(proxy_url, scheme)
    hosts = discover_job_proxies(yt_client, proxy_url)
    if not hosts:
        raise ValueError(
            f"{proxy_url} is a loopback address jobs cannot reach and {proxy_url}/hosts lists no proxies; "
            "set job_yt_proxy in the profile"
        )
    return ",".join(_with_scheme(h, scheme) for h in hosts)
