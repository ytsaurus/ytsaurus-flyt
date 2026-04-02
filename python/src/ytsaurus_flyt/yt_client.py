"""YtClient construction for flyt (proxy URL, localhost discovery quirks)."""

from __future__ import annotations

import os
from typing import Any, Dict, Optional

from yt.wrapper import YtClient


def env_yt_token() -> Optional[str]:
    """Non-empty token from env, or None so YtClient falls back to ~/.yt/token (same as ``yt`` CLI)."""
    for key in ("FLYT_YT_TOKEN", "YT_TOKEN"):
        raw = os.getenv(key)
        if raw and str(raw).strip():
            return str(raw).strip()
    return None


def yt_client_config_for_proxy(proxy: str) -> Optional[Dict[str, Any]]:
    p = (proxy or "").lower()
    if "127.0.0.1" in p or "localhost" in p or "[::1]" in p or p.startswith("::1") or p.startswith("[::1]"):
        return {
            "apply_remote_patch_at_start": False,
            "proxy": {"enable_proxy_discovery": False},
        }
    return None


def make_yt_client(proxy: str) -> YtClient:
    """Build ``YtClient`` for ``proxy`` with token from env (if set) and localhost-friendly config."""
    return YtClient(
        proxy=proxy,
        token=env_yt_token(),
        config=yt_client_config_for_proxy(proxy),
    )


def proxy_url_from_client(yt_client: YtClient) -> str:
    """Best-effort HTTP proxy URL string for logging and UI links."""
    cfg = getattr(yt_client, "config", None)
    if isinstance(cfg, dict):
        proxy = cfg.get("proxy") or {}
        if isinstance(proxy, dict):
            url = proxy.get("url")
            if url:
                return str(url)
    return "unknown"
