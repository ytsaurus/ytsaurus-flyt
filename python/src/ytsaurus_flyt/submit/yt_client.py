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


def _deep_merge(base: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively merge ``overrides`` into ``base`` (override wins; nested dicts merge)."""
    out = dict(base)
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = value
    return out


def yt_client_config_for_proxy(proxy: str, overrides: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    """Localhost-friendly defaults, with profile ``yt_client_config`` overrides merged on top."""
    p = (proxy or "").lower()
    base: Dict[str, Any] = {}
    if "127.0.0.1" in p or "localhost" in p or "[::1]" in p or p.startswith("::1") or p.startswith("[::1]"):
        base = {
            "apply_remote_patch_at_start": False,
            "proxy": {"enable_proxy_discovery": False},
        }
    merged = _deep_merge(base, overrides) if overrides else base
    return merged or None


def make_yt_client(proxy: str, config_overrides: Optional[Dict[str, Any]] = None) -> YtClient:
    """Build ``YtClient`` for ``proxy`` with token from env (if set) and merged config.

    ``config_overrides`` (a profile's ``yt_client_config``) is deep-merged over the
    localhost-friendly defaults.
    """
    return YtClient(
        proxy=proxy,
        token=env_yt_token(),
        config=yt_client_config_for_proxy(proxy, config_overrides),
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
