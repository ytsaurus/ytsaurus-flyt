"""Credential resolution for ytsaurus-flyt Vanilla operations.

Supports:
- Optional ``extra_secrets`` dict for programmatic callers (e.g. internal wrappers)
  that resolve credentials outside this package
- ``FLYT_SECURE_<NAME>`` mapped to secure_vault key NAME (e.g. ``FLYT_SECURE_MY_SECRET``)
- YT client token fallback for ``YT_USER`` / ``YT_TOKEN`` when not set in env or extras
"""

import logging
import os
from typing import Dict, Optional

from yt.wrapper import YtClient
from yt.wrapper.http_helpers import get_token, get_user_name

logger = logging.getLogger(__name__)

FLYT_SECURE_PREFIX = "FLYT_SECURE_"


def _collect_flyt_secure_from_env() -> Dict[str, str]:
    """Map FLYT_SECURE_<KEY>=value to {KEY: value}."""
    out: Dict[str, str] = {}
    prefix = FLYT_SECURE_PREFIX
    for k, v in os.environ.items():
        if not k.startswith(prefix) or len(k) <= len(prefix):
            continue
        if not v:
            continue
        vault_key = k[len(prefix) :]
        out[vault_key] = v
    return out


def get_secure_credentials(
    yt_client: YtClient,
    extra_secrets: Optional[Dict[str, str]] = None,
) -> Dict[str, str]:
    """Build secure_vault dict for the Vanilla operation.

    Merge order: ``extra_secrets`` first, then ``FLYT_SECURE_*`` environment variables
    (env wins on duplicate keys), then ``YT_USER`` / ``YT_TOKEN`` if still missing
    (from env or YT client).
    """
    extra = dict(extra_secrets or {})

    def _get_value(key: str, default: Optional[str] = None) -> Optional[str]:
        if key in extra and extra[key]:
            return extra[key]
        env_value = os.getenv(f"FLYT_{key}") or os.getenv(key)
        if env_value:
            return env_value
        return default

    yt_user = _get_value("YT_USER") or get_user_name(client=yt_client)
    yt_token = _get_value("YT_TOKEN") or get_token(client=yt_client)

    credentials: Dict[str, str] = {}
    for k, v in extra.items():
        if v:
            credentials[k] = v
    credentials.update(_collect_flyt_secure_from_env())
    if "YT_USER" not in credentials and yt_user:
        credentials["YT_USER"] = yt_user
    if "YT_TOKEN" not in credentials and yt_token:
        credentials["YT_TOKEN"] = yt_token

    for key, value in sorted(credentials.items()):
        status = "set" if value else "NOT SET"
        logger.info("Credential %s: %s", key, status)

    return credentials
