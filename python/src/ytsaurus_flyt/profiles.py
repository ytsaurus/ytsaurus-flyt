"""Named flyt profiles: ~/.config/flyt/profiles/<name>.yaml and active profile."""

from __future__ import annotations

import os
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import yaml

from ytsaurus_flyt.config import FlytConfig

PROFILE_META_KEYS = frozenset({"proxy", "pool", "preset", "cypress_base_path"})


def default_cypress_base_path(profile_name: str) -> str:
    """Default Cypress prefix for a new profile (per-cluster layout).

    Uses ``//home/flyt/clusters/<name>`` so team-wide assets can live under
    ``//home/flyt/libraries/`` (e.g. ``flink-connector-ytsaurus.jar``) without
    colliding with per-cluster cache paths.
    """
    return f"//home/flyt/clusters/{profile_name.strip()}"


def get_config_dir() -> Path:
    """Config root: FLYT_CONFIG_DIR or ~/.config/flyt (XDG-style)."""
    base = os.environ.get("FLYT_CONFIG_DIR", "").strip()
    if base:
        return Path(base).expanduser()
    return Path.home() / ".config" / "flyt"


def profiles_dir() -> Path:
    d = get_config_dir() / "profiles"
    d.mkdir(parents=True, exist_ok=True)
    return d


def active_profile_path() -> Path:
    return get_config_dir() / "active"


def profile_yaml_path(name: str) -> Path:
    return profiles_dir() / f"{name}.yaml"


def read_active_profile_name() -> Optional[str]:
    p = active_profile_path()
    if not p.is_file():
        return None
    try:
        raw = p.read_text(encoding="utf-8").strip()
        return raw or None
    except OSError:
        return None


def write_active_profile_name(name: Optional[str]) -> None:
    p = active_profile_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    if name:
        p.write_text(name + "\n", encoding="utf-8")
    elif p.exists():
        p.unlink()


def list_profile_names() -> list[str]:
    d = profiles_dir()
    if not d.is_dir():
        return []
    return sorted(p.stem for p in d.glob("*.yaml"))


def load_profile_dict(name: str) -> Dict[str, Any]:
    path = profile_yaml_path(name)
    if not path.is_file():
        raise FileNotFoundError(f"Profile not found: {name!r} ({path})")
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Invalid profile YAML (expected mapping): {path}")
    return data


def save_profile_dict(name: str, data: Dict[str, Any]) -> None:
    path = profile_yaml_path(name)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(
            data,
            f,
            default_flow_style=False,
            allow_unicode=True,
            sort_keys=False,
        )


def split_profile_meta(data: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Split profile YAML into meta (proxy, pool, preset, cypress_base_path) and Flyt fields."""
    meta: Dict[str, Any] = {}
    flyt: Dict[str, Any] = {}
    for k, v in data.items():
        if k in PROFILE_META_KEYS:
            meta[k] = v
        else:
            flyt[k] = v
    return meta, flyt


def apply_cypress_base_path(cfg: FlytConfig, cypress_base_path: str) -> FlytConfig:
    """Derive squashfs cache prefixes from cypress_base_path when not set."""
    base = (cypress_base_path or "").strip().rstrip("/")
    if not base:
        return cfg
    layers = (cfg.squashfs_layer_cache_prefix or "").strip()
    tools = (cfg.squashfs_tools_cache_prefix or "").strip()
    if not layers:
        cfg = replace(cfg, squashfs_layer_cache_prefix=f"{base}/layers")
    if not tools:
        cfg = replace(cfg, squashfs_tools_cache_prefix=f"{base}/tools")
    wheels = (cfg.wheel_cache_prefix or "").strip()
    if not wheels:
        cfg = replace(cfg, wheel_cache_prefix=f"{base}/wheels")
    return cfg


def dict_to_flyt_config(data: Dict[str, Any]) -> FlytConfig:
    """Build FlytConfig from a dict (only known fields)."""
    return FlytConfig(**{k: v for k, v in data.items() if k in FlytConfig.__dataclass_fields__})


def profile_dict_to_flyt_config(data: Dict[str, Any]) -> FlytConfig:
    """Full profile YAML dict to FlytConfig (meta keys stripped, cypress_base_path applied)."""
    meta, flyt = split_profile_meta(data)
    cfg = dict_to_flyt_config(flyt)
    cbp = meta.get("cypress_base_path")
    if isinstance(cbp, str) and cbp.strip():
        cfg = apply_cypress_base_path(cfg, cbp)
    return cfg


def _is_empty_value(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, str) and not v.strip():
        return True
    if isinstance(v, (list, dict)) and len(v) == 0:
        return True
    return False


def merge_yaml_dict(base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
    """Overlay wins for non-empty values."""
    out = dict(base)
    for k, v in overlay.items():
        if not _is_empty_value(v):
            out[k] = v
    return out


def merge_flyt_config(base: FlytConfig, *overlays: FlytConfig) -> FlytConfig:
    """Later overlays override earlier when values are non-empty."""
    d = asdict(base)
    for ov in overlays:
        for k, v in asdict(ov).items():
            if not _is_empty_value(v):
                d[k] = v
    return FlytConfig(**d)


def resolve_connection_from_profile(
    data: Dict[str, Any],
) -> Tuple[Optional[str], Optional[str], str]:
    """Return proxy, pool, preset from profile dict (may be None)."""
    meta, _ = split_profile_meta(data)
    proxy = meta.get("proxy")
    pool = meta.get("pool")
    preset = meta.get("preset") or "micro"
    if isinstance(proxy, str):
        proxy = proxy.strip() or None
    if isinstance(pool, str):
        pool = pool.strip() or None
    if isinstance(preset, str):
        preset = preset.strip() or "micro"
    return (
        proxy if isinstance(proxy, str) else None,
        pool if isinstance(pool, str) else None,
        preset,
    )


def resolve_effective_profile_name(cli_profile: Optional[str]) -> Optional[str]:
    """CLI --profile or FLYT_PROFILE env, then active file."""
    name = (cli_profile or os.environ.get("FLYT_PROFILE") or "").strip()
    if name:
        return name
    return read_active_profile_name()
