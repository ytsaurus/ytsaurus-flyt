"""Configuration for ytsaurus-flyt Vanilla operations."""

import warnings
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import yaml

_VALID_SQUASHFS_DELIVERY: Tuple[str, ...] = ("layer_paths", "sandbox_unpack")
_VALID_SQUASHFS_COMPRESSION: Tuple[str, ...] = ("gzip", "xz", "zstd", "lz4")

# Eclipse Temurin JRE feature version bundled into the layer by default.
DEFAULT_JAVA_VERSION = "11"


def _normalize_jar_basename_list(val: Any) -> List[str]:
    """Coerce YAML values to a list of non-empty basename strings (handles a stray string or list)."""
    if val is None:
        return []
    if isinstance(val, str):
        s = val.strip()
        return [s] if s else []
    if isinstance(val, (list, tuple)):
        out: List[str] = []
        for x in val:
            if isinstance(x, str) and x.strip():
                out.append(x.strip())
        return out
    return []


@dataclass
class FlytConfig:
    """Launcher configuration for PyFlink on YTsaurus Vanilla operations."""

    # --- JAR resolution ---
    jar_scan_folder: str = ""
    """YT path to a folder of JARs for ``flink/lib``, referenced by basename below."""

    runtime_jar_basenames: List[str] = field(default_factory=list)
    """Basenames (no version; optional ``.jar`` suffix) resolved from :attr:`jar_scan_folder` and
    delivered as ``file_paths`` into ``flink/lib`` at runtime (never baked into the layer)."""

    # --- Service ---
    service_name: str = ""
    """Service name (wheel cache paths, logging)."""

    # --- Runtime versions (bundled into the self-contained layer) ---
    java_version: str = DEFAULT_JAVA_VERSION
    """Eclipse Temurin JRE feature version to bundle into the layer (e.g. ``11``, ``17``); from the official image."""

    # --- Optional ---
    network_project: Optional[str] = None
    """YT network project for the Vanilla operation (optional)."""

    wheel_cache_prefix: Optional[str] = None
    """YT path prefix for caching service wheels (optional).
    Example: '//home/my-project/wheel_cache'
    """

    yt_ui_base_url: str = ""
    """Base URL for YT Web UI (for operation tracking links).
    Example: 'https://yt.my-cluster.example.com'
    """

    extra_file_paths: List[str] = field(default_factory=list)
    """Additional YT file paths to include in the operation (configs, etc.)."""

    flink_version: str = "1.20.1"
    """Apache Flink version for the runtime layer; flyt installs ``apache-flink==<flink_version>``.
    flyt ships the Flink runtime only — bring your own packages in a separate layer or your job wheel."""

    runtime_python_version: str = ""
    """Python ``major.minor`` (e.g. ``3.10``), required to build the layer. A relocatable CPython of
    this version is bundled in and pyflink wheels are built for it, so the job ignores the node's Python."""

    squashfs_layer_paths: List[str] = field(default_factory=list)
    """Ordered Cypress paths to pre-built ``.squashfs`` layers, used verbatim (no hash, no build) and
    delivered per :attr:`squashfs_layer_delivery`. Build with ``flyt build layer --upload <path>``;
    paths must already exist. Later layers overlay earlier ones."""

    squashfs_compression: str = "gzip"
    """``mksquashfs`` compression: ``gzip``, ``xz``, ``zstd``, or ``lz4`` (if supported)."""

    squashfs_layer_delivery: str = "layer_paths"
    """layer_paths: mount SquashFS on exec nodes. sandbox_unpack: ship .squashfs as file_paths and unpack in the sandbox."""

    unsquashfs_path: str = ""
    """Cypress path to an ``unsquashfs`` binary for ``sandbox_unpack`` (build with ``flyt build unsquashfs --upload <path>``).
    Shipped as a file_path so the job can unpack the layer. Empty ⇒ the exec image must provide ``unsquashfs`` on PATH."""

    extra_environment: Dict[str, str] = field(default_factory=dict)
    """Additional environment variables to set in the Vanilla operation."""

    max_failed_job_count: int = 10
    """Passed to the Vanilla operation builder (``max_failed_job_count``)."""

    pre_built_layer_paths: List[str] = field(default_factory=list)
    """Ready-made Cypress ``layer_paths`` (e.g. Porto layers) to use verbatim instead of a SquashFS layer."""

    yt_client_config: Optional[Dict[str, Any]] = None
    """Raw overrides deep-merged into the ``YtClient`` config. Use for cluster quirks,
    e.g. a proxy that advertises unreachable internal hosts::

        yt_client_config:
          proxy:
            enable_proxy_discovery: false
    """

    def __post_init__(self) -> None:
        d = (self.squashfs_layer_delivery or "").strip()
        c = (self.squashfs_compression or "").strip().lower()
        if d not in _VALID_SQUASHFS_DELIVERY:
            raise ValueError(f"squashfs_layer_delivery must be one of {list(_VALID_SQUASHFS_DELIVERY)}, got {d!r}")
        if c not in _VALID_SQUASHFS_COMPRESSION:
            raise ValueError(f"squashfs_compression must be one of {list(_VALID_SQUASHFS_COMPRESSION)}, got {c!r}")
        self.squashfs_layer_delivery = d
        self.squashfs_compression = c
        self.runtime_jar_basenames = _normalize_jar_basename_list(self.runtime_jar_basenames)

    @property
    def flink_requirements(self) -> List[str]:
        """pip requirements for the Flink runtime layer, derived from ``flink_version``."""
        v = (self.flink_version or "").strip()
        return [f"apache-flink=={v}"] if v else []

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to a plain dict (dataclass fields only)."""
        return asdict(self)

    def to_yaml(self) -> str:
        """Serialize to YAML text."""
        return yaml.safe_dump(
            self.to_dict(),
            default_flow_style=False,
            allow_unicode=True,
            sort_keys=False,
        )

    @classmethod
    def from_yaml(cls, path: str) -> "FlytConfig":
        """Load ``FlytConfig`` from a YAML file on disk (e.g. tests)."""
        with open(path, "r") as f:
            data = yaml.safe_load(f) or {}
        unknown = set(data) - set(cls.__dataclass_fields__)
        if unknown:
            warnings.warn(
                f"Ignoring unknown keys in {path}: {sorted(unknown)}",
                UserWarning,
                stacklevel=2,
            )
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


# Messages shared with validate_flyt_config row output
SQUASHFS_VALIDATE_FLINK_VERSION_MSG = "required for SquashFS (e.g. 1.20.1)"
SQUASHFS_VALIDATE_RUNTIME_VERSION_MSG = "required (e.g. 3.10); a matching CPython is bundled into the layer"


def require_squashfs_runtime_config(config: FlytConfig) -> None:
    """Raise if SquashFS layer prerequisites are missing."""
    if config.pre_built_layer_paths:
        return
    if not (config.flink_version or "").strip():
        raise ValueError("flink_version is required (e.g. 1.20.1) to build the SquashFS runtime layer.")
    if not (config.runtime_python_version or "").strip():
        raise ValueError('runtime_python_version is required (e.g. "3.10") to build the SquashFS runtime layer.')
