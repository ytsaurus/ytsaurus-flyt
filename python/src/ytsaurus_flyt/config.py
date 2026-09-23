"""Configuration for ytsaurus-flyt Vanilla operations."""

import math
import warnings
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import yaml

from ytsaurus_flyt.models import ClusterPreset

_VALID_SQUASHFS_DELIVERY: Tuple[str, ...] = ("layer_paths", "sandbox_unpack")
_VALID_CLUSTER_MODES: Tuple[str, ...] = ("minicluster", "application")
_VALID_SQUASHFS_COMPRESSION: Tuple[str, ...] = ("gzip", "xz", "zstd", "lz4")

# Typical OpenJDK 11 path on Debian/Ubuntu exec images; single source for FlytConfig default and CLI profile template.
DEFAULT_JAVA_HOME = "/usr/lib/jvm/java-11-openjdk-amd64"


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

    embed_squashfs_layer_jar_basenames: List[str] = field(default_factory=list)
    """Basenames (no version; optional ``.jar`` suffix) to resolve from :attr:`jar_scan_folder` and pack
    into the SquashFS layer. Listed together with :attr:`runtime_jar_basenames` for resolution; routing
    uses :func:`~ytsaurus_flyt.flink_lib_jars.partition_flink_lib_jars_for_delivery`. Disjoint from
    ``runtime_jar_basenames``.
    """

    runtime_jar_basenames: List[str] = field(default_factory=list)
    """Basenames to resolve from :attr:`jar_scan_folder` and deliver only as operation ``file_paths``
    (staged into ``flink/lib``), not inside the SquashFS. Disjoint from ``embed_squashfs_layer_jar_basenames``.
    """

    # --- Service ---
    service_name: str = ""
    """Service name (wheel cache paths, logging)."""

    # --- Environment ---
    java_home: str = DEFAULT_JAVA_HOME
    """Path to JAVA_HOME inside the job container (matches the SquashFS layout after unpack/mount)."""

    python_bin: str = "/usr/bin/python3"
    """Path to the Python binary on the exec node. Must match the ABI of
    ``runtime_python_version`` when using SquashFS (wheels in ``python-runtime/`` are
    built for that interpreter). If ``runtime_python_version`` is 3.10 but this path
    is Python 3.8, native deps (e.g. grpc) fail with import errors.
    """

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

    runtime_python_packages: List[str] = field(default_factory=list)
    """Package specs for the SquashFS layer build only (e.g. ``apache-flink==1.20.1``)."""

    runtime_python_version: str = ""
    """Python ABI for the SquashFS layer (e.g. ``3.10``); required when ``runtime_python_packages`` is set.
    Wheels and ``pip install`` run in ``python:<version>-slim`` (Docker/Podman on the build machine).
    Must match ``python_bin`` on exec nodes (same ``major.minor``).
    """

    squashfs_layer_cache_prefix: str = ""
    """Cypress directory for cached SquashFS layers (e.g. under ``//home/flyt/clusters/<profile>/layers``). Empty uses ``//tmp/flyt_squashfs_layers``."""

    squashfs_compression: str = "gzip"
    """``mksquashfs`` compression: ``gzip``, ``xz``, ``zstd``, or ``lz4`` (if supported)."""

    squashfs_layer_delivery: str = "layer_paths"
    """layer_paths: mount SquashFS on exec nodes. sandbox_unpack: ship .squashfs as file_paths and unpack in the sandbox."""

    squashfs_tools_cache_prefix: str = ""
    """Cypress directory for cached ``flyt_unsquashfs`` helper binary (``sandbox_unpack``). Empty uses ``//tmp/flyt_squashfs_tools``."""

    extra_environment: Dict[str, str] = field(default_factory=dict)
    """Additional environment variables to set in the Vanilla operation."""

    max_failed_job_count: int = 10
    """Passed to the Vanilla operation builder (``max_failed_job_count``)."""

    pre_built_layer_paths: List[str] = field(default_factory=list)
    """Pre-built Cypress layer paths (e.g. Porto layers) to use as ``layer_paths``
    instead of building a SquashFS layer.  When non-empty, ``ensure_runtime_layer``
    is skipped entirely.
    """

    # --- Cluster topology ---
    cluster_mode: str = "minicluster"
    """``minicluster``: one job runs the PyFlink script with an in-JVM MiniCluster (JobManager and
    TaskManager share a single JVM). ``application``: a Flink application cluster with a
    ``jobmanager`` task (one gang job) and a ``taskmanager`` task with :attr:`taskmanager_count` jobs.
    """

    taskmanager_count: Optional[int] = None
    """Number of TaskManager jobs (``application`` mode only). Unset: derived from :attr:`parallelism`
    (``ceil(parallelism / taskmanager_slots)``), or 1 when neither is set.
    """

    parallelism: Optional[int] = None
    """Default job parallelism (``parallelism.default``) for ``application`` mode. Also sizes the cluster
    when :attr:`taskmanager_count` is unset; with both set, ``taskmanager_count * taskmanager_slots`` must
    cover it. The pipeline's own ``set_parallelism`` still wins over the default.
    """

    taskmanager_preset: str = ""
    """``ClusterPreset`` name (``micro``, ``small``, ...) for TaskManager cpu/memory (``application`` mode).
    Empty uses the same preset as the JobManager.
    """

    taskmanager_slots: int = 1
    """``taskmanager.numberOfTaskSlots`` per TaskManager (``application`` mode)."""

    restart_completed_jobs: bool = True
    """Re-run the pipeline when it finishes successfully (streaming semantics, the operation never
    completes on its own). ``minicluster``: passed to the Vanilla task as ``restart_completed_jobs``.
    ``application``: the JobManager job re-runs the driver in place (gang tasks cannot use the YT
    option). Set ``False`` for batch jobs: the operation is completed once the driver exits 0.
    """

    discovery_timeout: int = 600
    """Seconds a TaskManager waits for the JobManager job of its incarnation to advertise its RPC
    endpoint (``application`` mode). On timeout the TaskManager job fails and YT restarts it, so a
    JobManager that never gets scheduled eventually fails the operation via ``max_failed_job_count``.
    """

    sidecar_command: str = ""
    """Shell command started in the background in every ``application``-mode job (JobManager and
    TaskManagers) right before Flink, from the service directory with the job environment and
    ``secure_vault`` keys exported; killed when the job exits. Use it for per-container helpers
    such as a metrics agent that every JVM must reach on ``localhost``.
    """

    flink_config: Dict[str, str] = field(default_factory=dict)
    """Extra Flink options for ``application`` mode, passed as ``-D key=value`` to the JobManager and
    TaskManagers after the generated ones (memory, ports, restart strategy), so they override them.
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
        m = (self.cluster_mode or "").strip().lower()
        if m not in _VALID_CLUSTER_MODES:
            raise ValueError(f"cluster_mode must be one of {list(_VALID_CLUSTER_MODES)}, got {m!r}")
        self.cluster_mode = m
        if self.taskmanager_count is not None:
            if int(self.taskmanager_count) < 1:
                raise ValueError(f"taskmanager_count must be >= 1, got {self.taskmanager_count!r}")
            self.taskmanager_count = int(self.taskmanager_count)
        if self.parallelism is not None:
            if int(self.parallelism) < 1:
                raise ValueError(f"parallelism must be >= 1, got {self.parallelism!r}")
            self.parallelism = int(self.parallelism)
        if int(self.taskmanager_slots) < 1:
            raise ValueError(f"taskmanager_slots must be >= 1, got {self.taskmanager_slots!r}")
        self.taskmanager_slots = int(self.taskmanager_slots)
        if int(self.discovery_timeout) < 1:
            raise ValueError(f"discovery_timeout must be >= 1 second, got {self.discovery_timeout!r}")
        self.discovery_timeout = int(self.discovery_timeout)
        if self.taskmanager_count is not None and self.parallelism is not None:
            capacity = self.taskmanager_count * self.taskmanager_slots
            if capacity < self.parallelism:
                raise ValueError(
                    f"parallelism {self.parallelism} does not fit into taskmanager_count * taskmanager_slots "
                    f"= {capacity}; raise taskmanager_count/taskmanager_slots or drop taskmanager_count to derive it"
                )
        self.taskmanager_preset = (self.taskmanager_preset or "").strip().lower()
        if self.taskmanager_preset and self.taskmanager_preset.upper() not in ClusterPreset.__members__:
            raise ValueError(
                f"taskmanager_preset must be one of {[m.lower() for m in ClusterPreset.__members__]}, "
                f"got {self.taskmanager_preset!r}"
            )
        self.flink_config = {str(k): str(v) for k, v in (self.flink_config or {}).items()}
        self.sidecar_command = (self.sidecar_command or "").strip()
        if self.sidecar_command and not self.is_application_cluster:
            raise ValueError("sidecar_command is only supported with cluster_mode: application")
        self.embed_squashfs_layer_jar_basenames = _normalize_jar_basename_list(self.embed_squashfs_layer_jar_basenames)
        self.runtime_jar_basenames = _normalize_jar_basename_list(self.runtime_jar_basenames)
        emb = set(self.embed_squashfs_layer_jar_basenames)
        run = set(self.runtime_jar_basenames)
        if emb & run:
            raise ValueError(
                "Basenames cannot appear in both embed_squashfs_layer_jar_basenames and runtime_jar_basenames: "
                f"{sorted(emb & run)}"
            )

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

    @property
    def is_application_cluster(self) -> bool:
        return self.cluster_mode == "application"

    @property
    def effective_taskmanager_count(self) -> int:
        """TaskManager jobs to start: explicit count, else enough slots for ``parallelism``, else 1."""
        if self.taskmanager_count is not None:
            return self.taskmanager_count
        if self.parallelism is not None:
            return max(1, math.ceil(self.parallelism / self.taskmanager_slots))
        return 1

    @property
    def effective_parallelism(self) -> int:
        """``parallelism.default`` for the cluster: explicit value, else every slot of every TaskManager."""
        if self.parallelism is not None:
            return self.parallelism
        return self.effective_taskmanager_count * self.taskmanager_slots


# Messages shared with validate_flyt_config row output
SQUASHFS_VALIDATE_RUNTIME_PACKAGES_MSG = "required for SquashFS"
SQUASHFS_VALIDATE_RUNTIME_VERSION_MSG = "required (e.g. 3.10); must match python_bin on exec nodes"


def require_squashfs_runtime_config(config: FlytConfig) -> None:
    """Raise if SquashFS layer prerequisites are missing."""
    if config.pre_built_layer_paths:
        return
    if not config.runtime_python_packages:
        raise ValueError("runtime_python_packages is required (e.g. apache-flink) to build the SquashFS runtime layer.")
    if not (config.runtime_python_version or "").strip():
        raise ValueError(
            'runtime_python_version is required (e.g. "3.10") for SquashFS; '
            "it must match python_bin on exec nodes (same major.minor)."
        )
