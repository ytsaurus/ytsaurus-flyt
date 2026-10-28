"""Configuration for ytsaurus-flyt Vanilla operations."""

import warnings
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import yaml

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

    job_yt_proxy: str = ""
    """YT HTTP proxy as reachable from inside the jobs, for ``application`` mode discovery.
    Defaults to the client proxy; set it when the client goes through a tunnel or port-forward
    (e.g. ``http-proxies.default.svc.cluster.local`` for a Kind cluster reached via localhost).
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

    max_failed_job_count: Optional[int] = None
    """Failed YT jobs before the operation fails (``max_failed_job_count``), passed as is when set. Unset:
    10 in ``minicluster`` mode, ``max(10, 5 * taskmanager_count)`` in ``application`` mode."""

    pre_built_layer_paths: List[str] = field(default_factory=list)
    """Pre-built Cypress layer paths (e.g. Porto layers) to use as ``layer_paths``
    instead of building a SquashFS layer.  When non-empty, ``ensure_runtime_layer``
    is skipped entirely.
    """

    # --- Cluster topology ---
    cluster_mode: str = "minicluster"
    """``minicluster``: one YT job runs the PyFlink script with an in-JVM MiniCluster (JobManager and
    TaskManager share a single JVM). ``application``: a Flink application cluster with a
    ``jobmanager`` task (one gang YT job) and a ``taskmanager`` task with :attr:`taskmanager_count` jobs.
    """

    taskmanager_count: int = 1
    """Number of TaskManager YT jobs (``application`` mode only): the cluster's capacity, like the
    TaskManager ``replicas`` of a standalone Flink cluster on Kubernetes. Size it for the highest
    operator parallelism of the pipeline: ``taskmanager_count * taskmanager.numberOfTaskSlots`` slots.
    Unless the Flink configuration sets ``parallelism.default``, it becomes exactly that many slots.
    """

    jobmanager_cpu: Optional[int] = None
    """CPU limit of the JobManager YT job (``application`` mode; default 2). Memory has no flyt field: the
    container gets ``jobmanager.memory.process.size`` from the Flink configuration (default 2g) plus 512m
    for the PyFlink driver and helpers. The ``preset`` applies to ``minicluster`` only.
    """

    taskmanager_cpu: Optional[int] = None
    """CPU limit of one TaskManager YT job (``application`` mode; default: one cpu per task slot, like
    ``kubernetes.taskmanager.cpu``). Memory has no flyt field: the
    container gets ``taskmanager.memory.process.size`` from the Flink configuration (default 4g) plus
    512m for the non-JVM processes.
    """

    discovery_timeout: int = 300
    """Seconds a TaskManager waits for the JobManager YT job of its incarnation to advertise its RPC
    endpoint (``application`` mode); the default matches Flink's ``taskmanager.registration.timeout``.
    On timeout the TaskManager YT job fails and YT restarts it, so a JobManager that never gets
    scheduled eventually fails the operation via ``max_failed_job_count``.
    """

    sidecar_command: str = ""
    """Shell command started in the background in every ``application``-mode job (JobManager and
    TaskManagers) right before Flink, from the service directory with the job environment and
    ``secure_vault`` keys exported; killed when the job exits. Use it for per-container helpers
    such as a metrics agent that every JVM must reach on ``localhost``. Ignored in ``minicluster``
    mode (the profile may keep it while the mode is chosen per run).
    """

    base_flink_conf_path: str = ""
    """Flink ``config.yaml`` (nested or flat keys) for ``application`` mode: a local file or a Cypress
    file path (``//...``). Merged over flyt's generated cluster options and under :attr:`flink_config_override`;
    the result is written to ``flink/conf/config.yaml`` in every JobManager/TaskManager YT job.
    Per-container keys (``jobmanager.rpc.address``, ``rest.address``, ``taskmanager.host``,
    ``taskmanager.resource-id``) are dropped: the run scripts set them. Ignored (with a warning) in
    ``minicluster`` mode, where the script sets its Flink options in code.
    """

    flink_config_override: Dict[str, str] = field(default_factory=dict)
    """Extra Flink options for ``application`` mode, applied over the generated options and
    :attr:`base_flink_conf_path` into the cluster ``config.yaml``. Ignored (with a warning) in ``minicluster``.
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
        # Positive integers; the cpu fields may also be None (resolved later from defaults / slots).
        for name in (
            "taskmanager_count",
            "discovery_timeout",
            "jobmanager_cpu",
            "taskmanager_cpu",
            "max_failed_job_count",
        ):
            value = getattr(self, name)
            if value is None and name in ("jobmanager_cpu", "taskmanager_cpu", "max_failed_job_count"):
                continue
            if value is None or int(value) < 1:
                raise ValueError(f"{name} must be >= 1, got {value!r}")
            setattr(self, name, int(value))
        self.flink_config_override = {str(k): str(v) for k, v in (self.flink_config_override or {}).items()}
        self.base_flink_conf_path = (self.base_flink_conf_path or "").strip()
        self.job_yt_proxy = (self.job_yt_proxy or "").strip()
        self.sidecar_command = (self.sidecar_command or "").strip()
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
