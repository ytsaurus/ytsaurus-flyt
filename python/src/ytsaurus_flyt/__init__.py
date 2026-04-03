"""ytsaurus-flyt: Run PyFlink jobs on YTsaurus Vanilla operations."""

from ytsaurus_flyt.config import FlytConfig
from ytsaurus_flyt.models import (
    ClusterParams,
    ClusterPreset,
    JobmanagerParams,
    OperationParams,
)

__all__ = [
    "FlytConfig",
    "launch_vanilla_job",
    "ClusterParams",
    "ClusterPreset",
    "JobmanagerParams",
    "OperationParams",
    "build_vanilla_operation_spec",
    "ensure_runtime_layer",
    "FlinkLibJarsResolveResult",
    "resolve_flink_lib_jars",
    "partition_flink_lib_jars_for_delivery",
    "get_secure_credentials",
    "validate_flyt_config",
    "make_yt_client",
]

__version__ = "0.1.0"


def __getattr__(name):
    """Lazy imports for modules that depend on yt.wrapper."""
    _lazy = {
        "launch_vanilla_job": "ytsaurus_flyt.launcher",
        "build_vanilla_operation_spec": "ytsaurus_flyt.spec",
        "ensure_runtime_layer": "ytsaurus_flyt.layer_builder",
        "FlinkLibJarsResolveResult": "ytsaurus_flyt.flink_lib_jars",
        "resolve_flink_lib_jars": "ytsaurus_flyt.flink_lib_jars",
        "partition_flink_lib_jars_for_delivery": "ytsaurus_flyt.flink_lib_jars",
        "get_secure_credentials": "ytsaurus_flyt.credentials",
        "validate_flyt_config": "ytsaurus_flyt.validate_config",
        "make_yt_client": "ytsaurus_flyt.yt_client",
    }
    if name in _lazy:
        import importlib
        return getattr(importlib.import_module(_lazy[name]), name)
    raise AttributeError(f"module 'ytsaurus_flyt' has no attribute {name!r}")
