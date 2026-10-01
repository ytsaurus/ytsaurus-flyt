"""ytsaurus-flyt: Run PyFlink jobs on YTsaurus Vanilla operations."""

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version

from ytsaurus_flyt.config.config import FlytConfig
from ytsaurus_flyt.config.models import (
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
    "FlinkLibJarsResolveResult",
    "resolve_flink_lib_jars",
    "get_secure_credentials",
    "validate_flyt_config",
    "make_yt_client",
]

try:
    __version__ = _pkg_version("ytsaurus-flyt")
except PackageNotFoundError:
    __version__ = "0.0.0+unknown"


def __getattr__(name):
    """Lazy imports for modules that depend on the YTsaurus client."""
    _lazy = {
        "launch_vanilla_job": "ytsaurus_flyt.submit.launcher",
        "build_vanilla_operation_spec": "ytsaurus_flyt.submit.spec",
        "FlinkLibJarsResolveResult": "ytsaurus_flyt.runtime.flink_lib_jars",
        "resolve_flink_lib_jars": "ytsaurus_flyt.runtime.flink_lib_jars",
        "get_secure_credentials": "ytsaurus_flyt.submit.credentials",
        "validate_flyt_config": "ytsaurus_flyt.config.validate_config",
        "make_yt_client": "ytsaurus_flyt.submit.yt_client",
    }
    if name in _lazy:
        import importlib

        return getattr(importlib.import_module(_lazy[name]), name)
    raise AttributeError(f"module 'ytsaurus_flyt' has no attribute {name!r}")
