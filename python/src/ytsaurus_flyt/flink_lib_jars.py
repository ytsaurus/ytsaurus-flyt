"""Resolve JARs from Cypress for Flink ``lib/`` (SquashFS layer vs Vanilla ``file_paths``)."""

from __future__ import annotations

import logging
import os
import posixpath
from dataclasses import dataclass
from typing import AbstractSet, List, Optional, Set, Tuple

from yt.wrapper import YtClient

from ytsaurus_flyt.config import FlytConfig
from ytsaurus_flyt.jar_utils import JarInfoExtractionError, extract_jar_info

logger = logging.getLogger(__name__)

_JAR_EXT = ".jar"


@dataclass(frozen=True)
class FlinkLibJarsResolveResult:
    yt_paths: List[str]
    extra_runtime_basenames: frozenset[str] = frozenset()


def _normalized_jar_basename(item: str) -> str:
    s = (item or "").strip()
    if s.lower().endswith(_JAR_EXT):
        s = s[: -len(_JAR_EXT)]
    return s


def _basenames_from_config_list(items: List[str], field_name: str) -> Set[str]:
    out: Set[str] = set()
    for item in items:
        if not isinstance(item, str):
            raise TypeError("%s must be a list of strings; got %r" % (field_name, item))
        b = _normalized_jar_basename(item)
        if b:
            out.add(b)
    return out


def _collect_config_jar_basenames(config: FlytConfig) -> Set[str]:
    b_run = _basenames_from_config_list(config.runtime_jar_basenames, "runtime_jar_basenames")
    b_emb = _basenames_from_config_list(config.embed_squashfs_layer_jar_basenames, "embed_squashfs_layer_jar_basenames")
    return b_run | b_emb


def resolve_flink_lib_jars(
    yt_client: YtClient,
    config: FlytConfig,
    extra_basenames: Optional[List[str]] = None,
) -> FlinkLibJarsResolveResult:
    """Latest JAR per basename under ``jar_scan_folder``. Optional ``extra_basenames`` → ``extra_runtime_basenames``."""
    empty = FlinkLibJarsResolveResult([], frozenset())

    jar_scan_folder = (config.jar_scan_folder or "").strip().rstrip("/")
    if not jar_scan_folder:
        logger.warning("jar_scan_folder is not configured, skipping Flink lib JAR resolution")
        return empty

    basenames_to_resolve = _collect_config_jar_basenames(config)
    extra_runtime: Set[str] = set()
    if extra_basenames:
        for x in extra_basenames:
            b = _normalized_jar_basename(x) if isinstance(x, str) else ""
            if b:
                basenames_to_resolve.add(b)
                extra_runtime.add(b)

    if not basenames_to_resolve:
        logger.info("No Flink lib JAR basenames to resolve")
        return empty

    logger.info("Resolving Flink lib JARs: %s", sorted(basenames_to_resolve))

    resolved_paths = []
    all_jars_in_folder = list(yt_client.list(jar_scan_folder))

    for basename in basenames_to_resolve:
        matches = []
        for jar_name in all_jars_in_folder:
            try:
                info = extract_jar_info(jar_name)
                if info.basename == basename:
                    matches.append((info.version, jar_name))
            except (ValueError, JarInfoExtractionError):
                pass

        if not matches:
            logger.warning(
                "Could not find JAR for basename %s in %s",
                basename,
                jar_scan_folder,
            )
            continue

        matches.sort(key=lambda x: x[0], reverse=True)
        latest_jar_name = matches[0][1]
        latest_jar_name = str(latest_jar_name).strip()
        if latest_jar_name.startswith("//"):
            resolved_path = latest_jar_name
        else:
            resolved_path = f"{jar_scan_folder}/{latest_jar_name}"
        logger.debug("Resolved %s to %s", basename, resolved_path)
        resolved_paths.append(resolved_path)

    return FlinkLibJarsResolveResult(resolved_paths, frozenset(extra_runtime))


def _resolved_jar_basename_key(yt_path: str) -> str:
    base_name = posixpath.basename(yt_path.rstrip("/"))
    try:
        return extract_jar_info(base_name).basename
    except (ValueError, JarInfoExtractionError):
        return base_name.rsplit(".", 1)[0] if "." in base_name else base_name


def partition_flink_lib_jars_for_delivery(
    config: FlytConfig,
    all_yt_paths: List[str],
    *,
    extra_runtime_basenames: Optional[AbstractSet[str]] = None,
) -> Tuple[List[str], List[str]]:
    """Return ``(squashfs_paths, file_paths_paths)``. ``extra_runtime_basenames`` always go to ``file_paths``."""
    extra_runtime = frozenset(extra_runtime_basenames or ())

    embed = _basenames_from_config_list(
        list(config.embed_squashfs_layer_jar_basenames or []),
        "embed_squashfs_layer_jar_basenames",
    )
    runtime_only = _basenames_from_config_list(
        list(config.runtime_jar_basenames or []),
        "runtime_jar_basenames",
    )

    if not embed and not runtime_only:
        if not extra_runtime:
            return (list(all_yt_paths), [])
        squashfs_paths: List[str] = []
        file_paths_paths: List[str] = []
        for yt_path in all_yt_paths:
            bkey = _resolved_jar_basename_key(yt_path)
            if bkey in extra_runtime:
                file_paths_paths.append(yt_path)
            else:
                squashfs_paths.append(yt_path)
        return (squashfs_paths, file_paths_paths)

    squashfs_paths = []
    file_paths_paths = []
    for yt_path in all_yt_paths:
        bkey = _resolved_jar_basename_key(yt_path)
        if bkey in extra_runtime:
            file_paths_paths.append(yt_path)
        elif bkey in runtime_only:
            file_paths_paths.append(yt_path)
        elif bkey in embed:
            squashfs_paths.append(yt_path)
        else:
            # Unlisted basename: if embed list non-empty use file_paths; else layer
            if embed:
                file_paths_paths.append(yt_path)
            else:
                squashfs_paths.append(yt_path)

    return (squashfs_paths, file_paths_paths)


def download_flink_lib_jars(yt_client: YtClient, yt_paths: List[str], output_dir: str) -> List[str]:
    if not yt_paths:
        return []

    os.makedirs(output_dir, exist_ok=True)
    local_paths: List[str] = []
    for yt_path in yt_paths:
        base = posixpath.basename(yt_path.rstrip("/"))
        local_path = os.path.join(output_dir, base)
        _read_yt_file_to_disk(yt_client, yt_path, local_path)
        local_paths.append(local_path)
    return local_paths


def _read_yt_file_to_disk(yt_client: YtClient, yt_path: str, local_path: str) -> None:
    # YtClient.read_file(path) returns bytes or a stream of chunks (no output-buffer overload).
    raw = yt_client.read_file(yt_path)
    if isinstance(raw, (bytes, bytearray)):
        data = bytes(raw)
    else:
        data = b"".join(raw)
    with open(local_path, "wb") as out:
        out.write(data)
