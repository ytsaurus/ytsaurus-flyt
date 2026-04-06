"""Utilities for parsing Flink JAR file names and extracting version info."""

from __future__ import annotations

import re
from collections import namedtuple
from functools import total_ordering
from pathlib import Path
from typing import Tuple

# Separator between parts of a JAR filename
JAR_NAME_PARTS_SEPARATOR = "-"

# SemVer regex (borrowed from distlib.version)
_SEMVER_RE = re.compile(
    r"^(\d+)\.(\d+)\.(\d+)"
    r"(-[a-z0-9]+(\.[a-z0-9-]+)*)?"
    r"(\+[a-z0-9]+(\.[a-z0-9-]+)*)?$",
    re.I,
)


class JarInfoExtractionError(ValueError):
    """Error raised on failure gathering jar info."""


JarInfo = namedtuple("JarInfo", ["basename", "version", "path"])


@total_ordering
class SemanticVersion:
    """Minimal semver implementation for JAR version comparison."""

    def __init__(self, version: str):
        self._string = version = version.strip()
        self._parts = self._parse(version)

    @staticmethod
    def _parse(s: str) -> Tuple:
        def make_tuple(s_inner, absent):
            if s_inner is None:
                return (absent,)
            parts = s_inner[1:].split(".")
            return tuple(p.zfill(8) if p.isdigit() else p for p in parts)

        m = _SEMVER_RE.match(s)
        if not m:
            raise UnsupportedVersionError(s)
        groups = m.groups()
        major, minor, patch = (int(i) for i in groups[:3])
        pre = make_tuple(groups[3], "|")
        build = make_tuple(groups[5], "*")
        return (major, minor, patch), pre, build

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SemanticVersion):
            return NotImplemented
        return self._parts == other._parts

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, SemanticVersion):
            return NotImplemented
        return self._parts < other._parts

    def __hash__(self) -> int:
        return hash(self._parts)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}('{self._string}')"


class UnsupportedVersionError(ValueError):
    """Raised when a version string is not semver-compatible."""


# Fallback version for JARs with unparseable versions
SEMVER_UNKNOWN = SemanticVersion("0.0.0-UNKNOWN")


def extract_jar_info(jar_path: str) -> JarInfo:
    """Extract basename and version from a JAR filename.

    Relies on the convention that JAR basenames do not contain dots,
    while version parts always contain dots. The first part with a dot
    marks the beginning of the version string.

    Pattern: ``<basename>-<version>.jar``

    Examples::

        >>> extract_jar_info("flink-connector-yt-0.2.0-SNAPSHOT.jar")
        JarInfo(basename='flink-connector-yt', version=..., path='flink-connector-yt-0.2.0-SNAPSHOT.jar')

    Args:
        jar_path: Path or filename of the JAR.

    Returns:
        JarInfo with basename, version, and original path.

    Raises:
        JarInfoExtractionError: If the filename cannot be parsed.
    """
    jar_full_name = Path(jar_path).stem
    jar_name_parts = jar_full_name.split(JAR_NAME_PARTS_SEPARATOR)

    # First part containing a dot starts the version segment
    jar_version_part_index = -1
    for index, part in enumerate(jar_name_parts):
        if "." in part:
            jar_version_part_index = index
            break

    if jar_version_part_index == -1:
        raise JarInfoExtractionError(
            f"Illegal jar path: '{jar_path}' must contain version with dots separated by '{JAR_NAME_PARTS_SEPARATOR}'"
        )
    if jar_version_part_index == 0:
        raise JarInfoExtractionError(
            f"Illegal jar path: '{jar_path}' must contain basename separated by '{JAR_NAME_PARTS_SEPARATOR}'"
        )

    basename = JAR_NAME_PARTS_SEPARATOR.join(jar_name_parts[:jar_version_part_index])
    raw_version = JAR_NAME_PARTS_SEPARATOR.join(jar_name_parts[jar_version_part_index:])
    try:
        version = SemanticVersion(raw_version)
    except UnsupportedVersionError:
        version = SEMVER_UNKNOWN

    return JarInfo(basename, version, jar_path)
