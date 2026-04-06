"""Tests for ytsaurus_flyt.jar_utils."""

import pytest

from ytsaurus_flyt.jar_utils import (
    JarInfoExtractionError,
    SemanticVersion,
    UnsupportedVersionError,
    extract_jar_info,
)


class TestSemanticVersion:
    def test_basic_version(self):
        v = SemanticVersion("1.2.3")
        assert repr(v) == "SemanticVersion('1.2.3')"

    def test_version_with_prerelease(self):
        v = SemanticVersion("1.0.0-SNAPSHOT")
        assert v is not None

    def test_comparison(self):
        assert SemanticVersion("1.0.0") < SemanticVersion("2.0.0")
        assert SemanticVersion("1.0.0") < SemanticVersion("1.1.0")
        assert SemanticVersion("1.0.0") < SemanticVersion("1.0.1")

    def test_equality(self):
        assert SemanticVersion("1.2.3") == SemanticVersion("1.2.3")

    def test_invalid_version(self):
        with pytest.raises(UnsupportedVersionError):
            SemanticVersion("not-a-version")


class TestExtractJarInfo:
    def test_basic_jar(self):
        info = extract_jar_info("flink-connector-yt-0.2.0.jar")
        assert info.basename == "flink-connector-yt"
        assert info.path == "flink-connector-yt-0.2.0.jar"

    def test_snapshot_jar(self):
        info = extract_jar_info("flink-connector-yt-0.2.0-SNAPSHOT.jar")
        assert info.basename == "flink-connector-yt"

    def test_full_path(self):
        info = extract_jar_info("/opt/lib/flink-yson-1.0.9.jar")
        assert info.basename == "flink-yson"

    def test_no_version(self):
        with pytest.raises(JarInfoExtractionError):
            extract_jar_info("flink-connector-yt.jar")

    def test_no_basename(self):
        with pytest.raises(JarInfoExtractionError):
            extract_jar_info("1.0.0.jar")

    def test_udf_jar(self):
        info = extract_jar_info("udf-common-1.1.12.jar")
        assert info.basename == "udf-common"

    def test_version_sorting(self):
        v1 = extract_jar_info("flink-yson-1.0.8.jar").version
        v2 = extract_jar_info("flink-yson-1.0.9.jar").version
        assert v1 < v2
