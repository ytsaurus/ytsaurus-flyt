"""Tests for ytsaurus_flyt.config.profiles."""

from pathlib import Path

from ytsaurus_flyt.config.config import FlytConfig
from ytsaurus_flyt.config.profiles import (
    merge_flyt_config,
    merge_yaml_dict,
    profile_dict_to_flyt_config,
    resolve_connection_from_profile,
    resolve_effective_profile_name,
)


def test_merge_yaml_dict():
    assert merge_yaml_dict({"a": 1, "b": 2}, {"b": 3, "c": ""}) == {"a": 1, "b": 3}


def test_merge_flyt_config_overrides_non_empty():
    base = FlytConfig(service_name="a")
    over = FlytConfig(service_name="b")
    m = merge_flyt_config(base, over)
    assert m.service_name == "b"


def test_profile_dict_to_flyt_config_warns_on_unknown_keys():
    import pytest

    with pytest.warns(UserWarning, match="pre_build_layer_paths"):
        cfg = profile_dict_to_flyt_config(
            {"proxy": "http://x", "pool": "p", "pre_build_layer_paths": ["//sys/flink/base.squashfs"]}
        )
    # the typo'd key is dropped; the correctly-spelled field stays empty
    assert cfg.squashfs_layer_paths == []
    assert cfg.pre_built_layer_paths == []


def test_profile_dict_to_flyt_config():
    d = {
        "proxy": "http://localhost:1",
        "pool": "default",
        "squashfs_layer_delivery": "sandbox_unpack",
        "squashfs_layer_paths": ["//sys/flink/runtime.squashfs"],
        "flink_version": "1.20.1",
        "runtime_python_version": "3.8",
    }
    cfg = profile_dict_to_flyt_config(d)
    # meta keys are stripped; flyt fields applied verbatim (no implicit cache derivation)
    assert cfg.squashfs_layer_delivery == "sandbox_unpack"
    assert cfg.squashfs_layer_paths == ["//sys/flink/runtime.squashfs"]


def test_resolve_connection_from_profile():
    pr, pl, pst = resolve_connection_from_profile({"proxy": "http://x", "pool": "p", "preset": "small"})
    assert pr == "http://x"
    assert pl == "p"
    assert pst == "small"


def test_resolve_effective_profile_name_env(monkeypatch, tmp_path: Path):
    """FLYT_PROFILE overrides active file."""
    monkeypatch.setenv("FLYT_PROFILE", "from-env")
    assert resolve_effective_profile_name(None) == "from-env"

    monkeypatch.delenv("FLYT_PROFILE", raising=False)
    monkeypatch.setenv("FLYT_CONFIG_DIR", str(tmp_path))
    active = tmp_path / "active"
    active.write_text("from-file\n", encoding="utf-8")
    assert resolve_effective_profile_name(None) == "from-file"

    assert resolve_effective_profile_name("cli") == "cli"
