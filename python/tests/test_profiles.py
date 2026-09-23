"""Tests for ytsaurus_flyt.profiles."""

from pathlib import Path

from ytsaurus_flyt.config import FlytConfig
from ytsaurus_flyt.profiles import (
    apply_cypress_base_path,
    default_cypress_base_path,
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


def test_default_cypress_base_path():
    assert default_cypress_base_path("prod") == "//home/flyt/clusters/prod"


def test_apply_cypress_base_path():
    cfg = FlytConfig()
    out = apply_cypress_base_path(cfg, "//home/flyt/clusters/x")
    assert out.squashfs_layer_cache_prefix == "//home/flyt/clusters/x/layers"
    assert out.squashfs_tools_cache_prefix == "//home/flyt/clusters/x/tools"
    assert out.wheel_cache_prefix == "//home/flyt/clusters/x/wheels"


def test_profile_dict_to_flyt_config():
    d = {
        "proxy": "http://localhost:1",
        "pool": "default",
        "cypress_base_path": "//home/flyt/clusters/t",
        "squashfs_layer_delivery": "sandbox_unpack",
        "runtime_python_packages": ["apache-flink==1.20.1"],
        "runtime_python_version": "3.8",
    }
    cfg = profile_dict_to_flyt_config(d)
    assert cfg.squashfs_layer_cache_prefix == "//home/flyt/clusters/t/layers"
    assert cfg.wheel_cache_prefix == "//home/flyt/clusters/t/wheels"


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
