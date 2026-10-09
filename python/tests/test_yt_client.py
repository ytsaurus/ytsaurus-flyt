"""Tests for YtClient config resolution and yt_client_config overrides."""

from ytsaurus_flyt.submit.yt_client import yt_client_config_for_proxy


def test_localhost_proxy_disables_discovery_by_default() -> None:
    cfg = yt_client_config_for_proxy("http://localhost:50005")
    assert cfg == {"apply_remote_patch_at_start": False, "proxy": {"enable_proxy_discovery": False}}


def test_remote_proxy_has_no_config_without_overrides() -> None:
    assert yt_client_config_for_proxy("https://cluster.example") is None


def test_overrides_apply_to_remote_proxy() -> None:
    cfg = yt_client_config_for_proxy(
        "https://cluster.example",
        {"proxy": {"enable_proxy_discovery": False}},
    )
    assert cfg == {"proxy": {"enable_proxy_discovery": False}}


def test_overrides_deep_merge_with_localhost_base() -> None:
    cfg = yt_client_config_for_proxy(
        "http://localhost:50005",
        {"proxy": {"url": "http://other"}},
    )
    # base proxy.enable_proxy_discovery is preserved; the override key is added alongside.
    assert cfg["proxy"] == {"enable_proxy_discovery": False, "url": "http://other"}
    assert cfg["apply_remote_patch_at_start"] is False
