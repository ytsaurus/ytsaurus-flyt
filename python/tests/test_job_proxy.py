"""The YT proxy handed to application-cluster jobs."""

import io
from unittest.mock import MagicMock

import pytest


def test_is_loopback_proxy():
    from ytsaurus_flyt.job_proxy import is_loopback_proxy

    assert is_loopback_proxy("http://localhost:50005")
    assert is_loopback_proxy("127.0.0.1:8080")
    assert is_loopback_proxy("http://[::1]:50005")
    assert is_loopback_proxy("http://yt.localhost:50005")
    assert not is_loopback_proxy("yt.example.com")
    assert not is_loopback_proxy("http://http-proxies.default.svc.cluster.local")


class _Resp(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def test_job_yt_proxy_resolution(monkeypatch):
    from ytsaurus_flyt import job_proxy as jp

    seen = {}

    def fake_urlopen(req, timeout):
        seen["url"] = req.full_url
        seen["auth"] = req.get_header("Authorization")
        return _Resp(b'["hp-0.http-proxies.default.svc.cluster.local", "hp-1.http-proxies.default.svc.cluster.local"]')

    monkeypatch.setattr(jp.urllib.request, "urlopen", fake_urlopen)
    client = MagicMock()
    client.config = {"token": "tok"}
    # explicit profile value wins, nothing is discovered
    assert jp.job_yt_proxy(client, "http://localhost:50005", "http://my-proxy") == "http://my-proxy"
    # a routable client proxy is reused as is
    assert jp.job_yt_proxy(client, "yt.example.com") == "http://yt.example.com"
    assert jp.job_yt_proxy(client, "https://yt.example.com") == "https://yt.example.com"
    # the client's scheme carries over to proxies given without one
    assert jp.job_yt_proxy(client, "https://yt.example.com", "jobs-proxy") == "https://jobs-proxy"
    assert "url" not in seen
    # loopback: jobs get the cluster's own proxy hosts
    assert jp.job_yt_proxy(client, "http://localhost:50005") == (
        "http://hp-0.http-proxies.default.svc.cluster.local,http://hp-1.http-proxies.default.svc.cluster.local"
    )
    assert seen["url"] == "http://localhost:50005/hosts"
    assert seen["auth"] == "OAuth tok"


def test_job_yt_proxy_requires_hosts_for_loopback(monkeypatch):
    from ytsaurus_flyt import job_proxy as jp

    monkeypatch.setattr(jp.urllib.request, "urlopen", lambda req, timeout: _Resp(b"[]"))
    with pytest.raises(ValueError, match="job_yt_proxy"):
        jp.job_yt_proxy(MagicMock(), "http://localhost:50005")
