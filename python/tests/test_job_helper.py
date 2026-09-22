"""In-job helper (run_scripts/flyt_job_helper.py): discovery waiting and YT HTTP calls."""

import importlib.util
import io
import json
from pathlib import Path

import pytest

_HELPER = Path(__file__).parent.parent / "src" / "ytsaurus_flyt" / "run_scripts" / "flyt_job_helper.py"


@pytest.fixture
def helper():
    spec = importlib.util.spec_from_file_location("flyt_job_helper", _HELPER)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def _record(**overrides):
    rec = {"host": "2a02::1", "rpc_port": 27051, "rest_port": 27050, "incarnation": "inc-2"}
    rec.update(overrides)
    return rec


def test_resolve_jobmanager_requires_record(helper, monkeypatch):
    monkeypatch.setattr(helper, "_tcp_open", lambda h, p: True)
    assert helper.resolve_jobmanager(None, "inc-2") is None
    assert helper.resolve_jobmanager({}, "inc-2") is None


def test_resolve_jobmanager_rejects_stale_incarnation(helper, monkeypatch):
    monkeypatch.setattr(helper, "_tcp_open", lambda h, p: True)
    assert helper.resolve_jobmanager(_record(incarnation="inc-1"), "inc-2") is None
    # no incarnation on either side (non-gang tasks / older YT): accept the record
    assert helper.resolve_jobmanager(_record(incarnation=""), "inc-2") == ("2a02::1", 27051)
    assert helper.resolve_jobmanager(_record(), "") == ("2a02::1", 27051)


def test_resolve_jobmanager_waits_for_rest_port(helper, monkeypatch):
    probes = []

    def fake_probe(host, port):
        probes.append((host, port))
        return False

    monkeypatch.setattr(helper, "_tcp_open", fake_probe)
    assert helper.resolve_jobmanager(_record(), "inc-2") is None
    assert probes == [("2a02::1", 27050)]


def test_wait_jobmanager_prints_endpoint_once_available(helper, monkeypatch, capsys):
    records = iter([None, _record(incarnation="inc-1"), _record()])
    monkeypatch.setattr(helper, "_read_record", lambda path: next(records))
    monkeypatch.setattr(helper, "_tcp_open", lambda h, p: True)
    monkeypatch.setattr(helper.time, "sleep", lambda s: None)
    rc = helper.main(["wait-jobmanager", "--path", "//d/op", "--incarnation", "inc-2", "--timeout", "30"])
    assert rc == 0
    assert capsys.readouterr().out.strip() == "2a02::1 27051"


def test_wait_jobmanager_times_out_and_survives_read_errors(helper, monkeypatch):
    def boom(path):
        raise RuntimeError("proxy down")

    monkeypatch.setattr(helper, "_read_record", boom)
    monkeypatch.setattr(helper.time, "sleep", lambda s: None)
    rc = helper.main(["wait-jobmanager", "--path", "//d/op", "--timeout", "0.01", "--poll-interval", "0"])
    assert rc == 1


def test_yt_error_resolve_detection_walks_inner_errors(helper):
    err = helper.YtError(400, {"code": 1, "inner_errors": [{"code": 500, "message": "no such node"}]})
    assert err.is_resolve_error()
    assert not helper.YtError(400, {"code": 1}).is_resolve_error()


def test_read_record_returns_none_for_missing_node(helper, monkeypatch):
    def fake_request(command, params, body=None, method=None):
        raise helper.YtError(400, {"code": 500})

    monkeypatch.setattr(helper, "yt_request", fake_request)
    assert helper._read_record("//d/op") is None


def test_read_record_parses_json_string_value(helper, monkeypatch):
    monkeypatch.setattr(helper, "yt_request", lambda c, p, body=None, method=None: {"value": json.dumps(_record())})
    assert helper._read_record("//d/op")["rpc_port"] == 27051


def test_publish_sets_record_then_expiration(helper, monkeypatch):
    calls = []

    def fake_request(command, params, body=None, method=None):
        calls.append((command, params, body, method))
        return {}

    monkeypatch.setattr(helper, "yt_request", fake_request)
    monkeypatch.setenv("YT_OPERATION_INCARNATION", "inc-7")
    monkeypatch.setenv("YT_OPERATION_ID", "op-1")
    rc = helper.main(
        ["publish", "--path", "//d/op-1", "--host", "2a02::1", "--rpc-port", "27051", "--rest-port", "27050"]
    )
    assert rc == 0
    assert [c[0] for c in calls] == ["set", "set"]
    cmd, params, body, method = calls[0]
    assert params == {"path": "//d/op-1", "recursive": "true", "force": "true"}
    assert method == "PUT"
    rec = json.loads(body)
    assert rec["host"] == "2a02::1" and rec["rpc_port"] == 27051 and rec["incarnation"] == "inc-7"
    assert calls[1][1] == {"path": "//d/op-1/@expiration_time"}
    assert calls[1][2].endswith("Z")


def test_remove_and_complete_operation_commands(helper, monkeypatch):
    calls = []
    monkeypatch.setattr(helper, "yt_request", lambda c, p, body=None, method=None: calls.append((c, p, method)) or {})
    assert helper.main(["remove", "--path", "//d/op"]) == 0
    assert helper.main(["complete-operation", "--operation-id", "op-1"]) == 0
    assert calls == [
        ("remove", {"path": "//d/op", "force": "true"}, "POST"),
        ("complete_operation", {"operation_id": "op-1"}, "POST"),
    ]


def test_yt_request_builds_v4_url_headers_and_decodes_json(helper, monkeypatch):
    seen = {}

    class FakeResp(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def fake_urlopen(req, timeout):
        seen["url"] = req.full_url
        seen["method"] = req.get_method()
        seen["headers"] = {k.lower(): v for k, v in req.header_items()}
        seen["data"] = req.data
        return FakeResp(b'{"value": "x"}')

    monkeypatch.setenv("FLYT_YT_PROXY", "proxy.example")
    monkeypatch.setenv("YT_SECURE_VAULT_YT_TOKEN", "tok")
    monkeypatch.setattr(helper.urllib.request, "urlopen", fake_urlopen)
    out = helper.yt_request("set", {"path": "//a"}, body="v", method="PUT")
    assert out == {"value": "x"}
    assert seen["url"] == "http://proxy.example/api/v4/set?path=%2F%2Fa"
    assert seen["method"] == "PUT"
    assert seen["headers"]["authorization"] == "OAuth tok"
    assert seen["headers"]["x-yt-input-format"] == '"json"'
    assert seen["headers"]["x-yt-output-format"] == '"json"'
    assert seen["data"] == b'"v"'


def test_yt_request_raises_yt_error_with_payload(helper, monkeypatch):
    import urllib.error

    def fake_urlopen(req, timeout):
        raise urllib.error.HTTPError(req.full_url, 400, "bad", {}, io.BytesIO(b'{"code": 500, "message": "nope"}'))

    monkeypatch.setenv("FLYT_YT_PROXY", "http://proxy.example:80")
    monkeypatch.setenv("YT_TOKEN", "tok")
    monkeypatch.delenv("YT_SECURE_VAULT_YT_TOKEN", raising=False)
    monkeypatch.setattr(helper.urllib.request, "urlopen", fake_urlopen)
    with pytest.raises(helper.YtError) as ei:
        helper.yt_request("get", {"path": "//a"})
    assert ei.value.is_resolve_error()


def test_parent_alive(helper):
    import os

    assert helper._parent_alive(os.getpid())
    assert helper._parent_alive(0)
    assert not helper._parent_alive(2**22 - 1)
