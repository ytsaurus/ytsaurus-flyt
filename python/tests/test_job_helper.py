"""In-job helper (run_scripts/flyt_job_helper.py): JobManager discovery through the YT API."""

import importlib.util
import io
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


_JM_CONFIG = {"jobmanager.rpc.address": "[2a02::fb]", "jobmanager.rpc.port": "27051", "rest.port": "27050"}


class _FakeYt:
    """Scripted yt_request: records calls, answers list_jobs / get_job."""

    def __init__(self, jobs, addresses, reject_incarnation_filter=False):
        self.jobs = jobs
        self.addresses = addresses
        self.reject = reject_incarnation_filter
        self.calls = []

    def __call__(self, command, params, body=None, method=None):
        self.calls.append((command, dict(params), method))
        if command == "list_jobs":
            if "operation_incarnation" in params and self.reject:
                raise RuntimeError("unknown parameter")  # replaced by YtError in the test
            return {"jobs": self.jobs}
        if command == "get_job":
            return {"exec_attributes": {"ip_addresses": self.addresses.get(params["job_id"], [])}}
        return {}


def _install(helper, monkeypatch, fake, configs=None, tcp_ok=True):
    monkeypatch.setattr(helper, "yt_request", fake)
    configs = configs if configs is not None else {}
    monkeypatch.setattr(helper, "fetch_jobmanager_config", lambda host, port: configs.get(host))
    monkeypatch.setattr(helper, "_tcp_open", lambda h, p: tcp_ok)
    monkeypatch.setattr(helper.time, "sleep", lambda s: None)


def test_wait_jobmanager_resolves_rpc_endpoint_from_rest_config(helper, monkeypatch, capsys):
    fake = _FakeYt(
        jobs=[{"id": "jm1", "task_name": "jobmanager", "operation_incarnation": "inc-2"}],
        addresses={"jm1": ["2a02::bb", "2a02::fb"]},
    )
    # backbone REST is filtered between containers; only the fastbone answers
    _install(helper, monkeypatch, fake, configs={"2a02::fb": _JM_CONFIG})
    rc = helper.main(["wait-jobmanager", "--operation-id", "op-1", "--incarnation", "inc-2", "--rest-port", "27050"])
    assert rc == 0
    assert capsys.readouterr().out.strip() == "jm1 2a02::fb 27051"
    assert fake.calls[0] == (
        "list_jobs",
        {"operation_id": "op-1", "task_name": "jobmanager", "state": "running", "operation_incarnation": "inc-2"},
        None,
    )
    assert fake.calls[1] == ("get_job", {"operation_id": "op-1", "job_id": "jm1"}, None)


def test_wait_jobmanager_falls_back_to_client_side_incarnation_filter(helper, monkeypatch, capsys):
    fake = _FakeYt(
        jobs=[
            {"id": "old", "operation_incarnation": "inc-1"},
            {"id": "new", "operation_incarnation": "inc-2"},
        ],
        addresses={"old": ["2a02::1"], "new": ["2a02::fb"]},
        reject_incarnation_filter=True,
    )

    def fake_with_yt_error(command, params, body=None, method=None):
        try:
            return fake(command, params, body, method)
        except RuntimeError:
            raise helper.YtError(400, {"code": 1, "message": "unknown parameter"})

    _install(helper, monkeypatch, fake_with_yt_error, configs={"2a02::fb": _JM_CONFIG, "2a02::1": _JM_CONFIG})
    rc = helper.main(["wait-jobmanager", "--operation-id", "op-1", "--incarnation", "inc-2", "--rest-port", "27050"])
    assert rc == 0
    assert capsys.readouterr().out.strip() == "new 2a02::fb 27051"
    # the first list_jobs carried the server filter, the retry did not
    assert "operation_incarnation" in fake.calls[0][1]
    assert "operation_incarnation" not in fake.calls[1][1]
    assert ("get_job", {"operation_id": "op-1", "job_id": "old"}, None) not in fake.calls


def test_wait_jobmanager_waits_for_rest_then_rpc(helper, monkeypatch, capsys):
    fake = _FakeYt(jobs=[{"id": "jm1"}], addresses={"jm1": ["2a02::fb"]})
    monkeypatch.setattr(helper, "yt_request", fake)
    rest = iter([None, _JM_CONFIG, _JM_CONFIG])
    monkeypatch.setattr(helper, "fetch_jobmanager_config", lambda host, port: next(rest))
    probes = iter([False, True])
    monkeypatch.setattr(helper, "_tcp_open", lambda h, p: next(probes))
    monkeypatch.setattr(helper.time, "sleep", lambda s: None)
    rc = helper.main(["wait-jobmanager", "--operation-id", "op-1", "--rest-port", "27050", "--timeout", "60"])
    assert rc == 0
    assert capsys.readouterr().out.strip() == "jm1 2a02::fb 27051"


def test_wait_jobmanager_times_out_and_survives_api_errors(helper, monkeypatch):
    def boom(command, params, body=None, method=None):
        raise RuntimeError("proxy down")

    monkeypatch.setattr(helper, "yt_request", boom)
    monkeypatch.setattr(helper.time, "sleep", lambda s: None)
    rc = helper.main(["wait-jobmanager", "--operation-id", "op-1", "--rest-port", "27050", "--timeout", "0.01"])
    assert rc == 1


def test_wait_jobmanager_keeps_polling_without_running_jobmanager(helper, monkeypatch, capsys):
    answers = iter([[], [{"id": "jm1"}]])
    fake = _FakeYt(jobs=[], addresses={"jm1": ["2a02::fb"]})
    original = fake.__call__

    def scripted(command, params, body=None, method=None):
        if command == "list_jobs":
            return {"jobs": next(answers)}
        return original(command, params, body, method)

    _install(helper, monkeypatch, scripted, configs={"2a02::fb": _JM_CONFIG})
    rc = helper.main(["wait-jobmanager", "--operation-id", "op-1", "--rest-port", "27050", "--timeout", "60"])
    assert rc == 0
    assert capsys.readouterr().out.strip() == "jm1 2a02::fb 27051"


def test_rpc_endpoint_from_config_normalizes_brackets_and_port(helper):
    assert helper.rpc_endpoint_from_config(_JM_CONFIG) == ("2a02::fb", 27051)
    assert helper.rpc_endpoint_from_config({"jobmanager.rpc.address": "10.0.0.5"}) == ("10.0.0.5", 6123)
    assert helper.rpc_endpoint_from_config({}) is None


def test_fetch_jobmanager_config_builds_ipv6_url_and_parses_entries(helper, monkeypatch):
    seen = {}

    class FakeResp(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def fake_urlopen(url, timeout):
        seen["url"] = url
        return FakeResp(
            b'[{"key": "jobmanager.rpc.address", "value": "2a02::fb"}, {"key": "rest.port", "value": "27050"}]'
        )

    monkeypatch.setattr(helper.urllib.request, "urlopen", fake_urlopen)
    cfg = helper.fetch_jobmanager_config("2a02::fb", 27050)
    assert seen["url"] == "http://[2a02::fb]:27050/jobmanager/config"
    assert cfg == {"jobmanager.rpc.address": "2a02::fb", "rest.port": "27050"}
    assert helper.fetch_jobmanager_config("10.0.0.5", 27050) is not None
    assert seen["url"] == "http://10.0.0.5:27050/jobmanager/config"


def test_fetch_jobmanager_config_returns_none_when_rest_is_down(helper, monkeypatch):
    def fake_urlopen(url, timeout):
        raise OSError("refused")

    monkeypatch.setattr(helper.urllib.request, "urlopen", fake_urlopen)
    assert helper.fetch_jobmanager_config("2a02::fb", 27050) is None


def test_complete_operation_command(helper, monkeypatch):
    calls = []
    monkeypatch.setattr(helper, "yt_request", lambda c, p, body=None, method=None: calls.append((c, p, method)) or {})
    assert helper.main(["complete-operation", "--operation-id", "op-1"]) == 0
    assert calls == [("complete_operation", {"operation_id": "op-1"}, "POST")]


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
        return FakeResp(b'{"jobs": []}')

    monkeypatch.setenv("FLYT_YT_PROXY", "proxy.example")
    monkeypatch.setenv("YT_SECURE_VAULT_YT_TOKEN", "tok")
    monkeypatch.setattr(helper.urllib.request, "urlopen", fake_urlopen)
    out = helper.yt_request("list_jobs", {"operation_id": "op-1"})
    assert out == {"jobs": []}
    assert seen["url"] == "http://proxy.example/api/v4/list_jobs?operation_id=op-1"
    assert seen["method"] == "GET"
    assert seen["headers"]["authorization"] == "OAuth tok"
    assert seen["headers"]["x-yt-output-format"] == '"json"'


def test_yt_request_raises_yt_error_with_codes(helper, monkeypatch):
    import urllib.error

    def fake_urlopen(req, timeout):
        raise urllib.error.HTTPError(
            req.full_url, 400, "bad", {}, io.BytesIO(b'{"code": 1, "inner_errors": [{"code": 500}]}')
        )

    monkeypatch.setenv("FLYT_YT_PROXY", "http://proxy.example:80")
    monkeypatch.setenv("YT_TOKEN", "tok")
    monkeypatch.delenv("YT_SECURE_VAULT_YT_TOKEN", raising=False)
    monkeypatch.setattr(helper.urllib.request, "urlopen", fake_urlopen)
    with pytest.raises(helper.YtError) as ei:
        helper.yt_request("get_job", {"operation_id": "op-1", "job_id": "j"})
    assert ei.value.codes() == {1, 500}
