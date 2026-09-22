"""In-job helper for flyt application mode: JobManager discovery in Cypress and operation completion.

Runs inside YT jobs with the layer's Python, standard library only. Talks to the YT HTTP proxy
(``FLYT_YT_PROXY``) with the operation's token (``YT_SECURE_VAULT_YT_TOKEN``).
"""

import argparse
import datetime
import json
import os
import socket
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

HEARTBEAT_INTERVAL_S = 300
RECORD_TTL_S = 24 * 3600
POLL_INTERVAL_S = 5
REQUEST_TIMEOUT_S = 30
TCP_PROBE_TIMEOUT_S = 3
YT_RESOLVE_ERROR_CODE = 500


def _log(msg):
    sys.stderr.write("[flyt_job_helper] %s\n" % msg)
    sys.stderr.flush()


def _proxy_base():
    raw = (os.environ.get("FLYT_YT_PROXY") or "").strip()
    if not raw:
        raise SystemExit("FLYT_YT_PROXY is not set in the job environment")
    if "://" not in raw:
        raw = "http://" + raw
    return raw.rstrip("/")


def _token():
    for key in ("YT_SECURE_VAULT_YT_TOKEN", "YT_TOKEN"):
        val = (os.environ.get(key) or "").strip()
        if val:
            return val
    raise SystemExit("No YT token in the job environment (secure_vault YT_TOKEN)")


class YtError(Exception):
    def __init__(self, status, payload):
        super().__init__("HTTP %s: %s" % (status, json.dumps(payload)[:2000]))
        self.status = status
        self.payload = payload if isinstance(payload, dict) else {}

    def codes(self):
        out = set()
        stack = [self.payload]
        while stack:
            err = stack.pop()
            if not isinstance(err, dict):
                continue
            if "code" in err:
                out.add(err.get("code"))
            stack.extend(err.get("inner_errors") or [])
        return out

    def is_resolve_error(self):
        return YT_RESOLVE_ERROR_CODE in self.codes()


def yt_request(command, params, body=None, method=None):
    """Call ``/api/v4/<command>``; returns the decoded JSON response (``{}`` when empty)."""
    url = "%s/api/v4/%s?%s" % (_proxy_base(), command, urllib.parse.urlencode(params))
    # Format headers are YSON/JSON literals, hence the quotes around the format name.
    headers = {
        "Authorization": "OAuth " + _token(),
        "X-YT-Output-Format": '"json"',
    }
    data = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["X-YT-Input-Format"] = '"json"'
    req = urllib.request.Request(
        url, data=data, headers=headers, method=method or ("POST" if data is not None else "GET")
    )
    try:
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_S) as resp:
            raw = resp.read()
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            payload = json.loads(raw.decode("utf-8")) if raw else {}
        except ValueError:
            payload = {"message": raw.decode("utf-8", "replace")}
        raise YtError(e.code, payload)
    if not raw:
        return {}
    return json.loads(raw.decode("utf-8"))


def _expiration_time(ttl_s):
    at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=ttl_s)
    return at.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _set_expiration(path, ttl_s):
    yt_request("set", {"path": path + "/@expiration_time"}, body=_expiration_time(ttl_s), method="PUT")


def _bool_str(val):
    return "true" if val else "false"


def cmd_publish(args):
    record = {
        "host": args.host,
        "ui_host": args.ui_host or args.host,
        "rpc_port": int(args.rpc_port),
        "rest_port": int(args.rest_port),
        "incarnation": os.environ.get("YT_OPERATION_INCARNATION") or "",
        "operation_id": os.environ.get("YT_OPERATION_ID") or "",
        "job_id": os.environ.get("YT_JOB_ID") or "",
        "published_at": _expiration_time(0),
    }
    yt_request(
        "set",
        {"path": args.path, "recursive": _bool_str(True), "force": _bool_str(True)},
        body=json.dumps(record),
        method="PUT",
    )
    _set_expiration(args.path, args.ttl)
    _log(
        "published JobManager %s:%s (incarnation %r) at %s"
        % (record["host"], record["rpc_port"], record["incarnation"], args.path)
    )
    return 0


def _parent_alive(pid):
    if pid <= 0:
        return True
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return os.getppid() != 1


def cmd_heartbeat(args):
    """Keep the discovery record alive while the JobManager runs; stops with the parent shell."""
    while True:
        slept = 0.0
        while slept < args.interval:
            if not _parent_alive(args.parent_pid):
                return 0
            time.sleep(1.0)
            slept += 1.0
        try:
            _set_expiration(args.path, args.ttl)
        except Exception as e:  # noqa: BLE001 - transient proxy errors must not kill the heartbeat
            _log("heartbeat failed (will retry): %s" % e)


def _read_record(path):
    """The published record, or ``None`` while the node does not exist yet."""
    try:
        resp = yt_request("get", {"path": path})
    except YtError as e:
        if e.is_resolve_error():
            return None
        raise
    value = resp.get("value") if isinstance(resp, dict) else resp
    if isinstance(value, str):
        return json.loads(value)
    return value if isinstance(value, dict) else None


def _tcp_open(host, port):
    try:
        with socket.create_connection((host, int(port)), timeout=TCP_PROBE_TIMEOUT_S):
            return True
    except OSError:
        return False


def resolve_jobmanager(record, incarnation):
    """``(host, rpc_port)`` when the record belongs to this incarnation and the REST port answers."""
    if not record:
        return None
    if incarnation and record.get("incarnation") and record.get("incarnation") != incarnation:
        _log("discovery record is from incarnation %r, waiting for %r" % (record.get("incarnation"), incarnation))
        return None
    host, rpc_port, rest_port = record.get("host"), record.get("rpc_port"), record.get("rest_port")
    if not host or not rpc_port:
        return None
    if rest_port and not _tcp_open(host, rest_port):
        _log("JobManager %s:%s not accepting connections yet" % (host, rest_port))
        return None
    return str(host), int(rpc_port)


def cmd_wait_jobmanager(args):
    deadline = time.monotonic() + args.timeout
    while time.monotonic() < deadline:
        try:
            found = resolve_jobmanager(_read_record(args.path), args.incarnation or "")
        except Exception as e:  # noqa: BLE001 - keep polling through transient proxy errors
            _log("discovery read failed (will retry): %s" % e)
            found = None
        if found:
            sys.stdout.write("%s %d\n" % found)
            sys.stdout.flush()
            return 0
        time.sleep(args.poll_interval)
    _log("no JobManager discovered at %s within %ss" % (args.path, args.timeout))
    return 1


def cmd_remove(args):
    yt_request("remove", {"path": args.path, "force": _bool_str(True)}, method="POST")
    return 0


def cmd_complete_operation(args):
    yt_request("complete_operation", {"operation_id": args.operation_id}, method="POST")
    _log("operation %s completed" % args.operation_id)
    return 0


def build_parser():
    p = argparse.ArgumentParser(prog="flyt_job_helper")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("publish")
    s.add_argument("--path", required=True)
    s.add_argument("--host", required=True, help="address TaskManagers connect to (fastbone when available)")
    s.add_argument("--ui-host", default="", help="address clients use for the Web UI")
    s.add_argument("--rpc-port", required=True, type=int)
    s.add_argument("--rest-port", required=True, type=int)
    s.add_argument("--ttl", type=int, default=RECORD_TTL_S)
    s.set_defaults(func=cmd_publish)

    s = sub.add_parser("heartbeat")
    s.add_argument("--path", required=True)
    s.add_argument("--parent-pid", type=int, default=0)
    s.add_argument("--interval", type=float, default=HEARTBEAT_INTERVAL_S)
    s.add_argument("--ttl", type=int, default=RECORD_TTL_S)
    s.set_defaults(func=cmd_heartbeat)

    s = sub.add_parser("wait-jobmanager")
    s.add_argument("--path", required=True)
    s.add_argument("--incarnation", default="")
    s.add_argument("--timeout", type=float, default=900.0)
    s.add_argument("--poll-interval", type=float, default=POLL_INTERVAL_S)
    s.set_defaults(func=cmd_wait_jobmanager)

    s = sub.add_parser("remove")
    s.add_argument("--path", required=True)
    s.set_defaults(func=cmd_remove)

    s = sub.add_parser("complete-operation")
    s.add_argument("--operation-id", required=True)
    s.set_defaults(func=cmd_complete_operation)
    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
