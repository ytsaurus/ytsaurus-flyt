"""In-job helper for flyt application mode: JobManager discovery and operation completion.

Runs inside YT jobs with the layer's Python, standard library only. Talks to the YT HTTP proxy
(``FLYT_YT_PROXY``) with the operation's token (``YT_SECURE_VAULT_YT_TOKEN``).

Discovery asks YT, not a registry: the TaskManager lists the running ``jobmanager`` job of its own
operation and incarnation, reads the job's addresses, and takes the RPC endpoint the JobManager
itself advertises in ``/jobmanager/config``.
"""

import argparse
import json
import os
import socket
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

JOBMANAGER_TASK = "jobmanager"
POLL_INTERVAL_MIN_S = 2.0
POLL_INTERVAL_MAX_S = 15.0
REQUEST_TIMEOUT_S = 30
REST_TIMEOUT_S = 5
TCP_PROBE_TIMEOUT_S = 3
DEFAULT_RPC_PORT = 6123
YT_UNKNOWN_PARAMETER_CODE = 1


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

    def messages(self):
        out = []
        stack = [self.payload]
        while stack:
            err = stack.pop()
            if not isinstance(err, dict):
                continue
            out.append(str(err.get("message") or ""))
            stack.extend(err.get("inner_errors") or [])
        return " | ".join(out)


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


def _bool_str(val):
    return "true" if val else "false"


# --- discovery -----------------------------------------------------------------------------------


def list_running_jobmanagers(operation_id, incarnation, use_server_filter=True):
    """Running ``jobmanager`` jobs of this operation; ``(jobs, server_filter_supported)``.

    Prefers the server-side ``operation_incarnation`` filter; falls back to filtering the
    ``operation_incarnation`` job attribute client-side on clusters that do not know the parameter.
    """
    params = {"operation_id": operation_id, "task_name": JOBMANAGER_TASK, "state": "running"}
    if incarnation and use_server_filter:
        try:
            resp = yt_request("list_jobs", dict(params, operation_incarnation=incarnation))
            return list(resp.get("jobs") or []), True
        except YtError as e:
            if YT_UNKNOWN_PARAMETER_CODE not in e.codes():
                raise
            _log("list_jobs does not support operation_incarnation here; filtering client-side")
    resp = yt_request("list_jobs", params)
    jobs = []
    for job in resp.get("jobs") or []:
        job_incarnation = job.get("operation_incarnation")
        if incarnation and job_incarnation and job_incarnation != incarnation:
            _log(
                "skipping JobManager %s from incarnation %r (ours is %r)"
                % (job.get("id"), job_incarnation, incarnation)
            )
            continue
        jobs.append(job)
    return jobs, False


def job_ip_addresses(operation_id, job_id):
    job = yt_request("get_job", {"operation_id": operation_id, "job_id": job_id})
    return list(((job or {}).get("exec_attributes") or {}).get("ip_addresses") or [])


def _url_host(host):
    return "[%s]" % host if ":" in host and not host.startswith("[") else host


def _strip_brackets(host):
    host = (host or "").strip()
    if host.startswith("[") and host.endswith("]"):
        return host[1:-1]
    return host


def fetch_jobmanager_config(host, rest_port):
    """``{key: value}`` from the Flink REST ``/jobmanager/config`` endpoint, or ``None``."""
    url = "http://%s:%d/jobmanager/config" % (_url_host(host), int(rest_port))
    try:
        with urllib.request.urlopen(url, timeout=REST_TIMEOUT_S) as resp:
            entries = json.loads(resp.read().decode("utf-8"))
    except (OSError, ValueError) as e:
        _log("REST %s not ready: %s" % (url, e))
        return None
    if not isinstance(entries, list):
        return None
    return {str(e.get("key")): str(e.get("value")) for e in entries if isinstance(e, dict) and "key" in e}


def rpc_endpoint_from_config(config):
    """``(host, port)`` the JobManager advertises for RPC (the address TaskManagers must use)."""
    host = _strip_brackets(config.get("jobmanager.rpc.address") or "")
    if not host:
        return None
    try:
        port = int(config.get("jobmanager.rpc.port") or DEFAULT_RPC_PORT)
    except ValueError:
        port = DEFAULT_RPC_PORT
    return host, port


def _tcp_open(host, port):
    try:
        with socket.create_connection((host, int(port)), timeout=TCP_PROBE_TIMEOUT_S):
            return True
    except OSError:
        return False


def resolve_jobmanager(operation_id, incarnation, rest_port, use_server_filter=True):
    """``(job_id, host, port, server_filter_supported)`` for a reachable JobManager, else ``None``."""
    jobs, server_filter = list_running_jobmanagers(operation_id, incarnation, use_server_filter)
    if not jobs:
        _log("no running JobManager job in operation %s yet" % operation_id)
        return None, server_filter
    for job in jobs:
        job_id = job.get("id") or job.get("job_id")
        if not job_id:
            continue
        addresses = job_ip_addresses(operation_id, job_id)
        if not addresses:
            _log("JobManager job %s has no addresses yet" % job_id)
            continue
        for addr in addresses:
            config = fetch_jobmanager_config(addr, rest_port)
            if not config:
                continue
            endpoint = rpc_endpoint_from_config(config)
            if not endpoint:
                _log("JobManager %s does not advertise jobmanager.rpc.address yet" % job_id)
                break
            host, port = endpoint
            if not _tcp_open(host, port):
                _log("JobManager RPC %s:%d (job %s) not accepting connections yet" % (host, port, job_id))
                break
            return (str(job_id), host, port), server_filter
    return None, server_filter


def cmd_wait_jobmanager(args):
    deadline = time.monotonic() + args.timeout
    interval = args.poll_interval
    use_server_filter = True
    while True:
        try:
            found, use_server_filter = resolve_jobmanager(
                args.operation_id, args.incarnation or "", args.rest_port, use_server_filter
            )
        except Exception as e:  # noqa: BLE001 - keep polling through transient proxy errors
            _log("discovery attempt failed (will retry): %s" % e)
            found = None
        if found:
            job_id, host, port = found
            _log("JobManager job %s advertises RPC %s:%d" % (job_id, host, port))
            sys.stdout.write("%s %s %d\n" % (job_id, host, port))
            sys.stdout.flush()
            return 0
        if time.monotonic() >= deadline:
            break
        time.sleep(min(interval, max(0.0, deadline - time.monotonic())))
        interval = min(interval * 1.5, POLL_INTERVAL_MAX_S)
    _log("no JobManager discovered for operation %s within %ss" % (args.operation_id, args.timeout))
    return 1


def cmd_complete_operation(args):
    yt_request("complete_operation", {"operation_id": args.operation_id}, method="POST")
    _log("operation %s completed" % args.operation_id)
    return 0


def build_parser():
    p = argparse.ArgumentParser(prog="flyt_job_helper")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("wait-jobmanager")
    s.add_argument("--operation-id", required=True)
    s.add_argument("--incarnation", default="")
    s.add_argument("--rest-port", type=int, required=True)
    s.add_argument("--timeout", type=float, default=600.0)
    s.add_argument("--poll-interval", type=float, default=POLL_INTERVAL_MIN_S)
    s.set_defaults(func=cmd_wait_jobmanager)

    s = sub.add_parser("complete-operation")
    s.add_argument("--operation-id", required=True)
    s.set_defaults(func=cmd_complete_operation)
    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
