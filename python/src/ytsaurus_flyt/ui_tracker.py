"""Background watcher that surfaces the Flink Web UI URL of a running Vanilla operation."""

from __future__ import annotations

import logging
import re
import socket
import threading
import webbrowser
from typing import List, Optional, Tuple

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from yt.wrapper import YtClient

logger = logging.getLogger(__name__)

FLINK_UI_PORT = 27050

_OP_ID_RE = re.compile(r"id=([0-9a-f-]{20,})", re.IGNORECASE)
_JOB_POLL_S = 5
_TCP_PROBE_S = 5
_TCP_TIMEOUT_S = 5
_MONITOR_INTERVAL_S = 10
_UI_FAIL_THRESHOLD = 3


class _OpIdCapture(logging.Handler):
    """Sniff the operation ID from the yt-client 'Operation started' log line."""

    def __init__(self) -> None:
        super().__init__()
        self.op_id: Optional[str] = None

    def emit(self, record: logging.LogRecord) -> None:
        if self.op_id is not None:
            return
        try:
            msg = record.getMessage()
        except Exception:
            return
        if "Operation started" not in msg:
            return
        m = _OP_ID_RE.search(msg)
        if m:
            self.op_id = m.group(1)


def _list_running_jobs(client: "YtClient", op_id: str) -> Optional[List[dict]]:
    try:
        resp = client.list_jobs(op_id, state="running")
    except Exception as exc:
        logger.debug("list_jobs(%s) failed: %s", op_id, exc)
        return None
    jobs = resp.get("jobs") if isinstance(resp, dict) else resp
    return list(jobs or [])


def _job_still_running(client: "YtClient", op_id: str, job_id: str) -> bool:
    jobs = _list_running_jobs(client, op_id)
    if jobs is None:
        return True  # assume alive on transient error
    return any((j.get("id") or j.get("job_id")) == job_id for j in jobs)


def _find_job_with_ipv6(client: "YtClient", op_id: str) -> Optional[Tuple[str, List[str]]]:
    for job in _list_running_jobs(client, op_id) or []:
        job_id = job.get("id") or job.get("job_id")
        if not job_id:
            continue
        try:
            full = client.get_job(op_id, job_id)
        except Exception as exc:
            logger.debug("get_job(%s, %s) failed: %s", op_id, job_id, exc)
            continue
        ips = ((full or {}).get("exec_attributes") or {}).get("ip_addresses") or []
        ipv6s = [ip for ip in ips if ":" in ip]
        if ipv6s:
            return job_id, ipv6s
    return None


def _probe_tcp(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=_TCP_TIMEOUT_S):
            return True
    except OSError:
        return False


def _wait_port_open(
    client: "YtClient",
    op_id: str,
    job_id: str,
    ipv6s: List[str],
    stop: threading.Event,
) -> Optional[str]:
    while not stop.is_set():
        for host in ipv6s:
            if _probe_tcp(host, FLINK_UI_PORT):
                return host
        if not _job_still_running(client, op_id, job_id):
            return None
        if stop.wait(_TCP_PROBE_S):
            return None
    return None


def _monitor(
    client: "YtClient",
    op_id: str,
    job_id: str,
    host: str,
    stop: threading.Event,
) -> None:
    fails = 0
    while not stop.is_set():
        if stop.wait(_MONITOR_INTERVAL_S):
            return
        if _probe_tcp(host, FLINK_UI_PORT):
            fails = 0
            continue
        fails += 1
        if fails >= _UI_FAIL_THRESHOLD or not _job_still_running(client, op_id, job_id):
            logger.info("Flink UI lost for job %s", job_id)
            return


def _watch_loop(
    client: "YtClient",
    op_id: str,
    stop: threading.Event,
    open_in_browser: bool,
) -> None:
    logger.info("Waiting for Flink Web UI...")
    first = True
    while not stop.is_set():
        found = _find_job_with_ipv6(client, op_id)
        if not found:
            if stop.wait(_JOB_POLL_S):
                return
            continue

        job_id, ipv6s = found
        host = _wait_port_open(client, op_id, job_id, ipv6s, stop)
        if not host:
            continue

        url = f"http://[{host}]:{FLINK_UI_PORT}"
        if first:
            logger.info("Flink Web UI: %s", url)
            if open_in_browser:
                try:
                    webbrowser.open(url)
                except Exception:
                    pass
            first = False
        else:
            logger.info("Job restarted (%s). Flink Web UI: %s", job_id, url)

        _monitor(client, op_id, job_id, host, stop)


def _thread_main(
    proxy: str,
    capture: _OpIdCapture,
    stop: threading.Event,
    open_in_browser: bool,
) -> None:
    # Wait for op ID to appear in logs
    while not stop.is_set() and capture.op_id is None:
        if stop.wait(1):
            return

    op_id = capture.op_id
    if not op_id:
        return

    from yt.wrapper import YtClient  # noqa: PLC0415

    _watch_loop(YtClient(proxy=proxy), op_id, stop, open_in_browser)


class FlinkUIWatcher:
    """Context manager: background thread that surfaces the Flink Web UI URL.

    Attaches to the Python logging system to capture the operation ID from the
    yt-client 'Operation started' log line, then polls list_jobs/get_job for the
    running job's IPv6, TCP-probes port 27050, and logs the URL when it comes up.
    Handles job restarts. Optionally opens the URL in the default browser.

    Usage::

        with FlinkUIWatcher(proxy="hahn.yt.yandex.net", open_in_browser=True):
            launch_vanilla_job(...)
    """

    def __init__(self, proxy: str, open_in_browser: bool = True) -> None:
        self._proxy = proxy
        self._open = open_in_browser
        self._capture = _OpIdCapture()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def __enter__(self) -> "FlinkUIWatcher":
        logging.getLogger().addHandler(self._capture)
        self._thread = threading.Thread(
            target=_thread_main,
            args=(self._proxy, self._capture, self._stop, self._open),
            daemon=True,
            name="flink-ui-watcher",
        )
        self._thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self._stop.set()
        logging.getLogger().removeHandler(self._capture)
