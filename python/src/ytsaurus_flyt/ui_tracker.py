"""Background watcher that surfaces the Flink Web UI URL of a running Vanilla operation."""

from __future__ import annotations

import logging
import re
import socket
import threading
import webbrowser
from typing import TYPE_CHECKING, List, Optional, Tuple

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
        self._op_id: Optional[str] = None
        self._event = threading.Event()

    @property
    def op_id(self) -> Optional[str]:
        return self._op_id

    def wait(self, stop: threading.Event) -> Optional[str]:
        """Block until op_id is captured or stop is set; return op_id or None."""
        while not stop.is_set():
            if self._event.wait(timeout=1):
                return self._op_id
        return None

    def emit(self, record: logging.LogRecord) -> None:
        if self._event.is_set():
            return
        try:
            msg = record.getMessage()
        except Exception:
            return
        if "Operation started" not in msg:
            return
        m = _OP_ID_RE.search(msg)
        if m:
            self._op_id = m.group(1)
            self._event.set()


def _list_running_jobs(client: "YtClient", op_id: str) -> Optional[List[dict]]:
    try:
        resp = client.list_jobs(op_id, job_state="running")
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


def _announce_ui_url(url: str, *, open_in_browser: bool) -> None:
    bar = "=" * (len(url) + 4)
    logger.info("%s", bar)
    logger.info("  Flink Web UI: %s", url)
    logger.info("%s", bar)
    if not open_in_browser:
        return
    try:
        opened = webbrowser.open(url)
    except Exception as exc:
        logger.warning("Could not auto-open Flink Web UI in a browser: %s. Open the URL above manually.", exc)
        return
    if not opened:
        logger.info("Browser did not open automatically. Open the URL above manually.")


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
            _announce_ui_url(url, open_in_browser=open_in_browser)
            first = False
        else:
            logger.info("Job restarted (%s). Flink Web UI: %s", job_id, url)

        _monitor(client, op_id, job_id, host, stop)


def find_ui_url_for_operation(client: "YtClient", op_id: str, *, probe: bool = True) -> Optional[str]:
    """Synchronously resolve the Flink Web UI URL for a running operation, or ``None``.

    Finds a running job with an IPv6 exec address; with ``probe`` (default) only
    returns hosts where port 27050 actually accepts connections.
    """
    found = _find_job_with_ipv6(client, op_id)
    if not found:
        return None
    _, ipv6s = found
    for host in ipv6s:
        if not probe or _probe_tcp(host, FLINK_UI_PORT):
            return f"http://[{host}]:{FLINK_UI_PORT}"
    return None


_TERMINAL_OP_STATES = frozenset({"aborted", "completed", "failed"})


def _operation_state(client: "YtClient", op_id: str) -> str:
    try:
        return str(client.get_operation_state(op_id))
    except Exception as exc:
        logger.debug("get_operation_state(%s) failed: %s", op_id, exc)
        return "unknown"


def wait_ui_url_for_operation(
    client: "YtClient",
    op_id: str,
    *,
    poll_interval_s: float = _JOB_POLL_S,
    stop: Optional[threading.Event] = None,
) -> Tuple[Optional[str], str]:
    """Block until the Flink Web UI is reachable or the operation reaches a terminal state.

    Returns ``(url, operation_state)``; ``url`` is ``None`` when the operation
    finished (or ``stop`` was set) before the UI came up.
    """
    stop = stop or threading.Event()
    while not stop.is_set():
        url = find_ui_url_for_operation(client, op_id)
        if url:
            return url, _operation_state(client, op_id)
        state = _operation_state(client, op_id)
        if state in _TERMINAL_OP_STATES:
            return None, state
        if stop.wait(poll_interval_s):
            break
    return None, _operation_state(client, op_id)


def _thread_main(
    proxy: str,
    capture: _OpIdCapture,
    stop: threading.Event,
    open_in_browser: bool,
) -> None:
    op_id = capture.wait(stop)
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

    Not useful with ``sync=False`` (detach mode) since the process exits before
    the watcher can find the job — skip it in that case.

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
        logging.getLogger("Yt").addHandler(self._capture)
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
        logging.getLogger("Yt").removeHandler(self._capture)
