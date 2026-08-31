"""Terse step-progress UI for ``flyt run`` (issue #48).

A dependency-free reporter that renders launcher phases as a spinner checklist on stderr,
degrading to one plain line per step off a TTY. ``LoggingReporter`` implements the same
interface via :mod:`logging` (the default in :func:`launch_vanilla_job`, and under ``-v``).
"""

from __future__ import annotations

import contextlib
import logging
import os
import sys
import threading
import time
from typing import Iterable, Iterator, List, Optional, Tuple

import click

_FRAMES_UNICODE = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
_FRAMES_ASCII = "|/-\\"
_TICK_S = 0.1


def _supports_ansi(stream: object) -> bool:
    if os.environ.get("NO_COLOR"):
        return False
    if os.environ.get("FORCE_COLOR"):
        return True
    return bool(getattr(stream, "isatty", lambda: False)())


def _can_encode(text: str, encoding: Optional[str]) -> bool:
    try:
        text.encode(encoding or "ascii")
        return True
    except (UnicodeError, LookupError):
        return False


class Reporter:
    """Phase-progress sink used by the launcher. The base is a no-op."""

    def header(self, profile_name: Optional[str]) -> None: ...

    def step(self, label: str) -> "contextlib.AbstractContextManager[None]":
        return contextlib.nullcontext()

    def line(self, message: str) -> None: ...

    def result(self, rows: Iterable[Tuple[str, str]]) -> None: ...

    def emit_above(self, text: str) -> None:
        """Print log output without disturbing an active step (no-op here)."""


class LoggingReporter(Reporter):
    """Maps steps onto a logger — the default sink and ``flyt run -v``."""

    def __init__(self, logger: logging.Logger, level: int = logging.INFO) -> None:
        self._logger = logger
        self._level = level

    def header(self, profile_name: Optional[str]) -> None:
        if profile_name:
            self._logger.log(self._level, "Profile: %r", profile_name)

    @contextlib.contextmanager
    def step(self, label: str) -> Iterator[None]:
        self._logger.log(self._level, "%s...", label)
        yield

    def line(self, message: str) -> None:
        self._logger.log(self._level, "%s", message)

    def result(self, rows: Iterable[Tuple[str, str]]) -> None:
        for key, value in rows:
            self._logger.log(self._level, "%s: %s", key, value)


class StepReporter(Reporter):
    """Animated checklist on stderr, with a plain-line fallback off a TTY."""

    def __init__(self, ansi: Optional[bool] = None) -> None:
        # Resolve the stream lazily at write time so a swapped-out sys.stderr
        # (e.g. click's CliRunner) is honored; glyphs degrade to ASCII when the
        # stream encoding can't carry them.
        self._ansi = _supports_ansi(sys.stderr) if ansi is None else ansi
        unicode_ok = _can_encode(_FRAMES_UNICODE + "✓✗▸•", getattr(sys.stderr, "encoding", None))
        self.frames = _FRAMES_UNICODE if unicode_ok else _FRAMES_ASCII
        self.ok, self.bad, self.sep, self.dot = ("✓", "✗", "▸", "•") if unicode_ok else ("+", "x", ">", "-")
        self._lock = threading.Lock()
        self._active: Optional["_ActiveStep"] = None  # step currently animating, if any

    def _style(self, text: str, **kwargs: object) -> str:
        return click.style(text, **kwargs) if self._ansi else text  # type: ignore[arg-type]

    def _write(self, text: str) -> None:
        with self._lock:
            sys.stderr.write(text)
            sys.stderr.flush()

    def emit_above(self, text: str) -> None:
        """Print ``text`` above the live spinner: wipe its line, write, let it redraw."""
        msg = text.rstrip("\n")
        if not msg:
            return
        with self._lock:
            if self._ansi and self._active is not None:
                sys.stderr.write("\r\x1b[K")  # clear the spinner line; the spin thread redraws it
            sys.stderr.write(msg + "\n")
            sys.stderr.flush()

    def header(self, profile_name: Optional[str]) -> None:
        name = self._style(repr(profile_name) if profile_name else "(none)", fg="cyan")
        self._write(f"{self._style('flyt', bold=True)} {self._style(self.sep, fg='bright_black')} profile {name}\n\n")

    def step(self, label: str) -> "_ActiveStep":
        return _ActiveStep(self, label)

    def line(self, message: str) -> None:
        self._write(f"  {self._style(self.dot, fg='bright_black')} {message}\n")

    def result(self, rows: Iterable[Tuple[str, str]]) -> None:
        rows = list(rows)
        if not rows:
            return
        width = max(len(k) for k, _ in rows)
        body = "".join(f"  {self._style(k.ljust(width), fg='bright_black')}  {v}\n" for k, v in rows)
        self._write(f"\n{body}")


class _ActiveStep:
    """One step: spinner + timer on a TTY, else a single completion line."""

    def __init__(self, reporter: StepReporter, label: str) -> None:
        self._r = reporter
        self._label = label
        self._start = 0.0
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def __enter__(self) -> "_ActiveStep":
        self._start = time.monotonic()
        self._r._active = self
        if self._r._ansi:
            self._thread = threading.Thread(target=self._spin, daemon=True, name="flyt-spinner")
            self._thread.start()
        return self

    def __exit__(self, exc_type: object, *_: object) -> bool:
        self._stop.set()
        if self._thread is not None:
            self._thread.join()
        self._r._active = None
        ok = exc_type is None
        mark = self._r._style(self._r.ok, fg="green") if ok else self._r._style(self._r.bad, fg="red")
        line = f"  {mark} {self._label}  {self._r._style(self._elapsed(), fg='bright_black')}"
        # On a TTY, overwrite the spinner line; otherwise just print it.
        self._r._write(f"\r{line}\x1b[K\n" if self._r._ansi else f"{line}\n")
        return False  # never swallow the exception

    def _spin(self) -> None:
        i = 0
        while not self._stop.is_set():
            frame = self._r._style(self._r.frames[i % len(self._r.frames)], fg="cyan")
            elapsed = self._r._style(self._elapsed(), fg="bright_black")
            self._r._write(f"\r  {frame} {self._label}  {elapsed}\x1b[K")
            i += 1
            self._stop.wait(_TICK_S)

    def _elapsed(self) -> str:
        return f"{time.monotonic() - self._start:.1f}s"


def make_reporter(verbose: bool, logger: logging.Logger) -> Reporter:
    """Verbose -> logging; terse -> the step UI."""
    return LoggingReporter(logger) if verbose else StepReporter()


class FirstLineFormatter(logging.Formatter):
    """Collapse a multi-line record to one line. If the first line is a bare header ending in
    ``:``, append the next line so the detail (e.g. ``StatusCode: 413``) survives."""

    def format(self, record: logging.LogRecord) -> str:
        full = super().format(record)
        lines = [ln.strip() for ln in full.splitlines() if ln.strip()]
        if not lines:
            return full
        if lines[0].endswith(":") and len(lines) > 1:
            return f"{lines[0]} {lines[1]}"
        return lines[0]


class ReporterLogHandler(logging.Handler):
    """Route log records through a reporter so they print cleanly above the spinner."""

    def __init__(self, reporter: Reporter, level: int = logging.NOTSET) -> None:
        super().__init__(level)
        self._reporter = reporter

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._reporter.emit_above(self.format(record))
        except Exception:
            self.handleError(record)


__all__: List[str] = [
    "Reporter",
    "LoggingReporter",
    "StepReporter",
    "make_reporter",
    "FirstLineFormatter",
    "ReporterLogHandler",
]
