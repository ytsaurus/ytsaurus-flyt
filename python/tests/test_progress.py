"""Tests for the terse step-progress reporter (issue #48)."""

import logging

import pytest

from ytsaurus_flyt.progress import LoggingReporter, StepReporter, make_reporter


def test_step_reporter_non_tty_emits_completion_line(capsys) -> None:
    rep = StepReporter(ansi=False)
    rep.header("kind-dev")
    with rep.step("Building service wheel"):
        pass
    err = capsys.readouterr().err
    assert "profile 'kind-dev'" in err
    assert "✓ Building service wheel" in err
    assert "s" in err  # elapsed suffix like "0.0s"


def test_step_reporter_marks_failure_and_reraises(capsys) -> None:
    rep = StepReporter(ansi=False)
    with pytest.raises(ValueError):
        with rep.step("Resolving Flink lib JARs"):
            raise ValueError("boom")
    err = capsys.readouterr().err
    assert "✗ Resolving Flink lib JARs" in err


def test_step_reporter_result_block_aligns_keys(capsys) -> None:
    rep = StepReporter(ansi=False)
    rep.result([("operation", "abc-123"), ("track", "http://yt/op")])
    err = capsys.readouterr().err
    assert "operation" in err and "abc-123" in err
    assert "track" in err and "http://yt/op" in err


def test_logging_reporter_logs_steps_and_results(caplog) -> None:
    logger = logging.getLogger("ytsaurus_flyt.test")
    rep = LoggingReporter(logger)
    with caplog.at_level(logging.INFO, logger="ytsaurus_flyt.test"):
        with rep.step("Fetching credentials"):
            pass
        rep.result([("operation", "op-1")])
    text = caplog.text
    assert "Fetching credentials" in text
    assert "operation" in text and "op-1" in text


def test_make_reporter_picks_type_by_verbosity() -> None:
    logger = logging.getLogger("ytsaurus_flyt.test")
    assert isinstance(make_reporter(verbose=True, logger=logger), LoggingReporter)
    assert isinstance(make_reporter(verbose=False, logger=logger), StepReporter)


def test_first_line_formatter_appends_detail_to_bare_header() -> None:
    from ytsaurus_flyt.progress import FirstLineFormatter

    f = FirstLineFormatter("%(message)s")
    rec = logging.LogRecord("Yt", logging.ERROR, "", 0, "Bad response:\nStatusCode: 413", None, None)
    assert f.format(rec) == "Bad response: StatusCode: 413"
    single = logging.LogRecord("Yt", logging.WARNING, "", 0, "Sleep 72s before retry", None, None)
    assert f.format(single) == "Sleep 72s before retry"


def test_reporter_log_handler_routes_through_emit_above() -> None:
    from ytsaurus_flyt.progress import FirstLineFormatter, ReporterLogHandler

    seen = []

    class _Rep(StepReporter):
        def emit_above(self, text: str) -> None:
            seen.append(text)

    handler = ReporterLogHandler(_Rep(ansi=False), level=logging.WARNING)
    handler.setFormatter(FirstLineFormatter("%(message)s"))
    handler.emit(logging.LogRecord("Yt", logging.WARNING, "", 0, "write failed", None, None))
    assert seen == ["write failed"]


def test_emit_above_clears_spinner_line_when_active(capsys) -> None:
    rep = StepReporter(ansi=True)
    with rep.step("Packing"):
        rep.emit_above("WARNING: upload retry")
    err = capsys.readouterr().err
    assert "WARNING: upload retry" in err
    assert "\x1b[K" in err  # spinner line was cleared before the message
