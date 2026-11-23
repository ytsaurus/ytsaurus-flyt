"""Run a Flink SQL script inside a flyt job on a PyFlink ``TableEnvironment``.

``flyt run`` copies this file into the service wheel as ``flyt_sql_job/run.py``,
so it must stay self-contained: no ``ytsaurus_flyt`` imports; PyFlink, ``sqlparse`` and
``yt.wrapper`` come from the runtime layer and are imported lazily. The pure helpers
(splitting, classification) are unit-tested without PyFlink.
"""

from __future__ import annotations

import argparse
import datetime as dt
import decimal
import json
import logging
import os
import re
import sys
from typing import Any, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger("flyt.sql_runner")

DEFAULT_ROW_LIMIT = 1000
RUNTIME_MODE_KEY = "execution.runtime-mode"

# Statement kinds returned by :func:`classify`.
KIND_SET = "set"
KIND_INSERT = "insert"
KIND_QUERY = "query"
KIND_SHOW = "show"
KIND_DDL = "ddl"

_STATEMENT_SET_HEAD = re.compile(r"^\s*(EXECUTE\s+STATEMENT\s+SET\s+BEGIN|BEGIN\s+STATEMENT\s+SET)\b", re.I)
_STATEMENT_SET_END = re.compile(r"^\s*END\s*$", re.I)


# --- script parsing ------------------------------------------------------


def split_statements(text: str) -> List[str]:
    """Split a script on ``;`` tokens (sqlparse lexer); comments are dropped, hints are kept.

    ``EXECUTE STATEMENT SET BEGIN ...; ...; END`` (and the legacy
    ``BEGIN STATEMENT SET; ...; END``) is returned as one statement.
    """
    from sqlparse import lexer, tokens

    pieces: List[str] = []
    buf: List[str] = []
    for ttype, value in lexer.tokenize(text):
        if ttype in tokens.Comment and not value.startswith("/*+"):
            buf.append("\n")
        elif ttype is tokens.Punctuation and value == ";":
            pieces.append("".join(buf))
            buf = []
        else:
            buf.append(value)
    pieces.append("".join(buf))
    return _merge_statement_sets([p.strip() for p in pieces if p.strip()])


def _merge_statement_sets(pieces: Sequence[str]) -> List[str]:
    out: List[str] = []
    block: Optional[List[str]] = None
    for piece in pieces:
        if block is None:
            if _STATEMENT_SET_HEAD.match(piece):
                head = _STATEMENT_SET_HEAD.match(piece).group(0)  # type: ignore[union-attr]
                first = piece[len(head) :].strip()
                block = [first] if first else []
            else:
                out.append(piece)
        elif _STATEMENT_SET_END.match(piece):
            out.append("EXECUTE STATEMENT SET BEGIN\n" + ";\n".join(block) + ";\nEND")
            block = None
        else:
            block.append(piece)
    if block is not None:
        raise ValueError("STATEMENT SET is not closed with END")
    return out


def _head_words(stmt: str, count: int = 3) -> List[str]:
    return [w.upper() for w in re.findall(r"[A-Za-z_]+", stmt)[:count]]


def classify(stmt: str) -> str:
    """Coarse statement kind: decides how the runner executes it."""
    words = _head_words(stmt)
    first = words[0] if words else ""
    if first in ("SET", "RESET"):
        return KIND_SET
    if first == "INSERT":
        return KIND_INSERT
    if first in ("SELECT", "WITH", "VALUES") or stmt.lstrip().startswith("("):
        return KIND_QUERY
    if first in ("SHOW", "DESCRIBE", "DESC", "EXPLAIN"):
        return KIND_SHOW
    return KIND_DDL


def _unquote(s: str) -> str:
    s = s.strip()
    if len(s) >= 2 and s[0] == s[-1] and s[0] in ("'", '"', "`"):
        inner = s[1:-1]
        return inner.replace("''", "'") if s[0] == "'" else inner
    return s


def parse_set(stmt: str) -> Optional[Tuple[str, Optional[str]]]:
    """``SET 'k' = 'v'`` -> ``(k, v)``; ``RESET 'k'`` -> ``(k, None)``; bare ``SET``/``RESET`` -> ``None``."""
    m = re.match(r"^\s*(SET|RESET)\b(.*)$", stmt, re.I | re.S)
    if not m:
        raise ValueError(f"Not a SET/RESET statement: {stmt!r}")
    body = m.group(2).strip()
    if not body:
        return None
    if m.group(1).upper() == "RESET":
        return _unquote(body), None
    if "=" not in body:
        raise ValueError(f"SET expects key = value, got: {stmt!r}")
    key, value = body.split("=", 1)
    return _unquote(key), _unquote(value)


def leading_settings(statements: Sequence[str]) -> Tuple[Dict[str, str], List[str]]:
    """Apply SET/RESET statements that precede the first other statement to the environment config.

    Returns the collected settings and the remaining statements. Later SETs are applied at runtime.
    """
    settings: Dict[str, str] = {}
    idx = 0
    for idx, stmt in enumerate(statements):
        if classify(stmt) != KIND_SET:
            return settings, list(statements[idx:])
        parsed = parse_set(stmt)
        if parsed is None:
            continue
        key, value = parsed
        if value is None:
            settings.pop(key, None)
        else:
            settings[key] = value
    return settings, []


def resolve_mode(settings: Dict[str, str], context_mode: str = "") -> str:
    """``batch`` or ``streaming``: the script's leading SET wins over the job's Flink configuration."""
    value = settings.get(RUNTIME_MODE_KEY) or context_mode or ""
    return "batch" if value.strip().lower() == "batch" else "streaming"


# --- result conversion ---------------------------------------------------

_PRIMITIVE_YT_TYPES = {
    "BigIntType": "int64",
    "IntType": "int32",
    "SmallIntType": "int16",
    "TinyIntType": "int8",
    "DoubleType": "double",
    "FloatType": "float",
    "BooleanType": "boolean",
    "VarCharType": "utf8",
    "CharType": "utf8",
    "VarBinaryType": "string",
    "BinaryType": "string",
}
_CONTAINER_TYPES = ("ArrayType", "MapType", "RowType", "MultisetType")


def yt_type_for(data_type: Any) -> str:
    """YT column type for a PyFlink ``DataType`` (by class name, so no PyFlink import is needed)."""
    name = type(data_type).__name__
    if name in _PRIMITIVE_YT_TYPES:
        return _PRIMITIVE_YT_TYPES[name]
    if name in _CONTAINER_TYPES:
        return "any"
    # DECIMAL, DATE, TIME, TIMESTAMP*: stored as their string form.
    return "utf8"


def to_yt_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str, bytes)):
        return value
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        return value.isoformat()
    if hasattr(value, "_values"):  # pyflink Row
        return [to_yt_value(v) for v in value._values]
    if isinstance(value, (list, tuple)):
        return [to_yt_value(v) for v in value]
    if isinstance(value, dict):
        return {str(k): to_yt_value(v) for k, v in value.items()}
    return str(value)


def _row_values(row: Any) -> List[Any]:
    values = getattr(row, "_values", None)
    return list(values) if values is not None else list(row)


def _emit(event: str, **fields: Any) -> None:
    """Machine-readable progress line on stderr (``FLYT_SQL {json}``) for callers such as a QT engine."""
    payload = {"event": event, **fields}
    print("FLYT_SQL " + json.dumps(payload, ensure_ascii=False, default=str), file=sys.stderr, flush=True)


def error_message(exc: BaseException) -> str:
    """Java cause chain of a failed statement (Flink puts the actual reason in a cause), or ``str(exc)``."""
    cause = getattr(exc, "java_exception", None)
    if cause is None:
        return str(exc)
    messages: List[str] = []
    while cause is not None:
        messages.append(str(cause.toString()))
        cause = cause.getCause()
    return "\n".join(messages)


# --- execution -------------------------------------------------------------


def context_runtime_mode() -> str:
    """``execution.runtime-mode`` of the job's ``config.yaml`` (flyt run -D, --flink-conf, the profile)."""
    from pyflink.java_gateway import get_gateway

    conf = get_gateway().jvm.org.apache.flink.configuration.GlobalConfiguration.loadConfiguration()
    return conf.getString(RUNTIME_MODE_KEY, "")


def create_table_environment(mode: str, settings: Dict[str, str]) -> Any:
    """A TableEnvironment over the job's context configuration, with the script's leading SETs on top."""
    from pyflink.common import Configuration
    from pyflink.table import EnvironmentSettings, TableEnvironment

    conf = Configuration()
    for key, value in settings.items():
        conf.set_string(key, value)
    builder = EnvironmentSettings.new_instance().with_configuration(conf)
    builder = builder.in_batch_mode() if mode == "batch" else builder.in_streaming_mode()
    return TableEnvironment.create(builder.build())


def collect_rows(table_result: Any, limit: int) -> Tuple[List[List[Any]], bool]:
    """Up to ``limit`` rows from a query result; the flag says whether more rows were available."""
    rows: List[List[Any]] = []
    truncated = False
    with table_result.collect() as it:
        for row in it:
            if len(rows) >= limit:
                truncated = True
                break
            rows.append([to_yt_value(v) for v in _row_values(row)])
    return rows, truncated


def result_columns(table_result: Any) -> List[Tuple[str, str]]:
    schema = table_result.get_table_schema()
    return [(name, yt_type_for(t)) for name, t in zip(schema.get_field_names(), schema.get_field_data_types())]


def write_result_table(
    path: str, proxy: str, columns: Sequence[Tuple[str, str]], rows: Sequence[Sequence[Any]]
) -> None:
    """Replace ``path`` with a static table holding the query result (token from ``YT_TOKEN``)."""
    from yt.wrapper import YtClient

    client = YtClient(proxy=proxy, token=os.environ.get("YT_TOKEN") or None)
    schema = [{"name": name, "type": yt_type} for name, yt_type in columns]
    client.create("table", path, recursive=True, force=True, attributes={"schema": schema})
    names = [name for name, _ in columns]
    client.write_table(path, [dict(zip(names, row)) for row in rows])


class SqlScriptRunner:
    def __init__(self, t_env: Any, *, result_table: Optional[str], yt_proxy: Optional[str], row_limit: int) -> None:
        self.t_env = t_env
        self.result_table = result_table
        self.yt_proxy = yt_proxy
        self.row_limit = row_limit
        self.statement_set = None
        self.pending_inserts = 0
        self.job_results: List[Any] = []
        self.query_count = 0

    def run(self, statements: Sequence[str]) -> None:
        for index, stmt in enumerate(statements):
            kind = classify(stmt)
            logger.info("Statement %d (%s): %s", index, kind, stmt.splitlines()[0][:120])
            try:
                self._execute(kind, stmt)
            except Exception as exc:
                _emit("statement_failed", index=index, kind=kind, error=error_message(exc))
                raise
        if self.pending_inserts:
            self.job_results.append(self.statement_set.execute())

    def _execute(self, kind: str, stmt: str) -> None:
        if kind == KIND_SET:
            self._apply_set(stmt)
        elif kind == KIND_INSERT:
            if self.statement_set is None:
                self.statement_set = self.t_env.create_statement_set()
            self.statement_set.add_insert_sql(stmt)
            self.pending_inserts += 1
        elif kind == KIND_QUERY:
            self._run_query(stmt)
        elif kind == KIND_SHOW:
            self.t_env.execute_sql(stmt).print()
        else:
            # DDL returns no job; a statement set, CREATE TABLE AS, EXECUTE PLAN, UPDATE or DELETE submits one.
            result = self.t_env.execute_sql(stmt)
            if result.get_job_client() is not None:
                self.job_results.append(result)

    def _apply_set(self, stmt: str) -> None:
        parsed = parse_set(stmt)
        if parsed is None:
            return
        key, value = parsed
        if value is None:
            logger.warning("RESET %r is not supported after the environment is created; ignored", key)
            return
        if key == RUNTIME_MODE_KEY:
            logger.warning("SET %s must precede the first statement to take effect; ignored", key)
            return
        self.t_env.get_config().set(key, value)

    def _run_query(self, stmt: str) -> None:
        self.query_count += 1
        result = self.t_env.execute_sql(stmt)
        columns = result_columns(result)
        rows, truncated = collect_rows(result, self.row_limit)
        if self.result_table:
            path = self.result_table if self.query_count == 1 else f"{self.result_table}_{self.query_count}"
            write_result_table(path, self.yt_proxy or "", columns, rows)
            _emit("query_result", path=path, row_count=len(rows), truncated=truncated, columns=columns)
        else:
            _emit("query_result", path=None, row_count=len(rows), truncated=truncated, columns=columns)
            print("\t".join(name for name, _ in columns), file=sys.stderr)
            for row in rows:
                print("\t".join("" if v is None else str(v) for v in row), file=sys.stderr)

    def wait_for_jobs(self) -> None:
        """Both cluster modes end the Flink cluster with the script, so it stays until its jobs finish."""
        for result in self.job_results:
            client = result.get_job_client()
            job_id = str(client.get_job_id()) if client is not None else None
            _emit("job_submitted", job_id=job_id)
        for result in self.job_results:
            result.wait()


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="flyt-sql-runner", description="Execute a Flink SQL script with PyFlink.")
    parser.add_argument("script", help="Path to the .sql file.")
    parser.add_argument("--result-table", default=None, help="YT table to (re)create with SELECT rows.")
    parser.add_argument("--yt-proxy", default=None, help="YT proxy for --result-table (token from YT_TOKEN).")
    parser.add_argument("--row-limit", type=int, default=DEFAULT_ROW_LIMIT, help="Max SELECT rows kept.")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    logging.basicConfig(
        stream=sys.stderr, level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    args = build_arg_parser().parse_args(argv)
    if args.result_table and not args.yt_proxy:
        raise SystemExit("--result-table requires --yt-proxy")

    with open(args.script, encoding="utf-8") as f:
        statements = split_statements(f.read())
    if not statements:
        raise SystemExit(f"No statements found in {args.script}")

    settings, remaining = leading_settings(statements)
    mode = resolve_mode(settings, context_runtime_mode())
    settings.pop(RUNTIME_MODE_KEY, None)
    _emit("script_started", statements=len(statements), mode=mode, settings=sorted(settings))

    t_env = create_table_environment(mode, settings)
    runner = SqlScriptRunner(t_env, result_table=args.result_table, yt_proxy=args.yt_proxy, row_limit=args.row_limit)
    runner.run(remaining)
    runner.wait_for_jobs()
    _emit("script_finished", queries=runner.query_count, jobs=len(runner.job_results))
    return 0


if __name__ == "__main__":
    sys.exit(main())
