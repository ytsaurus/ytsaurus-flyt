"""Pure helpers of the bundled SQL runner (no PyFlink needed)."""

import json

import pytest

from ytsaurus_flyt import sql_runner as r


def test_split_statements_respects_quotes_and_comments() -> None:
    script = """
    -- comment; with semicolon
    CREATE TABLE t (s STRING) WITH ('connector' = 'datagen', 'x' = 'a;b');
    /* block; comment */
    SELECT 'it''s; fine' FROM t;
    SELECT `we;ird` FROM t;
    SELECT s FROM t /*+ OPTIONS('k' = 'a;b') */ -- trailing; comment
    """
    assert r.split_statements(script) == [
        "CREATE TABLE t (s STRING) WITH ('connector' = 'datagen', 'x' = 'a;b')",
        "SELECT 'it''s; fine' FROM t",
        "SELECT `we;ird` FROM t",
        "SELECT s FROM t /*+ OPTIONS('k' = 'a;b') */",
    ]


def test_split_statements_keeps_statement_set_whole() -> None:
    script = (
        "EXECUTE STATEMENT SET BEGIN INSERT INTO a SELECT CASE WHEN 1 = 1 THEN 1 END; INSERT INTO b SELECT 2; END;"
        " SELECT 3;"
    )
    assert r.split_statements(script) == [
        "EXECUTE STATEMENT SET BEGIN\nINSERT INTO a SELECT CASE WHEN 1 = 1 THEN 1 END;\nINSERT INTO b SELECT 2;\nEND",
        "SELECT 3",
    ]
    legacy = "BEGIN STATEMENT SET; INSERT INTO a SELECT 1; END;"
    assert r.split_statements(legacy) == ["EXECUTE STATEMENT SET BEGIN\nINSERT INTO a SELECT 1;\nEND"]
    with pytest.raises(ValueError, match="not closed"):
        r.split_statements("EXECUTE STATEMENT SET BEGIN INSERT INTO a SELECT 1;")


@pytest.mark.parametrize(
    ("stmt", "kind"),
    [
        ("SET 'a' = 'b'", r.StatementKind.SET),
        ("reset 'a'", r.StatementKind.SET),
        ("ADD JAR '/x.jar'", r.StatementKind.DDL),
        ("INSERT INTO t SELECT 1", r.StatementKind.INSERT),
        ("EXECUTE STATEMENT SET BEGIN\nINSERT INTO t SELECT 1;\nEND", r.StatementKind.DDL),
        ("SELECT 1", r.StatementKind.QUERY),
        ("WITH c AS (SELECT 1) SELECT * FROM c", r.StatementKind.QUERY),
        ("(SELECT 1) UNION ALL (SELECT 2)", r.StatementKind.QUERY),
        ("SHOW TABLES", r.StatementKind.SHOW),
        ("EXPLAIN SELECT 1", r.StatementKind.SHOW),
        ("CREATE TABLE t (a INT) WITH ('connector' = 'print')", r.StatementKind.DDL),
        ("USE CATALOG c", r.StatementKind.DDL),
    ],
)
def test_classify(stmt: str, kind: r.StatementKind) -> None:
    assert r.classify(stmt) == kind


def test_parse_set_variants() -> None:
    assert r.parse_set("SET 'execution.runtime-mode' = 'batch'") == ("execution.runtime-mode", "batch")
    assert r.parse_set("SET parallelism.default=4") == ("parallelism.default", "4")
    assert r.parse_set("SET 'k' = 'a = b'") == ("k", "a = b")
    assert r.parse_set("RESET 'k'") == ("k", None)
    assert r.parse_set("SET") is None
    with pytest.raises(ValueError):
        r.parse_set("SET nonsense")


def test_leading_settings_and_mode() -> None:
    stmts = ["SET 'execution.runtime-mode' = 'batch'", "SET 'x' = '1'", "RESET 'x'", "SELECT 1", "SET 'y' = '2'"]
    settings, rest = r.leading_settings(stmts)
    assert settings == {"execution.runtime-mode": "batch"}
    assert rest == ["SELECT 1", "SET 'y' = '2'"]
    assert r.resolve_mode(settings) == "batch"
    assert r.resolve_mode(settings, "streaming") == "batch"
    assert r.resolve_mode({}, "BATCH") == "batch"
    assert r.resolve_mode({}) == "streaming"


def test_runner_batches_inserts_and_collects_queries(capsys) -> None:
    class FakeResult:
        def __init__(self, rows, job_client=None):
            self._rows = rows
            self.job_client = job_client

        def collect(self):
            rows = self._rows

            class It:
                def __enter__(self):
                    return iter(rows)

                def __exit__(self, *a):
                    return False

            return It()

        def get_table_schema(self):
            class BigIntType: ...

            class S:
                def get_field_names(self):
                    return ["n"]

                def get_field_data_types(self):
                    return [BigIntType()]

            return S()

        def print(self):
            pass

        def get_job_client(self):
            return self.job_client

    class FakeStatementSet:
        def __init__(self):
            self.sql = []

        def add_insert_sql(self, s):
            self.sql.append(s)

        def execute(self):
            return "job"

    class FakeConfig:
        def __init__(self):
            self.values = {}

        def set(self, k, v):
            self.values[k] = v

        def get(self, k, default):
            return self.values.get(k, default)

    class FakeEnv:
        def __init__(self):
            self.executed = []
            self.statement_set = FakeStatementSet()
            self.config = FakeConfig()

        def execute_sql(self, s):
            self.executed.append(s)
            return FakeResult([[1], [2], [3]], job_client="client" if s.startswith("CREATE TABLE c AS") else None)

        def create_statement_set(self):
            return self.statement_set

        def get_config(self):
            return self.config

    env = FakeEnv()
    runner = r.SqlScriptRunner(env, row_limit=2)
    runner.run(
        [
            "CREATE TABLE t (n BIGINT) WITH ('connector' = 'datagen')",
            "SET 'pipeline.name' = 'x'",
            "INSERT INTO a SELECT n FROM t",
            "SELECT n FROM t",
            "INSERT INTO b SELECT n FROM t",
            "CREATE TABLE c AS SELECT n FROM t",
        ]
    )
    assert env.executed[0].startswith("CREATE TABLE")
    assert env.config.values == {"pipeline.name": "x"}
    assert env.statement_set.sql == ["INSERT INTO a SELECT n FROM t", "INSERT INTO b SELECT n FROM t"]
    # CREATE TABLE AS submits a job right away; the batched INSERTs follow at the end of the script.
    assert [getattr(j, "job_client", j) for j in runner.job_results] == ["client", "job"]
    # the query is cut at --row-limit and echoed to the job log
    err = capsys.readouterr().err
    assert '"event": "query_result", "row_count": 2, "truncated": true, "columns": ["n"]' in err
    assert err.endswith("n\n1\n2\n")


def test_statement_failed_reports_the_java_cause_chain(capsys) -> None:
    class JavaError:
        def __init__(self, text, cause=None):
            self.text, self.cause = text, cause

        def toString(self):
            return self.text

        def getCause(self):
            return self.cause

    class Py4JError(Exception):
        java_exception = JavaError("TableException: failed", JavaError("ValidationException: Object 'x' not found"))

    class Env:
        def execute_sql(self, s):
            raise Py4JError("long java stack trace")

    runner = r.SqlScriptRunner(Env(), row_limit=1)
    with pytest.raises(Py4JError):
        runner.run(["DROP TABLE x"])
    event = json.loads(capsys.readouterr().err.split("FLYT_SQL ", 1)[1])
    assert event == {
        "event": "statement_failed",
        "index": 0,
        "kind": "ddl",
        "error": "TableException: failed\nValidationException: Object 'x' not found",
    }
    assert r.error_message(ValueError("plain")) == "plain"
