"""Pure helpers of the bundled SQL runner (no PyFlink needed)."""

import datetime as dt
import decimal

import pytest

from ytsaurus_flyt import sql_runner as r


def test_split_statements_respects_quotes_and_comments() -> None:
    script = """
    -- comment; with semicolon
    CREATE TABLE t (s STRING) WITH ('connector' = 'datagen', 'x' = 'a;b');
    /* block; comment */
    SELECT 'it''s; fine' FROM t;
    SELECT `we;ird` FROM t
    """
    assert r.split_statements(script) == [
        "CREATE TABLE t (s STRING) WITH ('connector' = 'datagen', 'x' = 'a;b')",
        "SELECT 'it''s; fine' FROM t",
        "SELECT `we;ird` FROM t",
    ]


def test_split_statements_keeps_statement_set_whole() -> None:
    script = "EXECUTE STATEMENT SET BEGIN INSERT INTO a SELECT 1; INSERT INTO b SELECT 2; END; SELECT 3;"
    assert r.split_statements(script) == [
        "EXECUTE STATEMENT SET BEGIN\nINSERT INTO a SELECT 1;\nINSERT INTO b SELECT 2;\nEND",
        "SELECT 3",
    ]
    legacy = "BEGIN STATEMENT SET; INSERT INTO a SELECT 1; END;"
    assert r.split_statements(legacy) == ["EXECUTE STATEMENT SET BEGIN\nINSERT INTO a SELECT 1;\nEND"]
    with pytest.raises(ValueError, match="not closed"):
        r.split_statements("EXECUTE STATEMENT SET BEGIN INSERT INTO a SELECT 1;")


@pytest.mark.parametrize(
    ("stmt", "kind"),
    [
        ("SET 'a' = 'b'", r.KIND_SET),
        ("reset 'a'", r.KIND_SET),
        ("ADD JAR '/x.jar'", r.KIND_ADD_JAR),
        ("INSERT INTO t SELECT 1", r.KIND_INSERT),
        ("EXECUTE STATEMENT SET BEGIN\nINSERT INTO t SELECT 1;\nEND", r.KIND_STATEMENT_SET),
        ("SELECT 1", r.KIND_QUERY),
        ("WITH c AS (SELECT 1) SELECT * FROM c", r.KIND_QUERY),
        ("(SELECT 1) UNION ALL (SELECT 2)", r.KIND_QUERY),
        ("SHOW TABLES", r.KIND_SHOW),
        ("EXPLAIN SELECT 1", r.KIND_SHOW),
        ("CREATE TABLE t (a INT) WITH ('connector' = 'print')", r.KIND_DDL),
        ("USE CATALOG c", r.KIND_DDL),
    ],
)
def test_classify(stmt: str, kind: str) -> None:
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


def test_parse_add_jar_adds_file_scheme() -> None:
    assert r.parse_add_jar("ADD JAR 'hdfs:///x.jar'") == "hdfs:///x.jar"
    assert r.parse_add_jar("ADD JAR '/opt/x.jar'") == "file:///opt/x.jar"


def test_yt_type_for_uses_class_names() -> None:
    class BigIntType: ...

    class ArrayType: ...

    class TimestampType: ...

    assert r.yt_type_for(BigIntType()) == "int64"
    assert r.yt_type_for(ArrayType()) == "any"
    assert r.yt_type_for(TimestampType()) == "utf8"


def test_to_yt_value_conversions() -> None:
    class Row:
        _values = [1, "a"]

    assert r.to_yt_value(decimal.Decimal("1.50")) == "1.50"
    assert r.to_yt_value(dt.datetime(2026, 1, 2, 3, 4, 5)) == "2026-01-02T03:04:05"
    assert r.to_yt_value(Row()) == [1, "a"]
    assert r.to_yt_value({"k": (1, None)}) == {"k": [1, None]}
    assert r.to_yt_value(b"raw") == b"raw"


def test_runner_batches_inserts_and_collects_queries(monkeypatch) -> None:
    class FakeResult:
        def __init__(self, rows):
            self._rows = rows

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
            return FakeResult([[1], [2], [3]])

        def create_statement_set(self):
            return self.statement_set

        def get_config(self):
            return self.config

    written = {}
    monkeypatch.setattr(r, "write_result_table", lambda path, proxy, cols, rows: written.update(path=path, rows=rows))

    env = FakeEnv()
    runner = r.SqlScriptRunner(env, result_table="//tmp/res", yt_proxy="p", row_limit=2)
    runner.run(
        [
            "CREATE TABLE t (n BIGINT) WITH ('connector' = 'datagen')",
            "SET 'pipeline.name' = 'x'",
            "INSERT INTO a SELECT n FROM t",
            "SELECT n FROM t",
            "INSERT INTO b SELECT n FROM t",
        ]
    )
    assert env.executed[0].startswith("CREATE TABLE")
    assert env.config.values == {"pipeline.name": "x"}
    assert env.statement_set.sql == ["INSERT INTO a SELECT n FROM t", "INSERT INTO b SELECT n FROM t"]
    assert runner.job_results == ["job"]
    assert written == {"path": "//tmp/res", "rows": [[1], [2]]}
