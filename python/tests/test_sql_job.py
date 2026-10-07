"""Packaging of SQL scripts into a flyt service project."""

from pathlib import Path

from ytsaurus_flyt import sql_runner
from ytsaurus_flyt.sql_job import SQL_JOB_PACKAGE, build_sql_job_project


def test_build_sql_job_project_layout(tmp_path: Path) -> None:
    sql = tmp_path / "q.sql"
    sql.write_text("SELECT 1;", encoding="utf-8")
    project = Path(build_sql_job_project(str(sql), str(tmp_path / "proj")))

    pkg = project / SQL_JOB_PACKAGE
    assert (project / "pyproject.toml").read_text(encoding="utf-8").count(SQL_JOB_PACKAGE) == 2
    assert (pkg / "__init__.py").is_file()
    assert (pkg / "script.sql").read_text(encoding="utf-8") == "SELECT 1;"
    assert (pkg / "run.py").read_text(encoding="utf-8") == Path(sql_runner.__file__).read_text(encoding="utf-8")
