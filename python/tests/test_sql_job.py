"""Packaging of SQL scripts into a flyt service project."""

from pathlib import Path

from ytsaurus_flyt import sql_runner
from ytsaurus_flyt.sql_job import SQL_JOB_PACKAGE, build_sql_job_project, upload_sql_script


def test_build_sql_job_project_holds_only_the_runner(tmp_path: Path) -> None:
    project = Path(build_sql_job_project(str(tmp_path / "proj")))

    pkg = project / SQL_JOB_PACKAGE
    assert SQL_JOB_PACKAGE in (project / "pyproject.toml").read_text(encoding="utf-8")
    assert sorted(p.name for p in pkg.iterdir()) == ["__init__.py", "run.py"]
    assert (pkg / "run.py").read_text(encoding="utf-8") == Path(sql_runner.__file__).read_text(encoding="utf-8")


def test_upload_sql_script_is_a_temporary_operation_file(tmp_path: Path) -> None:
    sql = tmp_path / "q.sql"
    sql.write_text("SELECT 1;", encoding="utf-8")

    class FakeYt:
        def __init__(self):
            self.files, self.removed = {}, []

        def mkdir(self, path, recursive):
            pass

        def write_file(self, path, stream):
            self.files[path] = stream.read()

        def remove(self, path, recursive, force):
            self.removed.append(path)

    yt = FakeYt()
    with upload_sql_script(yt, str(sql)) as remote:
        assert remote.endswith("/script.sql")
        assert yt.files == {remote: b"SELECT 1;"}
        assert yt.removed == []
    assert yt.removed == [remote.rsplit("/", 1)[0]]
