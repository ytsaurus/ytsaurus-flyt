"""Shipping of the SQL runner and the script as files of the operation."""

from pathlib import Path

from ytsaurus_flyt import sql_runner
from ytsaurus_flyt.sql_job import upload_sql_job


def test_upload_sql_job_ships_temporary_operation_files(tmp_path: Path) -> None:
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
    with upload_sql_job(yt, str(sql)) as (runner, script):
        tmp_dir = runner.rsplit("/", 1)[0]
        assert (runner, script) == (f"{tmp_dir}/sql_runner.py", f"{tmp_dir}/script.sql")
        assert yt.files == {runner: Path(sql_runner.__file__).read_bytes(), script: b"SELECT 1;"}
        assert yt.removed == []
    assert yt.removed == [tmp_dir]
