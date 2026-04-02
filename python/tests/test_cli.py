"""CLI behaviour (jobshell, profile banner)."""

from pathlib import Path

import yaml
from click.testing import CliRunner

from ytsaurus_flyt.__main__ import cli
from ytsaurus_flyt.profiles import default_cypress_base_path
from ytsaurus_flyt.yt_client import env_yt_token


def test_env_yt_token_ignores_empty_string(monkeypatch) -> None:
    monkeypatch.delenv("FLYT_YT_TOKEN", raising=False)
    monkeypatch.delenv("YT_TOKEN", raising=False)
    assert env_yt_token() is None
    monkeypatch.setenv("YT_TOKEN", "")
    monkeypatch.delenv("FLYT_YT_TOKEN", raising=False)
    assert env_yt_token() is None
    monkeypatch.setenv("YT_TOKEN", "  ")
    assert env_yt_token() is None
    monkeypatch.setenv("YT_TOKEN", "secret")
    assert env_yt_token() == "secret"


def _write_profile(tmp_path: Path, name: str, proxy: str = "http://cluster.example:80") -> None:
    profiles = tmp_path / "profiles"
    profiles.mkdir(parents=True)
    (profiles / f"{name}.yaml").write_text(
        f"proxy: {proxy}\npool: default\n",
        encoding="utf-8",
    )
    (tmp_path / "active").write_text(f"{name}\n", encoding="utf-8")


def test_jobshell_execve_passes_proxy_and_yt_token_env(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("FLYT_CONFIG_DIR", str(tmp_path))
    _write_profile(tmp_path, "p1")

    captured: dict = {}

    def fake_execve(path: str, argv: list, env: dict) -> None:  # type: ignore[type-arg]
        captured["path"] = path
        captured["argv"] = argv
        captured["env_token"] = env.get("YT_TOKEN")
        raise SystemExit(0)

    monkeypatch.setattr("ytsaurus_flyt.__main__.os.execve", fake_execve)
    monkeypatch.setattr("ytsaurus_flyt.__main__.shutil.which", lambda x: "/fake/yt" if x == "yt" else None)
    monkeypatch.setattr(
        "ytsaurus_flyt.__main__.resolve_default_jobshell_argv",
        lambda _c, _n: ["run-job-shell", "jid"],
    )
    monkeypatch.setenv("YT_TOKEN", "tok-test")

    runner = CliRunner()
    result = runner.invoke(cli, ["jobshell"], catch_exceptions=False)
    assert result.exit_code == 0
    assert captured["path"] == "/fake/yt"
    assert captured["argv"] == [
        "/fake/yt",
        "--proxy",
        "http://cluster.example:80",
        "run-job-shell",
        "jid",
    ]
    assert captured["env_token"] == "tok-test"


def test_jobshell_rejects_extra_arguments(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("FLYT_CONFIG_DIR", str(tmp_path))
    _write_profile(tmp_path, "p1")
    runner = CliRunner()
    result = runner.invoke(cli, ["jobshell", "list", "//tmp"], catch_exceptions=False)
    assert result.exit_code != 0


def test_jobshell_requires_profile(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("FLYT_CONFIG_DIR", str(tmp_path))
    (tmp_path / "profiles").mkdir(parents=True)

    runner = CliRunner()
    result = runner.invoke(cli, ["jobshell"], catch_exceptions=False)
    assert result.exit_code != 0
    assert "No profile selected" in result.output


def test_jobshell_no_args_uses_resolved_argv(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("FLYT_CONFIG_DIR", str(tmp_path))
    _write_profile(tmp_path, "p1")
    captured: dict = {}

    def fake_execve(path: str, argv: list, env: dict) -> None:  # type: ignore[type-arg]
        captured["argv"] = argv
        raise SystemExit(0)

    monkeypatch.setattr("ytsaurus_flyt.__main__.os.execve", fake_execve)
    monkeypatch.setattr("ytsaurus_flyt.__main__.shutil.which", lambda x: "/fake/yt" if x == "yt" else None)
    monkeypatch.setattr(
        "ytsaurus_flyt.__main__.resolve_default_jobshell_argv",
        lambda _c, _n: ["run-job-shell", "job-xyz"],
    )

    runner = CliRunner()
    result = runner.invoke(cli, ["jobshell"], catch_exceptions=False)
    assert result.exit_code == 0
    assert captured["argv"] == [
        "/fake/yt",
        "--proxy",
        "http://cluster.example:80",
        "run-job-shell",
        "job-xyz",
    ]


def test_jobshell_no_args_fails_when_no_running_flyt_job(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("FLYT_CONFIG_DIR", str(tmp_path))
    _write_profile(tmp_path, "p1")
    monkeypatch.setattr("ytsaurus_flyt.__main__.shutil.which", lambda x: "/fake/yt" if x == "yt" else None)
    monkeypatch.setattr("ytsaurus_flyt.__main__.resolve_default_jobshell_argv", lambda _c, _n: None)

    runner = CliRunner()
    result = runner.invoke(cli, ["jobshell"], catch_exceptions=False)
    assert result.exit_code != 0
    assert "flyt-profile" in result.output


def test_jobshell_profile_flag_overrides_active(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("FLYT_CONFIG_DIR", str(tmp_path))
    profiles = tmp_path / "profiles"
    profiles.mkdir(parents=True)
    (profiles / "p1.yaml").write_text("proxy: http://p1.example\npool: default\n", encoding="utf-8")
    (profiles / "p2.yaml").write_text("proxy: http://p2.example\npool: default\n", encoding="utf-8")
    (tmp_path / "active").write_text("p1\n", encoding="utf-8")
    captured: dict = {}
    seen_names: list = []

    def fake_execve(path: str, argv: list, env: dict) -> None:  # type: ignore[type-arg]
        captured["argv"] = argv
        raise SystemExit(0)

    def fake_resolve(_c: object, n: str) -> list:
        seen_names.append(n)
        return ["run-job-shell", "jid"]

    monkeypatch.setattr("ytsaurus_flyt.__main__.os.execve", fake_execve)
    monkeypatch.setattr("ytsaurus_flyt.__main__.shutil.which", lambda x: "/fake/yt" if x == "yt" else None)
    monkeypatch.setattr("ytsaurus_flyt.__main__.resolve_default_jobshell_argv", fake_resolve)

    runner = CliRunner()
    result = runner.invoke(cli, ["jobshell", "--profile", "p2"], catch_exceptions=False)
    assert result.exit_code == 0
    assert seen_names == ["p2"]
    assert captured["argv"][2] == "http://p2.example"


def test_jobshell_requires_yt_binary(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("FLYT_CONFIG_DIR", str(tmp_path))
    _write_profile(tmp_path, "p1")
    monkeypatch.setattr("ytsaurus_flyt.__main__.shutil.which", lambda x: None)

    runner = CliRunner()
    result = runner.invoke(cli, ["jobshell"], catch_exceptions=False)
    assert result.exit_code != 0
    assert "yt" in result.output.lower()


def test_validate_prints_profile_line(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("FLYT_CONFIG_DIR", str(tmp_path))
    profiles = tmp_path / "profiles"
    profiles.mkdir(parents=True)
    (profiles / "myprof.yaml").write_text(
        "proxy: http://cluster.example:80\npool: default\nruntime_python_packages: [apache-flink==1.20.1]\n",
        encoding="utf-8",
    )
    (tmp_path / "active").write_text("myprof\n", encoding="utf-8")
    monkeypatch.setattr(
        "ytsaurus_flyt.__main__.validate_flyt_config",
        lambda *a, **k: [("check", True, "ok")],
    )

    runner = CliRunner()
    result = runner.invoke(cli, ["validate"], catch_exceptions=False)
    assert result.exit_code == 0
    assert "Profile: 'myprof'" in result.output


def test_profile_import_writes_and_overrides(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("FLYT_CONFIG_DIR", str(tmp_path))
    src = tmp_path / "incoming.yaml"
    src.write_text(
        "proxy: http://old.example:80\npool: oldpool\npreset: micro\nruntime_python_packages: [apache-flink==1.20.1]\n",
        encoding="utf-8",
    )
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "profile",
            "import",
            str(src),
            "--as",
            "imp1",
            "--proxy",
            "http://new.example:80",
            "--pool",
            "newpool",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "Imported profile" in result.output
    assert "imp1" in result.output
    assert "Active profile set" in result.output
    out = tmp_path / "profiles" / "imp1.yaml"
    assert out.is_file()
    data = yaml.safe_load(out.read_text(encoding="utf-8"))
    assert data["proxy"] == "http://new.example:80"
    assert data["pool"] == "newpool"
    assert data["preset"] == "micro"
    assert data["cypress_base_path"] == default_cypress_base_path("imp1")

    list_result = runner.invoke(cli, ["profile", "list"], catch_exceptions=False)
    assert list_result.exit_code == 0
    assert str(out) in list_result.output

    dup = runner.invoke(
        cli,
        ["profile", "import", str(src), "--as", "imp1"],
        catch_exceptions=False,
    )
    assert dup.exit_code != 0
    assert "already exists" in dup.output
