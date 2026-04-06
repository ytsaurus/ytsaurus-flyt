"""Tests for cli_helpers."""

from pathlib import Path

from ytsaurus_flyt.cli_helpers import (
    job_command_relative_to_project,
    resolve_proxy_pool,
    split_job_command_tokens,
)


def test_job_command_relative_to_project_rewrites_repo_path(tmp_path: Path) -> None:
    proj = tmp_path / "ex"
    proj.mkdir()
    script = proj / "pipeline.py"
    script.write_text("# x", encoding="utf-8")
    (proj / "pyproject.toml").write_text("[project]\nname=x\nversion=0\n", encoding="utf-8")

    sub = proj / "pkg"
    sub.mkdir()
    nested = sub / "main.py"
    nested.write_text("# y", encoding="utf-8")

    assert job_command_relative_to_project(f"{proj}/pipeline.py", str(proj)) == "pipeline.py"
    assert job_command_relative_to_project(f"{sub}/main.py --a", str(proj)) == "pkg/main.py --a"


def test_split_job_command_tokens_respects_quotes() -> None:
    assert split_job_command_tokens("script.py --opt 'path with spaces'") == [
        "script.py",
        "--opt",
        "path with spaces",
    ]
    assert split_job_command_tokens('script.py --opt "path with spaces"') == [
        "script.py",
        "--opt",
        "path with spaces",
    ]


def test_split_job_command_tokens_unquoted_args() -> None:
    assert split_job_command_tokens("script.py --opt arg") == ["script.py", "--opt", "arg"]


def test_resolve_proxy_pool_cli_env_profile_order(monkeypatch) -> None:
    monkeypatch.delenv("FLYT_PROXY", raising=False)
    monkeypatch.delenv("YT_PROXY", raising=False)
    monkeypatch.delenv("FLYT_POOL", raising=False)
    monkeypatch.delenv("YT_POOL", raising=False)
    assert resolve_proxy_pool("http://prof", "pool-prof", None, None) == ("http://prof", "pool-prof")

    monkeypatch.setenv("FLYT_PROXY", "http://env")
    monkeypatch.setenv("FLYT_POOL", "pool-env")
    assert resolve_proxy_pool("http://prof", "pool-prof", None, None) == ("http://env", "pool-env")

    assert resolve_proxy_pool("http://prof", "pool-prof", "http://cli", "pool-cli") == (
        "http://cli",
        "pool-cli",
    )


def test_job_command_relative_to_project_unchanged_if_outside_project(tmp_path: Path) -> None:
    other = tmp_path / "other" / "x.py"
    other.parent.mkdir(parents=True)
    other.write_text("# z", encoding="utf-8")
    proj = tmp_path / "proj"
    proj.mkdir()
    (proj / "pyproject.toml").write_text("[project]\nname=p\nversion=0\n", encoding="utf-8")
    cmd = str(other)
    assert job_command_relative_to_project(cmd, str(proj)) == cmd
