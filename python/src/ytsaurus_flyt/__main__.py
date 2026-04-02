"""CLI entry point for ytsaurus-flyt."""

from __future__ import annotations

import contextlib
import logging
import os
import shlex
import shutil
import tempfile
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import click
import yaml

from ytsaurus_flyt.cli_helpers import find_pyproject_for_job, job_command_relative_to_project, resolve_proxy_pool
from ytsaurus_flyt.config import DEFAULT_JAVA_HOME, FlytConfig
from ytsaurus_flyt.flink_lib_jars import (
    download_flink_lib_jars,
    partition_flink_lib_jars_for_delivery,
    resolve_flink_lib_jars,
)
from ytsaurus_flyt.jobshell_resolve import flyt_profile_marker, resolve_default_jobshell_argv
from ytsaurus_flyt.launcher import launch_vanilla_job
from ytsaurus_flyt.layer_builder import (
    build_runtime_squashfs,
    compute_layer_hash,
    ensure_runtime_layer,
    ensure_unsquashfs_tool_remote,
    upload_squashfs_layer,
)
from ytsaurus_flyt.models import ClusterPreset
from ytsaurus_flyt.profiles import (
    default_cypress_base_path,
    list_profile_names,
    load_profile_dict,
    merge_yaml_dict,
    profile_dict_to_flyt_config,
    profile_yaml_path,
    read_active_profile_name,
    resolve_connection_from_profile,
    resolve_effective_profile_name,
    save_profile_dict,
    write_active_profile_name,
)
from ytsaurus_flyt.validate_config import validate_flyt_config
from ytsaurus_flyt.wheel_utils import build_wheel
from ytsaurus_flyt.yt_client import env_yt_token, make_yt_client


@click.group()
@click.option(
    "--profile",
    "global_profile",
    envvar="FLYT_PROFILE",
    default=None,
    help="Named profile (or set FLYT_PROFILE). Overrides active profile.",
)
@click.pass_context
def cli(ctx: click.Context, global_profile: Optional[str]) -> None:
    """flyt: PyFlink jobs on YTsaurus Vanilla operations."""
    ctx.ensure_object(dict)
    ctx.obj["profile"] = global_profile


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def _load_flyt_config_from_profile(ctx: Optional[click.Context]) -> Tuple[FlytConfig, Dict[str, Any]]:
    """Load FlytConfig and raw profile dict from the active or selected profile."""
    gp = ctx.obj.get("profile") if ctx else None
    name = resolve_effective_profile_name(gp)
    if not name:
        raise click.ClickException(
            "No profile selected. Run: flyt profile add <name> --proxy URL && flyt profile select <name>"
        )
    try:
        profile_data = load_profile_dict(name)
    except FileNotFoundError as e:
        raise click.ClickException(str(e)) from e
    cfg = profile_dict_to_flyt_config(profile_data)
    return cfg, profile_data


def _echo_profile_line(ctx: Optional[click.Context]) -> None:
    """Print the effective profile name (CLI --profile, FLYT_PROFILE, or active file)."""
    gp = ctx.obj.get("profile") if ctx else None
    name = resolve_effective_profile_name(gp)
    if name:
        click.echo(f"Profile: {name!r}")
    else:
        click.echo("Profile: (none)")


def _resolve_connection(
    profile_data: Dict[str, Any],
    cli_proxy: Optional[str],
    cli_pool: Optional[str],
) -> Tuple[str, str]:
    pr, pl, _ = resolve_connection_from_profile(profile_data)
    try:
        return resolve_proxy_pool(pr, pl, cli_proxy, cli_pool)
    except ValueError as e:
        raise click.ClickException(str(e)) from e


def _default_profile_body(proxy: str, pool: str, preset: str, cypress: str) -> Dict[str, Any]:
    return {
        "proxy": proxy,
        "pool": pool,
        "preset": preset,
        "cypress_base_path": cypress,
        "squashfs_layer_delivery": "layer_paths",
        "runtime_python_packages": ["apache-flink==1.20.1"],
        "runtime_python_version": "3.8",
        "java_home": DEFAULT_JAVA_HOME,
        "python_bin": "/usr/bin/python3",
        "jar_scan_folder": "",
    }


@cli.group("profile")
def profile_cli() -> None:
    """Manage named flyt profiles (proxy, pool, runtime defaults)."""


@profile_cli.command("add")
@click.argument("name")
@click.option("--proxy", required=True, help="YT HTTP proxy URL.")
@click.option("--pool", default="default", show_default=True)
@click.option("--preset", default="micro", show_default=True)
@click.option(
    "--cypress-base-path",
    default=None,
    help="Cypress prefix for layers/tools (default: //home/flyt/clusters/<name>).",
)
def profile_add(
    name: str,
    proxy: str,
    pool: str,
    preset: str,
    cypress_base_path: Optional[str],
) -> None:
    """Create a profile. If it is the first profile, it becomes active."""
    if profile_yaml_path(name).exists():
        raise click.ClickException(f"Profile already exists: {name!r}")
    cypress = (cypress_base_path or "").strip() or default_cypress_base_path(name)
    body = _default_profile_body(proxy, pool, preset, cypress)
    save_profile_dict(name, body)
    names = list_profile_names()
    if len(names) == 1 or not read_active_profile_name():
        write_active_profile_name(name)
        click.echo(f"Active profile set to {name!r}.")
    click.echo(f"Wrote profile {name!r} to {profile_yaml_path(name)}")


@profile_cli.command("import")
@click.argument("file", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option("--as", "name", required=True, help="Profile name to create.")
@click.option("--proxy", default=None, help="Override YT HTTP proxy URL.")
@click.option("--pool", default=None, help="Override pool.")
@click.option("--preset", default=None, help="Override preset.")
@click.option(
    "--cypress-base-path",
    default=None,
    help="Override Cypress prefix (default: from file, or //home/flyt/clusters/<name> if missing).",
)
def profile_import(
    file: Path,
    name: str,
    proxy: Optional[str],
    pool: Optional[str],
    preset: Optional[str],
    cypress_base_path: Optional[str],
) -> None:
    """Import a YAML file as a named profile. If it is the first profile, it becomes active."""
    if profile_yaml_path(name).exists():
        raise click.ClickException(f"Profile already exists: {name!r}")
    with open(file, encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    if not isinstance(raw, dict):
        raise click.ClickException("Profile file must contain a YAML mapping (object).")
    data: Dict[str, Any] = dict(raw)
    if proxy is not None:
        data["proxy"] = proxy.strip()
    if pool is not None:
        data["pool"] = pool.strip()
    if preset is not None:
        data["preset"] = preset.strip()
    cbp_cli = (cypress_base_path or "").strip()
    if cbp_cli:
        data["cypress_base_path"] = cbp_cli
    elif not (str(data.get("cypress_base_path") or "").strip()):
        data["cypress_base_path"] = default_cypress_base_path(name)
    save_profile_dict(name, data)
    out_path = profile_yaml_path(name)
    names = list_profile_names()
    if len(names) == 1 or not read_active_profile_name():
        write_active_profile_name(name)
        click.echo(f"Active profile set to {name!r}.")
    click.echo(f"Imported profile {name!r} to {out_path}")


@profile_cli.command("select")
@click.argument("name")
def profile_select(name: str) -> None:
    """Set the active profile."""
    if not profile_yaml_path(name).is_file():
        raise click.ClickException(f"Unknown profile: {name!r}")
    write_active_profile_name(name)
    click.echo(f"Active profile: {name!r}")


@profile_cli.command("remove")
@click.argument("name")
def profile_remove(name: str) -> None:
    """Delete a local profile file (does not delete Cypress data)."""
    p = profile_yaml_path(name)
    if not p.is_file():
        raise click.ClickException(f"Unknown profile: {name!r}")
    p.unlink()
    if read_active_profile_name() == name:
        write_active_profile_name(None)
        click.echo("Cleared active profile (no profile selected).")
    click.echo(f"Removed {name!r}")


@profile_cli.command("list")
def profile_list() -> None:
    """List profile names and mark the active one."""
    active = read_active_profile_name()
    names = list_profile_names()
    if not names:
        click.echo("No profiles. Use: flyt profile add <name> --proxy URL")
        return
    for n in names:
        mark = " *" if n == active else ""
        p = profile_yaml_path(n)
        click.echo(f"  {n}{mark} ({p})")


@profile_cli.command("show")
@click.argument("name", required=False)
def profile_show(name: Optional[str]) -> None:
    """Print a profile YAML (active profile if NAME omitted)."""
    n = name or read_active_profile_name()
    if not n:
        raise click.ClickException("No profile name and no active profile.")
    data = load_profile_dict(n)
    click.echo(yaml.safe_dump(data, default_flow_style=False, allow_unicode=True))


@cli.command("install")
@click.pass_context
@click.option("--force", is_flag=True, help="Rebuild SquashFS even if cached on Cypress.")
@click.option(
    "--reuse-layers",
    default=None,
    help="Use existing Cypress prefix for layers (sets .../layers and .../tools under it).",
)
def install(ctx: click.Context, force: bool, reuse_layers: Optional[str]) -> None:
    """Build runtime SquashFS (and unsquashfs helper) and upload to the active profile paths."""
    _setup_logging()
    _echo_profile_line(ctx)
    gp = ctx.obj.get("profile")
    name = resolve_effective_profile_name(gp)
    if not name:
        raise click.ClickException(
            "No profile selected. Run: flyt profile add <name> --proxy URL && flyt profile select <name>"
        )
    data = load_profile_dict(name)
    if reuse_layers:
        p = reuse_layers.rstrip("/")
        data = merge_yaml_dict(
            data,
            {
                "squashfs_layer_cache_prefix": f"{p}/layers",
                "squashfs_tools_cache_prefix": f"{p}/tools",
            },
        )
        save_profile_dict(name, data)
    cfg = profile_dict_to_flyt_config(data)
    proxy, _ = _resolve_connection(data, None, None)
    yt_client = make_yt_client(proxy)

    for row in validate_flyt_config(cfg, proxy=proxy, yt_client=yt_client):
        click.echo(f"  [{'OK' if row[1] else '!!'}] {row[0]}: {row[2]}")

    if not cfg.runtime_python_packages:
        raise click.ClickException("install requires runtime_python_packages in the profile.")

    flink_jars = resolve_flink_lib_jars(yt_client, cfg)
    flink_lib_jar_yt_paths = flink_jars.yt_paths
    jar_for_squashfs, _ = partition_flink_lib_jars_for_delivery(
        cfg,
        flink_lib_jar_yt_paths,
        extra_runtime_basenames=flink_jars.extra_runtime_basenames,
    )
    remote = ensure_runtime_layer(yt_client, cfg, jar_for_squashfs, force_rebuild=force)
    click.echo(f"Runtime layer: {remote}")
    if cfg.squashfs_layer_delivery == "sandbox_unpack":
        tool = ensure_unsquashfs_tool_remote(yt_client, cfg)
        click.echo(f"unsquashfs helper: {tool}")


@cli.command("validate")
@click.pass_context
@click.option("--proxy", default=None)
@click.option("--pool", default=None)
def validate(
    ctx: click.Context,
    proxy: Optional[str],
    pool: Optional[str],
) -> None:
    """Check profile and local tools (optional YT connectivity)."""
    _setup_logging()
    _echo_profile_line(ctx)
    cfg, profile_data = _load_flyt_config_from_profile(ctx)
    try:
        px, _ = _resolve_connection(profile_data, proxy, pool)
    except click.ClickException:
        px = None
    yt_client = None
    if px:
        yt_client = make_yt_client(px)
    rows = validate_flyt_config(cfg, proxy=px or None, yt_client=yt_client)
    ok_all = True
    for name, ok, msg in rows:
        ok_all = ok_all and ok
        click.echo(f"  [{'OK' if ok else '!!'}] {name}: {msg}")
    ctx.exit(0 if ok_all else 1)


@cli.command(
    "run",
    context_settings={"allow_interspersed_args": False},
)
@click.pass_context
@click.argument("job_argv", nargs=-1, required=True)
@click.option(
    "--proxy",
    default=None,
    help="YT HTTP proxy (overrides env / profile).",
)
@click.option("--pool", default=None, help="YT pool (overrides env / profile).")
@click.option(
    "--preset",
    type=click.Choice(["micro", "small", "large", "xlarge"], case_sensitive=False),
    default=None,
    help="Resource preset (default: from profile or micro).",
)
@click.option("--wheel", "wheel_path", default=None)
@click.option("--source-dir", "source_dir", default=None)
@click.option("--cache-wheel", is_flag=True)
@click.option(
    "--detach",
    is_flag=True,
    help="Submit the operation and exit without waiting for completion. "
    "Uses a persistent Cypress wheel path (same as --cache-wheel; requires wheel_cache_prefix or cypress_base_path).",
)
@click.option("--force-rebuild", "force_rebuild_layer", is_flag=True)
def run(
    ctx: click.Context,
    job_argv: Tuple[str, ...],
    proxy: Optional[str],
    pool: Optional[str],
    preset: Optional[str],
    wheel_path: Optional[str],
    source_dir: Optional[str],
    cache_wheel: bool,
    detach: bool,
    force_rebuild_layer: bool,
) -> None:
    """Launch a PyFlink job. Uses the active profile and auto wheel build."""
    _setup_logging()
    _echo_profile_line(ctx)
    job_command = shlex.join(list(job_argv))
    cfg, profile_data = _load_flyt_config_from_profile(ctx)
    if detach:
        wp = (cfg.wheel_cache_prefix or "").strip()
        if not wp:
            raise click.ClickException(
                "Detached runs require a persistent wheel path on Cypress. "
                "Set wheel_cache_prefix in the profile, or set cypress_base_path "
                "(e.g. via `flyt profile add`) so flyt can use <cypress_base_path>/wheels."
            )
    cache_wheel_effective = cache_wheel or detach
    proxy_f, pool_f = _resolve_connection(profile_data, proxy, pool)
    _, _, pst = resolve_connection_from_profile(profile_data)
    preset_s = (preset or "").strip().lower() or (pst or "micro").strip().lower()
    try:
        preset_enum = ClusterPreset[preset_s.upper()]
    except KeyError as e:
        raise click.ClickException("Invalid preset %r. Use one of: micro, small, large, xlarge." % (preset_s,)) from e

    with contextlib.ExitStack() as stack:
        project_root: Optional[str] = None
        if not wheel_path and not source_dir:
            proj = find_pyproject_for_job(job_command)
            if proj:
                logging.getLogger(__name__).info("Building wheel from %s", proj)
                wdir = stack.enter_context(tempfile.TemporaryDirectory(prefix="flyt_wheel_"))
                wheel_path = build_wheel(proj, output_dir=wdir)
                project_root = proj
            else:
                raise click.ClickException(
                    "Pass --wheel or --source-dir, or add pyproject.toml next to the job script."
                )
        elif source_dir:
            project_root = str(Path(source_dir).expanduser().resolve())

        if project_root:
            job_command = job_command_relative_to_project(job_command, project_root)

        gp = ctx.obj.get("profile")
        launch_vanilla_job(
            config=cfg,
            yt_client=make_yt_client(proxy_f),
            job_command=job_command,
            pool=pool_f,
            preset=preset_enum,
            wheel_path=wheel_path,
            source_dir=source_dir,
            cache_wheel=cache_wheel_effective,
            sync=not detach,
            force_rebuild_layer=force_rebuild_layer,
            profile_name=resolve_effective_profile_name(gp),
        )


@cli.command("build-layer")
@click.pass_context
@click.option(
    "--output",
    "output_path",
    required=True,
    type=click.Path(dir_okay=False),
)
@click.option("--upload", is_flag=True)
@click.option("--force-rebuild", "force_rebuild", is_flag=True)
def build_layer(
    ctx: click.Context,
    output_path: str,
    upload: bool,
    force_rebuild: bool,
) -> None:
    """Build a runtime SquashFS locally from the active profile (and optionally upload to Cypress)."""
    _setup_logging()
    _echo_profile_line(ctx)
    cfg, profile_data = _load_flyt_config_from_profile(ctx)
    if not cfg.runtime_python_packages:
        raise click.ClickException("runtime_python_packages is required in the profile to build a layer.")

    proxy, _ = _resolve_connection(profile_data, None, None)
    yt_client = make_yt_client(proxy)

    flink_jars = resolve_flink_lib_jars(yt_client, cfg)
    jar_for_squashfs, _ = partition_flink_lib_jars_for_delivery(
        cfg,
        flink_jars.yt_paths,
        extra_runtime_basenames=flink_jars.extra_runtime_basenames,
    )

    with tempfile.TemporaryDirectory(prefix="flyt_build_layer_") as tmp:
        jar_dir = os.path.join(tmp, "jars")
        local_jars = download_flink_lib_jars(yt_client, jar_for_squashfs, jar_dir)
        build_runtime_squashfs(cfg, local_jars, output_path)

    if upload:
        h = compute_layer_hash(cfg, jar_for_squashfs)
        prefix = (cfg.squashfs_layer_cache_prefix or "").strip() or "//tmp/flyt_squashfs_layers"
        remote = f"{prefix.rstrip('/')}/runtime-{h}.squashfs"
        if force_rebuild and yt_client.exists(remote):
            yt_client.remove(remote, force=True)
        upload_squashfs_layer(
            yt_client,
            output_path,
            remote,
            set_filesystem_attribute=(cfg.squashfs_layer_delivery == "layer_paths"),
        )
        click.echo(f"Uploaded layer to {remote}")
        if cfg.squashfs_layer_delivery == "sandbox_unpack":
            tool = ensure_unsquashfs_tool_remote(yt_client, cfg)
            click.echo(f"Uploaded unsquashfs helper to {tool}")


@cli.command("jobshell")
@click.option(
    "--profile",
    "jobshell_profile",
    default=None,
    help="Profile for proxy and job lookup (overrides flyt --profile / FLYT_PROFILE / active).",
)
@click.pass_context
def jobshell(ctx: click.Context, jobshell_profile: Optional[str]) -> None:
    """Attach to the sandbox of the running ``flyt run`` job for the selected profile."""
    parent = ctx.parent
    global_profile = parent.obj.get("profile") if parent and parent.obj else None
    gp = jobshell_profile or global_profile
    name = resolve_effective_profile_name(gp)
    if name:
        click.echo(f"Profile: {name!r}")
    else:
        click.echo("Profile: (none)")
    if not name:
        raise click.ClickException(
            "No profile selected. Use --profile, set FLYT_PROFILE, or: "
            "flyt profile add <name> --proxy URL && flyt profile select <name>"
        )
    data = load_profile_dict(name)
    proxy, _ = _resolve_connection(data, None, None)
    yt_bin = shutil.which("yt")
    if not yt_bin:
        raise click.ClickException(
            "The 'yt' executable was not found in PATH. Install ytsaurus-client (pip install ytsaurus-client)."
        )
    yt_client = make_yt_client(proxy)
    resolved = resolve_default_jobshell_argv(yt_client, name)
    if not resolved:
        raise click.ClickException(
            "No running job found for this profile's flyt operation "
            f"(title must contain {flyt_profile_marker(name)!r}). "
            "Run `flyt run ...` with this profile while the job is still running."
        )
    extra = resolved
    argv = [yt_bin, "--proxy", proxy, *extra]
    env = os.environ.copy()
    token = env_yt_token()
    if token:
        env["YT_TOKEN"] = token
    os.execve(yt_bin, argv, env)


def main() -> None:
    cli()


if __name__ == "__main__":
    main()
