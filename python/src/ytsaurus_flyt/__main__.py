"""CLI entry point for ytsaurus-flyt."""

from __future__ import annotations

import contextlib
import logging
import os
import shlex
import shutil
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Iterator, Optional, Tuple

import click
import yaml

if TYPE_CHECKING:
    from ytsaurus_flyt.progress import Reporter

from ytsaurus_flyt.cli_helpers import find_pyproject_for_job, job_command_relative_to_project, resolve_proxy_pool
from ytsaurus_flyt.config.config import FlytConfig
from ytsaurus_flyt.config.models import ClusterPreset
from ytsaurus_flyt.config.profiles import (
    list_profile_names,
    load_profile_dict,
    profile_dict_to_flyt_config,
    profile_yaml_path,
    read_active_profile_name,
    resolve_connection_from_profile,
    resolve_effective_profile_name,
    save_profile_dict,
    write_active_profile_name,
)
from ytsaurus_flyt.config.validate_config import validate_flyt_config
from ytsaurus_flyt.runtime.layer_builder import (
    build_runtime_squashfs,
    build_unsquashfs_binary,
    upload_local_file,
    upload_squashfs_layer,
)
from ytsaurus_flyt.runtime.wheel_utils import build_wheel
from ytsaurus_flyt.submit.launcher import launch_vanilla_job
from ytsaurus_flyt.submit.yt_client import env_yt_token, make_yt_client
from ytsaurus_flyt.tracking.jobshell_resolve import flyt_profile_marker, resolve_default_jobshell_argv


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


_UI_TRACKER_LOGGER = "ytsaurus_flyt.tracking.ui_tracker"
_YT_LOGGER = "Yt"  # yt client's own logger (propagate=False, owns its stderr handler)


def _configure_run_logging(verbose: bool, debug: bool, reporter: "Reporter") -> None:
    """Configure ``flyt run`` logging: terse routes warnings/errors through ``reporter`` (above the
    spinner) while keeping INFO flowing to the Flink UI watcher; -v adds INFO, --debug adds yt DEBUG."""
    from ytsaurus_flyt.progress import FirstLineFormatter, ReporterLogHandler  # noqa: PLC0415

    root = logging.getLogger()
    for h in root.handlers[:]:
        root.removeHandler(h)
    yt = logging.getLogger(_YT_LOGGER)
    ui = logging.getLogger(_UI_TRACKER_LOGGER)
    for h in ui.handlers[:]:
        ui.removeHandler(h)
    ui.propagate, ui.level = True, logging.NOTSET

    if verbose or debug:
        level = logging.DEBUG if debug else logging.INFO
        logging.basicConfig(level=level, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s", force=True)
        yt.setLevel(level)
        for h in yt.handlers:
            h.setLevel(level)
        if debug:
            os.environ.setdefault("YT_LOG_LEVEL", "DEBUG")
        return

    # Terse: warnings/errors go through the reporter (printed above the spinner), one line each.
    root.setLevel(logging.INFO)  # keep records flowing to the watcher's Yt handler
    flyt_handler = ReporterLogHandler(reporter, level=logging.WARNING)
    flyt_handler.setFormatter(FirstLineFormatter("%(levelname)s: %(message)s"))
    root.addHandler(flyt_handler)

    # Replace the yt client's own stderr handler so its retry/error spam can't trample the spinner.
    for h in yt.handlers[:]:
        yt.removeHandler(h)
    yt.setLevel(logging.INFO)
    yt.propagate = False
    yt_handler = ReporterLogHandler(reporter, level=logging.WARNING)
    yt_handler.setFormatter(FirstLineFormatter("%(message)s"))
    yt.addHandler(yt_handler)

    # The watcher's Flink Web UI announcement (INFO) stays visible, also via the reporter.
    ui.setLevel(logging.INFO)
    ui.propagate = False
    ui_handler = ReporterLogHandler(reporter, level=logging.INFO)
    ui_handler.setFormatter(logging.Formatter("%(message)s"))
    ui.addHandler(ui_handler)


def _cli_reporter(verbose: bool = False, debug: bool = False) -> "Reporter":
    """Build the shared progress reporter (used by every command) and route logging through it."""
    from ytsaurus_flyt.progress import make_reporter  # noqa: PLC0415

    rep = make_reporter(verbose or debug, logging.getLogger("ytsaurus_flyt"))
    _configure_run_logging(verbose, debug, rep)
    return rep


def _format_launch_error(exc: Exception) -> str:
    """Terse error: the lead line, plus the real last line for wrapped subprocess failures."""
    lines = [ln.strip() for ln in str(exc).splitlines() if ln.strip()]
    hint = "Run with -v for the full traceback (--debug for yt client logs)."
    if not lines:
        return f"{exc.__class__.__name__}\n{hint}"
    summary = lines[0]
    tail = lines[-1]
    if tail != summary and tail not in ("stdout:", "stderr:"):
        summary = f"{summary}\n  → {tail}"
    return f"{summary}\n{hint}"


@contextlib.contextmanager
def _clean_errors(verbose: bool) -> Iterator[None]:
    """Turn launcher failures into a terse one-line error (full traceback under -v)."""
    try:
        yield
    except click.ClickException:
        raise
    except Exception as exc:  # noqa: BLE001 - surfaced cleanly, re-raised under -v
        if verbose:
            raise
        raise click.ClickException(_format_launch_error(exc)) from exc


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


def _default_profile_body(proxy: str, pool: str, preset: str) -> Dict[str, Any]:
    return {
        "proxy": proxy,
        "pool": pool,
        "preset": preset,
        "squashfs_layer_delivery": "layer_paths",
        "squashfs_layer_paths": [],
        "flink_version": "1.20.1",
        "runtime_python_version": "3.10",
        "java_version": "11",
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
def profile_add(
    name: str,
    proxy: str,
    pool: str,
    preset: str,
) -> None:
    """Create a profile. If it is the first profile, it becomes active."""
    rep = _cli_reporter()
    rep.header(name)
    if profile_yaml_path(name).exists():
        raise click.ClickException(f"Profile already exists: {name!r}")
    body = _default_profile_body(proxy, pool, preset)
    save_profile_dict(name, body)
    names = list_profile_names()
    if len(names) == 1 or not read_active_profile_name():
        write_active_profile_name(name)
        rep.line(f"Active profile set to {name!r}.")
    rep.line(f"Wrote profile {name!r} to {profile_yaml_path(name)}")


@profile_cli.command("import")
@click.argument("file", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option("--as", "name", required=True, help="Profile name to create.")
@click.option("--proxy", default=None, help="Override YT HTTP proxy URL.")
@click.option("--pool", default=None, help="Override pool.")
@click.option("--preset", default=None, help="Override preset.")
def profile_import(
    file: Path,
    name: str,
    proxy: Optional[str],
    pool: Optional[str],
    preset: Optional[str],
) -> None:
    """Import a YAML file as a named profile. If it is the first profile, it becomes active."""
    rep = _cli_reporter()
    rep.header(name)
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
    save_profile_dict(name, data)
    out_path = profile_yaml_path(name)
    names = list_profile_names()
    if len(names) == 1 or not read_active_profile_name():
        write_active_profile_name(name)
        rep.line(f"Active profile set to {name!r}.")
    rep.line(f"Imported profile {name!r} to {out_path}")


@profile_cli.command("select")
@click.argument("name")
def profile_select(name: str) -> None:
    """Set the active profile."""
    rep = _cli_reporter()
    rep.header(name)
    if not profile_yaml_path(name).is_file():
        raise click.ClickException(f"Unknown profile: {name!r}")
    write_active_profile_name(name)
    rep.line(f"Active profile: {name!r}")


@profile_cli.command("update")
@click.argument("name")
@click.argument("file", type=click.Path(exists=True, dir_okay=False, path_type=Path))
def profile_update(name: str, file: Path) -> None:
    """Replace an existing profile's body with the YAML at FILE."""
    rep = _cli_reporter()
    rep.header(name)
    if not profile_yaml_path(name).is_file():
        raise click.ClickException(f"Unknown profile: {name!r}. Use `flyt profile add` or `flyt profile import`.")
    with open(file, encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    if not isinstance(raw, dict):
        raise click.ClickException("Profile file must contain a YAML mapping (object).")
    save_profile_dict(name, dict(raw))
    rep.line(f"Updated profile {name!r} from {file} at {profile_yaml_path(name)}")


@profile_cli.command("remove")
@click.argument("name")
def profile_remove(name: str) -> None:
    """Delete a local profile file (does not delete Cypress data)."""
    rep = _cli_reporter()
    rep.header(name)
    p = profile_yaml_path(name)
    if not p.is_file():
        raise click.ClickException(f"Unknown profile: {name!r}")
    p.unlink()
    if read_active_profile_name() == name:
        write_active_profile_name(None)
        rep.line("Cleared active profile (no profile selected).")
    rep.line(f"Removed {name!r}")


@profile_cli.command("list")
def profile_list() -> None:
    """List profile names and mark the active one."""
    rep = _cli_reporter()
    active = read_active_profile_name()
    rep.header(active)
    names = list_profile_names()
    if not names:
        rep.line("No profiles. Use: flyt profile add <name> --proxy URL")
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
    rep = _cli_reporter()
    rep.header(n)
    if not n:
        raise click.ClickException("No profile name and no active profile.")
    data = load_profile_dict(n)
    click.echo(yaml.safe_dump(data, default_flow_style=False, allow_unicode=True))


@cli.group("build")
def build_cli() -> None:
    """Build flyt artifacts locally and upload them to explicit Cypress paths."""


def _verbose_debug_options(f):  # noqa: ANN001
    f = click.option("--debug", is_flag=True, default=False, help="Verbose logs plus yt client debug logging.")(f)
    f = click.option("-v", "--verbose", is_flag=True, default=False, help="Show full logs instead of steps.")(f)
    return f


@build_cli.command("layer")
@click.pass_context
@click.option(
    "--output", "output_path", default=None, type=click.Path(dir_okay=False), help="Write the .squashfs here."
)
@click.option("--upload", "upload_path", default=None, help="Upload to this explicit Cypress path.")
@_verbose_debug_options
def build_layer(
    ctx: click.Context, output_path: Optional[str], upload_path: Optional[str], verbose: bool, debug: bool
) -> None:
    """Build the Flink runtime SquashFS layer (python + JRE + pyflink).

    A local operation — no cluster credentials unless --upload. JARs are never in the layer.
    """
    reporter = _cli_reporter(verbose, debug)
    reporter.header(resolve_effective_profile_name(ctx.obj.get("profile")))
    if not output_path and not upload_path:
        raise click.ClickException("Specify --output FILE and/or --upload //cypress/path.")
    cfg, profile_data = _load_flyt_config_from_profile(ctx)
    if not (cfg.flink_version or "").strip():
        raise click.ClickException("flink_version is required in the profile to build a layer.")

    with _clean_errors(verbose or debug), tempfile.TemporaryDirectory(prefix="flyt_build_layer_") as tmp:
        local = output_path or os.path.join(tmp, "runtime.squashfs")
        build_runtime_squashfs(cfg, local, reporter=reporter)
        rows = [("layer", output_path)] if output_path else []
        if upload_path:
            proxy, _ = _resolve_connection(profile_data, None, None)
            yt_client = make_yt_client(proxy, cfg.yt_client_config)
            with reporter.step("Uploading layer to Cypress"):
                upload_squashfs_layer(
                    yt_client,
                    local,
                    upload_path,
                    set_filesystem_attribute=(cfg.squashfs_layer_delivery == "layer_paths"),
                )
            rows.append(("uploaded", upload_path))
        reporter.result(rows)


@build_cli.command("unsquashfs")
@click.pass_context
@click.option("--output", "output_path", default=None, type=click.Path(dir_okay=False), help="Write the binary here.")
@click.option("--upload", "upload_path", default=None, help="Upload to this explicit Cypress path.")
@_verbose_debug_options
def build_unsquashfs(
    ctx: click.Context, output_path: Optional[str], upload_path: Optional[str], verbose: bool, debug: bool
) -> None:
    """Build a static unsquashfs helper for sandbox_unpack clusters without squashfs-tools.

    Point the profile's unsquashfs_path at the uploaded binary. Local build; --upload needs creds.
    """
    reporter = _cli_reporter(verbose, debug)
    reporter.header(resolve_effective_profile_name(ctx.obj.get("profile")))
    if not output_path and not upload_path:
        raise click.ClickException("Specify --output FILE and/or --upload //cypress/path.")

    with _clean_errors(verbose or debug), tempfile.TemporaryDirectory(prefix="flyt_unsquashfs_") as tmp:
        local = output_path or os.path.join(tmp, "unsquashfs")
        with reporter.step("Building unsquashfs helper"):
            build_unsquashfs_binary(local)
        rows = [("unsquashfs", output_path)] if output_path else []
        if upload_path:
            cfg, profile_data = _load_flyt_config_from_profile(ctx)
            proxy, _ = _resolve_connection(profile_data, None, None)
            yt_client = make_yt_client(proxy, cfg.yt_client_config)
            with reporter.step("Uploading helper to Cypress"):
                upload_local_file(yt_client, local, upload_path)
            rows.append(("uploaded", upload_path))
        reporter.result(rows)


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
    rep = _cli_reporter()
    rep.header(resolve_effective_profile_name(ctx.obj.get("profile")))
    cfg, profile_data = _load_flyt_config_from_profile(ctx)
    try:
        px, _ = _resolve_connection(profile_data, proxy, pool)
    except click.ClickException:
        px = None
    yt_client = None
    if px:
        yt_client = make_yt_client(px, cfg.yt_client_config)
    rows = validate_flyt_config(cfg, proxy=px or None, yt_client=yt_client)
    ok_all = True
    for name, ok, msg in rows:
        ok_all = ok_all and ok
        rep.line(f"[{'OK' if ok else '!!'}] {name}: {msg}")
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
    "-d",
    "--detach",
    is_flag=True,
    help="Submit the operation, wait until it materializes, print the tracking link and exit. "
    "Pass --cache-wheel to reuse a persistent Cypress wheel across runs; otherwise a temporary "
    "wheel is used (safe: YT snapshot-locks file_paths once the operation materializes).",
)
@click.option(
    "--headless",
    is_flag=True,
    default=False,
    help="Do not open Flink Web UI in the browser when it becomes reachable.",
)
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    default=False,
    help="Show full launcher logs instead of the terse step view.",
)
@click.option(
    "--debug",
    is_flag=True,
    default=False,
    help="Verbose launcher logs plus yt client debug logging (implies -v).",
)
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
    headless: bool,
    verbose: bool,
    debug: bool,
) -> None:
    """Launch a PyFlink job. Uses the active profile and auto wheel build."""
    reporter = _cli_reporter(verbose, debug)
    reporter.header(resolve_effective_profile_name(ctx.obj.get("profile")))
    job_command = shlex.join(list(job_argv))
    cfg, profile_data = _load_flyt_config_from_profile(ctx)
    proxy_f, pool_f = _resolve_connection(profile_data, proxy, pool)
    _, _, pst = resolve_connection_from_profile(profile_data)
    preset_s = (preset or "").strip().lower() or (pst or "micro").strip().lower()
    try:
        preset_enum = ClusterPreset[preset_s.upper()]
    except KeyError as e:
        raise click.ClickException("Invalid preset %r. Use one of: micro, small, large, xlarge." % (preset_s,)) from e

    with _clean_errors(verbose), contextlib.ExitStack() as stack:
        project_root: Optional[str] = None
        if not wheel_path and not source_dir:
            proj = find_pyproject_for_job(job_command)
            if proj:
                wdir = stack.enter_context(tempfile.TemporaryDirectory(prefix="flyt_wheel_"))
                with reporter.step("Building service wheel"):
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

        from ytsaurus_flyt.tracking.ui_tracker import FlinkUIWatcher  # noqa: PLC0415

        gp = ctx.obj.get("profile")
        # Watcher is pointless in detach mode: the process exits right after
        # launch_vanilla_job returns (sync=False), killing the daemon thread
        # before it has a chance to find the job.
        watcher: contextlib.AbstractContextManager = (
            FlinkUIWatcher(proxy=proxy_f, open_in_browser=not headless) if not detach else contextlib.nullcontext()
        )
        with watcher:
            launch_vanilla_job(
                config=cfg,
                yt_client=make_yt_client(proxy_f, cfg.yt_client_config),
                job_command=job_command,
                pool=pool_f,
                preset=preset_enum,
                wheel_path=wheel_path,
                source_dir=source_dir,
                cache_wheel=cache_wheel,
                sync=not detach,
                profile_name=resolve_effective_profile_name(gp),
                reporter=reporter,
            )


@cli.command("jobshell")
@click.option(
    "--profile",
    "jobshell_profile",
    default=None,
    help="Profile for proxy and job lookup (overrides flyt --profile / FLYT_PROFILE / active).",
)
@click.pass_context
def jobshell(ctx: click.Context, jobshell_profile: Optional[str]) -> None:
    """Attach to the sandbox of the running 'flyt run' job for the selected profile."""
    parent = ctx.parent
    global_profile = parent.obj.get("profile") if parent and parent.obj else None
    gp = jobshell_profile or global_profile
    name = resolve_effective_profile_name(gp)
    rep = _cli_reporter()
    rep.header(name)
    if not name:
        raise click.ClickException(
            "No profile selected. Use --profile, set FLYT_PROFILE, or: "
            "flyt profile add <name> --proxy URL && flyt profile select <name>"
        )
    data = load_profile_dict(name)
    proxy, _ = _resolve_connection(data, None, None)
    cfg = profile_dict_to_flyt_config(data)
    yt_bin = shutil.which("yt")
    if not yt_bin:
        raise click.ClickException(
            "The 'yt' executable was not found in PATH. Install ytsaurus-client (pip install ytsaurus-client)."
        )
    yt_client = make_yt_client(proxy, cfg.yt_client_config)
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


@cli.command("ui")
@click.pass_context
@click.option("--proxy", default=None, help="YT HTTP proxy (overrides env / profile).")
@click.option(
    "--operation",
    "operation_id",
    default=None,
    help="Operation ID (default: newest running flyt operation for the profile).",
)
@click.option(
    "--wait",
    is_flag=True,
    help="Wait until the Flink Web UI is reachable or the operation reaches a terminal state.",
)
@click.option("--open", "open_browser", is_flag=True, help="Open the URL in the default browser.")
def ui(
    ctx: click.Context,
    proxy: Optional[str],
    operation_id: Optional[str],
    wait: bool,
    open_browser: bool,
) -> None:
    """Find and print the Flink Web UI URL of a running flyt operation."""
    rep = _cli_reporter()
    rep.header(resolve_effective_profile_name(ctx.obj.get("profile")))
    cfg, profile_data = _load_flyt_config_from_profile(ctx)
    proxy_f, _ = _resolve_connection(profile_data, proxy, None)
    yt_client = make_yt_client(proxy_f, cfg.yt_client_config)

    from ytsaurus_flyt.tracking.jobshell_resolve import list_running_flyt_operations  # noqa: PLC0415
    from ytsaurus_flyt.tracking.ui_tracker import find_ui_url_for_operation, wait_ui_url_for_operation  # noqa: PLC0415

    op_id = (operation_id or "").strip()
    if not op_id:
        gp = ctx.obj.get("profile")
        name = resolve_effective_profile_name(gp)
        ops = list_running_flyt_operations(yt_client, name) if name else []
        if not ops:
            raise click.ClickException(
                "No running flyt operation found for this profile. "
                "Pass --operation <id>, or start one with `flyt run ...`."
            )
        op_id = str(ops[0]["id"])
        rep.line(f"Operation: {op_id}")

    if wait:
        url, state = wait_ui_url_for_operation(yt_client, op_id)
        if not url:
            raise click.ClickException(f"Operation {op_id} reached state {state!r} before the Flink UI came up.")
    else:
        url = find_ui_url_for_operation(yt_client, op_id)
        if not url:
            raise click.ClickException(
                f"Flink UI not reachable for operation {op_id} (job not running yet, or port 27050 not open). "
                "Use --wait to block until it comes up."
            )
    click.echo(url)
    if open_browser:
        import webbrowser  # noqa: PLC0415

        webbrowser.open(url)


def main() -> None:
    cli()


if __name__ == "__main__":
    main()
