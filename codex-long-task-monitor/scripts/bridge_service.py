#!/usr/bin/env python3
"""Install and manage the optional Codex monitor delivery daemon.

Nothing is installed implicitly. ``install`` validates the bridge config,
prints a deterministic dry-run when requested, refuses overwrite by default,
and keeps a recoverable backup when replacement or uninstall is explicit.
Service definitions contain only local paths; secrets remain in the private
bridge configuration file.
"""

from __future__ import annotations

import argparse
import fcntl
from contextlib import contextmanager
import json
import os
import plistlib
import re
import stat
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict


_SCRIPT_DIR = str(Path(__file__).resolve().parent)
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)
import semantic_events as se


PREFIX = "codex-monitor.bridge-service"
SERVICE_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,79}")


def _platform(value: str | None) -> str:
    if value:
        return value
    if sys.platform == "darwin":
        return "darwin"
    if sys.platform.startswith("linux"):
        return "linux"
    raise se.SemanticEventError("service_platform_unsupported")


def _validate_service_name(name: str) -> str:
    if not SERVICE_RE.fullmatch(name):
        raise se.SemanticEventError("service_name_invalid")
    return name


def _default_service_dir(platform: str) -> Path:
    if platform == "darwin":
        return Path.home() / "Library" / "LaunchAgents"
    return Path.home() / ".config" / "systemd" / "user"


def _service_path(args: argparse.Namespace, platform: str) -> Path:
    directory = Path(args.service_dir) if args.service_dir else _default_service_dir(platform)
    suffix = ".plist" if platform == "darwin" else ".service"
    return directory.expanduser() / f"{_validate_service_name(args.service_name)}{suffix}"


def _ensure_service_directory(path: Path) -> None:
    """Create missing service directories; never chmod an existing directory."""
    try:
        info = path.lstat()
    except FileNotFoundError:
        parent = path.parent
        if parent == path:
            raise se.SemanticEventError("service_directory_missing")
        _ensure_service_directory(parent)
        try:
            path.mkdir(mode=0o700)
        except FileExistsError:
            pass
        info = path.lstat()
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise se.SemanticEventError("service_directory_unsafe")


def _resolved_inputs(args: argparse.Namespace) -> tuple[Path, Path, Path, Dict[str, Any]]:
    state_dir = Path(args.state_dir).expanduser().resolve(strict=False)
    config_path = Path(args.bridge_config).expanduser().resolve(strict=True)
    config = se.load_bridge_config(config_path)
    bridge = Path(__file__).resolve().with_name("app_server_bridge.py")
    if not bridge.is_file():
        raise se.SemanticEventError("bridge_script_missing")
    return state_dir, config_path, bridge, config


def _systemd_quote(value: str) -> str:
    if "\n" in value or "\r" in value or "\x00" in value:
        raise se.SemanticEventError("service_path_invalid")
    escaped = value.replace("%", "%%").replace("$", "$$")
    return '"' + escaped.replace("\\", "\\\\").replace('"', '\\"') + '"'


def render_systemd(state_dir: Path, config_path: Path, bridge: Path) -> bytes:
    argv = [
        sys.executable, str(bridge), "deliver",
        "--state-dir", str(state_dir),
        "--bridge-config", str(config_path),
        "--exit-zero-if-disabled",
    ]
    command = " ".join(_systemd_quote(item) for item in argv)
    return (
        "[Unit]\n"
        "Description=Codex monitor semantic-event delivery bridge\n"
        "After=default.target\n\n"
        "[Service]\n"
        "Type=simple\n"
        f"ExecStart={command}\n"
        "Restart=on-failure\n"
        "RestartSec=5\n"
        "UMask=0077\n"
        "NoNewPrivileges=true\n"
        "PrivateTmp=true\n\n"
        "[Install]\n"
        "WantedBy=default.target\n"
    ).encode()


def render_launchd(
    service_name: str, state_dir: Path, config_path: Path, bridge: Path
) -> bytes:
    payload = {
        "Label": service_name,
        "ProgramArguments": [
            sys.executable, str(bridge), "deliver",
            "--state-dir", str(state_dir),
            "--bridge-config", str(config_path),
            "--exit-zero-if-disabled",
        ],
        "RunAtLoad": True,
        "KeepAlive": {"SuccessfulExit": False},
        "ProcessType": "Background",
    }
    return plistlib.dumps(payload, fmt=plistlib.FMT_XML, sort_keys=True)


def _definition(args: argparse.Namespace, platform: str) -> tuple[Path, bytes, Dict[str, Any]]:
    state_dir, config_path, bridge, config = _resolved_inputs(args)
    path = _service_path(args, platform)
    content = (
        render_launchd(args.service_name, state_dir, config_path, bridge)
        if platform == "darwin"
        else render_systemd(state_dir, config_path, bridge)
    )
    metadata = {
        "service_path": str(path),
        "state_dir": str(state_dir),
        "bridge_config": str(config_path),
        "bridge_enabled": config["enabled"],
        "instance_id": config["instance_id"],
    }
    return path, content, metadata


def _write_definition(path: Path, content: bytes, replace: bool) -> str | None:
    _ensure_service_directory(path.parent)
    lock_path = path.parent / ".codex-monitor-service.lock"
    with open(lock_path, "a+", encoding="utf-8") as lock_handle:
        os.chmod(lock_path, 0o600)
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        return _write_definition_locked(path, content, replace)


@contextmanager
def _lifecycle_lock(path: Path):
    _ensure_service_directory(path.parent)
    lock_path = path.parent / ".codex-monitor-lifecycle.lock"
    with open(lock_path, "a+", encoding="utf-8") as lock_handle:
        os.chmod(lock_path, 0o600)
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        yield


def _write_definition_locked(path: Path, content: bytes, replace: bool) -> str | None:
    backup = None
    exists = path.exists() or path.is_symlink()
    if exists:
        info = path.lstat()
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
            raise se.SemanticEventError("service_definition_unsafe")
        if not replace:
            raise se.SemanticEventError("service_definition_exists")
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(str(temporary), flags, 0o600)
    try:
        view = memoryview(content)
        while view:
            written = os.write(fd, view)
            if written <= 0:
                raise OSError("short write while installing service definition")
            view = view[written:]
        os.fsync(fd)
    finally:
        os.close(fd)
    try:
        if exists:
            backup_path = path.with_name(
                f"{path.name}.bak.{time.time_ns()}.{os.getpid()}"
            )
            os.link(path, backup_path, follow_symlinks=False)
            backup = str(backup_path)
            os.replace(temporary, path)
        else:
            os.link(temporary, path, follow_symlinks=False)
        os.chmod(path, 0o600)
        se.fsync_directory(path.parent)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
    return backup


def _run(command: list[str], timeout: float = 30) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command, text=True, capture_output=True, check=False, timeout=timeout
    )


def _manager_action(platform: str, action: str, name: str, path: Path) -> Dict[str, Any]:
    best_effort = action == "rollback-absent"
    if platform == "linux":
        unit = f"{name}.service"
        commands: list[list[str]] = []
        if action == "reload":
            commands = [["systemctl", "--user", "daemon-reload"]]
        elif action == "enable-start":
            commands = [
                ["systemctl", "--user", "daemon-reload"],
                ["systemctl", "--user", "enable", "--now", unit],
            ]
        elif action in {"start", "stop", "restart"}:
            commands = [["systemctl", "--user", action, unit]]
        elif action == "reload-restart":
            commands = [
                ["systemctl", "--user", "daemon-reload"],
                ["systemctl", "--user", "restart", unit],
            ]
        elif action == "rollback-absent":
            commands = [
                ["systemctl", "--user", "stop", unit],
                ["systemctl", "--user", "disable", unit],
                ["systemctl", "--user", "daemon-reload"],
            ]
        elif action == "status":
            commands = [["systemctl", "--user", "is-active", unit]]
        else:
            raise se.SemanticEventError("service_action_invalid")
    else:
        domain = f"gui/{os.getuid()}"
        label = f"{domain}/{name}"
        if action == "enable-start":
            commands = [["launchctl", "bootstrap", domain, str(path)]]
        elif action == "start":
            commands = [["launchctl", "kickstart", label]]
        elif action == "restart":
            commands = [["launchctl", "kickstart", "-k", label]]
        elif action == "reload-restart":
            commands = [
                ["launchctl", "bootout", label],
                ["launchctl", "bootstrap", domain, str(path)],
            ]
        elif action == "rollback-absent":
            commands = [["launchctl", "bootout", label]]
        elif action == "stop":
            commands = [["launchctl", "bootout", label]]
        elif action == "status":
            commands = [["launchctl", "print", label]]
        elif action == "reload":
            commands = []
        else:
            raise se.SemanticEventError("service_action_invalid")
    results = []
    tolerated_indexes: set[int] = set()
    for index, command in enumerate(commands):
        result = _run(command)
        results.append({
            "command": command,
            "returncode": result.returncode,
            "stdout": result.stdout.strip()[:500],
            "stderr": result.stderr.strip()[:500],
        })
        # launchctl bootout reports nonzero when a plist exists on disk but
        # the agent is not currently loaded. Repair must still bootstrap it.
        launch_output = f"{result.stdout}\n{result.stderr}".lower()
        inactive_marker = any(marker in launch_output for marker in (
            "could not find", "no such process", "not found", "not loaded",
        ))
        tolerated_inactive_bootout = (
            platform == "darwin" and command[1] == "bootout"
            and action in {"reload-restart", "stop"} and inactive_marker
        )
        if tolerated_inactive_bootout:
            tolerated_indexes.add(index)
        if result.returncode != 0 and not best_effort and not tolerated_inactive_bootout:
            break
    ok = all(
        item["returncode"] == 0 or idx in tolerated_indexes
        for idx, item in enumerate(results)
    )
    return {"ok": ok, "results": results}


def _rollback_definition(path: Path, backup: str | None) -> Dict[str, Any]:
    """Move the failed definition aside and atomically restore its predecessor."""
    _ensure_service_directory(path.parent)
    lock_path = path.parent / ".codex-monitor-service.lock"
    with open(lock_path, "a+", encoding="utf-8") as lock_handle:
        os.chmod(lock_path, 0o600)
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        failed_path = path.with_name(
            f"{path.name}.failed.{time.time_ns()}.{os.getpid()}"
        )
        if path.exists() and not path.is_symlink():
            path.rename(failed_path)
        restored = False
        if backup is not None and Path(backup).is_file() and not Path(backup).is_symlink():
            os.replace(backup, path)
            restored = True
        se.fsync_directory(path.parent)
    return {"restored_previous": restored, "failed_definition": str(failed_path)}


def _activation_audit(args: argparse.Namespace) -> Dict[str, Any]:
    config = se.load_bridge_config(Path(args.bridge_config))
    audit = se.audit_bridge_activation(
        se.outbox_root(Path(args.state_dir)), config
    )
    audit["config_enabled"] = config["enabled"]
    return audit


def _require_bridge_activated(args: argparse.Namespace) -> Dict[str, Any]:
    """Require the same durable activation receipt enforced by deliver."""
    config = se.load_bridge_config(Path(args.bridge_config))
    if not config["enabled"]:
        raise se.SemanticEventError("bridge_disabled")
    return se.read_bridge_activation(se.outbox_root(Path(args.state_dir)), config)


def install_command(args: argparse.Namespace) -> int:
    platform = _platform(args.platform)
    path, content, metadata = _definition(args, platform)
    payload = {
        "schema_version": f"{PREFIX}.install/v1",
        "platform": platform,
        **metadata,
    }
    if args.dry_run:
        activation_audit = _activation_audit(args)
        payload["activation_audit"] = activation_audit
        payload.update({"state": "dry_run", "definition": content.decode()})
        print(json.dumps(payload, sort_keys=True))
        return 0
    if args.start and not args.no_manager and not metadata["bridge_enabled"]:
        raise se.SemanticEventError("bridge_disabled")
    with _lifecycle_lock(path):
        activation_audit = _activation_audit(args)
        if args.start and not args.no_manager:
            _require_bridge_activated(args)
        backup = _write_definition(path, content, args.replace)
        manager = {"ok": True, "results": []}
        if not args.no_manager:
            manager = _manager_action(
                platform, "enable-start" if args.start else "reload",
                args.service_name, path,
            )
        rollback = None
        if not manager["ok"]:
            rollback = _rollback_definition(path, backup)
            rollback["manager"] = _manager_action(
                platform,
                "reload-restart" if rollback["restored_previous"] else "rollback-absent",
                args.service_name,
                path,
            )
    payload.update({
        "state": "installed" if manager["ok"] else "manager_failed_rolled_back",
        "backup": backup if manager["ok"] else None,
        "manager": manager,
        "rollback": rollback,
        "activation_audit": activation_audit,
    })
    print(json.dumps(payload, sort_keys=True))
    return 0 if manager["ok"] else 4


def status_command(args: argparse.Namespace) -> int:
    platform = _platform(args.platform)
    path = _service_path(args, platform)
    installed = path.is_file() and not path.is_symlink()
    manager = {"ok": False, "results": []}
    if installed and not args.no_manager:
        manager = _manager_action(platform, "status", args.service_name, path)
    print(json.dumps({
        "schema_version": f"{PREFIX}.status/v1",
        "platform": platform,
        "service_path": str(path),
        "installed": installed,
        "active": manager["ok"] if not args.no_manager else None,
        "manager": manager,
    }, sort_keys=True))
    return 0 if installed else 3


def repair_command(args: argparse.Namespace) -> int:
    platform = _platform(args.platform)
    path, expected, metadata = _definition(args, platform)
    if args.apply and not args.no_manager and not metadata["bridge_enabled"]:
        raise se.SemanticEventError("bridge_disabled")
    with _lifecycle_lock(path):
        activation_audit = _activation_audit(args)
        current = None
        if path.is_file() and not path.is_symlink():
            current = path.read_bytes()
        state = "healthy" if current == expected else "missing" if current is None else "drifted"
        backup = None
        manager = {"ok": True, "results": []}
        original_state = state
        if state != "healthy" and args.apply:
            if not args.no_manager:
                _require_bridge_activated(args)
            backup = _write_definition(path, expected, replace=current is not None)
            state = "repaired"
            if not args.no_manager:
                manager = _manager_action(
                    platform,
                    "enable-start" if original_state == "missing" else "reload-restart",
                    args.service_name,
                    path,
                )
        rollback = None
        if state == "repaired" and not manager["ok"]:
            rollback = _rollback_definition(path, backup)
            rollback["manager"] = _manager_action(
                platform,
                "reload-restart" if rollback["restored_previous"] else "rollback-absent",
                args.service_name,
                path,
            )
            state = "manager_failed_rolled_back"
    print(json.dumps({
        "schema_version": f"{PREFIX}.repair/v1",
        "platform": platform,
        **metadata,
        "activation_audit": activation_audit,
        "state": state,
        "applied": bool(args.apply and state == "repaired"),
        "backup": backup,
        "manager": manager,
        "rollback": rollback,
    }, sort_keys=True))
    if not manager["ok"]:
        return 4
    return 0 if state in {"healthy", "repaired"} else 3


def action_command(args: argparse.Namespace) -> int:
    platform = _platform(args.platform)
    path = _service_path(args, platform)
    with _lifecycle_lock(path):
        activation_audit = None
        if args.command in {"start", "restart"}:
            activation_audit = _activation_audit(args)
            _require_bridge_activated(args)
        if not path.is_file() or path.is_symlink():
            raise se.SemanticEventError("service_not_installed")
        result = _manager_action(platform, args.command, args.service_name, path)
    print(json.dumps({
        "schema_version": f"{PREFIX}.action/v1",
        "action": args.command,
        "state": "completed" if result["ok"] else "failed",
        "manager": result,
        "activation_audit": activation_audit,
    }, sort_keys=True))
    return 0 if result["ok"] else 4


def uninstall_command(args: argparse.Namespace) -> int:
    if not args.i_mean_it:
        print(json.dumps({
            "schema_version": f"{PREFIX}.uninstall/v1",
            "state": "confirmation_required",
        }, sort_keys=True))
        return 4
    platform = _platform(args.platform)
    path = _service_path(args, platform)
    with _lifecycle_lock(path):
        if not path.exists():
            print(json.dumps({
                "schema_version": f"{PREFIX}.uninstall/v1", "state": "not_installed"
            }, sort_keys=True))
            return 3
        info = path.lstat()
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
            raise se.SemanticEventError("service_definition_unsafe")
        manager = {"ok": True, "results": []}
        if not args.no_manager:
            manager = _manager_action(platform, "stop", args.service_name, path)
            if not manager["ok"]:
                print(json.dumps({
                    "schema_version": f"{PREFIX}.uninstall/v1",
                    "state": "manager_stop_failed",
                    "service_path": str(path),
                    "manager": manager,
                }, sort_keys=True))
                return 4
        recovered = path.with_name(
            f"{path.name}.removed.{time.time_ns()}.{os.getpid()}"
        )
        path.rename(recovered)
        se.fsync_directory(path.parent)
        if not args.no_manager:
            reload_result = _manager_action(platform, "reload", args.service_name, path)
            manager["results"].extend(reload_result["results"])
            manager["ok"] = manager["ok"] and reload_result["ok"]
    print(json.dumps({
        "schema_version": f"{PREFIX}.uninstall/v1",
        "state": "removed_recoverably",
        "recoverable_path": str(recovered),
        "manager": manager,
    }, sort_keys=True))
    return 0 if manager["ok"] else 4


def logs_command(args: argparse.Namespace) -> int:
    platform = _platform(args.platform)
    if platform == "linux":
        command = [
            "journalctl", "--user", "-u", f"{args.service_name}.service",
            "--no-pager", "-n", str(args.lines),
        ]
    else:
        command = [
            "log", "show", "--style", "compact", "--last", args.since,
            "--predicate", f'process == "{Path(sys.executable).name}"',
        ]
    result = _run(command)
    sys.stdout.write(result.stdout)
    if result.returncode != 0:
        sys.stderr.write(result.stderr)
    return result.returncode


def positive_int(value: str) -> int:
    result = int(value)
    if result <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return result


def _common(subparser: argparse.ArgumentParser) -> None:
    subparser.add_argument(
        "--service-name", required=True,
        help="unique user-service name; use a different name per state/config",
    )
    subparser.add_argument("--service-dir", type=Path)
    subparser.add_argument("--platform", choices=("linux", "darwin"))


def _activation_arguments(subparser: argparse.ArgumentParser) -> None:
    subparser.add_argument("--state-dir", type=Path, required=True)
    subparser.add_argument("--bridge-config", type=Path, required=True)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    sub = result.add_subparsers(dest="command", required=True)

    install = sub.add_parser("install", help="install an opt-in user service")
    _common(install)
    install.add_argument("--state-dir", type=Path, required=True)
    install.add_argument("--bridge-config", type=Path, required=True)
    install.add_argument("--dry-run", action="store_true")
    install.add_argument("--replace", action="store_true")
    install.add_argument("--start", action="store_true")
    install.add_argument("--no-manager", action="store_true")
    install.set_defaults(func=install_command)

    status = sub.add_parser("status", help="show installed and manager state")
    _common(status)
    status.add_argument("--no-manager", action="store_true")
    status.set_defaults(func=status_command)

    repair = sub.add_parser("repair", help="detect or repair service-definition drift")
    _common(repair)
    repair.add_argument("--state-dir", type=Path, required=True)
    repair.add_argument("--bridge-config", type=Path, required=True)
    repair.add_argument("--apply", action="store_true")
    repair.add_argument("--no-manager", action="store_true")
    repair.set_defaults(func=repair_command)

    for name in ("start", "stop", "restart"):
        action = sub.add_parser(name, help=f"{name} the installed user service")
        _common(action)
        if name in {"start", "restart"}:
            _activation_arguments(action)
        action.set_defaults(func=action_command)

    uninstall = sub.add_parser("uninstall", help="recoverably remove the service")
    _common(uninstall)
    uninstall.add_argument("--i-mean-it", action="store_true")
    uninstall.add_argument("--no-manager", action="store_true")
    uninstall.set_defaults(func=uninstall_command)

    logs = sub.add_parser("logs", help="show recent service logs")
    _common(logs)
    logs.add_argument("--lines", type=positive_int, default=100)
    logs.add_argument("--since", default="1h")
    logs.set_defaults(func=logs_command)
    return result


def main() -> int:
    args = parser().parse_args()
    try:
        return int(args.func(args))
    except (OSError, ValueError, subprocess.TimeoutExpired, se.SemanticEventError) as exc:
        print(json.dumps({
            "schema_version": f"{PREFIX}.error/v1",
            "state": "error",
            "reason": exc.reason if isinstance(exc, se.SemanticEventError) else type(exc).__name__,
            "detail": exc.detail if isinstance(exc, se.SemanticEventError) else str(exc),
        }, sort_keys=True))
        return 12


if __name__ == "__main__":
    raise SystemExit(main())
