#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"

DEFAULT_INVESTIGATION_NAME = os.getenv("INVESTIGATION_NAME", "tsarstvie")
DEFAULT_TDB_USERNAME = os.getenv("TYPEDB_USERNAME", "admin")
DEFAULT_TDB_PASSWORD = os.getenv("TYPEDB_PASSWORD", "password")
DEFAULT_TDB_STARTUP_TIMEOUT = int(os.getenv("TYPEDB_STARTUP_TIMEOUT", "60"))
OLD_DB_NAME = "old_DB"
NEW_DB_NAME = "new_DB"


class MigrationWorkflowError(RuntimeError):
    """Ошибка orchestration-скрипта миграции."""


MigrationCallable = Callable[[Any, Any], bool | None]


def ensure_src_on_path() -> None:
    src_path = str(SRC_DIR)
    if src_path not in sys.path:
        sys.path.insert(0, src_path)


def resolve_existing_path(raw_path: str, *, label: str) -> Path:
    path = Path(raw_path).expanduser().resolve()
    if not path.exists():
        raise MigrationWorkflowError(f"{label} does not exist: {path}")
    return path


def resolve_dump_dir(raw_path: str) -> Path:
    dump_dir = resolve_existing_path(raw_path, label="Old dump directory")
    if not dump_dir.is_dir():
        raise MigrationWorkflowError(f"Old dump path must be a directory: {dump_dir}")

    schema_path = dump_dir / "schema"
    data_path = dump_dir / "data"
    if not schema_path.is_file():
        raise MigrationWorkflowError(f"Old dump directory must contain file 'schema': {schema_path}")
    if not data_path.is_file():
        raise MigrationWorkflowError(f"Old dump directory must contain file 'data': {data_path}")
    return dump_dir


def resolve_templates_dir(raw_path: str, *, label: str) -> Path:
    candidate = resolve_existing_path(raw_path, label=label)
    templates_dir = candidate.parent if candidate.is_file() else candidate
    if not templates_dir.is_dir():
        raise MigrationWorkflowError(f"{label} must resolve to a directory: {templates_dir}")

    specification_path = templates_dir / "specification.json"
    if not specification_path.is_file():
        raise MigrationWorkflowError(
            f"{label} must contain specification.json: {specification_path}"
        )
    return templates_dir


def resolve_schema_file(raw_path: str, *, label: str) -> Path:
    candidate = resolve_existing_path(raw_path, label=label)
    schema_path = candidate / "schema" if candidate.is_dir() else candidate
    if not schema_path.is_file():
        raise MigrationWorkflowError(f"{label} must resolve to a schema file: {schema_path}")
    return schema_path


def interpret_migration_result(result: object) -> bool:
    if result is None:
        return True
    if isinstance(result, bool):
        return result
    raise MigrationWorkflowError(
        "Migration function must return bool or None."
    )


def stage_file_for_console(source_path: Path, staged_path: Path) -> Path:
    """
    Готовит безопасный локальный путь для typedb console.

    Внутренний parser команды `database import/export` работает со строкой
    `--command` как с собственным CLI и не обязан интерпретировать shell-like
    кавычки. Поэтому вместо передачи исходных путей пользователя мы
    подготавливаем staging-файлы без пробелов в имени и передаём уже их.
    """
    staged_path.parent.mkdir(parents=True, exist_ok=True)
    if staged_path.exists() or staged_path.is_symlink():
        staged_path.unlink()

    try:
        staged_path.symlink_to(source_path)
    except OSError:
        shutil.copy2(source_path, staged_path)

    return staged_path


def replace_file_from_staging(staged_path: Path, target_path: Path) -> None:
    """Перемещает экспортированный файл из staging в целевой путь."""
    target_path.parent.mkdir(parents=True, exist_ok=True)
    if target_path.exists() or target_path.is_symlink():
        target_path.unlink()
    shutil.move(str(staged_path), str(target_path))


def read_log_tail(log_file: Path, *, lines: int = 40) -> str:
    if not log_file.is_file():
        return ""
    try:
        content = log_file.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception:
        return ""
    return "\n".join(content[-lines:])


def find_free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def load_migration_callable(script_path: Path) -> MigrationCallable:
    ensure_src_on_path()

    spec = importlib.util.spec_from_file_location("user_migration_script", script_path)
    if spec is None or spec.loader is None:
        raise MigrationWorkflowError(f"Failed to load migration script: {script_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    migrate_callable = getattr(module, "migrate", None)
    if not callable(migrate_callable):
        raise MigrationWorkflowError(
            f"Migration script must define callable 'migrate(old_client, new_client)': {script_path}"
        )
    return migrate_callable


@dataclass
class LocalTypeDBServer:
    typedb_bin: str
    address: str
    http_address: str
    username: str
    password: str
    data_dir: Path
    log_file: Path
    startup_timeout: int
    tls_disabled: bool = True
    process: subprocess.Popen[str] | None = None
    _log_handle: Any | None = None

    def start(self) -> None:
        typedb_path = shutil.which(self.typedb_bin)
        if typedb_path is None:
            raise MigrationWorkflowError(
                f"TypeDB binary '{self.typedb_bin}' was not found in PATH."
            )

        self.typedb_bin = typedb_path
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        self._log_handle = self.log_file.open("a", encoding="utf-8")

        command = [
            self.typedb_bin,
            "server",
            f"--server.address={self.address}",
            f"--server.http.address={self.http_address}",
            f"--storage.data-directory={self.data_dir}",
        ]

        self.process = subprocess.Popen(
            command,
            stdout=self._log_handle,
            stderr=subprocess.STDOUT,
            text=True,
        )
        self.wait_until_ready()

    def stop(self) -> None:
        try:
            if self.process is not None and self.process.poll() is None:
                self.process.terminate()
                try:
                    self.process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(timeout=10)
        finally:
            if self._log_handle is not None:
                self._log_handle.close()
                self._log_handle = None

    def console(self, command: str, *, check: bool = True) -> subprocess.CompletedProcess[str]:
        cli_command = [
            self.typedb_bin,
            "console",
            "--address",
            self.address,
            "--username",
            self.username,
            "--password",
            self.password,
        ]
        if self.tls_disabled:
            cli_command.append("--tls-disabled")
        cli_command.extend(["--command", command])

        result = subprocess.run(
            cli_command,
            capture_output=True,
            text=True,
        )
        if check and result.returncode != 0:
            message = (result.stderr or result.stdout or "").strip()
            raise MigrationWorkflowError(
                f"TypeDB console command failed: {command}\n{message}"
            )
        return result

    def wait_until_ready(self) -> None:
        deadline = time.time() + self.startup_timeout
        last_error = ""

        while time.time() < deadline:
            if self.process is not None and self.process.poll() is not None:
                tail = read_log_tail(self.log_file)
                raise MigrationWorkflowError(
                    "TypeDB server exited during startup.\n"
                    f"Log tail:\n{tail}"
                )

            result = self.console("database list", check=False)
            if result.returncode == 0:
                return

            last_error = (result.stderr or result.stdout or "").strip()
            time.sleep(1)

        tail = read_log_tail(self.log_file)
        raise MigrationWorkflowError(
            f"TypeDB did not become ready within {self.startup_timeout} seconds.\n"
            f"Last console error: {last_error}\n"
            f"Log tail:\n{tail}"
        )

    def _console_transfer_dir(self, *, direction: str, db_name: str) -> Path:
        transfer_dir = self.log_file.parent / "console-transfer" / direction / db_name
        transfer_dir.mkdir(parents=True, exist_ok=True)
        return transfer_dir

    def _prepare_import_paths(self, db_name: str, schema_path: Path, data_path: Path) -> tuple[Path, Path]:
        transfer_dir = self._console_transfer_dir(direction="import", db_name=db_name)
        staged_schema = stage_file_for_console(schema_path, transfer_dir / "schema")
        staged_data = stage_file_for_console(data_path, transfer_dir / "data")
        return staged_schema, staged_data

    def _prepare_export_paths(self, db_name: str) -> tuple[Path, Path]:
        transfer_dir = self._console_transfer_dir(direction="export", db_name=db_name)
        staged_schema = transfer_dir / "schema"
        staged_data = transfer_dir / "data"
        for path in (staged_schema, staged_data):
            if path.exists() or path.is_symlink():
                path.unlink()
        return staged_schema, staged_data

    def import_database(self, db_name: str, schema_path: Path, data_path: Path) -> None:
        staged_schema, staged_data = self._prepare_import_paths(db_name, schema_path, data_path)
        command = (
            f"database import {db_name} "
            f"{staged_schema} "
            f"{staged_data}"
        )
        self.console(command)

    def export_database(self, db_name: str, schema_path: Path, data_path: Path) -> None:
        staged_schema, staged_data = self._prepare_export_paths(db_name)
        command = (
            f"database export {db_name} "
            f"{staged_schema} "
            f"{staged_data}"
        )
        self.console(command)
        replace_file_from_staging(staged_schema, schema_path)
        replace_file_from_staging(staged_data, data_path)


def build_clients(
    *,
    typedb_address: str,
    username: str,
    password: str,
    investigation_name: str,
    old_templates_dir: Path,
    new_templates_dir: Path,
    new_schema_path: Path,
) -> tuple[Any, Any]:
    ensure_src_on_path()
    try:
        from typedb_client import TypeDBClient
    except ModuleNotFoundError as e:
        if e.name == "typedb":
            raise MigrationWorkflowError(
                "Python module 'typedb' is not available in the current interpreter.\n"
                "Install project dependencies for the same Python you use to run migration.\n"
                f"Current interpreter: {sys.executable}\n"
                "Expected package: typedb-driver==3.7.0\n"
                "Example:\n"
                "  python3 -m venv .venv\n"
                "  source .venv/bin/activate\n"
                "  python -m pip install -r IB-graph-server/requirements.txt"
            ) from e
        raise

    old_client = TypeDBClient(
        db_name=OLD_DB_NAME,
        templates_dir=old_templates_dir,
        investigation_name=investigation_name,
        typedb_address=typedb_address,
        username=username,
        password=password,
        tls_enabled=False,
        auto_bootstrap=False,
        allow_debug_operations=True,
    )

    new_client = TypeDBClient(
        db_name=NEW_DB_NAME,
        templates_dir=new_templates_dir,
        schema_path=new_schema_path,
        investigation_name=investigation_name,
        typedb_address=typedb_address,
        username=username,
        password=password,
        tls_enabled=False,
        auto_bootstrap=True,
        bootstrap_default_board=False,
        allow_debug_operations=True,
    )

    return old_client, new_client


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Runs a local TypeDB-based migration workflow: "
            "imports old dump, creates new DB with new schema, "
            "executes custom migration code, and exports the new DB."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--old-dump-dir",
        required=True,
        help="Directory that contains the old dump files 'schema' and 'data'.",
    )
    parser.add_argument(
        "--old-templates",
        required=True,
        help=(
            "Directory with old DB TypeQL templates/specification, or any file inside it "
            "(for example db/v0.1 or db/v0.1/node-update.tql)."
        ),
    )
    parser.add_argument(
        "--new-schema",
        required=True,
        help=(
            "Directory containing the new schema file 'schema', or a direct path to that schema file."
        ),
    )
    parser.add_argument(
        "--new-templates",
        required=True,
        help=(
            "Directory with new DB TypeQL templates/specification, or any file inside it "
            "(for example db/v0.2 or db/v0.2/node-update.tql)."
        ),
    )
    parser.add_argument(
        "--migration-script",
        required=True,
        help="Python file that defines migrate(old_client, new_client) -> bool | None.",
    )
    parser.add_argument(
        "--typedb-bin",
        default="typedb",
        help="TypeDB executable to run locally.",
    )
    parser.add_argument(
        "--investigation-name",
        default=DEFAULT_INVESTIGATION_NAME,
        help="Investigation name expected by the old and new template sets.",
    )
    parser.add_argument(
        "--typedb-username",
        default=DEFAULT_TDB_USERNAME,
        help="Username for local TypeDB console connections.",
    )
    parser.add_argument(
        "--typedb-password",
        default=DEFAULT_TDB_PASSWORD,
        help="Password for local TypeDB console connections.",
    )
    parser.add_argument(
        "--startup-timeout",
        type=int,
        default=DEFAULT_TDB_STARTUP_TIMEOUT,
        help="Seconds to wait for the temporary TypeDB server to become ready.",
    )
    parser.add_argument(
        "--workspace-dir",
        default="",
        help="Optional directory for temporary TypeDB data/logs. If omitted, a temp directory is used.",
    )
    parser.add_argument(
        "--keep-workspace",
        action="store_true",
        help="Keep workspace directory after a successful run.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    old_dump_dir = resolve_dump_dir(args.old_dump_dir)
    old_templates_dir = resolve_templates_dir(args.old_templates, label="Old templates path")
    new_schema_path = resolve_schema_file(args.new_schema, label="New schema path")
    new_templates_dir = resolve_templates_dir(args.new_templates, label="New templates path")
    migration_script_path = resolve_existing_path(
        args.migration_script,
        label="Migration script",
    )

    if not migration_script_path.is_file():
        raise MigrationWorkflowError(f"Migration script must be a file: {migration_script_path}")

    if args.workspace_dir:
        workspace_dir = Path(args.workspace_dir).expanduser().resolve()
        workspace_dir.mkdir(parents=True, exist_ok=True)
        workspace_is_temporary = False
    else:
        workspace_dir = Path(tempfile.mkdtemp(prefix="ib-typedb-migration-"))
        workspace_is_temporary = True

    success = False
    typedb_address = f"127.0.0.1:{find_free_tcp_port()}"
    typedb_http_address = f"127.0.0.1:{find_free_tcp_port()}"
    server = LocalTypeDBServer(
        typedb_bin=args.typedb_bin,
        address=typedb_address,
        http_address=typedb_http_address,
        username=args.typedb_username,
        password=args.typedb_password,
        data_dir=workspace_dir / "typedb-data",
        log_file=workspace_dir / "typedb.log",
        startup_timeout=args.startup_timeout,
    )

    old_client = None
    new_client = None

    try:
        print(f"[migration] Workspace: {workspace_dir}")
        print(f"[migration] Starting temporary TypeDB at {typedb_address}")
        server.start()

        print(f"[migration] Importing old dump from {old_dump_dir}")
        server.import_database(
            OLD_DB_NAME,
            old_dump_dir / "schema",
            old_dump_dir / "data",
        )

        print("[migration] Creating old/new TypeDB clients")
        old_client, new_client = build_clients(
            typedb_address=typedb_address,
            username=args.typedb_username,
            password=args.typedb_password,
            investigation_name=args.investigation_name,
            old_templates_dir=old_templates_dir,
            new_templates_dir=new_templates_dir,
            new_schema_path=new_schema_path,
        )

        print(f"[migration] Loading migration script {migration_script_path}")
        migrate = load_migration_callable(migration_script_path)

        print("[migration] Running custom migration code")
        migration_ok = interpret_migration_result(migrate(old_client, new_client))
        if not migration_ok:
            raise MigrationWorkflowError("Migration script returned failure.")

        export_data_path = new_schema_path.parent / "data"
        print(
            f"[migration] Exporting new database to {new_schema_path.parent} "
            f"(schema={new_schema_path.name}, data={export_data_path.name})"
        )
        server.export_database(
            NEW_DB_NAME,
            new_schema_path,
            export_data_path,
        )

        success = True
        print("[migration] Migration completed successfully.")
        return 0
    finally:
        if new_client is not None:
            try:
                new_client.close()
            except Exception:
                pass

        if old_client is not None:
            try:
                old_client.close()
            except Exception:
                pass

        server.stop()

        if workspace_is_temporary and success and not args.keep_workspace:
            shutil.rmtree(workspace_dir, ignore_errors=True)
        else:
            print(f"[migration] Workspace kept at: {workspace_dir}")


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except MigrationWorkflowError as exc:
        print(f"[migration] ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
