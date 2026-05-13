from __future__ import annotations

import sys
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from migration_workflow.run_migration import (  # noqa: E402
    LocalTypeDBServer,
    MigrationWorkflowError,
    interpret_migration_result,
    replace_file_from_staging,
    resolve_dump_dir,
    resolve_schema_file,
    resolve_templates_dir,
    stage_file_for_console,
)


def test_resolve_dump_dir_accepts_valid_directory(tmp_path: Path) -> None:
    dump_dir = tmp_path / "dump"
    dump_dir.mkdir()
    (dump_dir / "schema").write_text("define", encoding="utf-8")
    (dump_dir / "data").write_bytes(b"typedb-data")

    resolved = resolve_dump_dir(str(dump_dir))

    assert resolved == dump_dir.resolve()


def test_resolve_dump_dir_requires_schema_and_data(tmp_path: Path) -> None:
    dump_dir = tmp_path / "dump"
    dump_dir.mkdir()
    (dump_dir / "schema").write_text("define", encoding="utf-8")

    with pytest.raises(MigrationWorkflowError) as excinfo:
        resolve_dump_dir(str(dump_dir))

    assert "must contain file 'data'" in str(excinfo.value)


def test_resolve_templates_dir_accepts_file_inside_templates_directory(tmp_path: Path) -> None:
    templates_dir = tmp_path / "db" / "v0.1"
    templates_dir.mkdir(parents=True)
    (templates_dir / "specification.json").write_text("[]", encoding="utf-8")
    template_file = templates_dir / "node-update.tql"
    template_file.write_text("match;", encoding="utf-8")

    resolved = resolve_templates_dir(str(template_file), label="Templates")

    assert resolved == templates_dir.resolve()


def test_resolve_schema_file_accepts_directory(tmp_path: Path) -> None:
    schema_dir = tmp_path / "db" / "v0.2"
    schema_dir.mkdir(parents=True)
    schema_file = schema_dir / "schema"
    schema_file.write_text("define", encoding="utf-8")

    resolved = resolve_schema_file(str(schema_dir), label="Schema")

    assert resolved == schema_file.resolve()


def test_interpret_migration_result_supports_bool_and_none() -> None:
    assert interpret_migration_result(True) is True
    assert interpret_migration_result(False) is False
    assert interpret_migration_result(None) is True


def test_interpret_migration_result_rejects_other_types() -> None:
    with pytest.raises(MigrationWorkflowError) as excinfo:
        interpret_migration_result("success")

    assert "must return bool or None" in str(excinfo.value)


def test_stage_file_for_console_handles_source_path_with_spaces(tmp_path: Path) -> None:
    source_dir = tmp_path / "source with spaces"
    source_dir.mkdir()
    source_path = source_dir / "schema file"
    source_path.write_text("define", encoding="utf-8")

    staged_path = tmp_path / "console-transfer" / "schema"
    result = stage_file_for_console(source_path, staged_path)

    assert result == staged_path
    assert staged_path.exists()
    assert staged_path.read_text(encoding="utf-8") == "define"


def test_replace_file_from_staging_overwrites_existing_target(tmp_path: Path) -> None:
    staged_path = tmp_path / "console-transfer" / "data"
    staged_path.parent.mkdir(parents=True)
    staged_path.write_text("new", encoding="utf-8")

    target_dir = tmp_path / "target with spaces"
    target_dir.mkdir()
    target_path = target_dir / "data"
    target_path.write_text("old", encoding="utf-8")

    replace_file_from_staging(staged_path, target_path)

    assert target_path.read_text(encoding="utf-8") == "new"
    assert not staged_path.exists()


def test_local_typedb_server_import_uses_staged_paths(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    source_dir = tmp_path / "dump with spaces"
    source_dir.mkdir()
    schema_path = source_dir / "schema"
    data_path = source_dir / "data"
    schema_path.write_text("define", encoding="utf-8")
    data_path.write_bytes(b"typedb-data")

    workspace_dir = tmp_path / "workspace"
    server = LocalTypeDBServer(
        typedb_bin="typedb",
        address="127.0.0.1:1729",
        http_address="127.0.0.1:8000",
        username="admin",
        password="password",
        data_dir=workspace_dir / "typedb-data",
        log_file=workspace_dir / "typedb.log",
        startup_timeout=1,
    )

    captured: dict[str, str] = {}

    def fake_console(command: str, *, check: bool = True):
        captured["command"] = command
        return None

    monkeypatch.setattr(server, "console", fake_console)

    server.import_database("old_DB", schema_path, data_path)

    assert '"' not in captured["command"]
    assert str(schema_path) not in captured["command"]
    assert str(data_path) not in captured["command"]
    assert "database import old_DB " in captured["command"]
    assert (workspace_dir / "console-transfer" / "import" / "old_DB" / "schema").exists()
    assert (workspace_dir / "console-transfer" / "import" / "old_DB" / "data").exists()
