# tests/test_typeql_template_driver.py

import json
import os
from pathlib import Path

import pytest

from ..typeql_template_driver import (
    TypeQLTemplateDriver,
    TemplateDriverError,
    SpecificationError,
    TemplateFileError,
    OperationError,
)


# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------

def _write_json(path: Path, data) -> None:
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def _setup_valid_structure(tmp_path: Path) -> tuple[Path, str]:
    """
    Создаёт валидную структуру db/v0.1 с specification.json и шаблонами.
    Возвращает (db_root, version).
    """
    db_root = tmp_path / "db"
    version = "v0.1"
    version_dir = db_root / version
    version_dir.mkdir(parents=True, exist_ok=True)

    spec = [
        {
            "operation": "op1",
            "file": "op1.tql",
            "description": "Test operation 1",
            "params": ["param1", "param2"],
            "output": None,
        },
        {
            "operation": "op2",
            "file": "op2.tql",
            "description": "Test operation 2",
            "params": [],
            "output": None,
        },
    ]
    _write_json(version_dir / "specification.json", spec)

    # Шаблоны
    (version_dir / "op1.tql").write_text(
        "match $x has name {param1}; insert $x has age {param2};",
        encoding="utf-8",
    )
    (version_dir / "op2.tql").write_text(
        "match $y isa thing; get;",
        encoding="utf-8",
    )

    return db_root, version


# ---------------------------------------------------------------------------
# Тесты инициализации (__init__) и структуры
# ---------------------------------------------------------------------------

def test_init_loads_spec_and_templates(tmp_path: Path) -> None:
    db_root, version = _setup_valid_structure(tmp_path)

    driver = TypeQLTemplateDriver(db_root=str(db_root), version=version)

    assert driver.version == version
    assert Path(driver.base_dir) == db_root / version

    # операции загружены
    assert driver.has_operation("op1")
    assert driver.has_operation("op2")
    assert not driver.has_operation("unknown-op")

    # required_params
    assert driver.required_params("op1") == {"param1", "param2"}
    assert driver.required_params("op2") == set()


def test_init_missing_version_dir_raises_specification_error(tmp_path: Path) -> None:
    db_root = tmp_path / "db"
    db_root.mkdir(parents=True, exist_ok=True)

    with pytest.raises(SpecificationError) as excinfo:
        TypeQLTemplateDriver(db_root=str(db_root), version="nonexistent")

    assert "does not exist or is not a directory" in str(excinfo.value)


def test_init_missing_spec_file_raises_specification_error(tmp_path: Path) -> None:
    db_root = tmp_path / "db"
    version = "v0.1"
    version_dir = db_root / version
    version_dir.mkdir(parents=True, exist_ok=True)

    with pytest.raises(SpecificationError) as excinfo:
        TypeQLTemplateDriver(db_root=str(db_root), version=version)

    assert "Specification file" in str(excinfo.value)
    assert "does not exist or is not a file" in str(excinfo.value)


def test_init_invalid_spec_type_raises_specification_error(tmp_path: Path) -> None:
    db_root = tmp_path / "db"
    version = "v0.1"
    version_dir = db_root / version
    version_dir.mkdir(parents=True, exist_ok=True)

    # Вместо списка кладём объект
    _write_json(version_dir / "specification.json", {"operation": "bad"})

    with pytest.raises(SpecificationError) as excinfo:
        TypeQLTemplateDriver(db_root=str(db_root), version=version)

    assert "must contain a JSON array of operations" in str(excinfo.value)


def test_init_missing_template_file_raises_template_file_error(tmp_path: Path) -> None:
    db_root = tmp_path / "db"
    version = "v0.1"
    version_dir = db_root / version
    version_dir.mkdir(parents=True, exist_ok=True)

    spec = [
        {
            "operation": "op1",
            "file": "op1.tql",
            "description": "Test operation 1",
            "params": ["param1"],
            "output": None,
        },
    ]
    _write_json(version_dir / "specification.json", spec)

    # op1.tql не создаём

    with pytest.raises(TemplateFileError) as excinfo:
        TypeQLTemplateDriver(db_root=str(db_root), version=version)

    assert "does not exist" in str(excinfo.value)
    assert "op1.tql" in str(excinfo.value)


def test_init_duplicate_operation_raises_specification_error(tmp_path: Path) -> None:
    db_root = tmp_path / "db"
    version = "v0.1"
    version_dir = db_root / version
    version_dir.mkdir(parents=True, exist_ok=True)

    spec = [
        {
            "operation": "op1",
            "file": "op1.tql",
            "description": "Test operation 1",
            "params": ["param1"],
            "output": None,
        },
        {
            "operation": "op1",
            "file": "op1_dup.tql",
            "description": "Duplicate operation",
            "params": ["param1"],
            "output": None,
        },
    ]
    _write_json(version_dir / "specification.json", spec)
    (version_dir / "op1.tql").write_text("match {param1};", encoding="utf-8")
    (version_dir / "op1_dup.tql").write_text("match {param1};", encoding="utf-8")

    with pytest.raises(SpecificationError) as excinfo:
        TypeQLTemplateDriver(db_root=str(db_root), version=version)

    assert "Duplicate operation name in specification" in str(excinfo.value)


# ---------------------------------------------------------------------------
# Тесты has_operation, required_params, describe_operation
# ---------------------------------------------------------------------------

def test_has_operation_and_required_params(tmp_path: Path) -> None:
    db_root, version = _setup_valid_structure(tmp_path)
    driver = TypeQLTemplateDriver(db_root=str(db_root), version=version)

    assert driver.has_operation("op1")
    assert not driver.has_operation("nope")

    assert driver.required_params("op1") == {"param1", "param2"}

    with pytest.raises(OperationError) as excinfo:
        driver.required_params("nope")
    assert "Unknown operation" in str(excinfo.value)


def test_describe_operation_success(tmp_path: Path) -> None:
    db_root, version = _setup_valid_structure(tmp_path)
    driver = TypeQLTemplateDriver(db_root=str(db_root), version=version)

    meta = driver.describe_operation("op1")

    assert meta["operation"] == "op1"
    assert meta["file"] == "op1.tql"
    assert meta["description"] == "Test operation 1"
    assert set(meta["params"]) == {"param1", "param2"}
    assert meta["output"] is None


def test_describe_operation_unknown_raises(tmp_path: Path) -> None:
    db_root, version = _setup_valid_structure(tmp_path)
    driver = TypeQLTemplateDriver(db_root=str(db_root), version=version)

    with pytest.raises(OperationError) as excinfo:
        driver.describe_operation("unknown-op")

    assert "Unknown operation" in str(excinfo.value)


# ---------------------------------------------------------------------------
# Тесты get_operation
# ---------------------------------------------------------------------------

def test_get_operation_success_with_params_dict(tmp_path: Path) -> None:
    db_root, version = _setup_valid_structure(tmp_path)
    driver = TypeQLTemplateDriver(db_root=str(db_root), version=version)

    query = driver.get_operation("op1", params={"param1": "Alice", "param2": 42})

    assert "Alice" in query
    assert "42" in query
    assert "param1" not in query  # плейсхолдеры заменены


def test_get_operation_success_with_kwargs_and_override(tmp_path: Path) -> None:
    db_root, version = _setup_valid_structure(tmp_path)
    driver = TypeQLTemplateDriver(db_root=str(db_root), version=version)

    # params dict даёт одно значение, kwargs должны его перетереть
    query = driver.get_operation(
        "op1",
        params={"param1": "Wrong", "param2": 10},
        param1="Correct",
    )

    assert "Correct" in query
    assert "Wrong" not in query
    assert "10" in query


def test_get_operation_missing_required_param_raises(tmp_path: Path) -> None:
    db_root, version = _setup_valid_structure(tmp_path)
    driver = TypeQLTemplateDriver(db_root=str(db_root), version=version)

    # Не передаём param2
    with pytest.raises(OperationError) as excinfo:
        driver.get_operation("op1", params={"param1": "Alice"})

    msg = str(excinfo.value)
    assert "missing required parameter(s)" in msg
    assert "param2" in msg


def test_get_operation_extra_param_raises(tmp_path: Path) -> None:
    db_root, version = _setup_valid_structure(tmp_path)
    driver = TypeQLTemplateDriver(db_root=str(db_root), version=version)

    # Передаём лишний параметр
    with pytest.raises(OperationError) as excinfo:
        driver.get_operation(
            "op1",
            params={"param1": "Alice", "param2": 42, "extra": "oops"},
        )

    msg = str(excinfo.value)
    assert "received unknown parameter(s)" in msg
    assert "extra" in msg


def test_get_operation_unknown_op_raises(tmp_path: Path) -> None:
    db_root, version = _setup_valid_structure(tmp_path)
    driver = TypeQLTemplateDriver(db_root=str(db_root), version=version)

    with pytest.raises(OperationError) as excinfo:
        driver.get_operation("no-such-op")

    assert "Unknown operation" in str(excinfo.value)


def test_get_operation_non_dict_params_raises(tmp_path: Path) -> None:
    db_root, version = _setup_valid_structure(tmp_path)
    driver = TypeQLTemplateDriver(db_root=str(db_root), version=version)

    with pytest.raises(OperationError) as excinfo:
        driver.get_operation("op1", params=["not", "a", "dict"])  # type: ignore[arg-type]

    assert "must be a dict" in str(excinfo.value)


def test_get_operation_format_runtime_error_wrapped(tmp_path: Path) -> None:
    """
    Провоцируем ошибку форматирования, не связанную с отсутствующим ключом,
    чтобы проверить общий перехват и оборачивание в OperationError.
    """
    db_root = tmp_path / "db"
    version = "v0.1"
    version_dir = db_root / version
    version_dir.mkdir(parents=True, exist_ok=True)

    # Определяем один operation с некорректным шаблоном (например, кривой формат spec)
    spec = [
        {
            "operation": "bad-format",
            "file": "bad-format.tql",
            "description": "Bad format operation",
            "params": ["param"],
            "output": None,
        },
    ]
    _write_json(version_dir / "specification.json", spec)

    # Нечто, что вызовет ValueError при format (например, некорректный форматный спецификатор)
    (version_dir / "bad-format.tql").write_text(
        "value: {param!999}",  # !999 — невалидный conversion
        encoding="utf-8",
    )

    driver = TypeQLTemplateDriver(db_root=str(db_root), version=version)

    with pytest.raises(OperationError) as excinfo:
        driver.get_operation("bad-format", param="x")

    assert "Failed to format template" in str(excinfo.value)

def main() -> int:
    """Запустить все тесты в этом файле как самостоятельный скрипт."""
    import pytest
    from pathlib import Path

    return pytest.main([str(Path(__file__))])

if __name__ == "__main__":
    raise SystemExit(main())
