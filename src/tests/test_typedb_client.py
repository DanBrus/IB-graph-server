import os
from typing import Any

import pytest

from ..typedb_client import (
    TypeDBClient,
    OperationIsNotAllowed,
    TypeDBClientError,
)


TEST_INVESTIGATION_NAME = "test"
TEST_VERSION = "0.0"
TEST_DB_NAME = "test_db"


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> TypeDBClient:
    """
    Интеграционный клиент для сценария:
    - INVESTIGATION_NAME = "test"
    - DEBUG_DB = "1" (разрешены debug-операции)
    - TYPEDB_DB_NAME = "test_db"
    """
    monkeypatch.setenv("INVESTIGATION_NAME", TEST_INVESTIGATION_NAME)
    monkeypatch.setenv("DEBUG_DB", "1")
    monkeypatch.setenv("TYPEDB_DB_NAME", TEST_DB_NAME)

    c = TypeDBClient(db_name=TEST_DB_NAME)

    # Создаём тестовую БД
    c.create_database(TEST_DB_NAME)

    yield c

    # После тестов удаляем тестовую БД
    try:
        c.drop_database(TEST_DB_NAME)
    except TypeDBClientError:
        pass

    c.close()

def test_debug_operations_not_allowed_without_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Проверяем, что investigation_create/delete запрещены, если DEBUG_DB не установлена,
    при этом используем тестовую БД test_db.
    """
    monkeypatch.setenv("INVESTIGATION_NAME", TEST_INVESTIGATION_NAME)
    monkeypatch.setenv("TYPEDB_DB_NAME", TEST_DB_NAME)
    monkeypatch.delenv("DEBUG_DB", raising=False)

    client = TypeDBClient(db_name=TEST_DB_NAME)
    try:
        # Создаём тестовую БД

        with pytest.raises(OperationIsNotAllowed):
            client.investigation_create()

        with pytest.raises(OperationIsNotAllowed):
            client.investigation_delete()
    finally:
        # Удаляем тестовую БД
        try:
            client.drop_database(TEST_DB_NAME)
        except TypeDBClientError:
            pass

        client.close()

def test_investigation_lifecycle(client: TypeDBClient) -> None:
    """
    Сценарий:
    1. Указание имени доски - test
    2. Создание нулевой версии доски
    3. Добавление новых нод
    4. Добавление новых рёбер
    5. Получение данных о нодах и рёбрах и сравнение с ожидаемым результатом
    6. Удаление всех созданных нод и рёбер
    7. Удаление версии доски
    8. Удаление созданного расследования.
    """

    # 1. Указание имени доски - test
    assert os.getenv("INVESTIGATION_NAME") == TEST_INVESTIGATION_NAME

    # 2. Создание нулевой версии доски
    # если шаблон investigation-create делает что-то ещё — тут можно считать это частью сценария
    client.investigation_create()
    client.graph_by_version_create(version=TEST_VERSION)
    client.set_active_version(version=TEST_VERSION)
    client.load_active_version()

    # 3. Добавление новых нод
    nodes = [
        {
            "node_id": 1,
            "name": "Alice",
            "pos_x": 10.0,
            "pos_y": 20.0,
            "picture_path": "/images/alice.png",
            "description": "First test node",
        },
        {
            "node_id": 2,
            "name": "Bob",
            "pos_x": 30.0,
            "pos_y": 40.0,
            "picture_path": "/images/bob.png",
            "description": "Second test node",
        },
    ]

    for n in nodes:
        client.node_create(
            node_id=n["node_id"],
            name=n["name"],
            pos_x=n["pos_x"],
            pos_y=n["pos_y"],
            picture_path=n["picture_path"],
            description=n["description"],
            version=TEST_VERSION,
        )

    # 4. Добавление новых рёбер
    edges = [
        {
            "edge_id": 3,
            "node1_id": 1,
            "node2_id": 2,
            "description": "Connection between Alice and Bob",
        }
    ]

    for e in edges:
        client.edge_create(
            edge_id=e["edge_id"],
            node1_id=e["node1_id"],
            node2_id=e["node2_id"],
            description=e["description"],
            version=TEST_VERSION,
        )

    # 5. Получение данных и сравнение с ожидаемым результатом
    data: dict[str, Any] = client.graph_by_version_get(version=TEST_VERSION)

    # Ожидаем структуру с ключами board_metadata, nodes, edges
    assert isinstance(data, dict)
    assert "nodes" in data
    assert "edges" in data

    # Проверяем, что наши ноды присутствуют
    # Предполагаем, что каждая нода содержит хотя бы поле "node_id"
    got_nodes = {n["node_id"]: n for n in data["nodes"]}
    for expected in nodes:
        node_id = expected["node_id"]
        assert node_id in got_nodes, f"Node {node_id} not found in graph"

        got = got_nodes[node_id]
        # по возможности сверяем основные поля, если они присутствуют
        for key in ("name", "pos_x", "pos_y", "description"):
            if key in got:
                assert got[key] == expected[key], f"Mismatch for node {node_id} field {key}"

    # Проверяем, что наши рёбра присутствуют
    got_edges = {e["edge_id"]: e for e in data["edges"]}
    for expected in edges:
        edge_id = expected["edge_id"]
        assert edge_id in got_edges, f"Edge {edge_id} not found in graph"

        got = got_edges[edge_id]
        for key in ("node1_id", "node2_id", "description"):
            if key in got:
                assert got[key] == expected[key], f"Mismatch for edge {edge_id} field {key}"

    # 6. Удаление всех созданных рёбер и нод
    for e in edges:
        client.edge_delete(edge_id=e["edge_id"], version=TEST_VERSION)

    for n in nodes:
        client.node_delete(node_id=n["node_id"], version=TEST_VERSION)

    # Проверяем, что версия теперь пустая (или хотя бы наши сущности исчезли)
    data_after_cleanup: dict[str, Any] = client.graph_by_version_get(version=TEST_VERSION)
    got_nodes_after = {n["node_id"] for n in data_after_cleanup.get("nodes", [])}
    got_edges_after = {e["edge_id"] for e in data_after_cleanup.get("edges", [])}

    for n in nodes:
        assert n["node_id"] not in got_nodes_after

    for e in edges:
        assert e["edge_id"] not in got_edges_after

    # 7. Удаление версии доски
    client.graph_by_version_delete(version=TEST_VERSION)

    # 8. Удаление созданного расследования
    client.investigation_delete()

def main() -> int:
    """Запуск всех тестов из этого файла как самостоятельного скрипта."""
    import pytest
    return pytest.main([__file__])

if __name__ == "__main__":
    raise SystemExit(main())