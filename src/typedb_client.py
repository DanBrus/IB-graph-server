# src/typedb_client.py

from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Any

from typeql_template_driver import (  # type: ignore[import]
    TypeQLTemplateDriver,
    TemplateDriverError,
    SpecificationError,
    OperationError,
    TemplateFileError,
)

from typedb.driver import (
    TypeDB,
    Credentials,
    DriverOptions,
    TransactionType,
)

from typedb.api.answer.query_answer import QueryAnswer  # type: ignore[import]


# ---------------------------------------------------------------------------
# Свои исключения
# ---------------------------------------------------------------------------

class TypeDBClientError(Exception):
    """Базовое исключение клиента доски расследований."""
    pass


class TemplateProcessingError(TypeDBClientError):
    """Ошибка подготовки TypeQL-запроса на основе шаблона."""
    pass


class QueryExecutionError(TypeDBClientError):
    """Ошибка выполнения запроса в TypeDB."""
    pass


class ActiveVersionError(TypeDBClientError):
    """Ошибка, связанная с получением или использованием active_version."""
    pass

class OperationIsNotAllowed(TypeDBClientError):
    pass

# ---------------------------------------------------------------------------
# Конфиг подключения к TypeDB
# ---------------------------------------------------------------------------

TYPEDB_ADDRESS = os.getenv("TYPEDB_ADDRESS", "localhost:1729")
TYPEDB_USERNAME = os.getenv("TYPEDB_USERNAME", "admin")
TYPEDB_PASSWORD = os.getenv("TYPEDB_PASSWORD", "password")
TYPEDB_TLS_ENABLED = os.getenv("TYPEDB_TLS_ENABLED", "false").lower() == "true"
TYPEDB_TLS_CA = os.getenv("TYPEDB_TLS_CA")  # путь к CA, если TLS включён

TYPEDB_DB_NAME = os.getenv("TYPEDB_DB_NAME", "investigation_board")
BOARD_SCHEMA_VERSION = os.getenv("BOARD_SCHEMA_VERSION", "v0.1")

# Глобальное имя расследования
INVESTIGATION_NAME = os.getenv("INVESTIGATION_NAME", "tsarstvie")

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TEMPLATES_ROOT = PROJECT_ROOT / "db"
QUERIES_DIR = TEMPLATES_ROOT / BOARD_SCHEMA_VERSION


class TypeDBClient:
    """
    Клиент TypeDB + драйвер шаблонов.

    При инициализации:
      - подключается к TypeDB;
      - создаёт TypeQLTemplateDriver;
      - выполняет операцию "get-active-version" и запоминает active_version.
    """

    def __init__(
        self,
        db_name: str = TYPEDB_DB_NAME,
        *,
        template_version: str = BOARD_SCHEMA_VERSION,
        typedb_address: str = TYPEDB_ADDRESS,
        username: str = TYPEDB_USERNAME,
        password: str = TYPEDB_PASSWORD,
        tls_enabled: bool = TYPEDB_TLS_ENABLED,
        tls_ca: str | None = TYPEDB_TLS_CA,
    ) -> None:
        self.db_name = db_name
        self.active_version: str | None = None

        # TypeDB Driver
        try:
            credentials = Credentials(username, password)
            options = DriverOptions(
                is_tls_enabled=tls_enabled,
                tls_root_ca_path=tls_ca,
            )
            self.driver: Any = TypeDB.driver(typedb_address, credentials, options)
        except Exception as e:
            raise TypeDBClientError(
                f"Failed to connect to TypeDB at '{typedb_address}' "
                f"for database '{db_name}': {e}"
            ) from e

        # Templates Driver
        try:
            self.template_driver = TypeQLTemplateDriver(
                db_root=str(TEMPLATES_ROOT),
                version=template_version,
                spec_filename="specification.json",
            )
        except (SpecificationError, TemplateFileError) as e:
            # Закроем драйвер, раз уж клиент не взлетел
            try:
                self.driver.close()
            except Exception:
                pass
            raise TemplateProcessingError(
                f"Failed to initialise template driver for version '{template_version}': {e}"
            ) from e
        except Exception as e:
            try:
                self.driver.close()
            except Exception:
                pass
            raise TypeDBClientError(
                f"Unexpected error while initialising template driver: {e}"
            ) from e

    def close(self) -> None:
        """Закрывает соединение с TypeDB."""
        try:
            self.driver.close()
        except Exception:
            pass

    # -------------------- вспомогательные методы --------------------

    def ensure_database_exists(self) -> None:
        try:
            if not self.driver.databases.contains(self.db_name):
                self.driver.databases.create(self.db_name)
        except Exception as e:
            raise TypeDBClientError(
                f"Failed to ensure database '{self.db_name}' exists: {e}"
            ) from e
        
    def create_database(self, db_name: str | None = None) -> None:
        """
        Явно создаёт базу данных с указанным именем (или self.db_name),
        если она ещё не существует.
        """
        self._ensure_debug_allowed()
        name = db_name or self.db_name
        try:
            if not self.driver.databases.contains(name):
                self.driver.databases.create(name)
        except Exception as e:
            raise TypeDBClientError(
                f"Failed to create database '{name}': {e}"
            ) from e

    def drop_database(self, db_name: str | None = None) -> None:
        """
        Удаляет базу данных с указанным именем (или self.db_name),
        если она существует.
        """
        self._ensure_debug_allowed()
        name = db_name or self.db_name
        try:
            if self.driver.databases.contains(name):
                self.driver.databases.delete(name)
        except Exception as e:
            raise TypeDBClientError(
                f"Failed to drop database '{name}': {e}"
            ) from e

    def _ensure_debug_allowed(self) -> None:
        if os.getenv("DEBUG_DB") is None:
            raise OperationIsNotAllowed(
                "This operation is allowed only when DEBUG_DB environment variable is set."
            )
        
    @contextmanager
    def transaction(self, tx_type: TransactionType) -> Iterator[Any]:
        try:
            tx = self.driver.transaction(self.db_name, tx_type)
        except Exception as e:
            raise QueryExecutionError(
                f"Failed to open {tx_type.name} transaction for database '{self.db_name}': {e}"
            ) from e

        try:
            yield tx
        finally:
            try:
                tx.close()
            except Exception:
                pass

    def list_databases(self) -> list[str]:
        try:
            return [db.name for db in self.driver.databases.all()]
        except Exception as e:
            raise TypeDBClientError(
                f"Failed to list databases: {e}"
            ) from e

    def _resolve_version(self, version: str | None) -> str:
        """
        Если версия не передана, используем активную.
        """
        if version is not None:
            return version
        if self.active_version is None:
            raise ActiveVersionError(
                "Active version is not set and no explicit version was provided."
            )
        return self.active_version

    # ---------------------------------------------------------------------------
    # Инициализационный запрос: get-active-version
    # ---------------------------------------------------------------------------

    def load_active_version(self) -> None:
        """
        Retrieves active version of the board.
        """
        op_name = "get-active-version"

        try:
            query = self.template_driver.get_operation(
                op_name,
                investigation_name=INVESTIGATION_NAME,
            )
        except TemplateDriverError as e:
            raise TemplateProcessingError(
                f"Failed to build query for operation '{op_name}' "
                f"(investigation_name='{INVESTIGATION_NAME}'): {e}"
            ) from e

        # Здесь уже docs — список dict, транзакция внутри _execute_read
        docs = self._execute_read(op_name, query)

        active_version_value: str | None = None
        try:
            for doc in docs:
                if "active_version" in doc:
                    active_version_value = doc["active_version"]
                    break
        except Exception as e:
            raise QueryExecutionError(
                f"Failed to inspect documents from '{op_name}': {e}"
            ) from e

        if active_version_value is None:
            raise ActiveVersionError(
                f"Operation '{op_name}' did not return 'active_version' "
                f"for investigation_name='{INVESTIGATION_NAME}'."
            )

        self.active_version = active_version_value

    def get_active_version(self) -> str | None:
        """Retrieves active version of the board."""
        return self.active_version

    # ---------------------------------------------------------------------------
    # Операции из specification.json
    # ---------------------------------------------------------------------------

    def _build_query(self, op_name: str, **params: Any) -> str:
        """Общий helper для сборки запроса с красивой ошибкой."""
        try:
            return self.template_driver.get_operation(op_name, **params)
        except TemplateDriverError as e:
            raise TemplateProcessingError(
                f"Failed to build query for operation '{op_name}' with params {params}: {e}"
            ) from e

    def _execute_write(self, op_name: str, query: str) -> None:
        """Общий helper для write-запросов."""
        try:
            with self.transaction(TransactionType.WRITE) as tx:
                tx.query(query).resolve()
                tx.commit()
        except Exception as e:
            raise QueryExecutionError(
                f"Failed to execute write operation '{op_name}' "
                f"on database '{self.db_name}': {e}"
            ) from e

    def _execute_read(self, op_name: str, query: str) -> list[dict[str, Any]]:
        """Общий helper для read-запросов. Возвращает список документов (dict)."""
        try:
            with self.transaction(TransactionType.READ) as tx:
                answer = tx.query(query).resolve()
                # ВАЖНО: материализуем итератор внутри транзакции
                docs_iter = answer.as_concept_documents()
                docs = list(docs_iter)
                return docs
        except Exception as e:
            raise QueryExecutionError(
                f"Failed to execute read operation '{op_name}' "
                f"on database '{self.db_name}': {e}"
            ) from e


    # ------------------------------ node-* ------------------------------

    def node_create(
        self,
        node_id: str,
        name: str,
        pos_x: float,
        pos_y: float,
        picture_path: str,
        description: str,
        *,
        version: str | None = None,
    ) -> None:
        """Creates a node within the specified investigation board version."""
        op_name = "node-create"
        resolved_version = self._resolve_version(version)

        query = self._build_query(
            op_name,
            investigation_name=INVESTIGATION_NAME,
            version=resolved_version,
            node_id=node_id,
            name=name,
            pos_x=pos_x,
            pos_y=pos_y,
            picture_path=picture_path,
            description=description,
        )
        self._execute_write(op_name, query)

    def node_update(
        self,
        node_id: str,
        name: str,
        pos_x: float,
        pos_y: float,
        picture_path: str,
        description: str,
        *,
        version: str | None = None,
    ) -> None:
        """Updates properties of a node within the specified board version."""
        op_name = "node-update"
        resolved_version = self._resolve_version(version)

        query = self._build_query(
            op_name,
            investigation_name=INVESTIGATION_NAME,
            version=resolved_version,
            node_id=node_id,
            name=name,
            pos_x=pos_x,
            pos_y=pos_y,
            picture_path=picture_path,
            description=description,
        )
        self._execute_write(op_name, query)

    def node_delete(
        self,
        node_id: str,
        *,
        version: str | None = None,
    ) -> None:
        """Deletes a node and its association with the specified board version."""
        op_name = "node-delete"
        resolved_version = self._resolve_version(version)

        query = self._build_query(
            op_name,
            investigation_name=INVESTIGATION_NAME,
            version=resolved_version,
            node_id=node_id,
        )
        self._execute_write(op_name, query)

    # ------------------------------ edge-* ------------------------------

    def edge_create(
        self,
        edge_id: str,
        node1_id: str,
        node2_id: str,
        description: str,
        *,
        version: str | None = None,
    ) -> None:
        """Creates an edge between two nodes within the specified board version."""
        op_name = "edge-create"
        resolved_version = self._resolve_version(version)

        query = self._build_query(
            op_name,
            investigation_name=INVESTIGATION_NAME,
            version=resolved_version,
            edge_id=edge_id,
            node1_id=node1_id,
            node2_id=node2_id,
            description=description,
        )
        self._execute_write(op_name, query)

    def edge_update(
        self,
        edge_id: str,
        description: str,
        *,
        version: str | None = None,
    ) -> None:
        """Updates the description of an edge."""
        op_name = "edge-update"
        resolved_version = self._resolve_version(version)

        query = self._build_query(
            op_name,
            investigation_name=INVESTIGATION_NAME,
            version=resolved_version,
            edge_id=edge_id,
            description=description,
        )
        self._execute_write(op_name, query)

    def edge_delete(
        self,
        edge_id: str,
        *,
        version: str | None = None,
    ) -> None:
        """Deletes an edge and its association with the specified board version."""
        op_name = "edge-delete"
        resolved_version = self._resolve_version(version)

        query = self._build_query(
            op_name,
            investigation_name=INVESTIGATION_NAME,
            version=resolved_version,
            edge_id=edge_id,
        )
        self._execute_write(op_name, query)

    # --------------------------- graph-by-version-* ---------------------------

    def graph_by_version_get(
        self,
        *,
        version: str | None = None,
    ) -> dict[str, Any]:
        """Retrieves all nodes and edges of the specified investigation board version."""
        op_name = "graph-by-version-get"
        resolved_version = self._resolve_version(version)

        query = self._build_query(
            op_name,
            investigation_name=INVESTIGATION_NAME,
            version=resolved_version,
        )

        docs = self._execute_read(op_name, query)

        if not docs:
            raise QueryExecutionError(
                f"Operation '{op_name}' returned no documents "
                f"(investigation_name='{INVESTIGATION_NAME}', version='{resolved_version}')."
            )

        if len(docs) > 1:
            raise QueryExecutionError(
                f"Operation '{op_name}' returned multiple documents ({len(docs)}), "
                f"but expected exactly one."
            )

        return docs[0]


    def graph_by_version_delete(
        self,
        *,
        version: str | None = None,
    ) -> None:
        """Deletes all nodes and edges of the specified board version, then deletes the board version itself."""
        op_name = "graph-by-version-delete"
        resolved_version = self._resolve_version(version)

        query = self._build_query(
            op_name,
            investigation_name=INVESTIGATION_NAME,
            version=resolved_version,
        )
        self._execute_write(op_name, query)

    def graph_by_version_create(
        self,
        *,
        version: str,
    ) -> None:
        """Creates a new board version."""
        op_name = "graph-by-version-create"

        query = self._build_query(
            op_name,
            investigation_name=INVESTIGATION_NAME,
            version=version,
        )
        self._execute_write(op_name, query)

    # --------------------------- set-active-version ---------------------------

    def set_active_version(self, version: str) -> None:
        """Sets default board version."""
        op_name = "set-active-version"

        query = self._build_query(
            op_name,
            investigation_name=INVESTIGATION_NAME,
            version=version,
        )
        self._execute_write(op_name, query)
        self.active_version = version
    
    def get_versions(self) -> dict[str, Any]:
        """Gest versions of current investigation."""
        op_name = "get-versions"

        query = self._build_query(
            op_name,
            investigation_name=INVESTIGATION_NAME,
        )

        docs = self._execute_read(op_name, query)

        if not docs:
            raise QueryExecutionError(
                f"Operation '{op_name}' returned no documents "
                f"(investigation_name='{INVESTIGATION_NAME}', version='{resolved_version}')."
            )

        if len(docs) > 1:
            raise QueryExecutionError(
                f"Operation '{op_name}' returned multiple documents ({len(docs)}), "
                f"but expected exactly one."
            )

        return docs[0]
    
    def investigation_create(self) -> None:
        """Creates investigation with name INVESTIGATION_NAME (debug only) and applies full schema."""
        # Разрешаем только в debug-режиме
        self._ensure_debug_allowed()

        # 1. Читаем файл со схемой
        schema_path = QUERIES_DIR / "schema"
        try:
            schema_text = schema_path.read_text(encoding="utf-8")
        except Exception as e:
            raise TypeDBClientError(
                f"Failed to read schema file '{schema_path}': {e}"
            ) from e

        # 2. Применяем schema к текущей БД
        try:
            with self.transaction(TransactionType.SCHEMA) as tx:
                tx.query(schema_text).resolve()
                tx.commit()
        except Exception as e:
            raise QueryExecutionError(
                f"Failed to apply schema from file '{schema_path}' "
                f"to database '{self.db_name}': {e}"
            ) from e

        # 3. Выполняем шаблонную операцию создания расследования
        op_name = "investigation-create"
        query = self._build_query(
            op_name,
            investigation_name=INVESTIGATION_NAME,
        )
        self._execute_write(op_name, query)

    def investigation_delete(self) -> None:
        """Deletes entire investigation with all versions (debug only)."""
        self._ensure_debug_allowed()

        op_name = "investigation-delete"
        query = self._build_query(
            op_name,
            investigation_name=INVESTIGATION_NAME,
        )
        self._execute_write(op_name, query)

    # --------------------------- update-graph ---------------------------

    def update_graph(
        self,
        *,
        version: str,
        nodes: list[Any],
        edges: list[Any],
    ) -> None:
        """
        Обновляет граф версии `version`, приводя его в соответствие с переданными nodes/edges.
        Ожидает, что элементы nodes/edges либо dict, либо объекты с нужными атрибутами
        (NodeDTO / EdgeDTO). Используется утка-типизация.
        """
        # 1. Текущий граф из БД
        db_graph = self.graph_by_version_get(version=version)
        db_nodes = db_graph["nodes"]
        db_edges = db_graph["edges"]

        # ---------- helpers для DTO/dict ----------

        def get_field(obj: Any, field: str) -> Any:
            if isinstance(obj, dict):
                return obj.get(field)
            return getattr(obj, field)

        # ---------- Подготовка словарей по id ----------

        # Ноды в БД
        db_nodes_by_id: dict[str, dict[str, Any]] = {
            str(n["node_id"]): n
            for n in db_nodes
        }
        # Ноды с фронта
        new_nodes_by_id: dict[str, Any] = {}
        for n in nodes:
            node_id = str(get_field(n, "node_id"))
            new_nodes_by_id[node_id] = n

        # Рёбра в БД
        db_edges_by_id: dict[str, dict[str, Any]] = {
            str(e["edge_id"]): e
            for e in db_edges
        }
        # Рёбра с фронта
        new_edges_by_id: dict[str, Any] = {}
        for e in edges:
            edge_id = str(get_field(e, "edge_id"))
            new_edges_by_id[edge_id] = e

        resolved_version = self._resolve_version(version)

        # ===================== НОДЫ =====================

        # 1) Удаляем ноды, которых больше нет во входных данных
        for node_id_str in list(db_nodes_by_id.keys()):
            if node_id_str not in new_nodes_by_id:
                self.node_delete(
                    node_id=node_id_str,
                    version=resolved_version,
                )

        # 2) Создаём ноды, которых нет в БД
        for node_id_str, node_obj in new_nodes_by_id.items():
            if node_id_str not in db_nodes_by_id:
                self.node_create(
                    node_id=node_id_str,
                    name=get_field(node_obj, "name"),
                    pos_x=float(get_field(node_obj, "pos_x")),
                    pos_y=float(get_field(node_obj, "pos_y")),
                    picture_path=get_field(node_obj, "picture_path"),
                    description=get_field(node_obj, "description"),
                    version=resolved_version,
                )

        # 3) Обновляем ноды, которые есть и там, и там, но отличаются
        for node_id_str, db_node in db_nodes_by_id.items():
            if node_id_str not in new_nodes_by_id:
                continue

            node_obj = new_nodes_by_id[node_id_str]

            new_name = get_field(node_obj, "name")
            new_pos_x = float(get_field(node_obj, "pos_x"))
            new_pos_y = float(get_field(node_obj, "pos_y"))
            new_picture = get_field(node_obj, "picture_path")
            new_desc = get_field(node_obj, "description")

            need_update = (
                db_node.get("name") != new_name
                or float(db_node.get("pos_x")) != new_pos_x
                or float(db_node.get("pos_y")) != new_pos_y
                or db_node.get("picture_path") != new_picture
                or db_node.get("description") != new_desc
            )

            if need_update:
                self.node_update(
                    node_id=node_id_str,
                    name=new_name,
                    pos_x=new_pos_x,
                    pos_y=new_pos_y,
                    picture_path=new_picture,
                    description=new_desc,
                    version=resolved_version,
                )

        # ===================== РЁБРА =====================

        # 1) Удаляем рёбра, которых больше нет
        for edge_id_str in list(db_edges_by_id.keys()):
            if edge_id_str not in new_edges_by_id:
                self.edge_delete(
                    edge_id=edge_id_str,
                    version=resolved_version,
                )

        # 2) Создаём рёбра, которых нет в БД
        for edge_id_str, edge_obj in new_edges_by_id.items():
            if edge_id_str not in db_edges_by_id:
                self.edge_create(
                    edge_id=edge_id_str,
                    node1_id=str(get_field(edge_obj, "node1")),
                    node2_id=str(get_field(edge_obj, "node2")),
                    description=get_field(edge_obj, "description"),
                    version=resolved_version,
                )

        # 3) Обновляем рёбра, которые есть и там, и там
        for edge_id_str, db_edge in db_edges_by_id.items():
            if edge_id_str not in new_edges_by_id:
                continue

            edge_obj = new_edges_by_id[edge_id_str]

            db_node1 = str(db_edge.get("node1"))
            db_node2 = str(db_edge.get("node2"))
            db_desc = db_edge.get("description")

            new_node1 = str(get_field(edge_obj, "node1"))
            new_node2 = str(get_field(edge_obj, "node2"))
            new_desc = get_field(edge_obj, "description")

            endpoints_changed = (db_node1 != new_node1) or (db_node2 != new_node2)
            desc_changed = db_desc != new_desc

            if endpoints_changed:
                # Меняем концы ребра — удаляем и создаём заново с тем же id
                self.edge_delete(
                    edge_id=edge_id_str,
                    version=resolved_version,
                )
                self.edge_create(
                    edge_id=edge_id_str,
                    node1_id=new_node1,
                    node2_id=new_node2,
                    description=new_desc,
                    version=resolved_version,
                )
            elif desc_changed:
                self.edge_update(
                    edge_id=edge_id_str,
                    description=new_desc,
                    version=resolved_version,
                )
