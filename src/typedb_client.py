# src/typedb_client.py

from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Any

from board_schema_config import BOARD_SCHEMA_VERSION
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
# путь к CA, если TLS включён; пустые строки превращаем в None, чтобы драйвер не пытался
# открывать несуществующий путь
TYPEDB_TLS_CA = os.getenv("TYPEDB_TLS_CA") or None

TYPEDB_DB_NAME = os.getenv("TYPEDB_DB_NAME", "tsarstvie-investigation")

# Глобальное имя расследования
INVESTIGATION_NAME = os.getenv("INVESTIGATION_NAME", "tsarstvie")

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TEMPLATES_ROOT = PROJECT_ROOT / "db"
QUERIES_DIR = TEMPLATES_ROOT / BOARD_SCHEMA_VERSION
DEFAULT_BOOTSTRAP_BOARD_VERSION = "0.0"
DEFAULT_BOOTSTRAP_BOARD_NAME = "Basic board"
DEFAULT_BOOTSTRAP_BOARD_DESCRIPTION = "This board has created to fill empty database"


def resolve_template_layout(
    *,
    template_version: str | None = None,
    templates_dir: str | Path | None = None,
) -> tuple[Path, Path, str]:
    """
    Возвращает (db_root, templates_dir, version) для TypeQLTemplateDriver.

    По умолчанию используется стандартная структура проекта `db/<version>`.
    Для миграций можно передать прямой путь до конкретной директории шаблонов.
    """
    if templates_dir is not None:
        resolved_templates_dir = Path(templates_dir).expanduser().resolve()
        return resolved_templates_dir.parent, resolved_templates_dir, resolved_templates_dir.name

    resolved_version = template_version or BOARD_SCHEMA_VERSION
    resolved_templates_dir = TEMPLATES_ROOT / resolved_version
    return TEMPLATES_ROOT, resolved_templates_dir, resolved_version


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
        templates_dir: str | Path | None = None,
        schema_path: str | Path | None = None,
        investigation_name: str = INVESTIGATION_NAME,
        typedb_address: str = TYPEDB_ADDRESS,
        username: str = TYPEDB_USERNAME,
        password: str = TYPEDB_PASSWORD,
        tls_enabled: bool = TYPEDB_TLS_ENABLED,
        tls_ca: str | None = TYPEDB_TLS_CA,
        auto_bootstrap: bool = True,
        bootstrap_default_board: bool = True,
        allow_debug_operations: bool = False,
    ) -> None:
        self.db_name = db_name
        self.active_version: str | None = None
        self.investigation_name = investigation_name
        self.auto_bootstrap = auto_bootstrap
        self.bootstrap_default_board = bootstrap_default_board
        self.allow_debug_operations = allow_debug_operations
        self.templates_root, self.templates_dir, self.template_version = resolve_template_layout(
            template_version=template_version,
            templates_dir=templates_dir,
        )
        self.schema_path = (
            Path(schema_path).expanduser().resolve()
            if schema_path is not None
            else self.templates_dir / "schema"
        )

        # TypeDB Driver
        try:
            credentials = Credentials(username, password)
            tls_ca_for_driver = tls_ca or None
            options = DriverOptions(
                is_tls_enabled=tls_enabled,
                tls_root_ca_path=tls_ca_for_driver,
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
                db_root=str(self.templates_root),
                version=self.template_version,
                spec_filename="specification.json",
            )
        except (SpecificationError, TemplateFileError) as e:
            # Закроем драйвер, раз уж клиент не взлетел
            try:
                self.driver.close()
            except Exception:
                pass
            raise TemplateProcessingError(
                f"Failed to initialise template driver from '{self.templates_dir}': {e}"
            ) from e
        except Exception as e:
            try:
                self.driver.close()
            except Exception:
                pass
            raise TypeDBClientError(
                f"Unexpected error while initialising template driver: {e}"
            ) from e

        # Если сервер доступен, но нужной БД нет — создаём её и расследование
        try:
            self._bootstrap_database_if_missing()
        except Exception:
            try:
                self.driver.close()
            except Exception:
                pass
            raise

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
        if self.allow_debug_operations or os.getenv("DEBUG_DB") is not None:
            return
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

    def _bootstrap_database_if_missing(self) -> None:
        """
        Автоматически создаёт базу данных и расследование, если БД ещё не существует.
        """
        if not self.auto_bootstrap:
            return

        try:
            exists = self.driver.databases.contains(self.db_name)
        except Exception as e:
            raise TypeDBClientError(
                f"Failed to check whether database '{self.db_name}' exists: {e}"
            ) from e

        if exists:
            return

        try:
            self.driver.databases.create(self.db_name)
        except Exception as e:
            raise TypeDBClientError(
                f"Failed to create database '{self.db_name}' automatically: {e}"
            ) from e

        try:
            self.initialize_database(create_default_board=self.bootstrap_default_board)
        except TypeDBClientError:
            raise
        except Exception as e:
            raise TypeDBClientError(
                f"Failed to initialise investigation '{self.investigation_name}' "
                f"in database '{self.db_name}': {e}"
            ) from e

    def initialize_database(self, *, create_default_board: bool = False) -> None:
        """
        Применяет схему и создаёт расследование в пустой БД.

        Для основного runtime можно дополнительно создать стартовую доску `0.0`,
        а для миграций оставить новую БД полностью пустой, кроме схемы и расследования.
        """
        self.apply_schema()
        self.create_investigation_record()

        if create_default_board:
            self.graph_by_version_create(
                version=DEFAULT_BOOTSTRAP_BOARD_VERSION,
                name=DEFAULT_BOOTSTRAP_BOARD_NAME,
                description=DEFAULT_BOOTSTRAP_BOARD_DESCRIPTION,
            )
            self.set_active_version(version=DEFAULT_BOOTSTRAP_BOARD_VERSION)
        

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
                investigation_name=self.investigation_name,
            )
        except TemplateDriverError as e:
            raise TemplateProcessingError(
                f"Failed to build query for operation '{op_name}' "
                f"(investigation_name='{self.investigation_name}'): {e}"
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
                f"for investigation_name='{self.investigation_name}'."
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

    def _operation_requires_param(self, op_name: str, param_name: str) -> bool:
        """Проверяет, требуется ли параметр конкретной версии шаблона."""
        try:
            return param_name in self.template_driver.required_params(op_name)
        except OperationError as e:
            raise TemplateProcessingError(
                f"Failed to inspect parameters for operation '{op_name}': {e}"
            ) from e

    def _typeql_bool(self, value: bool) -> str:
        """TypeQL ожидает булевы литералы в нижнем регистре."""
        return "true" if value else "false"

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
                return self._read_docs_in_transaction(tx, query)
        except Exception as e:
            raise QueryExecutionError(
                f"Failed to execute read operation '{op_name}' "
                f"on database '{self.db_name}': {e}"
            ) from e

    def _execute_read_queries(
        self,
        operations: list[tuple[str, str]],
    ) -> dict[str, list[dict[str, Any]]]:
        """Выполняет несколько read-запросов в одной транзакции."""
        try:
            with self.transaction(TransactionType.READ) as tx:
                return {
                    op_name: self._read_docs_in_transaction(tx, query)
                    for op_name, query in operations
                }
        except Exception as e:
            operation_names = ", ".join(op_name for op_name, _ in operations)
            raise QueryExecutionError(
                f"Failed to execute read operations ({operation_names}) "
                f"on database '{self.db_name}': {e}"
            ) from e

    def _read_docs_in_transaction(self, tx: Any, query: str) -> list[dict[str, Any]]:
        answer = tx.query(query).resolve()
        # ВАЖНО: материализуем итератор внутри транзакции
        return list(answer.as_concept_documents())


    # ------------------------------ node-* ------------------------------

    def node_create(
        self,
        node_id: str,
        name: str,
        pos_x: float,
        pos_y: float,
        picture_path: str,
        node_type: str,
        description: str,
        *,
        version: str | None = None,
    ) -> None:
        """Creates a node within the specified investigation board version."""
        op_name = "node-create"
        resolved_version = self._resolve_version(version)

        query = self._build_query(
            op_name,
            investigation_name=self.investigation_name,
            version=resolved_version,
            node_id=node_id,
            name=name,
            pos_x=pos_x,
            pos_y=pos_y,
            picture_path=picture_path,
            node_type=node_type,
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
        node_type: str,
        description: str,
        *,
        version: str | None = None,
    ) -> None:
        """Updates properties of a node within the specified board version."""
        op_name = "node-update"
        resolved_version = self._resolve_version(version)

        query = self._build_query(
            op_name,
            investigation_name=self.investigation_name,
            version=resolved_version,
            node_id=node_id,
            name=name,
            pos_x=pos_x,
            pos_y=pos_y,
            picture_path=picture_path,
            node_type=node_type,
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
            investigation_name=self.investigation_name,
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
            investigation_name=self.investigation_name,
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
            investigation_name=self.investigation_name,
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
            investigation_name=self.investigation_name,
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
        metadata_op_name = "board-version-metadata-get"
        nodes_op_name = "nodes-by-version-get"
        edges_op_name = "edges-by-version-get"
        resolved_version = self._resolve_version(version)

        docs_by_operation = self._execute_read_queries([
            (
                metadata_op_name,
                self._build_query(
                    metadata_op_name,
                    investigation_name=self.investigation_name,
                    version=resolved_version,
                ),
            ),
            (
                nodes_op_name,
                self._build_query(
                    nodes_op_name,
                    investigation_name=self.investigation_name,
                    version=resolved_version,
                ),
            ),
            (
                edges_op_name,
                self._build_query(
                    edges_op_name,
                    investigation_name=self.investigation_name,
                    version=resolved_version,
                ),
            ),
        ])

        metadata_docs = docs_by_operation[metadata_op_name]
        if not metadata_docs:
            raise QueryExecutionError(
                f"Operation '{metadata_op_name}' returned no documents "
                f"(investigation_name='{self.investigation_name}', version='{resolved_version}')."
            )

        if len(metadata_docs) > 1:
            raise QueryExecutionError(
                f"Operation '{metadata_op_name}' returned multiple documents ({len(metadata_docs)}), "
                f"but expected exactly one."
            )

        graph = dict(metadata_docs[0])
        graph["nodes"] = docs_by_operation[nodes_op_name]
        graph["edges"] = docs_by_operation[edges_op_name]
        return graph

    def nodes_by_version_get(
        self,
        *,
        version: str | None = None,
        node_id: Any = None,
        ids: list[Any] | None = None,
        name: str | None = None,
        has_picture: bool | None = None,
    ) -> list[dict[str, Any]]:
        """Retrieves nodes of the specified investigation board version."""
        op_name = "nodes-by-version-get"
        resolved_version = self._resolve_version(version)

        query = self._build_query(
            op_name,
            investigation_name=self.investigation_name,
            version=resolved_version,
        )

        nodes = self._execute_read(op_name, query)
        node_id_filter = str(node_id) if node_id is not None else None
        ids_filter = {str(item) for item in ids} if ids is not None else None

        def node_matches(node: dict[str, Any]) -> bool:
            current_node_id = str(node.get("node_id"))
            if node_id_filter is not None and current_node_id != node_id_filter:
                return False
            if ids_filter is not None and current_node_id not in ids_filter:
                return False
            if name is not None and node.get("name") != name:
                return False
            if has_picture is not None:
                picture_path = node.get("picture_path")
                node_has_picture = picture_path is not None and picture_path != ""
                if node_has_picture != has_picture:
                    return False
            return True

        return [node for node in nodes if node_matches(node)]

    def edges_by_version_get(
        self,
        *,
        version: str | None = None,
        edge_id: Any = None,
        ids: list[Any] | None = None,
        node_id: Any = None,
        from_id: Any = None,
        to_id: Any = None,
    ) -> list[dict[str, Any]]:
        """Retrieves edges of the specified investigation board version."""
        op_name = "edges-by-version-get"
        resolved_version = self._resolve_version(version)

        query = self._build_query(
            op_name,
            investigation_name=self.investigation_name,
            version=resolved_version,
        )

        edges = self._execute_read(op_name, query)
        edge_id_filter = str(edge_id) if edge_id is not None else None
        ids_filter = {str(item) for item in ids} if ids is not None else None
        node_id_filter = str(node_id) if node_id is not None else None
        from_id_filter = str(from_id) if from_id is not None else None
        to_id_filter = str(to_id) if to_id is not None else None

        def edge_matches(edge: dict[str, Any]) -> bool:
            current_edge_id = str(edge.get("edge_id"))
            node1 = str(edge.get("node1"))
            node2 = str(edge.get("node2"))

            if edge_id_filter is not None and current_edge_id != edge_id_filter:
                return False
            if ids_filter is not None and current_edge_id not in ids_filter:
                return False
            if node_id_filter is not None and node_id_filter not in {node1, node2}:
                return False
            if from_id_filter is not None and node1 != from_id_filter:
                return False
            if to_id_filter is not None and node2 != to_id_filter:
                return False
            return True

        return [edge for edge in edges if edge_matches(edge)]


    def graph_by_version_delete(
        self,
        *,
        version: str | None = None,
    ) -> None:
        """Deletes all nodes and edges of the specified board version, then deletes the board version itself."""
        op_name = "graph-by-version-delete"
        if version == None:
            raise QueryExecutionError(
                f"Operation '{op_name}' requieres 'version' to be set."
            )

        query = self._build_query(
            op_name,
            investigation_name=self.investigation_name,
            version=version,
        )
        self._execute_write(op_name, query)

    def graph_by_version_create(
        self,
        *,
        version: str,
        name: str,
        description: str,
        is_published: bool = False,
    ) -> None:
        """Creates a new board version."""
        op_name = "graph-by-version-create"

        query_params: dict[str, Any] = {
            "investigation_name": self.investigation_name,
            "version": version,
            "name": name,
            "description": description,
        }
        # v01_to_v02_migration: v0.2 requires is_published, while v0.1 must keep working unchanged.
        if self._operation_requires_param(op_name, "is_published"):
            query_params["is_published"] = self._typeql_bool(is_published)

        query = self._build_query(op_name, **query_params)
        self._execute_write(op_name, query)

    # --------------------------- set-active-version ---------------------------

    def set_active_version(self, version: str) -> None:
        """Sets default board version."""
        op_name = "set-active-version"

        query = self._build_query(
            op_name,
            investigation_name=self.investigation_name,
            version=version,
        )
        self._execute_write(op_name, query)
        self.active_version = version
    
    def get_versions(self) -> dict[str, Any]:
        """Gest versions of current investigation."""
        op_name = "get-versions"

        query = self._build_query(
            op_name,
            investigation_name=self.investigation_name,
        )

        docs = self._execute_read(op_name, query)

        if not docs:
            raise QueryExecutionError(
                f"Operation '{op_name}' returned no documents "
                f"(investigation_name='{self.investigation_name}')."
            )

        if len(docs) > 1:
            raise QueryExecutionError(
                f"Operation '{op_name}' returned multiple documents ({len(docs)}), "
                f"but expected exactly one."
            )

        return docs[0]
    
    def investigation_create(self) -> None:
        """Creates investigation with name self.investigation_name (debug only) and applies full schema."""
        # Разрешаем только в debug-режиме
        self._ensure_debug_allowed()

        self.initialize_database(create_default_board=False)

    def apply_schema(self, schema_path: str | Path | None = None) -> None:
        """Применяет схему к текущей базе данных."""
        resolved_schema_path = (
            Path(schema_path).expanduser().resolve()
            if schema_path is not None
            else self.schema_path
        )
        try:
            schema_text = resolved_schema_path.read_text(encoding="utf-8")
        except Exception as e:
            raise TypeDBClientError(
                f"Failed to read schema file '{resolved_schema_path}': {e}"
            ) from e

        try:
            with self.transaction(TransactionType.SCHEMA) as tx:
                tx.query(schema_text).resolve()
                tx.commit()
        except Exception as e:
            raise QueryExecutionError(
                f"Failed to apply schema from file '{resolved_schema_path}' "
                f"to database '{self.db_name}': {e}"
            ) from e

    def create_investigation_record(self) -> None:
        """Создаёт запись расследования в уже инициализированной схеме."""
        op_name = "investigation-create"
        query = self._build_query(
            op_name,
            investigation_name=self.investigation_name,
        )
        self._execute_write(op_name, query)

    def investigation_delete(self) -> None:
        """Deletes entire investigation with all versions (debug only)."""
        self._ensure_debug_allowed()

        op_name = "investigation-delete"
        query = self._build_query(
            op_name,
            investigation_name=self.investigation_name,
        )
        self._execute_write(op_name, query)

    # --------------------------- update-graph ---------------------------

    def update_graph(
        self,
        *,
        version: str,
        nodes: list[Any],
        edges: list[Any],
        is_published: bool | None = None,
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
        write_queries: list[tuple[str, str]] = []

        def enqueue_write(op_name: str, **params: Any) -> None:
            query = self._build_query(
                op_name,
                investigation_name=self.investigation_name,
                version=resolved_version,
                **params,
            )
            write_queries.append((op_name, query))

        def normalize_optional_bool(value: Any) -> bool | None:
            if value is None or isinstance(value, bool):
                return value
            if isinstance(value, str):
                lowered = value.strip().lower()
                if lowered == "true":
                    return True
                if lowered == "false":
                    return False
            return bool(value)

        # v01_to_v02_migration: v0.2 can update board publication metadata during a regular board save,
        # while v0.1 must ignore the optional field and keep working unchanged.
        if is_published is not None and self.template_driver.has_operation("board-version-update"):
            current_is_published = normalize_optional_bool(db_graph.get("is_published"))
            if current_is_published != is_published:
                enqueue_write(
                    "board-version-update",
                    is_published=self._typeql_bool(is_published),
                )

        # ===================== РЁБРА: подготовка =====================

        edges_to_delete: list[str] = []
        edges_to_recreate: list[str] = []
        edges_to_update: list[str] = []

        for edge_id_str, db_edge in db_edges_by_id.items():
            if edge_id_str not in new_edges_by_id:
                edges_to_delete.append(edge_id_str)
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
                edges_to_delete.append(edge_id_str)
                edges_to_recreate.append(edge_id_str)
            elif desc_changed:
                edges_to_update.append(edge_id_str)

        edges_to_recreate_set = set(edges_to_recreate)

        # Сначала удаляем рёбра, чтобы ноды можно было удалить или перепривязать.
        for edge_id_str in edges_to_delete:
            enqueue_write("edge-delete", edge_id=edge_id_str)

        # ===================== НОДЫ =====================

        # 1) Удаляем ноды, которых больше нет во входных данных
        for node_id_str in list(db_nodes_by_id.keys()):
            if node_id_str not in new_nodes_by_id:
                enqueue_write("node-delete", node_id=node_id_str)

        # 2) Создаём ноды, которых нет в БД
        for node_id_str, node_obj in new_nodes_by_id.items():
            if node_id_str not in db_nodes_by_id:
                enqueue_write(
                    "node-create",
                    node_id=node_id_str,
                    name=get_field(node_obj, "name"),
                    pos_x=float(get_field(node_obj, "pos_x")),
                    pos_y=float(get_field(node_obj, "pos_y")),
                    picture_path=get_field(node_obj, "picture_path"),
                    node_type=get_field(node_obj, "node_type"),
                    description=get_field(node_obj, "description"),
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
            new_type = get_field(node_obj, "node_type")
            new_desc = get_field(node_obj, "description")

            need_update = (
                db_node.get("name") != new_name
                or float(db_node.get("pos_x")) != new_pos_x
                or float(db_node.get("pos_y")) != new_pos_y
                or db_node.get("picture_path") != new_picture
                or db_node.get("node_type") != new_type
                or db_node.get("description") != new_desc
            )

            if need_update:
                enqueue_write(
                    "node-update",
                    node_id=node_id_str,
                    name=new_name,
                    pos_x=new_pos_x,
                    pos_y=new_pos_y,
                    picture_path=new_picture,
                    node_type=new_type,
                    description=new_desc,
                )

        # ===================== РЁБРА =====================

        # 1) Создаём новые рёбра и рёбра с изменившимися концами
        for edge_id_str, edge_obj in new_edges_by_id.items():
            if edge_id_str not in db_edges_by_id or edge_id_str in edges_to_recreate_set:
                enqueue_write(
                    "edge-create",
                    edge_id=edge_id_str,
                    node1_id=str(get_field(edge_obj, "node1")),
                    node2_id=str(get_field(edge_obj, "node2")),
                    description=get_field(edge_obj, "description"),
                )

        # 2) Обновляем рёбра, у которых поменялось только описание
        for edge_id_str in edges_to_update:
            edge_obj = new_edges_by_id[edge_id_str]
            enqueue_write(
                "edge-update",
                edge_id=edge_id_str,
                description=get_field(edge_obj, "description"),
            )

        if not write_queries:
            return

        operation_names = ", ".join(op_name for op_name, _ in write_queries)
        try:
            with self.transaction(TransactionType.WRITE) as tx:
                for _, query in write_queries:
                    tx.query(query).resolve()
                tx.commit()
        except Exception as e:
            raise QueryExecutionError(
                f"Failed to execute graph update operations ({operation_names}) "
                f"on database '{self.db_name}': {e}"
            ) from e
