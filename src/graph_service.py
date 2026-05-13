from functools import wraps
from typing import List, Optional

from board_schema_config import CURRENT_BOARD_SCHEMA
from typedb_client import TypeDBClient

from graph_models import (
    BoardDTO,
    NodeDTO,
    EdgeDTO,
    VersionDTO,
)

LOG_API_METHOD_EXECUTION = True

def log_api_method_execution(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if LOG_API_METHOD_EXECUTION:
            print(f"[GraphService] {func.__name__} started")

        try:
            return func(*args, **kwargs)
        finally:
            if LOG_API_METHOD_EXECUTION:
                print(f"[GraphService] {func.__name__} finished")

    return wrapper


class GraphService:
    """
    Сервис работы с графом.
    """

    def __init__(self):
        self.client = TypeDBClient()
        self.client.load_active_version()

    # --------- ЧТЕНИЕ --------- #

    @log_api_method_execution
    def get_board(self, version: Optional[str]) -> BoardDTO:
        db_data = self.client.graph_by_version_get(version=version)
        print(f"[GraphService] board version requested: {db_data['version']}")
        return db_data

    @log_api_method_execution
    def get_nodes(self, version, node_id, ids, name, has_picture):
        nodes = self.client.nodes_by_version_get(
            version=version,
            node_id=node_id,
            ids=ids,
            name=name,
            has_picture=has_picture,
        )
        print(f"[GraphService] board version nodes requested: {version}")
        return nodes

    @log_api_method_execution
    def get_edges(self, version, edge_id, ids, node_id, from_id, to_id):
        # Исходим из корректности данных в БД: если edge присутствует,
        # то присутствуют и ноды, к которым он относится.
        edges = self.client.edges_by_version_get(
            version=version,
            edge_id=edge_id,
            ids=ids,
            node_id=node_id,
            from_id=from_id,
            to_id=to_id,
        )
        print(f"[GraphService] board version edges requested: {version}")
        return edges

    @log_api_method_execution
    def get_versions(self) -> List[VersionDTO]:
        print(f"[GraphService] board versions requested")
        return self.client.get_versions()["versions"]

    @log_api_method_execution
    def get_active_version(self) -> str:
        print(f"[GraphService] board active version requested")
        return self.client.get_active_version()

    # --------- ЗАПИСЬ --------- #

    @log_api_method_execution
    def create_version(
        self,
        version: str,
        name: str,
        description: str,
        is_published: Optional[bool] = None,
    ) -> dict:
        """
        Создать пустую версию доски.
        """
        create_kwargs = {
            "version": version,
            "name": name,
            "description": description,
        }
        # v01_to_v02_migration: one shared schema config controls whether API should pass publication state.
        if CURRENT_BOARD_SCHEMA.supports_is_published:
            create_kwargs["is_published"] = (
                bool(is_published) if is_published is not None else False
            )

        self.client.graph_by_version_create(**create_kwargs)
        print(f"[GraphService] create version created: {version}")
        return {"status": "ok"}

    @log_api_method_execution
    def delete_version(self, version: str) -> dict:
        """
        Удалить указанную версию.
        """
        self.client.graph_by_version_delete(version=version)
        print(f"[GraphService] version deleted: {version}")
        return {"status": "ok"}

    @log_api_method_execution
    def set_active_version(self, version: str) -> dict:
        """
        Установить активную версию.
        """
        self.client.set_active_version(version)
        print(f"[GraphService] board active version set: {version}")
        return {"status": "ok"}

    @log_api_method_execution
    def update_graph(
        self,
        version: str,
        nodes,
        edges,
        is_published: Optional[bool] = None,
    ):
        update_kwargs = {
            "version": version,
            "nodes": nodes,
            "edges": edges,
        }
        # v01_to_v02_migration: update_graph can forward publication state only when the active schema supports it.
        if CURRENT_BOARD_SCHEMA.supports_is_published:
            update_kwargs["is_published"] = is_published
            print(f"[GraphService] update_graph: {CURRENT_BOARD_SCHEMA.supports_is_published}")

        self.client.update_graph(**update_kwargs)
        print(f"[GraphService] board version got update: {version}")
        return {"status": "ok"}
