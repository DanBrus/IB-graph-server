from functools import wraps
from typing import List, Optional

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
        db_data = self.client.graph_by_version_get(version=version)
        print(f"[GraphService] board version nodes requested: {db_data['version']}")
        return db_data["nodes"]

    @log_api_method_execution
    def get_edges(self, version, edge_id, ids, node_id, from_id, to_id):
        db_data = self.client.graph_by_version_get(version=version)
        print(f"[GraphService] board version edges requested: {db_data['version']}")
        return db_data["edges"]

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
    def create_version(self, version: str, name: str, description: str) -> dict:
        """
        Создать пустую версию доски.
        """
        self.client.graph_by_version_create(version=version, name=name, description=description)
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
    def update_graph(self, version: str, nodes, edges):
        self.client.update_graph(
            version=version,
            nodes=nodes,
            edges=edges,
        )
        print(f"[GraphService] board version got update: {version}")
        return {"status": "ok"}
