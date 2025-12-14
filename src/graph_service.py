from typing import List, Optional

from typedb_client import TypeDBClient

from graph_models import (
    BoardDTO,
    NodeDTO,
    EdgeDTO,
    VersionDTO,
)


class GraphService:
    """
    Сервис работы с графом.
    """

    def __init__(self):
        self.client = TypeDBClient()
        self.client.load_active_version()

    # --------- ЧТЕНИЕ --------- #

    def get_board(self, version: Optional[str]) -> BoardDTO:
        db_data = self.client.graph_by_version_get(version=version)
        print(f"[GraphService] board version requested: {db_data['version']}")
        return db_data

    def get_nodes(self, version, node_id, ids, name, has_picture):
        db_data = self.client.graph_by_version_get(version=version)
        print(f"[GraphService] board version nodes requested: {db_data['version']}")
        return db_data["nodes"]

    def get_edges(self, version, edge_id, ids, node_id, from_id, to_id):
        db_data = self.client.graph_by_version_get(version=version)
        print(f"[GraphService] board version edges requested: {db_data['version']}")
        return db_data["edges"]

    def get_versions(self) -> List[VersionDTO]:
        print(f"[GraphService] board versions requested")
        return self.client.get_versions()["versions"]

    def get_active_version(self) -> str:
        print(f"[GraphService] board active version requested")
        return self.client.get_active_version()

    # --------- ЗАПИСЬ --------- #

    def create_version(self, version: str) -> dict:
        """
        Создать пустую версию доски.
        """
        self.client.graph_by_version_create(version=version)
        print(f"[GraphService] create version created: {version}")
        return {"status": "ok"}

    def delete_version(self, version: str) -> dict:
        """
        Создать пустую версию доски.
        """
        self.client.graph_by_version_delete(version=version)
        print(f"[GraphService] create version deleted: {version}")
        return {"status": "ok"}

    def set_active_version(self, version: str) -> dict:
        """
        Установить активную версию.
        """
        self.client.set_active_version(version)
        print(f"[GraphService] board active version set: {version}")
        return {"status": "ok"}

    def update_graph(self, version: str, nodes, edges):
        self.client.update_graph(
            version=version,
            nodes=nodes,
            edges=edges,
        )
        print(f"[GraphService] board version got update: {version}")
        return {"status": "ok"}
