from __future__ import annotations

import time
from decimal import Decimal, InvalidOperation
from functools import wraps
from typing import Any, List, Optional

from typedb_client import TypeDBClient

from graph_models import BoardDTO, CanonicalEntityDTO, EdgeDTO, NodeDTO, VersionDTO

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


class BoardVersionResolutionError(ValueError):
    """Raised when API version/b_id cannot be resolved to an existing board."""


class GraphService:
    """
    Сервис graph API поверх схемы TypeDB v0.3.
    """

    def __init__(self):
        # Графовое API уже работает поверх новой схемы.
        # Стартовую доску автоматически не создаём: активная доска
        # теперь вычисляется как board с максимальным b_id.
        self.client = TypeDBClient(
            template_version="v0.3",
            bootstrap_default_board=False,
        )

    # --------- Вспомогательные методы --------- #

    def _build_query(self, op_name: str, **params: Any) -> str:
        return self.client._build_query(op_name, **params)

    def _read_docs(self, op_name: str, **params: Any) -> list[dict[str, Any]]:
        query = self._build_query(op_name, **params)
        return self.client._execute_read(op_name, query)

    def _first_doc(
        self,
        docs: list[dict[str, Any]],
        *,
        label: str,
        allow_empty: bool = False,
    ) -> dict[str, Any]:
        if not docs:
            if allow_empty:
                return {}
            raise BoardVersionResolutionError(f"Operation '{label}' returned no documents.")
        if len(docs) > 1:
            raise BoardVersionResolutionError(
                f"Operation '{label}' returned multiple documents ({len(docs)})."
            )
        return dict(docs[0])

    def _as_text(self, value: Any, default: str = "") -> str:
        if value is None:
            return default
        return str(value)

    def _as_optional_text(self, value: Any) -> str | None:
        text = self._as_text(value).strip()
        return text if text else None

    def _as_bool(self, value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return False
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered == "true":
                return True
            if lowered == "false":
                return False
        return bool(value)

    def _as_int(self, value: Any, *, default: int = 0) -> int:
        if value is None:
            return default
        return int(value)

    def _as_float(self, value: Any, *, default: float = 0.0) -> float:
        if value is None:
            return default
        return float(value)

    def _as_decimal(self, value: Any, *, default: Decimal = Decimal("0")) -> Decimal:
        if value is None:
            return default
        if isinstance(value, Decimal):
            return value
        if isinstance(value, float):
            return Decimal(str(value))
        return Decimal(str(value))

    def _unwrap_singleton_value(self, value: Any) -> Any:
        current = value
        while isinstance(current, dict) and len(current) == 1:
            current = next(iter(current.values()))
        return current

    def _extract_scalar_list(self, values: Any) -> list[Any]:
        if not isinstance(values, list):
            return []

        result: list[Any] = []
        for value in values:
            unwrapped = self._unwrap_singleton_value(value)
            if isinstance(unwrapped, dict) or unwrapped is None:
                continue
            result.append(unwrapped)
        return result

    def _stringify_board_id(self, board_id: Any) -> str:
        return format(self._as_decimal(board_id), "f")

    def _serialize_board_id(self, board_id: Any) -> float:
        return float(self._as_decimal(board_id))

    def _parse_requested_board_id(self, version: Any) -> Decimal:
        raw_version = self._as_text(version).strip()
        if not raw_version:
            raise BoardVersionResolutionError("version must not be empty.")

        normalized = raw_version[1:] if raw_version.lower().startswith("s") else raw_version
        try:
            return Decimal(normalized)
        except InvalidOperation as exc:
            raise BoardVersionResolutionError(
                f"version '{raw_version}' cannot be converted to board id."
            ) from exc

    def _list_boards(self) -> list[dict[str, Any]]:
        docs = self._read_docs(
            "return-all-boards-in-investigation",
            investigation_name=self.client.investigation_name,
        )
        payload = self._first_doc(
            docs,
            label="return-all-boards-in-investigation",
            allow_empty=True,
        )

        boards: list[dict[str, Any]] = []
        for raw_board in payload.get("boards", []):
            if not isinstance(raw_board, dict):
                continue
            boards.append(
                {
                    "b_id": self._as_decimal(raw_board.get("b_id")),
                    "name": self._as_text(raw_board.get("name")),
                    "description": self._as_text(raw_board.get("description")),
                    "is_published": self._as_bool(raw_board.get("is_published")),
                }
            )

        boards.sort(key=lambda item: item["b_id"])
        return boards

    def _normalize_canonical_entities_payload(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        entities: list[dict[str, Any]] = []

        for raw_entity in payload.get("canonical_entities", []):
            if not isinstance(raw_entity, dict):
                continue

            entity_id = self._as_text(raw_entity.get("en_id"))
            if not entity_id:
                continue

            picture_paths = [
                self._as_text(value)
                for value in self._extract_scalar_list(raw_entity.get("picture_paths"))
                if self._as_text(value)
            ]
            entities.append(
                {
                    "en_id": entity_id,
                    "name": self._as_text(raw_entity.get("name")),
                    "entity_type": self._as_text(raw_entity.get("entity_type")),
                    "picture_paths": picture_paths,
                }
            )

        entities.sort(key=lambda item: (item["name"], item["en_id"]))
        return entities

    def _load_canonical_entities(self) -> list[dict[str, Any]]:
        docs = self._read_docs(
            "return-all-canonical-entities-in-investigation",
            investigation_name=self.client.investigation_name,
        )
        payload = self._first_doc(
            docs,
            label="return-all-canonical-entities-in-investigation",
            allow_empty=True,
        )
        return self._normalize_canonical_entities_payload(payload)

    def _select_board(
        self,
        boards: list[dict[str, Any]],
        version: Optional[str],
    ) -> dict[str, Any]:
        if not boards:
            raise BoardVersionResolutionError("No boards found in investigation.")

        if version is None:
            return boards[-1]

        requested_b_id = self._parse_requested_board_id(version)
        for board in boards:
            if board["b_id"] == requested_b_id:
                return board

        raise BoardVersionResolutionError(
            f"Board with id {self._stringify_board_id(requested_b_id)} was not found."
        )

    def _normalize_chunk_payload(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        chunks: list[dict[str, Any]] = []
        for raw_chunk in payload.get("text_chunks", []):
            if not isinstance(raw_chunk, dict):
                continue
            description = self._as_text(raw_chunk.get("description")).strip()
            if not description:
                continue
            chunks.append(
                {
                    "c_id": self._as_int(raw_chunk.get("c_id")),
                    "chunk_priority": self._as_int(raw_chunk.get("chunk_priority")),
                    "description": description,
                    "timecode": self._as_text(raw_chunk.get("timecode")),
                }
            )

        chunks.sort(
            key=lambda item: (
                item["chunk_priority"],
                item["c_id"],
                item["description"],
            )
        )
        return chunks

    def _serialize_chunks(self, chunks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "c_id": self._as_int(chunk.get("c_id")),
                "description": self._as_text(chunk.get("description")),
                "chunk_priority": self._as_int(chunk.get("chunk_priority")),
                "timecode": self._as_text(chunk.get("timecode")),
            }
            for chunk in chunks
        ]

    def _load_board_snapshot(self, version: Optional[str]) -> dict[str, Any]:
        started_at = time.perf_counter()

        boards = self._list_boards()
        board = self._select_board(boards, version)
        board_id = board["b_id"]

        top_level_queries = [
            (
                "nodes",
                self._build_query("return-all-nodes-in-board", b_id=board_id),
            ),
            (
                "edges",
                self._build_query("return-all-edges-in-board", b_id=board_id),
            ),
            (
                "entities",
                self._build_query(
                    "return-all-canonical-entities-in-investigation",
                    investigation_name=self.client.investigation_name,
                ),
            ),
        ]
        top_level_docs = self.client._execute_read_queries(top_level_queries)

        nodes_payload = self._first_doc(
            top_level_docs["nodes"],
            label="return-all-nodes-in-board",
            allow_empty=True,
        )
        edges_payload = self._first_doc(
            top_level_docs["edges"],
            label="return-all-edges-in-board",
            allow_empty=True,
        )
        entities_payload = self._first_doc(
            top_level_docs["entities"],
            label="return-all-canonical-entities-in-investigation",
            allow_empty=True,
        )

        raw_nodes = [
            item for item in nodes_payload.get("nodes", [])
            if isinstance(item, dict)
        ]
        raw_edges = [
            item for item in edges_payload.get("edges", [])
            if isinstance(item, dict)
        ]

        chunk_queries: list[tuple[str, str]] = []
        for raw_edge in raw_edges:
            edge_id = self._as_int(raw_edge.get("ed_id"))
            chunk_queries.append(
                (
                    f"edge-chunks:{edge_id}",
                    self._build_query("return-all-text-chunks-in-edge", ed_id=edge_id),
                )
            )
        for raw_node in raw_nodes:
            node_id = self._as_int(raw_node.get("n_id"))
            chunk_queries.append(
                (
                    f"node-chunks:{node_id}",
                    self._build_query("return-all-text-chunks-in-node", n_id=node_id),
                )
            )

        chunk_docs = self.client._execute_read_queries(chunk_queries) if chunk_queries else {}

        entities = self._normalize_canonical_entities_payload(entities_payload)
        entities_by_id = {
            entity["en_id"]: entity
            for entity in entities
        }

        edges: list[dict[str, Any]] = []

        for raw_edge in sorted(raw_edges, key=lambda item: self._as_int(item.get("ed_id"))):
            edge_id = self._as_int(raw_edge.get("ed_id"))
            node1 = self._as_int(raw_edge.get("endpoint_1_n_id"))
            node2 = self._as_int(raw_edge.get("endpoint_2_n_id"))

            edge_chunk_payload = self._first_doc(
                chunk_docs.get(f"edge-chunks:{edge_id}", []),
                label=f"return-all-text-chunks-in-edge:{edge_id}",
                allow_empty=True,
            )
            edge_chunks = self._normalize_chunk_payload(edge_chunk_payload)

            edges.append(
                {
                    "edge_id": edge_id,
                    "node1": node1,
                    "node2": node2,
                    "description": self._serialize_chunks(edge_chunks),
                }
            )

        nodes: list[dict[str, Any]] = []
        for raw_node in sorted(raw_nodes, key=lambda item: self._as_int(item.get("n_id"))):
            node_id = self._as_int(raw_node.get("n_id"))
            entity_id = self._as_text(
                self._unwrap_singleton_value(raw_node.get("entity_en_id"))
            )
            entity = entities_by_id.get(entity_id)
            if entity is None:
                raise BoardVersionResolutionError(
                    f"canonical-entity '{entity_id}' for node {node_id} was not found."
                )

            node_chunk_payload = self._first_doc(
                chunk_docs.get(f"node-chunks:{node_id}", []),
                label=f"return-all-text-chunks-in-node:{node_id}",
                allow_empty=True,
            )
            node_chunks = self._normalize_chunk_payload(node_chunk_payload)

            picture_paths = entity["picture_paths"]
            nodes.append(
                {
                    "node_id": node_id,
                    "name": entity["name"],
                    "pos_x": self._as_float(raw_node.get("pos_x")),
                    "pos_y": self._as_float(raw_node.get("pos_y")),
                    "node_type": entity["entity_type"],
                    "picture_path": picture_paths[-1] if picture_paths else None,
                    "description": self._serialize_chunks(node_chunks),
                }
            )

        duration_ms = (time.perf_counter() - started_at) * 1000
        print(
            "[GraphService] board snapshot assembled "
            f"(b_id={self._stringify_board_id(board_id)}, nodes={len(nodes)}, "
            f"edges={len(edges)}, duration_ms={duration_ms:.2f})"
        )

        return {
            "nodes": nodes,
            "edges": edges,
            "version": self._serialize_board_id(board_id),
            "description": board["description"],
            "board_name": board["name"],
            "is_published": board["is_published"],
        }

    # --------- ЧТЕНИЕ --------- #

    @log_api_method_execution
    def get_board(self, version: Optional[str]) -> BoardDTO:
        board = self._load_board_snapshot(version)
        print(f"[GraphService] board requested: {board['version']}")
        return board

    @log_api_method_execution
    def get_nodes(
        self,
        version: Optional[str],
        node_id: Any,
        ids: list[Any] | None,
        name: str | None,
        has_picture: bool | None,
    ) -> List[NodeDTO]:
        board = self._load_board_snapshot(version)
        nodes = list(board["nodes"])

        node_id_filter = str(node_id) if node_id is not None else None
        ids_filter = {str(item) for item in ids} if ids is not None else None

        def matches(node: dict[str, Any]) -> bool:
            current_node_id = str(node.get("node_id"))
            if node_id_filter is not None and current_node_id != node_id_filter:
                return False
            if ids_filter is not None and current_node_id not in ids_filter:
                return False
            if name is not None and node.get("name") != name:
                return False
            if has_picture is not None:
                picture_path = self._as_optional_text(node.get("picture_path"))
                node_has_picture = picture_path is not None
                if node_has_picture != has_picture:
                    return False
            return True

        filtered = [node for node in nodes if matches(node)]
        print(f"[GraphService] board nodes requested: {board['version']}")
        return filtered

    @log_api_method_execution
    def get_edges(
        self,
        version: Optional[str],
        edge_id: Any,
        ids: list[Any] | None,
        node_id: Any,
        from_id: Any,
        to_id: Any,
    ) -> List[EdgeDTO]:
        board = self._load_board_snapshot(version)
        edges = list(board["edges"])

        edge_id_filter = str(edge_id) if edge_id is not None else None
        ids_filter = {str(item) for item in ids} if ids is not None else None
        node_id_filter = str(node_id) if node_id is not None else None
        from_id_filter = str(from_id) if from_id is not None else None
        to_id_filter = str(to_id) if to_id is not None else None

        def matches(edge: dict[str, Any]) -> bool:
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

        filtered = [edge for edge in edges if matches(edge)]
        print(f"[GraphService] board edges requested: {board['version']}")
        return filtered

    @log_api_method_execution
    def get_versions(self) -> List[VersionDTO]:
        boards = self._list_boards()
        print("[GraphService] board versions requested")
        return [
            {
                "version": self._serialize_board_id(board["b_id"]),
                "name": board["name"],
                "description": board["description"],
                "is_published": board["is_published"],
            }
            for board in boards
        ]

    @log_api_method_execution
    def get_canonical_entities(self) -> List[CanonicalEntityDTO]:
        entities = self._load_canonical_entities()
        print("[GraphService] canonical entities requested")
        return entities

    # --------- ЗАПИСЬ --------- #

    @log_api_method_execution
    def create_version(
        self,
        version: Any,
        name: str,
        description: str,
        is_published: Optional[bool] = None,
    ) -> dict:
        board_id = self._parse_requested_board_id(version)
        for board in self._list_boards():
            if board["b_id"] == board_id:
                raise BoardVersionResolutionError(
                    f"Board with id {self._stringify_board_id(board_id)} already exists."
                )

        query = self._build_query(
            "board-create",
            investigation_name=self.client.investigation_name,
            b_id=board_id,
            name=name,
            description=description,
            is_published=self.client._typeql_bool(bool(is_published)),
        )
        self.client._execute_write("board-create", query)
        print(f"[GraphService] board created: {self._stringify_board_id(board_id)}")
        return {"status": "ok"}
