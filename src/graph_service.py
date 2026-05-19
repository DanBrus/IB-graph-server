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


class CanonicalEntitySyncError(ValueError):
    """Raised when canonical-entity list update is invalid."""

    def __init__(self, detail: Any):
        super().__init__(str(detail))
        self.detail = detail


class BoardSyncError(ValueError):
    """Raised when board update payload is invalid."""

    def __init__(self, detail: Any):
        super().__init__(str(detail))
        self.detail = detail


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
        self._free_ids = {
            "node_id": 1,
            "edge_id": 1,
            "chunk_id": 1,
        }
        self._defragment_ids()

    # --------- Вспомогательные методы --------- #

    def _build_query(self, op_name: str, **params: Any) -> str:
        return self.client._build_query(op_name, **params)

    def _read_docs(self, op_name: str, **params: Any) -> list[dict[str, Any]]:
        query = self._build_query(op_name, **params)
        return self.client._execute_read(op_name, query)

    def _dump_model(self, value: Any) -> dict[str, Any]:
        if hasattr(value, "model_dump"):
            return value.model_dump()
        if hasattr(value, "dict"):
            return value.dict()
        if isinstance(value, dict):
            return dict(value)
        return dict(vars(value))

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
                    "merged_to": self._as_optional_text(
                        self._unwrap_singleton_value(raw_entity.get("merged_to"))
                    ),
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

    def _normalize_requested_canonical_entity(self, entity: Any) -> dict[str, Any]:
        if hasattr(entity, "model_dump"):
            raw_entity = entity.model_dump()
        elif hasattr(entity, "dict"):
            raw_entity = entity.dict()
        elif isinstance(entity, dict):
            raw_entity = dict(entity)
        else:
            raw_entity = dict(vars(entity))

        en_id = self._as_optional_text(raw_entity.get("en_id"))
        name = self._as_optional_text(raw_entity.get("name"))
        entity_type = self._as_optional_text(raw_entity.get("entity_type"))
        merged_to = self._as_optional_text(raw_entity.get("merged_to"))

        if en_id is None:
            raise CanonicalEntitySyncError({"error": "canonical-entity en_id must not be empty."})
        if name is None:
            raise CanonicalEntitySyncError(
                {"error": f"canonical-entity '{en_id}' name must not be empty."}
            )
        if entity_type is None:
            raise CanonicalEntitySyncError(
                {"error": f"canonical-entity '{en_id}' entity_type must not be empty."}
            )

        picture_paths: list[str] = []
        raw_picture_paths = raw_entity.get("picture_paths") or []
        if not isinstance(raw_picture_paths, list):
            raise CanonicalEntitySyncError(
                {
                    "error": f"canonical-entity '{en_id}' picture_paths must be a list.",
                }
            )

        for raw_picture_path in raw_picture_paths:
            picture_path = self._as_optional_text(raw_picture_path)
            if picture_path is None:
                continue
            picture_paths.append(picture_path)

        return {
            "en_id": en_id,
            "name": name,
            "entity_type": entity_type,
            "picture_paths": picture_paths,
            "merged_to": merged_to,
        }

    def _normalize_requested_canonical_entities(self, entities: list[Any]) -> list[dict[str, Any]]:
        normalized_entities: list[dict[str, Any]] = []
        duplicate_ids: set[str] = set()
        seen_ids: set[str] = set()

        for entity in entities:
            normalized_entity = self._normalize_requested_canonical_entity(entity)
            entity_id = normalized_entity["en_id"]
            if entity_id in seen_ids:
                duplicate_ids.add(entity_id)
                continue
            seen_ids.add(entity_id)
            normalized_entities.append(normalized_entity)

        if duplicate_ids:
            raise CanonicalEntitySyncError(
                {
                    "error": "Duplicate canonical-entity ids in request.",
                    "en_ids": sorted(duplicate_ids),
                }
            )

        return normalized_entities

    def _canonical_entity_state_equal(
        self,
        left: dict[str, Any],
        right: dict[str, Any],
    ) -> bool:
        return (
            left.get("name") == right.get("name")
            and left.get("entity_type") == right.get("entity_type")
            and list(left.get("picture_paths", [])) == list(right.get("picture_paths", []))
            and left.get("merged_to") == right.get("merged_to")
        )

    def _resolve_deleted_merge_target(
        self,
        target_id: str,
        current_merge_map: dict[str, str],
        deleted_ids: set[str],
    ) -> str | None:
        path: list[str] = []
        current_target = target_id

        while current_target in deleted_ids:
            if current_target in path:
                cycle = path[path.index(current_target):] + [current_target]
                raise CanonicalEntitySyncError(
                    {
                        "error": "Existing merge chain contains a cycle.",
                        "cycle": cycle,
                    }
                )

            path.append(current_target)
            current_target = current_merge_map.get(current_target)
            if current_target is None:
                return None

        return current_target

    def _validate_merge_cycles(self, merge_map: dict[str, str]) -> None:
        visited: set[str] = set()

        for start in merge_map:
            if start in visited:
                continue

            path: list[str] = []
            positions: dict[str, int] = {}
            current = start

            while current in merge_map:
                if current in positions:
                    cycle = path[positions[current]:] + [current]
                    raise CanonicalEntitySyncError(
                        {
                            "error": "Merged-to cycle detected.",
                            "cycle": cycle,
                        }
                    )
                if current in visited:
                    break

                positions[current] = len(path)
                path.append(current)
                current = merge_map[current]

            visited.update(path)

    def _blocking_board_ids_by_entity(self, entity_ids: list[str]) -> dict[str, list[float]]:
        operations = [
            (
                f"blocking-boards:{entity_id}",
                self._build_query(
                    "return-all-board-ids-containing-canonical-entity-nodes",
                    en_id=entity_id,
                ),
            )
            for entity_id in entity_ids
        ]
        raw_results = self.client._execute_read_queries(operations) if operations else {}

        blocking_by_entity: dict[str, list[float]] = {}
        for entity_id in entity_ids:
            payload = self._first_doc(
                raw_results.get(f"blocking-boards:{entity_id}", []),
                label=f"return-all-board-ids-containing-canonical-entity-nodes:{entity_id}",
                allow_empty=True,
            )
            board_ids = sorted(
                {
                    self._serialize_board_id(board_id)
                    for board_id in self._extract_scalar_list(payload.get("board_ids"))
                }
            )
            blocking_by_entity[entity_id] = board_ids

        return blocking_by_entity

    def _append_picture_path_queries(
        self,
        operations: list[tuple[str, str]],
        *,
        en_id: str,
        picture_paths: list[str],
    ) -> None:
        operations.append(
            (
                "canonical-entity-clear-picture-paths",
                self._build_query("canonical-entity-clear-picture-paths", en_id=en_id),
            )
        )
        for picture_path in picture_paths:
            operations.append(
                (
                    "canonical-entity-add-picture-path",
                    self._build_query(
                        "canonical-entity-add-picture-path",
                        en_id=en_id,
                        picture_path=picture_path,
                    ),
                )
            )

    def _normalize_requested_chunks(
        self,
        raw_chunks: Any,
        *,
        owner_type: str,
        owner_id: int,
    ) -> list[dict[str, Any]]:
        if raw_chunks is None:
            return []
        if not isinstance(raw_chunks, list):
            raise BoardSyncError(
                {
                    "error": f"{owner_type} {owner_id} description must be a list of ChunkDTO.",
                }
            )

        chunks: list[dict[str, Any]] = []
        duplicate_ids: set[int] = set()
        seen_ids: set[int] = set()

        for raw_chunk in raw_chunks:
            chunk = self._dump_model(raw_chunk)
            try:
                chunk_id = self._as_int(chunk.get("c_id"))
                chunk_priority = self._as_int(chunk.get("chunk_priority"))
            except (TypeError, ValueError) as exc:
                raise BoardSyncError(
                    {
                        "error": f"{owner_type} {owner_id} contains chunk with invalid numeric fields.",
                    }
                ) from exc

            description = self._as_text(chunk.get("description")).strip()
            if not description:
                raise BoardSyncError(
                    {
                        "error": f"{owner_type} {owner_id} contains chunk with empty description.",
                        "c_id": chunk_id,
                    }
                )

            if chunk_id in seen_ids:
                duplicate_ids.add(chunk_id)
                continue
            seen_ids.add(chunk_id)

            chunks.append(
                {
                    "c_id": chunk_id,
                    "description": description,
                    "chunk_priority": chunk_priority,
                    "timecode": self._as_text(chunk.get("timecode")),
                }
            )

        if duplicate_ids:
            raise BoardSyncError(
                {
                    "error": f"{owner_type} {owner_id} contains duplicate chunk ids.",
                    "chunk_ids": sorted(duplicate_ids),
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

    def _normalize_requested_node(self, value: Any) -> dict[str, Any]:
        raw_node = self._dump_model(value)

        try:
            node_id = self._as_int(raw_node.get("node_id"))
            pos_x = self._as_float(raw_node.get("pos_x"))
            pos_y = self._as_float(raw_node.get("pos_y"))
        except (TypeError, ValueError) as exc:
            raise BoardSyncError({"error": "Node contains invalid numeric fields."}) from exc

        ce_id = self._as_optional_text(raw_node.get("ce_id"))
        if ce_id is None:
            raise BoardSyncError(
                {
                    "error": f"Node {node_id} must be bound to canonical-entity via ce_id.",
                }
            )

        return {
            "node_id": node_id,
            "ce_id": ce_id,
            "pos_x": pos_x,
            "pos_y": pos_y,
            "description": self._normalize_requested_chunks(
                raw_node.get("description"),
                owner_type="node",
                owner_id=node_id,
            ),
        }

    def _normalize_requested_nodes(self, values: Any) -> list[dict[str, Any]]:
        if values is None:
            return []
        if not isinstance(values, list):
            raise BoardSyncError({"error": "Board nodes must be a list."})

        nodes: list[dict[str, Any]] = []
        duplicate_ids: set[int] = set()
        seen_ids: set[int] = set()

        for raw_node in values:
            node = self._normalize_requested_node(raw_node)
            node_id = node["node_id"]
            if node_id in seen_ids:
                duplicate_ids.add(node_id)
                continue
            seen_ids.add(node_id)
            nodes.append(node)

        if duplicate_ids:
            raise BoardSyncError(
                {
                    "error": "Board contains duplicate node ids.",
                    "node_ids": sorted(duplicate_ids),
                }
            )

        return nodes

    def _normalize_requested_edge(self, value: Any) -> dict[str, Any]:
        raw_edge = self._dump_model(value)

        try:
            edge_id = self._as_int(raw_edge.get("edge_id"))
            node1 = self._as_int(raw_edge.get("node1"))
            node2 = self._as_int(raw_edge.get("node2"))
        except (TypeError, ValueError) as exc:
            raise BoardSyncError({"error": "Edge contains invalid numeric fields."}) from exc

        return {
            "edge_id": edge_id,
            "node1": node1,
            "node2": node2,
            "description": self._normalize_requested_chunks(
                raw_edge.get("description"),
                owner_type="edge",
                owner_id=edge_id,
            ),
        }

    def _normalize_requested_edges(self, values: Any) -> list[dict[str, Any]]:
        if values is None:
            return []
        if not isinstance(values, list):
            raise BoardSyncError({"error": "Board edges must be a list."})

        edges: list[dict[str, Any]] = []
        duplicate_ids: set[int] = set()
        seen_ids: set[int] = set()

        for raw_edge in values:
            edge = self._normalize_requested_edge(raw_edge)
            edge_id = edge["edge_id"]
            if edge_id in seen_ids:
                duplicate_ids.add(edge_id)
                continue
            seen_ids.add(edge_id)
            edges.append(edge)

        if duplicate_ids:
            raise BoardSyncError(
                {
                    "error": "Board contains duplicate edge ids.",
                    "edge_ids": sorted(duplicate_ids),
                }
            )

        return edges

    def _normalize_requested_board(self, value: Any) -> dict[str, Any]:
        raw_board = self._dump_model(value)

        board_id = self._parse_requested_board_id(raw_board.get("version"))
        return {
            "b_id": board_id,
            "board_name": None
            if raw_board.get("board_name") is None
            else self._as_text(raw_board.get("board_name")),
            "description": None
            if raw_board.get("description") is None
            else self._as_text(raw_board.get("description")),
            "is_published": None
            if raw_board.get("is_published") is None
            else self._as_bool(raw_board.get("is_published")),
            "nodes": self._normalize_requested_nodes(raw_board.get("nodes")),
            "edges": self._normalize_requested_edges(raw_board.get("edges")),
        }

    def _diff_chunks(
        self,
        current_chunks: list[dict[str, Any]],
        desired_chunks: list[dict[str, Any]],
    ) -> tuple[list[int], list[dict[str, Any]], list[dict[str, Any]]]:
        current_by_id = {
            self._as_int(chunk.get("c_id")): dict(chunk)
            for chunk in current_chunks
        }
        desired_by_id = {
            self._as_int(chunk.get("c_id")): dict(chunk)
            for chunk in desired_chunks
        }

        chunk_ids_to_delete = sorted(current_by_id.keys() - desired_by_id.keys())
        chunks_to_create = [
            desired_by_id[chunk_id]
            for chunk_id in sorted(desired_by_id.keys() - current_by_id.keys())
        ]

        chunks_to_update: list[dict[str, Any]] = []
        for chunk_id in sorted(current_by_id.keys() & desired_by_id.keys()):
            current_chunk = current_by_id[chunk_id]
            desired_chunk = desired_by_id[chunk_id]
            if (
                self._as_text(current_chunk.get("description"))
                != self._as_text(desired_chunk.get("description"))
                or self._as_int(current_chunk.get("chunk_priority"))
                != self._as_int(desired_chunk.get("chunk_priority"))
                or self._as_text(current_chunk.get("timecode"))
                != self._as_text(desired_chunk.get("timecode"))
            ):
                chunks_to_update.append(desired_chunk)

        return chunk_ids_to_delete, chunks_to_update, chunks_to_create

    def _validate_new_graph_ids_available(
        self,
        *,
        added_node_ids: set[int],
        added_edge_ids: set[int],
        added_chunk_ids: set[int],
    ) -> None:
        operations: list[tuple[str, str]] = []

        for node_id in sorted(added_node_ids):
            operations.append(
                (
                    f"find-node:{node_id}",
                    self._build_query("find-node-by-id", n_id=node_id),
                )
            )
        for edge_id in sorted(added_edge_ids):
            operations.append(
                (
                    f"find-edge:{edge_id}",
                    self._build_query("find-edge-by-id", ed_id=edge_id),
                )
            )
        for chunk_id in sorted(added_chunk_ids):
            operations.append(
                (
                    f"find-chunk:{chunk_id}",
                    self._build_query("find-text-chunk-by-id", c_id=chunk_id),
                )
            )

        raw_results = self.client._execute_read_queries(operations) if operations else {}

        conflicting_node_ids = [
            node_id
            for node_id in sorted(added_node_ids)
            if raw_results.get(f"find-node:{node_id}")
        ]
        conflicting_edge_ids = [
            edge_id
            for edge_id in sorted(added_edge_ids)
            if raw_results.get(f"find-edge:{edge_id}")
        ]
        conflicting_chunk_ids = [
            chunk_id
            for chunk_id in sorted(added_chunk_ids)
            if raw_results.get(f"find-chunk:{chunk_id}")
        ]

        if conflicting_node_ids or conflicting_edge_ids or conflicting_chunk_ids:
            raise BoardSyncError(
                {
                    "error": "Some ids already exist in database.",
                    "node_ids": conflicting_node_ids,
                    "edge_ids": conflicting_edge_ids,
                    "chunk_ids": conflicting_chunk_ids,
                }
            )

    def _collect_global_graph_records(
        self,
    ) -> tuple[dict[int, dict[str, Any]], dict[int, dict[str, Any]], dict[int, dict[str, Any]]]:
        boards = self._list_boards()
        if not boards:
            return {}, {}, {}

        graph_queries: list[tuple[str, str]] = []
        for board in boards:
            board_id = board["b_id"]
            board_key = self._stringify_board_id(board_id)
            graph_queries.append(
                (
                    f"nodes:{board_key}",
                    self._build_query("return-all-nodes-in-board", b_id=board_id),
                )
            )
            graph_queries.append(
                (
                    f"edges:{board_key}",
                    self._build_query("return-all-edges-in-board", b_id=board_id),
                )
            )

        graph_results = self.client._execute_read_queries(graph_queries)

        node_records: dict[int, dict[str, Any]] = {}
        edge_records: dict[int, dict[str, Any]] = {}
        chunk_queries: list[tuple[str, str]] = []

        for board in boards:
            board_key = self._stringify_board_id(board["b_id"])

            nodes_payload = self._first_doc(
                graph_results.get(f"nodes:{board_key}", []),
                label=f"return-all-nodes-in-board:{board_key}",
                allow_empty=True,
            )
            for raw_node in nodes_payload.get("nodes", []):
                if not isinstance(raw_node, dict):
                    continue
                node_id = self._as_int(raw_node.get("n_id"))
                node_records[node_id] = {
                    "pos_x": self._as_float(raw_node.get("pos_x")),
                    "pos_y": self._as_float(raw_node.get("pos_y")),
                }
                chunk_queries.append(
                    (
                        f"node-chunks:{node_id}",
                        self._build_query("return-all-text-chunks-in-node", n_id=node_id),
                    )
                )

            edges_payload = self._first_doc(
                graph_results.get(f"edges:{board_key}", []),
                label=f"return-all-edges-in-board:{board_key}",
                allow_empty=True,
            )
            for raw_edge in edges_payload.get("edges", []):
                if not isinstance(raw_edge, dict):
                    continue
                edge_id = self._as_int(raw_edge.get("ed_id"))
                edge_records[edge_id] = {
                    "node1": self._as_int(raw_edge.get("endpoint_1_n_id")),
                    "node2": self._as_int(raw_edge.get("endpoint_2_n_id")),
                }
                chunk_queries.append(
                    (
                        f"edge-chunks:{edge_id}",
                        self._build_query("return-all-text-chunks-in-edge", ed_id=edge_id),
                    )
                )

        chunk_results = self.client._execute_read_queries(chunk_queries) if chunk_queries else {}
        chunk_records: dict[int, dict[str, Any]] = {}

        for node_id in sorted(node_records):
            payload = self._first_doc(
                chunk_results.get(f"node-chunks:{node_id}", []),
                label=f"return-all-text-chunks-in-node:{node_id}",
                allow_empty=True,
            )
            for chunk in self._normalize_chunk_payload(payload):
                chunk_records[chunk["c_id"]] = dict(chunk)

        for edge_id in sorted(edge_records):
            payload = self._first_doc(
                chunk_results.get(f"edge-chunks:{edge_id}", []),
                label=f"return-all-text-chunks-in-edge:{edge_id}",
                allow_empty=True,
            )
            for chunk in self._normalize_chunk_payload(payload):
                chunk_records[chunk["c_id"]] = dict(chunk)

        return node_records, edge_records, chunk_records

    def _smallest_free_positive_id(self, ids: set[int]) -> int:
        candidate = 1
        while candidate in ids:
            candidate += 1
        return candidate

    def _build_id_defragmentation_map(self, ids: set[int]) -> dict[int, int]:
        mapping: dict[int, int] = {}
        for next_id, current_id in enumerate(sorted(ids), start=1):
            if current_id != next_id:
                mapping[current_id] = next_id
        return mapping

    def _append_node_id_defragmentation_queries(
        self,
        operations: list[tuple[str, str]],
        *,
        node_records: dict[int, dict[str, Any]],
        mapping: dict[int, int],
    ) -> None:
        if not mapping:
            return

        temp_start = max(node_records) + 1 if node_records else 1
        temp_ids = {
            node_id: temp_start + index
            for index, node_id in enumerate(sorted(mapping))
        }

        for node_id in sorted(mapping):
            record = node_records[node_id]
            operations.append(
                (
                    "node-update",
                    self._build_query(
                        "node-update",
                        n_id=node_id,
                        new_n_id=temp_ids[node_id],
                        new_pos_x=record["pos_x"],
                        new_pos_y=record["pos_y"],
                    ),
                )
            )

        for node_id in sorted(mapping):
            record = node_records[node_id]
            operations.append(
                (
                    "node-update",
                    self._build_query(
                        "node-update",
                        n_id=temp_ids[node_id],
                        new_n_id=mapping[node_id],
                        new_pos_x=record["pos_x"],
                        new_pos_y=record["pos_y"],
                    ),
                )
            )

    def _append_edge_id_defragmentation_queries(
        self,
        operations: list[tuple[str, str]],
        *,
        edge_records: dict[int, dict[str, Any]],
        mapping: dict[int, int],
    ) -> None:
        if not mapping:
            return

        temp_start = max(edge_records) + 1 if edge_records else 1
        temp_ids = {
            edge_id: temp_start + index
            for index, edge_id in enumerate(sorted(mapping))
        }

        for edge_id in sorted(mapping):
            record = edge_records[edge_id]
            operations.append(
                (
                    "edge-update",
                    self._build_query(
                        "edge-update",
                        ed_id=edge_id,
                        new_ed_id=temp_ids[edge_id],
                        new_endpoint_1_n_id=record["node1"],
                        new_endpoint_2_n_id=record["node2"],
                    ),
                )
            )

        for edge_id in sorted(mapping):
            record = edge_records[edge_id]
            operations.append(
                (
                    "edge-update",
                    self._build_query(
                        "edge-update",
                        ed_id=temp_ids[edge_id],
                        new_ed_id=mapping[edge_id],
                        new_endpoint_1_n_id=record["node1"],
                        new_endpoint_2_n_id=record["node2"],
                    ),
                )
            )

    def _append_chunk_id_defragmentation_queries(
        self,
        operations: list[tuple[str, str]],
        *,
        chunk_records: dict[int, dict[str, Any]],
        mapping: dict[int, int],
    ) -> None:
        if not mapping:
            return

        temp_start = max(chunk_records) + 1 if chunk_records else 1
        temp_ids = {
            chunk_id: temp_start + index
            for index, chunk_id in enumerate(sorted(mapping))
        }

        for chunk_id in sorted(mapping):
            record = chunk_records[chunk_id]
            operations.append(
                (
                    "text-chunk-update",
                    self._build_query(
                        "text-chunk-update",
                        c_id=chunk_id,
                        new_c_id=temp_ids[chunk_id],
                        new_description=record["description"],
                        new_chunk_priority=record["chunk_priority"],
                        new_timecode=record["timecode"],
                    ),
                )
            )

        for chunk_id in sorted(mapping):
            record = chunk_records[chunk_id]
            operations.append(
                (
                    "text-chunk-update",
                    self._build_query(
                        "text-chunk-update",
                        c_id=temp_ids[chunk_id],
                        new_c_id=mapping[chunk_id],
                        new_description=record["description"],
                        new_chunk_priority=record["chunk_priority"],
                        new_timecode=record["timecode"],
                    ),
                )
            )

    def _refresh_free_ids_state(self) -> None:
        node_records, edge_records, chunk_records = self._collect_global_graph_records()
        self._free_ids = {
            "node_id": self._smallest_free_positive_id(set(node_records)),
            "edge_id": self._smallest_free_positive_id(set(edge_records)),
            "chunk_id": self._smallest_free_positive_id(set(chunk_records)),
        }

    def _defragment_node_ids(
        self,
        *,
        node_records: dict[int, dict[str, Any]] | None = None,
    ) -> int:
        resolved_node_records = node_records or {}
        if node_records is None:
            resolved_node_records, _, _ = self._collect_global_graph_records()

        node_mapping = self._build_id_defragmentation_map(set(resolved_node_records))
        if not node_mapping:
            return 0

        write_operations: list[tuple[str, str]] = []
        self._append_node_id_defragmentation_queries(
            write_operations,
            node_records=resolved_node_records,
            mapping=node_mapping,
        )
        self.client._execute_write_queries(write_operations)
        print(f"[GraphService] node ids defragmented (count={len(node_mapping)})")
        return len(node_mapping)

    def _defragment_edge_ids(
        self,
        *,
        edge_records: dict[int, dict[str, Any]] | None = None,
    ) -> int:
        resolved_edge_records = edge_records or {}
        if edge_records is None:
            _, resolved_edge_records, _ = self._collect_global_graph_records()

        edge_mapping = self._build_id_defragmentation_map(set(resolved_edge_records))
        if not edge_mapping:
            return 0

        write_operations: list[tuple[str, str]] = []
        self._append_edge_id_defragmentation_queries(
            write_operations,
            edge_records=resolved_edge_records,
            mapping=edge_mapping,
        )
        self.client._execute_write_queries(write_operations)
        print(f"[GraphService] edge ids defragmented (count={len(edge_mapping)})")
        return len(edge_mapping)

    def _defragment_chunk_ids(
        self,
        *,
        chunk_records: dict[int, dict[str, Any]] | None = None,
    ) -> int:
        resolved_chunk_records = chunk_records or {}
        if chunk_records is None:
            _, _, resolved_chunk_records = self._collect_global_graph_records()

        chunk_mapping = self._build_id_defragmentation_map(set(resolved_chunk_records))
        if not chunk_mapping:
            return 0

        write_operations: list[tuple[str, str]] = []
        self._append_chunk_id_defragmentation_queries(
            write_operations,
            chunk_records=resolved_chunk_records,
            mapping=chunk_mapping,
        )
        self.client._execute_write_queries(write_operations)
        print(f"[GraphService] chunk ids defragmented (count={len(chunk_mapping)})")
        return len(chunk_mapping)

    def _defragment_ids(
        self,
        *,
        defragment_nodes: bool = True,
        defragment_edges: bool = True,
        defragment_chunks: bool = True,
    ) -> None:
        if not any((defragment_nodes, defragment_edges, defragment_chunks)):
            self._refresh_free_ids_state()
            return

        node_records, edge_records, chunk_records = self._collect_global_graph_records()
        edge_count = 0
        node_count = 0
        chunk_count = 0

        if defragment_edges:
            edge_count = self._defragment_edge_ids(edge_records=edge_records)
        if defragment_nodes:
            node_count = self._defragment_node_ids(node_records=node_records)
        if defragment_chunks:
            chunk_count = self._defragment_chunk_ids(chunk_records=chunk_records)

        print(
            "[GraphService] defragmentation summary "
            f"(nodes={node_count}, edges={edge_count}, chunks={chunk_count})"
        )
        self._refresh_free_ids_state()

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
                    "ce_id": entity["en_id"],
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

    @log_api_method_execution
    def get_free_ids(self) -> dict[str, int]:
        return dict(self._free_ids)

    @log_api_method_execution
    def update_canonical_entities(self, payload: list[Any]) -> dict[str, Any]:
        requested_entities = self._normalize_requested_canonical_entities(payload)
        requested_by_id = {
            entity["en_id"]: dict(entity)
            for entity in requested_entities
        }

        current_entities = self._load_canonical_entities()
        current_by_id = {
            entity["en_id"]: dict(entity)
            for entity in current_entities
        }

        current_ids = set(current_by_id)
        requested_ids = set(requested_by_id)

        added_ids = requested_ids - current_ids
        deleted_ids = current_ids - requested_ids
        shared_ids = requested_ids & current_ids

        current_merge_map = {
            entity_id: entity["merged_to"]
            for entity_id, entity in current_by_id.items()
            if entity.get("merged_to") is not None
        }

        projected_by_id = {
            entity_id: dict(entity)
            for entity_id, entity in requested_by_id.items()
        }

        for entity_id in shared_ids:
            desired_target = projected_by_id[entity_id].get("merged_to")
            if desired_target not in deleted_ids:
                continue

            current_target = current_by_id[entity_id].get("merged_to")
            if current_target != desired_target:
                raise CanonicalEntitySyncError(
                    {
                        "error": "canonical-entity merged_to points to entity scheduled for deletion.",
                        "en_id": entity_id,
                        "merged_to": desired_target,
                    }
                )

            projected_by_id[entity_id]["merged_to"] = self._resolve_deleted_merge_target(
                desired_target,
                current_merge_map,
                deleted_ids,
            )

        for entity_id in added_ids:
            desired_target = projected_by_id[entity_id].get("merged_to")
            if desired_target in deleted_ids:
                raise CanonicalEntitySyncError(
                    {
                        "error": "New canonical-entity cannot merge into entity scheduled for deletion.",
                        "en_id": entity_id,
                        "merged_to": desired_target,
                    }
                )

        changed_ids = {
            entity_id
            for entity_id in shared_ids
            if not self._canonical_entity_state_equal(
                current_by_id[entity_id],
                projected_by_id[entity_id],
            )
        }

        overlapping_ids = sorted(added_ids & changed_ids)
        if overlapping_ids:
            raise CanonicalEntitySyncError(
                {
                    "error": "Same canonical-entity id cannot be both added and updated.",
                    "en_ids": overlapping_ids,
                }
            )

        for entity_id, entity in projected_by_id.items():
            merged_to = entity.get("merged_to")
            if merged_to is None:
                continue
            if merged_to == entity_id:
                raise CanonicalEntitySyncError(
                    {
                        "error": "canonical-entity cannot be merged into itself.",
                        "en_id": entity_id,
                    }
                )
            if merged_to not in projected_by_id:
                raise CanonicalEntitySyncError(
                    {
                        "error": "canonical-entity merged_to points to unknown entity.",
                        "en_id": entity_id,
                        "merged_to": merged_to,
                    }
                )

        self._validate_merge_cycles(
            {
                entity_id: entity["merged_to"]
                for entity_id, entity in projected_by_id.items()
                if entity.get("merged_to") is not None
            }
        )

        blocking_by_entity = self._blocking_board_ids_by_entity(sorted(deleted_ids))
        for entity_id in sorted(deleted_ids):
            blocking_boards = blocking_by_entity.get(entity_id, [])
            if blocking_boards:
                print(
                    "[GraphService] canonical-entity deletion blocked "
                    f"(en_id={entity_id}, boards={blocking_boards})"
                )
                raise CanonicalEntitySyncError(blocking_boards)

        write_operations: list[tuple[str, str]] = []

        for entity_id in sorted(deleted_ids):
            write_operations.append(
                (
                    "canonical-entity-delete",
                    self._build_query("canonical-entity-delete", en_id=entity_id),
                )
            )

        for entity_id in sorted(added_ids):
            entity = projected_by_id[entity_id]
            write_operations.append(
                (
                    "canonical-entity-create",
                    self._build_query(
                        "canonical-entity-create",
                        investigation_name=self.client.investigation_name,
                        en_id=entity["en_id"],
                        name=entity["name"],
                        entity_type=entity["entity_type"],
                    ),
                )
            )

        for entity_id in sorted(added_ids):
            entity = projected_by_id[entity_id]
            self._append_picture_path_queries(
                write_operations,
                en_id=entity_id,
                picture_paths=entity["picture_paths"],
            )

        for entity_id in sorted(added_ids):
            merged_to = projected_by_id[entity_id].get("merged_to")
            if merged_to is None:
                continue
            write_operations.append(
                (
                    "entity-merged-to-create",
                    self._build_query(
                        "entity-merged-to-create",
                        source_en_id=entity_id,
                        target_en_id=merged_to,
                    ),
                )
            )

        for entity_id in sorted(changed_ids):
            current_entity = current_by_id[entity_id]
            desired_entity = projected_by_id[entity_id]

            if (
                current_entity["name"] != desired_entity["name"]
                or current_entity["entity_type"] != desired_entity["entity_type"]
            ):
                write_operations.append(
                    (
                        "canonical-entity-update-core",
                        self._build_query(
                            "canonical-entity-update-core",
                            en_id=entity_id,
                            new_name=desired_entity["name"],
                            new_entity_type=desired_entity["entity_type"],
                        ),
                    )
                )

            if current_entity["picture_paths"] != desired_entity["picture_paths"]:
                self._append_picture_path_queries(
                    write_operations,
                    en_id=entity_id,
                    picture_paths=desired_entity["picture_paths"],
                )

            current_merged_to = current_entity.get("merged_to")
            desired_merged_to = desired_entity.get("merged_to")
            if current_merged_to != desired_merged_to:
                if current_merged_to is not None:
                    write_operations.append(
                        (
                            "entity-merged-to-delete-by-source",
                            self._build_query(
                                "entity-merged-to-delete-by-source",
                                source_en_id=entity_id,
                            ),
                        )
                    )
                if desired_merged_to is not None:
                    write_operations.append(
                        (
                            "entity-merged-to-create",
                            self._build_query(
                                "entity-merged-to-create",
                                source_en_id=entity_id,
                                target_en_id=desired_merged_to,
                            ),
                        )
                    )

        self.client._execute_write_queries(write_operations)
        print(
            "[GraphService] canonical entities updated "
            f"(added={len(added_ids)}, deleted={len(deleted_ids)}, updated={len(changed_ids)})"
        )
        return {"status": "ok"}

    @log_api_method_execution
    def update_board(self, payload: Any) -> dict[str, Any]:
        requested_board = self._normalize_requested_board(payload)
        board_id = requested_board["b_id"]
        board_version = self._stringify_board_id(board_id)
        current_board = self._load_board_snapshot(version=board_version)

        desired_board_name = (
            current_board["board_name"]
            if requested_board["board_name"] is None
            else requested_board["board_name"]
        )
        desired_description = (
            current_board["description"]
            if requested_board["description"] is None
            else requested_board["description"]
        )
        desired_is_published = (
            current_board["is_published"]
            if requested_board["is_published"] is None
            else requested_board["is_published"]
        )

        requested_nodes = requested_board["nodes"]
        requested_edges = requested_board["edges"]
        requested_nodes_by_id = {
            node["node_id"]: dict(node)
            for node in requested_nodes
        }
        requested_edges_by_id = {
            edge["edge_id"]: dict(edge)
            for edge in requested_edges
        }
        current_nodes_by_id = {
            self._as_int(node["node_id"]): dict(node)
            for node in current_board["nodes"]
        }
        current_edges_by_id = {
            self._as_int(edge["edge_id"]): dict(edge)
            for edge in current_board["edges"]
        }

        ce_to_node_ids: dict[str, list[int]] = {}
        for node in requested_nodes:
            ce_to_node_ids.setdefault(node["ce_id"], []).append(node["node_id"])

        duplicate_ce_bindings = [
            {
                "ce_id": ce_id,
                "node_ids": sorted(node_ids),
            }
            for ce_id, node_ids in sorted(ce_to_node_ids.items())
            if len(node_ids) > 1
        ]
        if duplicate_ce_bindings:
            raise BoardSyncError(
                {
                    "error": "A board cannot contain multiple nodes bound to the same canonical-entity.",
                    "bindings": duplicate_ce_bindings,
                }
            )

        known_entity_ids = {
            entity["en_id"]
            for entity in self._load_canonical_entities()
        }
        missing_entity_ids = sorted(
            {
                node["ce_id"]
                for node in requested_nodes
                if node["ce_id"] not in known_entity_ids
            }
        )
        if missing_entity_ids:
            raise BoardSyncError(
                {
                    "error": "Some nodes reference unknown canonical-entity ids.",
                    "ce_ids": missing_entity_ids,
                }
            )

        invalid_edges: list[dict[str, Any]] = []
        requested_node_ids = set(requested_nodes_by_id)
        for edge in requested_edges:
            missing_node_ids = sorted(
                {
                    node_id
                    for node_id in (edge["node1"], edge["node2"])
                    if node_id not in requested_node_ids
                }
            )
            if missing_node_ids:
                invalid_edges.append(
                    {
                        "edge_id": edge["edge_id"],
                        "missing_node_ids": missing_node_ids,
                    }
                )

        if invalid_edges:
            raise BoardSyncError(
                {
                    "error": "Some edges reference nodes absent from the board payload.",
                    "edges": invalid_edges,
                }
            )

        chunk_owners: dict[int, list[dict[str, Any]]] = {}
        for node in requested_nodes:
            for chunk in node["description"]:
                chunk_owners.setdefault(chunk["c_id"], []).append(
                    {
                        "owner_type": "node",
                        "owner_id": node["node_id"],
                    }
                )
        for edge in requested_edges:
            for chunk in edge["description"]:
                chunk_owners.setdefault(chunk["c_id"], []).append(
                    {
                        "owner_type": "edge",
                        "owner_id": edge["edge_id"],
                    }
                )

        duplicate_chunk_bindings = [
            {
                "c_id": chunk_id,
                "owners": owners,
            }
            for chunk_id, owners in sorted(chunk_owners.items())
            if len(owners) > 1
        ]
        if duplicate_chunk_bindings:
            raise BoardSyncError(
                {
                    "error": "Chunk ids must be unique within board payload.",
                    "chunks": duplicate_chunk_bindings,
                }
            )

        current_chunk_ids = {
            self._as_int(chunk["c_id"])
            for node in current_board["nodes"]
            for chunk in node.get("description", [])
        } | {
            self._as_int(chunk["c_id"])
            for edge in current_board["edges"]
            for chunk in edge.get("description", [])
        }
        requested_chunk_ids = set(chunk_owners)

        added_node_ids = set(requested_nodes_by_id) - set(current_nodes_by_id)
        added_edge_ids = set(requested_edges_by_id) - set(current_edges_by_id)
        added_chunk_ids = requested_chunk_ids - current_chunk_ids

        self._validate_new_graph_ids_available(
            added_node_ids=added_node_ids,
            added_edge_ids=added_edge_ids,
            added_chunk_ids=added_chunk_ids,
        )

        write_operations: list[tuple[str, str]] = []

        if (
            self._as_text(current_board["board_name"]) != self._as_text(desired_board_name)
            or self._as_text(current_board["description"]) != self._as_text(desired_description)
            or self._as_bool(current_board["is_published"]) != self._as_bool(desired_is_published)
        ):
            write_operations.append(
                (
                    "board-update",
                    self._build_query(
                        "board-update",
                        b_id=board_id,
                        new_b_id=board_id,
                        new_name=desired_board_name,
                        new_description=desired_description,
                        new_is_published=self.client._typeql_bool(bool(desired_is_published)),
                    ),
                )
            )

        edge_ids_to_delete: set[int] = set()
        edge_ids_to_recreate: set[int] = set()
        edge_chunk_deletes: list[int] = []
        edge_chunk_updates: list[dict[str, Any]] = []
        edge_chunk_creates: list[tuple[int, dict[str, Any]]] = []

        for edge_id, current_edge in current_edges_by_id.items():
            desired_edge = requested_edges_by_id.get(edge_id)
            if desired_edge is None:
                edge_ids_to_delete.add(edge_id)
                continue

            endpoints_changed = (
                self._as_int(current_edge.get("node1")) != desired_edge["node1"]
                or self._as_int(current_edge.get("node2")) != desired_edge["node2"]
            )
            if endpoints_changed:
                edge_ids_to_delete.add(edge_id)
                edge_ids_to_recreate.add(edge_id)
                continue

            chunk_ids_to_delete, chunks_to_update, chunks_to_create = self._diff_chunks(
                list(current_edge.get("description", [])),
                desired_edge["description"],
            )
            edge_chunk_deletes.extend(chunk_ids_to_delete)
            edge_chunk_updates.extend(chunks_to_update)
            edge_chunk_creates.extend((edge_id, chunk) for chunk in chunks_to_create)

        for edge_id in sorted(edge_ids_to_delete):
            write_operations.append(
                (
                    "edge-delete",
                    self._build_query("edge-delete", ed_id=edge_id),
                )
            )

        node_ids_to_delete = sorted(set(current_nodes_by_id) - set(requested_nodes_by_id))
        node_ids_to_create = sorted(added_node_ids)
        nodes_to_rebind: list[tuple[int, str]] = []
        node_position_updates: list[dict[str, Any]] = []
        node_chunk_deletes: list[int] = []
        node_chunk_updates: list[dict[str, Any]] = []
        node_chunk_creates: list[tuple[int, dict[str, Any]]] = []

        for node_id, current_node in current_nodes_by_id.items():
            desired_node = requested_nodes_by_id.get(node_id)
            if desired_node is None:
                continue

            if self._as_text(current_node.get("ce_id")) != desired_node["ce_id"]:
                nodes_to_rebind.append((node_id, desired_node["ce_id"]))

            if (
                self._as_float(current_node.get("pos_x")) != desired_node["pos_x"]
                or self._as_float(current_node.get("pos_y")) != desired_node["pos_y"]
            ):
                node_position_updates.append(desired_node)

            chunk_ids_to_delete, chunks_to_update, chunks_to_create = self._diff_chunks(
                list(current_node.get("description", [])),
                desired_node["description"],
            )
            node_chunk_deletes.extend(chunk_ids_to_delete)
            node_chunk_updates.extend(chunks_to_update)
            node_chunk_creates.extend((node_id, chunk) for chunk in chunks_to_create)

        for node_id in node_ids_to_delete:
            write_operations.append(
                (
                    "node-delete",
                    self._build_query("node-delete", n_id=node_id),
                )
            )

        for node_id in node_ids_to_create:
            node = requested_nodes_by_id[node_id]
            write_operations.append(
                (
                    "node-create",
                    self._build_query(
                        "node-create",
                        b_id=board_id,
                        en_id=node["ce_id"],
                        n_id=node_id,
                        pos_x=node["pos_x"],
                        pos_y=node["pos_y"],
                    ),
                )
            )

        for node_id, ce_id in sorted(nodes_to_rebind):
            write_operations.append(
                (
                    "entity-has-node-delete-by-node",
                    self._build_query("entity-has-node-delete-by-node", n_id=node_id),
                )
            )
            write_operations.append(
                (
                    "entity-has-node-create",
                    self._build_query(
                        "entity-has-node-create",
                        en_id=ce_id,
                        n_id=node_id,
                    ),
                )
            )

        for node in sorted(node_position_updates, key=lambda item: item["node_id"]):
            write_operations.append(
                (
                    "node-update",
                    self._build_query(
                        "node-update",
                        n_id=node["node_id"],
                        new_n_id=node["node_id"],
                        new_pos_x=node["pos_x"],
                        new_pos_y=node["pos_y"],
                    ),
                )
            )

        for chunk_id in sorted(set(node_chunk_deletes) | set(edge_chunk_deletes)):
            write_operations.append(
                (
                    "text-chunk-delete",
                    self._build_query("text-chunk-delete", c_id=chunk_id),
                )
            )

        chunk_updates = {
            chunk["c_id"]: chunk
            for chunk in node_chunk_updates + edge_chunk_updates
        }
        for chunk_id in sorted(chunk_updates):
            chunk = chunk_updates[chunk_id]
            write_operations.append(
                (
                    "text-chunk-update",
                    self._build_query(
                        "text-chunk-update",
                        c_id=chunk_id,
                        new_c_id=chunk_id,
                        new_description=chunk["description"],
                        new_chunk_priority=chunk["chunk_priority"],
                        new_timecode=chunk["timecode"],
                    ),
                )
            )

        edge_ids_to_create = sorted(set(added_edge_ids) | edge_ids_to_recreate)
        for edge_id in edge_ids_to_create:
            edge = requested_edges_by_id[edge_id]
            write_operations.append(
                (
                    "edge-create",
                    self._build_query(
                        "edge-create",
                        b_id=board_id,
                        endpoint_1_n_id=edge["node1"],
                        endpoint_2_n_id=edge["node2"],
                        ed_id=edge_id,
                    ),
                )
            )

        for node_id in node_ids_to_create:
            node = requested_nodes_by_id[node_id]
            for chunk in node["description"]:
                node_chunk_creates.append((node_id, chunk))

        for edge_id in edge_ids_to_create:
            edge = requested_edges_by_id[edge_id]
            for chunk in edge["description"]:
                edge_chunk_creates.append((edge_id, chunk))

        node_chunk_create_map = {
            chunk["c_id"]: (node_id, chunk)
            for node_id, chunk in node_chunk_creates
        }
        edge_chunk_create_map = {
            chunk["c_id"]: (edge_id, chunk)
            for edge_id, chunk in edge_chunk_creates
        }

        for chunk_id in sorted(node_chunk_create_map):
            node_id, chunk = node_chunk_create_map[chunk_id]
            write_operations.append(
                (
                    "text-chunk-create-for-node",
                    self._build_query(
                        "text-chunk-create-for-node",
                        n_id=node_id,
                        c_id=chunk_id,
                        description=chunk["description"],
                        chunk_priority=chunk["chunk_priority"],
                        timecode=chunk["timecode"],
                    ),
                )
            )

        for chunk_id in sorted(edge_chunk_create_map):
            edge_id, chunk = edge_chunk_create_map[chunk_id]
            write_operations.append(
                (
                    "text-chunk-create-for-edge",
                    self._build_query(
                        "text-chunk-create-for-edge",
                        ed_id=edge_id,
                        c_id=chunk_id,
                        description=chunk["description"],
                        chunk_priority=chunk["chunk_priority"],
                        timecode=chunk["timecode"],
                    ),
                )
            )

        self.client._execute_write_queries(write_operations)

        node_ids_changed = bool(node_ids_to_delete or node_ids_to_create)
        edge_ids_changed = bool(edge_ids_to_delete or edge_ids_to_create)
        chunk_ids_changed = bool(
            node_chunk_deletes
            or edge_chunk_deletes
            or node_chunk_creates
            or edge_chunk_creates
        )

        if node_ids_changed or edge_ids_changed or chunk_ids_changed:
            self._defragment_ids(
                defragment_nodes=node_ids_changed,
                defragment_edges=edge_ids_changed,
                defragment_chunks=chunk_ids_changed,
            )
        else:
            self._refresh_free_ids_state()
        print(
            "[GraphService] board updated "
            f"(b_id={board_version}, nodes={len(requested_nodes)}, edges={len(requested_edges)})"
        )
        return {"status": "ok"}

    @log_api_method_execution
    def delete_version(self, version: Any) -> dict[str, Any]:
        if version is None:
            raise BoardVersionResolutionError("version query parameter is required.")

        board_id = self._parse_requested_board_id(version)
        boards = self._list_boards()
        self._select_board(boards, self._stringify_board_id(board_id))

        self.client._execute_write(
            "board-delete",
            self._build_query("board-delete", b_id=board_id),
        )
        self._defragment_ids()
        print(f"[GraphService] board deleted: {self._stringify_board_id(board_id)}")
        return {"status": "ok"}

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
        self._refresh_free_ids_state()
        print(f"[GraphService] board created: {self._stringify_board_id(board_id)}")
        return {"status": "ok"}
