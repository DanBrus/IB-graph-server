from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any


@dataclass
class EntityAggregate:
    name: str
    entity_type: str | None = None
    candidate_types: list[str] = field(default_factory=list)
    _candidate_type_seen: set[str] = field(default_factory=set)
    picture_paths: list[str] = field(default_factory=list)
    _picture_path_seen: set[str] = field(default_factory=set)
    en_id: int | None = None

    def add_candidate_type(self, entity_type: str) -> None:
        if entity_type in self._candidate_type_seen:
            return
        self._candidate_type_seen.add(entity_type)
        self.candidate_types.append(entity_type)

    def add_picture_path(self, picture_path: str) -> None:
        if picture_path in self._picture_path_seen:
            return
        self._picture_path_seen.add(picture_path)
        self.picture_paths.append(picture_path)


@dataclass
class ChunkDraft:
    text: str
    priority: int
    migrated_to_edge: bool = False


@dataclass
class EdgeChunkDraft:
    text: str
    priority: int


def _prompt_entity_type_choice(
    *,
    entity_name: str,
    current_type: str,
    candidate_types: list[str],
    version: str,
    node_id: Any,
) -> str:
    if current_type not in candidate_types:
        candidate_types = [current_type, *candidate_types]

    default_index = candidate_types.index(current_type) + 1

    print("")
    print("[migration][warning] canonical-entity type conflict detected")
    print(f"  entity name: {entity_name!r}")
    print(f"  version: {version!r}")
    print(f"  node_id: {node_id!r}")
    print("  choose the canonical-entity type to keep:")
    for index, candidate_type in enumerate(candidate_types, start=1):
        current_marker = " (current)" if candidate_type == current_type else ""
        print(f"    {index}. {candidate_type}{current_marker}")

    while True:
        try:
            answer = input(
                f"Select type [1-{len(candidate_types)}] "
                f"(Enter keeps {default_index}): "
            ).strip()
        except EOFError as exc:
            raise RuntimeError(
                "Migration requires interactive input to resolve canonical-entity type conflicts."
            ) from exc

        if not answer:
            return candidate_types[default_index - 1]

        try:
            selected_index = int(answer)
        except ValueError:
            print("Please enter a number from the list above.")
            continue

        if 1 <= selected_index <= len(candidate_types):
            return candidate_types[selected_index - 1]

        print("Selected number is out of range. Try again.")


def _prompt_entity_name_match_choice(
    *,
    node_name: str,
    candidate_entity_name: str,
    version: str,
    board_name: str,
    node_id: Any,
    distance: int,
) -> bool:
    print("")
    print("[migration][question] fuzzy canonical-entity name match detected")
    print(f"  board version: {version!r}")
    print(f"  board name: {board_name!r}")
    print(f"  node_id: {node_id!r}")
    print(f"  node name: {node_name!r}")
    print(f"  candidate canonical-entity: {candidate_entity_name!r}")
    print(f"  edit distance: {distance}")
    print("  choose whether this node belongs to the candidate canonical-entity:")
    print("    1. yes")
    print("    2. no (default)")

    while True:
        try:
            answer = input("Select option [1-2] (Enter keeps 2): ").strip()
        except EOFError as exc:
            raise RuntimeError(
                "Migration requires interactive input to resolve fuzzy canonical-entity name matches."
            ) from exc

        if not answer:
            return False

        if answer == "1":
            return True

        if answer == "2":
            return False

        print("Please enter 1 or 2.")


def _as_text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    return str(value)


def _normalize_text(value: Any, default: str = "") -> str:
    return _as_text(value, default=default).strip()


def _normalize_optional_text(value: Any) -> str | None:
    text = _normalize_text(value)
    return text if text else None


def _normalize_bool(value: Any) -> bool:
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


def _as_int(value: Any) -> int:
    return int(value)


def _parse_board_id(version: Any) -> Decimal:
    version_text = _normalize_text(version)
    if version_text.startswith("s"):
        version_text = version_text[1:]
    try:
        return Decimal(version_text)
    except InvalidOperation as exc:
        raise ValueError(f"Failed to convert version to b_id: {version_text!r}") from exc


def _split_description_into_chunks(description: Any) -> list[ChunkDraft]:
    text = _as_text(description).replace("\r\n", "\n").replace("\r", "\n")
    if not text.strip():
        return []

    raw_chunks = re.split(r"\n\s*\n+", text)
    chunks: list[ChunkDraft] = []
    seen: set[str] = set()
    raw_priority = 0

    for raw_chunk in raw_chunks:
        chunk_text = raw_chunk.strip()
        if not chunk_text:
            continue

        if chunk_text not in seen:
            chunks.append(ChunkDraft(text=chunk_text, priority=raw_priority))
            seen.add(chunk_text)

        raw_priority += 1

    return chunks


def _max_distance_for_match(text_a: str, text_b: str) -> int | None:
    min_length = min(len(text_a), len(text_b))
    if min_length == 0:
        return None
    return int(math.ceil(min_length * 0.02))


def _levenshtein_distance_at_most(text_a: str, text_b: str, max_distance: int) -> int | None:
    if abs(len(text_a) - len(text_b)) > max_distance:
        return None

    if len(text_a) > len(text_b):
        text_a, text_b = text_b, text_a

    previous_row = list(range(len(text_a) + 1))

    for idx_b, char_b in enumerate(text_b, start=1):
        current_row = [idx_b]
        row_min = current_row[0]

        for idx_a, char_a in enumerate(text_a, start=1):
            insert_cost = current_row[idx_a - 1] + 1
            delete_cost = previous_row[idx_a] + 1
            replace_cost = previous_row[idx_a - 1] + (char_a != char_b)
            value = min(insert_cost, delete_cost, replace_cost)
            current_row.append(value)
            if value < row_min:
                row_min = value

        if row_min > max_distance:
            return None

        previous_row = current_row

    distance = previous_row[-1]
    if distance > max_distance:
        return None
    return distance


def _match_text_distance(text_a: str, text_b: str) -> int | None:
    max_distance = _max_distance_for_match(text_a, text_b)
    if max_distance is None:
        return None
    return _levenshtein_distance_at_most(text_a, text_b, max_distance)


def _match_chunk_distance(text_a: str, text_b: str) -> int | None:
    return _match_text_distance(text_a, text_b)


def _derive_edge_chunk_text(text_a: str, text_b: str) -> str:
    if len(text_a) >= len(text_b):
        return text_a
    return text_b


def _round_chunk_priority(priority_a: int, priority_b: int) -> int:
    return int(round((priority_a + priority_b) / 2))


def _match_edge_chunks(node_chunks_a: list[ChunkDraft], node_chunks_b: list[ChunkDraft]) -> list[EdgeChunkDraft]:
    candidates: list[tuple[int, int, int, int]] = []

    for index_a, chunk_a in enumerate(node_chunks_a):
        for index_b, chunk_b in enumerate(node_chunks_b):
            distance = _match_chunk_distance(chunk_a.text, chunk_b.text)
            if distance is None:
                continue
            candidates.append(
                (
                    distance,
                    abs(chunk_a.priority - chunk_b.priority),
                    index_a,
                    index_b,
                )
            )

    candidates.sort()

    used_a: set[int] = set()
    used_b: set[int] = set()
    edge_chunks: list[EdgeChunkDraft] = []

    for _, _, index_a, index_b in candidates:
        if index_a in used_a or index_b in used_b:
            continue

        chunk_a = node_chunks_a[index_a]
        chunk_b = node_chunks_b[index_b]
        used_a.add(index_a)
        used_b.add(index_b)

        chunk_a.migrated_to_edge = True
        chunk_b.migrated_to_edge = True

        edge_chunks.append(
            EdgeChunkDraft(
                text=_derive_edge_chunk_text(chunk_a.text, chunk_b.text),
                priority=_round_chunk_priority(chunk_a.priority, chunk_b.priority),
            )
        )

    return edge_chunks


def _escape_typeql_string(value: str) -> str:
    escaped = value.replace("\\", "\\\\")
    escaped = escaped.replace("\"", "\\\"")
    escaped = escaped.replace("\'", "\\\'")
    return escaped


def _build_new_query(new_client, operation: str, **params: Any) -> str:
    return new_client._build_query(operation, **params)


def _run_new_write(new_client, operation: str, **params: Any) -> None:
    query = _build_new_query(new_client, operation, **params)
    new_client._execute_write(operation, query)


def _run_new_raw_write(new_client, operation: str, query: str) -> None:
    new_client._execute_write(operation, query)


def _insert_entity_picture_path(new_client, *, en_id: int, picture_path: str) -> None:
    query = (
        "match\n"
        f"  $entity isa canonical-entity, has en_id {en_id};\n"
        "insert\n"
        f"  $entity has picture_path \"{_escape_typeql_string(picture_path)}\";\n"
        "end;"
    )
    _run_new_raw_write(new_client, "migration-add-entity-picture-path", query)


def _sorted_versions(old_client) -> list[dict[str, Any]]:
    versions_payload = old_client.get_versions()
    versions = list(versions_payload.get("versions", []))
    return sorted(versions, key=lambda item: (_parse_board_id(item["version"]), _as_text(item["version"])))


def _sorted_nodes_for_version(old_client, *, version: str) -> list[dict[str, Any]]:
    nodes = list(old_client.nodes_by_version_get(version=version))
    return sorted(nodes, key=lambda item: _as_int(item["node_id"]))


def _sorted_edges_for_version(old_client, *, version: str) -> list[dict[str, Any]]:
    edges = list(old_client.edges_by_version_get(version=version))
    return sorted(edges, key=lambda item: _as_int(item["edge_id"]))


def _resolve_entity_name_for_node(
    *,
    entities_by_name: dict[str, EntityAggregate],
    node_name: str,
    version: str,
    board_name: str,
    node_id: int,
) -> str:
    entity = entities_by_name.get(node_name)
    if entity is not None:
        return entity.name

    fuzzy_candidates: list[tuple[int, int, str]] = []
    for entity_name in entities_by_name:
        distance = _match_text_distance(node_name, entity_name)
        if distance is None:
            continue
        fuzzy_candidates.append((distance, abs(len(node_name) - len(entity_name)), entity_name))

    fuzzy_candidates.sort()

    for distance, _, entity_name in fuzzy_candidates:
        if _prompt_entity_name_match_choice(
            node_name=node_name,
            candidate_entity_name=entity_name,
            version=version,
            board_name=board_name,
            node_id=node_id,
            distance=distance,
        ):
            return entity_name

    return node_name


def _collect_entities(
    old_client, versions: list[dict[str, Any]]
) -> tuple[dict[str, EntityAggregate], dict[tuple[str, int], str]]:
    entities_by_name: dict[str, EntityAggregate] = {}
    node_entity_name_map: dict[tuple[str, int], str] = {}

    for version_meta in versions:
        version = _as_text(version_meta["version"])
        board_name = _as_text(version_meta.get("name"), default=version)
        for node in _sorted_nodes_for_version(old_client, version=version):
            node_id = _as_int(node["node_id"])
            node_name = _normalize_text(node.get("name"))
            if not node_name:
                raise ValueError(f"Node without name in version {version!r}: {node}")

            entity_name = _resolve_entity_name_for_node(
                entities_by_name=entities_by_name,
                node_name=node_name,
                version=version,
                board_name=board_name,
                node_id=node_id,
            )
            entity = entities_by_name.get(entity_name)
            if entity is None:
                entity = EntityAggregate(name=entity_name)
                entities_by_name[entity_name] = entity

            node_entity_name_map[(version, node_id)] = entity.name

            node_type = _normalize_optional_text(node.get("node_type"))
            if node_type:
                entity.add_candidate_type(node_type)
                if entity.entity_type is None:
                    entity.entity_type = node_type
                elif entity.entity_type != node_type:
                    entity.entity_type = _prompt_entity_type_choice(
                        entity_name=node_name,
                        current_type=entity.entity_type,
                        candidate_types=list(entity.candidate_types),
                        version=version,
                        node_id=node.get("node_id"),
                    )

            picture_path = _normalize_optional_text(node.get("picture_path"))
            if picture_path:
                entity.add_picture_path(picture_path)

    for index, entity_name in enumerate(sorted(entities_by_name), start=1):
        entity = entities_by_name[entity_name]
        if entity.entity_type is None:
            raise ValueError(
                f"canonical-entity {entity_name!r} has no non-empty node_type in v0.2 data"
            )
        entity.en_id = index

    return entities_by_name, node_entity_name_map


def _create_entities(new_client, entities_by_name: dict[str, EntityAggregate]) -> None:
    for entity_name in sorted(entities_by_name):
        entity = entities_by_name[entity_name]
        assert entity.en_id is not None
        assert entity.entity_type is not None

        _run_new_write(
            new_client,
            "canonical-entity-create",
            investigation_name=new_client.investigation_name,
            en_id=entity.en_id,
            name=entity.name,
            entity_type=entity.entity_type,
        )

        for picture_path in entity.picture_paths:
            _insert_entity_picture_path(
                new_client,
                en_id=entity.en_id,
                picture_path=picture_path,
            )


def migrate(old_client, new_client) -> bool:
    """
    Schema migration v0.2 -> v0.3.

    Assumes the orchestration workflow has already:
    - imported the old dump into old_DB;
    - created new_DB with schema v0.3;
    - created the target investigation record in new_DB.
    """
    versions = _sorted_versions(old_client)
    entities_by_name, node_entity_name_map = _collect_entities(old_client, versions)
    _create_entities(new_client, entities_by_name)

    next_node_id = 1
    next_edge_id = 1
    next_chunk_id = 1

    board_id_map: dict[str, Decimal] = {}
    node_id_map: dict[tuple[str, int], int] = {}
    edge_id_map: dict[tuple[str, int], int] = {}

    for version_meta in versions:
        version = _as_text(version_meta["version"])
        board_id = _parse_board_id(version)
        board_id_map[version] = board_id

        _run_new_write(
            new_client,
            "board-create",
            investigation_name=new_client.investigation_name,
            b_id=board_id,
            name=_as_text(version_meta.get("name"), default=version),
            description=_as_text(version_meta.get("description")),
            is_published=new_client._typeql_bool(_normalize_bool(version_meta.get("is_published"))),
        )

        old_nodes = _sorted_nodes_for_version(old_client, version=version)
        node_chunks: dict[int, list[ChunkDraft]] = {}

        for old_node in old_nodes:
            old_node_id = _as_int(old_node["node_id"])
            new_node_id = next_node_id
            next_node_id += 1

            node_id_map[(version, old_node_id)] = new_node_id
            entity_name = node_entity_name_map[(version, old_node_id)]
            entity = entities_by_name[entity_name]
            assert entity.en_id is not None

            _run_new_write(
                new_client,
                "node-create",
                b_id=board_id,
                en_id=entity.en_id,
                n_id=new_node_id,
                pos_x=float(old_node.get("pos_x") or 0.0),
                pos_y=float(old_node.get("pos_y") or 0.0),
            )

            node_chunks[old_node_id] = _split_description_into_chunks(old_node.get("description"))

        edge_chunk_drafts: dict[int, list[EdgeChunkDraft]] = {}
        old_edges = _sorted_edges_for_version(old_client, version=version)

        for old_edge in old_edges:
            old_edge_id = _as_int(old_edge["edge_id"])
            new_edge_id = next_edge_id
            next_edge_id += 1

            old_node_1_id = _as_int(old_edge["node1"])
            old_node_2_id = _as_int(old_edge["node2"])
            new_node_1_id = node_id_map[(version, old_node_1_id)]
            new_node_2_id = node_id_map[(version, old_node_2_id)]

            edge_id_map[(version, old_edge_id)] = new_edge_id

            _run_new_write(
                new_client,
                "edge-create",
                b_id=board_id,
                endpoint_1_n_id=new_node_1_id,
                endpoint_2_n_id=new_node_2_id,
                ed_id=new_edge_id,
            )

            edge_chunk_drafts[new_edge_id] = _match_edge_chunks(
                node_chunks_a=node_chunks.get(old_node_1_id, []),
                node_chunks_b=node_chunks.get(old_node_2_id, []),
            )

        for old_edge in old_edges:
            old_edge_id = _as_int(old_edge["edge_id"])
            new_edge_id = edge_id_map[(version, old_edge_id)]
            for edge_chunk in edge_chunk_drafts.get(new_edge_id, []):
                _run_new_write(
                    new_client,
                    "text-chunk-create-for-edge",
                    ed_id=new_edge_id,
                    c_id=next_chunk_id,
                    description=edge_chunk.text,
                    chunk_priority=edge_chunk.priority,
                    timecode="",
                )
                next_chunk_id += 1

        for old_node in old_nodes:
            old_node_id = _as_int(old_node["node_id"])
            new_node_id = node_id_map[(version, old_node_id)]

            for chunk in sorted(node_chunks.get(old_node_id, []), key=lambda item: item.priority):
                if chunk.migrated_to_edge:
                    continue

                _run_new_write(
                    new_client,
                    "text-chunk-create-for-node",
                    n_id=new_node_id,
                    c_id=next_chunk_id,
                    description=chunk.text,
                    chunk_priority=chunk.priority,
                    timecode="",
                )
                next_chunk_id += 1

    return True
