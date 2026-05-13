from __future__ import annotations


def migrate(old_client, new_client) -> bool:
    """
    Пример "прямой" миграции для близких схем.

    Этот скрипт годится как стартовая точка, если новая схема по смыслу
    совместима со старой и данные можно переносить почти один-в-один.
    Для реальной schema migration обычно нужно заменить этот файл своим.
    """
    versions_payload = old_client.get_versions()
    versions = versions_payload.get("versions", [])

    for version_meta in versions:
        version = str(version_meta["version"])
        new_client.graph_by_version_create(
            version=version,
            name=str(version_meta.get("name") or version),
            description=str(version_meta.get("description") or ""),
        )

        for node in old_client.nodes_by_version_get(version=version):
            new_client.node_create(
                node_id=str(node["node_id"]),
                name=str(node.get("name") or ""),
                pos_x=float(node.get("pos_x") or 0.0),
                pos_y=float(node.get("pos_y") or 0.0),
                picture_path=str(node.get("picture_path") or ""),
                node_type=str(node.get("node_type") or ""),
                description=str(node.get("description") or ""),
                version=version,
            )

        for edge in old_client.edges_by_version_get(version=version):
            new_client.edge_create(
                edge_id=str(edge["edge_id"]),
                node1_id=str(edge["node1"]),
                node2_id=str(edge["node2"]),
                description=str(edge.get("description") or ""),
                version=version,
            )

    try:
        old_client.load_active_version()
    except Exception:
        return True

    active_version = old_client.get_active_version()
    if active_version:
        new_client.set_active_version(version=str(active_version))

    return True
