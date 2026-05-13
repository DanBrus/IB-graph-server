from __future__ import annotations

from typing import Any


def _as_text(value: Any, default: str = "") -> str:
    """
    Приводит значения к строке и подставляет default для None.

    Этот helper обычно удобно оставлять даже в "простых" миграциях:
    так проще пережить частично пустые поля и не размазывать одинаковые
    проверки по всему migration-script.
    """
    if value is None:
        return default
    return str(value)


def _copy_version_with_publish_flag(new_client, version_meta: dict[str, Any]) -> str:
    """
    Создаёт board-version уже по новой schema v0.2.

    Главное отличие от 1-в-1 копии:
    - в v0.2 у board-version появился обязательный атрибут is_published;
    - для всех существующих версий мы принудительно выставляем его в true.

    Именно такие места обычно и меняются при schema migration:
    сюда добавляют новые обязательные поля, вычисляют значения по старым
    данным и т.д.
    """
    version = _as_text(version_meta["version"])

    new_client.graph_by_version_create(
        version=version,
        name=_as_text(version_meta.get("name"), default=version),
        description=_as_text(version_meta.get("description")),
        is_published=True,
    )
    return version


def _copy_nodes_for_version(old_client, new_client, *, version: str) -> None:
    """
    Копирует ноды выбранной версии без трансформации.

    Если в будущей schema у нод появятся новые обязательные поля,
    менять mapping нужно именно здесь.
    """
    for node in old_client.nodes_by_version_get(version=version):
        new_client.node_create(
            node_id=_as_text(node["node_id"]),
            name=_as_text(node.get("name")),
            pos_x=float(node.get("pos_x") or 0.0),
            pos_y=float(node.get("pos_y") or 0.0),
            picture_path=_as_text(node.get("picture_path")),
            node_type=_as_text(node.get("node_type")),
            description=_as_text(node.get("description")),
            version=version,
        )


def _copy_edges_for_version(old_client, new_client, *, version: str) -> None:
    """
    Копирует рёбра после нод.

    Порядок важен: edge ссылается на уже существующие endpoint-ноды.
    """
    for edge in old_client.edges_by_version_get(version=version):
        new_client.edge_create(
            edge_id=_as_text(edge["edge_id"]),
            node1_id=_as_text(edge["node1"]),
            node2_id=_as_text(edge["node2"]),
            description=_as_text(edge.get("description")),
            version=version,
        )


def _copy_active_version(old_client, new_client) -> None:
    """
    Переносит active_version в новую БД, если она была определена.
    """
    try:
        old_client.load_active_version()
    except Exception:
        return

    active_version = old_client.get_active_version()
    if active_version:
        new_client.set_active_version(version=_as_text(active_version))


def migrate(old_client, new_client) -> bool:
    """
    Пример миграции со schema v0.1 на v0.2.

    Предполагается, что:
    - old_client работает со старыми шаблонами v0.1;
    - new_client работает с новыми шаблонами v0.2;
    - основная structural-разница между версиями сейчас состоит в том,
      что у board-version появился обязательный bool-атрибут is_published.

    Возвращаем True при успехе. Исключение или False будут трактоваться
    orchestration-скриптом как ошибка миграции.
    """
    versions_payload = old_client.get_versions()
    versions = versions_payload.get("versions", [])

    created_versions: list[str] = []
    for version_meta in versions:
        created_versions.append(
            _copy_version_with_publish_flag(new_client, version_meta)
        )

    for version in created_versions:
        _copy_nodes_for_version(old_client, new_client, version=version)
        _copy_edges_for_version(old_client, new_client, version=version)

    _copy_active_version(old_client, new_client)

    return True
