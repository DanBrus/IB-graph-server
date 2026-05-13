from __future__ import annotations

from typing import Any


def _as_text(value: Any, default: str = "") -> str:
    """
    Нормализует текстовые поля, которые могут быть None.

    В простом 1-в-1 сценарии это удобно, чтобы migration-script не падал
    на необязательных атрибутах. В более сложных миграциях здесь можно:
    - переименовывать значения;
    - чистить мусорные данные;
    - раскладывать старое поле на несколько новых.
    """
    if value is None:
        return default
    return str(value)


def _copy_version(new_client, version_meta: dict[str, Any]) -> str:
    """
    Создаёт в новой БД board-version с теми же метаданными.

    ВАЖНО: версии надо создавать до копирования нод и рёбер, потому что
    и ноды, и рёбра привязаны к конкретной версии доски.
    """
    version = _as_text(version_meta["version"])

    new_client.graph_by_version_create(
        version=version,
        name=_as_text(version_meta.get("name"), default=version),
        description=_as_text(version_meta.get("description")),
    )
    return version


def _copy_nodes_for_version(old_client, new_client, *, version: str) -> None:
    """
    Копирует все ноды выбранной версии.

    Если следующая schema будет отличаться от текущей, то это место обычно
    становится основной точкой изменения:
    - можно маппить старые поля на новые;
    - можно пропускать устаревшие поля;
    - можно добавлять вычисляемые значения.
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
    Копирует все рёбра выбранной версии.

    Рёбра копируются после нод, потому что они ссылаются на endpoint-ы.
    Если в будущей миграции изменится модель связей, перерабатывать логику
    нужно именно здесь.
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
    Переносит active_version, если она вообще задана в старой БД.

    Для 1-в-1 копии это безопасно. В более сложных миграциях можно:
    - не переносить active_version вообще;
    - заменить её на новую версию по умолчанию;
    - валидировать, что версия сохранилась после трансформации.
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
    Пример миграции "1-в-1" из старой БД в новую.

    Что уже сделал основной workflow до вызова этой функции:
    1. Поднял временный TypeDB.
    2. Импортировал старый dump в old_DB.
    3. Создал new_DB по целевой schema.
    4. Передал сюда два готовых TypeDBClient.

    Что должна сделать эта функция:
    1. Прочитать данные из old_client.
    2. Записать данные в new_client.
    3. Вернуть True/None при успехе или False при ожидаемой ошибке.

    Если здесь вылетит исключение, основной workflow тоже завершится ошибкой.
    """
    versions_payload = old_client.get_versions()
    versions = versions_payload.get("versions", [])

    # Сначала создаём все версии доски в новой БД.
    # Это удобно даже для будущих миграций: структура переносится отдельно
    # от наполнения, и дальше уже можно по версиям копировать содержимое.
    ordered_versions: list[str] = []
    for version_meta in versions:
        version = _copy_version(new_client, version_meta)
        ordered_versions.append(version)

    # Затем по каждой версии переносим ноды, а после них рёбра.
    # Такой порядок нужен из-за ссылочной зависимости рёбер от нод.
    for version in ordered_versions:
        _copy_nodes_for_version(old_client, new_client, version=version)
        _copy_edges_for_version(old_client, new_client, version=version)

    # В конце восстанавливаем active_version.
    _copy_active_version(old_client, new_client)

    return True
