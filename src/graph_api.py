from typing import List, Optional

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

from graph_models import (
    BasicResponseDTO,
    BoardDTO,
    NodeDTO,
    EdgeDTO,
    VersionDTO,
    ActiveVersionDTO,
)
from graph_service import GraphService


service = GraphService()
app = FastAPI(title="Graph Server (Investigation Board)")
origins = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --------- Доска целиком --------- #

@app.get("/graph/board", response_model=BoardDTO)
def get_board(version: Optional[str] = Query(None)):
    """
    Доска расследований целиком:
    - без version -> актуальная версия
    - с version -> конкретная версия
    """
    return service.get_board(version=version)


@app.put("/graph/board", response_model=BasicResponseDTO)
def update_board(payload: BoardDTO):
    """
    Обновление существующей версии доски:
    - payload.version — обязательная версия
    - payload.nodes / payload.edges — новое состояние графа
    """
    result = service.update_graph(
        version=payload.version,
        nodes=payload.nodes,
        edges=payload.edges,
    )
    return BasicResponseDTO(**result)


# --------- Ноды --------- #

@app.get("/graph/nodes", response_model=List[NodeDTO])
def get_nodes(
    version: Optional[int] = Query(None),
    id: Optional[str] = Query(None),
    ids: Optional[List[str]] = Query(None),
    name: Optional[str] = Query(None),
    hasPicture: Optional[bool] = Query(None, alias="hasPicture"),
):
    """
    Все ноды доски (или по фильтру):
    - version: номер версии (по умолчанию актуальная)
    - id: одна нода по id
    - ids: несколько id (?ids=n1&ids=n2)
    - name: фильтр по имени
    - hasPicture: true/false – наличие/отсутствие картинки
    """
    return service.get_nodes(
        version=version,
        node_id=id,
        ids=ids,
        name=name,
        has_picture=hasPicture,
    )


# --------- Рёбра --------- #

@app.get("/graph/edges", response_model=List[EdgeDTO])
def get_edges(
    version: Optional[int] = Query(None),
    id: Optional[str] = Query(None),
    ids: Optional[List[str]] = Query(None),
    nodeId: Optional[str] = Query(None, alias="nodeId"),
    from_: Optional[str] = Query(None, alias="from"),
    to: Optional[str] = Query(None),
):
    """
    Все edge (или по фильтру):
    - version: номер версии (по умолчанию актуальная)
    - id: одно ребро по id
    - ids: несколько id (?ids=e1&ids=e2)
    - nodeId: все рёбра, где участвует указанная нода
    - from: фильтр по node1
    - to: фильтр по node2
    """
    return service.get_edges(
        version=version,
        edge_id=id,
        ids=ids,
        node_id=nodeId,
        from_id=from_,
        to_id=to,
    )


# --------- Версии доски --------- #

@app.get("/graph/versions", response_model=List[VersionDTO])
def get_versions():
    """
    Список имеющихся версий доски.
    """
    return service.get_versions()


@app.get("/graph/active_version", response_model=ActiveVersionDTO)
def get_active_version():
    """
    Номер текущей актуальной версии доски.
    """
    return ActiveVersionDTO(version=service.get_active_version())


# --------- Создание / удаление версии --------- #

@app.post("/graph/versions", response_model=BasicResponseDTO)
def create_version(payload: ActiveVersionDTO):
    """
    Создать пустую версию доски.
    """
    result = service.create_version(payload.version)
    return BasicResponseDTO(**result)

@app.delete("/graph/versions", response_model=BasicResponseDTO)
def delete_version(payload: ActiveVersionDTO):
    """
    Удалить версию доски.
    """
    result = service.delete_version(payload.version)
    return BasicResponseDTO(**result)

# --------- Изменение текущей актуальной версии --------- #

@app.post("/graph/active_version", response_model=BasicResponseDTO)
def set_active_version(payload: ActiveVersionDTO):
    """
    ИЗМЕНЕНИЕ текущей актуальной версии доски.
    """
    # Предполагаем, что SetActiveVersionRequestDTO содержит поле `version`
    result = service.set_active_version(payload.version)
    return BasicResponseDTO(**result)
