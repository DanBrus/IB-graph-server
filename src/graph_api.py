from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware

from graph_models import (
    BasicResponseDTO,
    BoardDTO,
    CanonicalEntityDTO,
    EdgeDTO,
    FreeIdsDTO,
    NodeDTO,
    VersionDTO,
)
from graph_service import (
    BoardSyncError,
    BoardVersionResolutionError,
    CanonicalEntitySyncError,
    GraphService,
)


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
    - без version -> доска с максимальным b_id
    - с version -> доска по указанному b_id
    """
    try:
        return service.get_board(version=version)
    except BoardVersionResolutionError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc


@app.put("/graph/board", response_model=BasicResponseDTO)
def update_board(payload: BoardDTO):
    """
    Полная синхронизация доски по переданному payload.
    """
    try:
        result = service.update_board(payload)
        return BasicResponseDTO(**result)
    except BoardVersionResolutionError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    except BoardSyncError as exc:
        raise HTTPException(
            status_code=status.HTTP_406_NOT_ACCEPTABLE,
            detail=exc.detail,
        ) from exc


# --------- Ноды --------- #

@app.get("/graph/nodes", response_model=List[NodeDTO])
def get_nodes(
    version: Optional[str] = Query(None),
    id: Optional[str] = Query(None),
    ids: Optional[List[str]] = Query(None),
    name: Optional[str] = Query(None),
    hasPicture: Optional[bool] = Query(None, alias="hasPicture"),
):
    """
    Все ноды доски (или по фильтру):
    - version: b_id доски (по умолчанию максимальный)
    - id: одна нода по id
    - ids: несколько id (?ids=n1&ids=n2)
    - name: фильтр по имени
    - hasPicture: true/false – наличие/отсутствие картинки
    """
    try:
        return service.get_nodes(
            version=version,
            node_id=id,
            ids=ids,
            name=name,
            has_picture=hasPicture,
        )
    except BoardVersionResolutionError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc


# --------- Рёбра --------- #

@app.get("/graph/edges", response_model=List[EdgeDTO])
def get_edges(
    version: Optional[str] = Query(None),
    id: Optional[str] = Query(None),
    ids: Optional[List[str]] = Query(None),
    nodeId: Optional[str] = Query(None, alias="nodeId"),
    from_: Optional[str] = Query(None, alias="from"),
    to: Optional[str] = Query(None),
):
    """
    Все edge (или по фильтру):
    - version: b_id доски (по умолчанию максимальный)
    - id: одно ребро по id
    - ids: несколько id (?ids=e1&ids=e2)
    - nodeId: все рёбра, где участвует указанная нода
    - from: фильтр по node1
    - to: фильтр по node2
    """
    try:
        return service.get_edges(
            version=version,
            edge_id=id,
            ids=ids,
            node_id=nodeId,
            from_id=from_,
            to_id=to,
        )
    except BoardVersionResolutionError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc


# --------- Canonical Entities --------- #

@app.get("/graph/canonical-entities", response_model=List[CanonicalEntityDTO])
def get_canonical_entities():
    """
    Все canonical-entity текущего investigation.
    """
    return service.get_canonical_entities()


@app.put("/graph/canonical-entities", response_model=BasicResponseDTO)
def update_canonical_entities(payload: List[CanonicalEntityDTO]):
    """
    Полная синхронизация списка canonical-entity текущего investigation.
    """
    try:
        result = service.update_canonical_entities(payload)
        return BasicResponseDTO(**result)
    except CanonicalEntitySyncError as exc:
        raise HTTPException(
            status_code=status.HTTP_406_NOT_ACCEPTABLE,
            detail=exc.detail,
        ) from exc


# --------- Версии доски --------- #

@app.get("/graph/free-ids", response_model=FreeIdsDTO)
def get_free_ids():
    """
    Минимальные свободные id для node / edge / chunk.
    """
    return service.get_free_ids()

@app.get("/graph/versions", response_model=List[VersionDTO])
def get_versions():
    """
    Список имеющихся b_id досок.
    """
    return service.get_versions()


# --------- Создание / удаление версии --------- #

@app.post("/graph/versions", response_model=BasicResponseDTO)
def create_version(payload: VersionDTO):
    """
    Создать пустую версию доски.
    """
    try:
        result = service.create_version(
            version=payload.version,
            name=payload.name,
            description=payload.description,
            is_published=payload.is_published,
        )
        return BasicResponseDTO(**result)
    except BoardVersionResolutionError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc

@app.delete("/graph/versions", response_model=BasicResponseDTO)
def delete_version(version: Optional[str] = Query(None)):
    """
    Удалить версию доски.
    """
    try:
        result = service.delete_version(version=version)
        return BasicResponseDTO(**result)
    except BoardVersionResolutionError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
