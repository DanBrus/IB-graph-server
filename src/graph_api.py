from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware

from graph_models import (
    BasicResponseDTO,
    BoardDTO,
    CanonicalEntityDTO,
    NodeDTO,
    EdgeDTO,
    VersionDTO,
)
from graph_service import BoardVersionResolutionError, GraphService


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
    Обновление доски временно отключено.
    """
    print(f"[graph_api] PUT /graph/board rejected for version={payload.version!r}")
    raise HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail="Board updates are temporarily disabled.",
    )


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


# --------- Версии доски --------- #

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
def delete_version(version: Optional[float] = Query(None)):
    """
    Удалить версию доски.
    """
    print(f"[graph_api] DELETE /graph/versions rejected for version={version!r}")
    raise HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail="Board deletion is temporarily disabled.",
    )
