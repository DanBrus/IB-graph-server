from typing import List, Optional
from pydantic import BaseModel, Field

class BasicResponseDTO(BaseModel):
    status: str

class ChunkDTO(BaseModel):
    c_id: int
    description: str
    chunk_priority: int
    timecode: str
    board_source: Optional[float] = None

class NodeDTO(BaseModel):
    node_id: int
    ce_id: int
    name: str
    pos_x: float
    pos_y: float
    node_type: Optional[str] = None
    picture_path: Optional[str] = None
    description: List[ChunkDTO] = Field(default_factory=list)

class EdgeDTO(BaseModel):
    edge_id: int
    node1: int
    node2: int
    description: List[ChunkDTO] = Field(default_factory=list)

class CanonicalEntityDTO(BaseModel):
    en_id: int
    name: str
    entity_type: str
    picture_paths: List[str] = Field(default_factory=list)
    merged_to: Optional[int] = None

class FreeIdsDTO(BaseModel):
    ce_id: int
    node_id: int
    edge_id: int
    chunk_id: int

class BoardDTO(BaseModel):
    nodes: List[NodeDTO]
    edges: List[EdgeDTO]
    version: float
    description: Optional[str] = None
    board_name: Optional[str] = None
    is_published: Optional[bool] = None

class VersionDTO(BaseModel):
    version: float
    name: str
    description: str
    is_published: Optional[bool] = None
