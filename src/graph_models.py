from typing import List, Optional
from pydantic import BaseModel

class BasicResponseDTO(BaseModel):
    status: str

class NodeDTO(BaseModel):
    node_id: int
    name: str
    pos_x: float
    pos_y: float
    node_type: Optional[str] = None
    picture_path: Optional[str] = None
    description: Optional[str] = None

class EdgeDTO(BaseModel):
    edge_id: int
    node1: int
    node2: int
    description: Optional[str] = None

class BoardDTO(BaseModel):
    nodes: List[NodeDTO]
    edges: List[EdgeDTO]
    version: str
    description: Optional[str] = None
    board_name: Optional[str] = None

class VersionDTO(BaseModel):
    version: str
    name: str
    description: str

class ActiveVersionDTO(BaseModel):
    version: str

class CreateVersionRequestDTO(BaseModel):
    baseVersion: Optional[int] = None
    nodes: List[NodeDTO]
    edges: List[EdgeDTO]
