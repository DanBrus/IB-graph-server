from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class BoardSchemaRuntimeConfig:
    version: str
    supports_is_published: bool = False


# v01_to_v02_migration: this module is the single switch point for the app's active board schema.
BOARD_SCHEMA_VERSION = os.getenv("BOARD_SCHEMA_VERSION", "v0.2")

_KNOWN_SCHEMA_CONFIGS: dict[str, BoardSchemaRuntimeConfig] = {
    "v0.1": BoardSchemaRuntimeConfig(
        version="v0.1",
        supports_is_published=False,
    ),
    "v0.2": BoardSchemaRuntimeConfig(
        version="v0.2",
        supports_is_published=True,
    ),
}

CURRENT_BOARD_SCHEMA = _KNOWN_SCHEMA_CONFIGS.get(
    BOARD_SCHEMA_VERSION,
    BoardSchemaRuntimeConfig(version=BOARD_SCHEMA_VERSION),
)
