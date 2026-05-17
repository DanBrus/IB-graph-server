from __future__ import annotations

import os

# Active schema version used to resolve DB templates at runtime.
BOARD_SCHEMA_VERSION = os.getenv("BOARD_SCHEMA_VERSION", "v0.3")
