from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from src.etl.config import get_settings
from src.etl.pipeline import bootstrap_source_database


if __name__ == "__main__":
    settings = get_settings()
    db_path = bootstrap_source_database(settings)
    print(f"Grammys source database ready at: {db_path}")

