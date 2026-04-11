from __future__ import annotations

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from src.etl.config import get_settings
from src.etl.pipeline import run_pipeline


if __name__ == "__main__":
    settings = get_settings()
    summary = run_pipeline(settings)
    print(json.dumps(summary, indent=2))

