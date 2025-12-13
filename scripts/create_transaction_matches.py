"""Compatibility wrapper.

The implementation was moved to ``src/etl/create_transaction_matches.py``.
This script remains so existing commands keep working.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.etl.create_transaction_matches import main


if __name__ == "__main__":
    main()
