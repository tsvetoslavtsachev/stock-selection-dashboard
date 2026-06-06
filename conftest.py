"""
pytest bootstrap for the stock-selection-dashboard.

Ensures the project root is on ``sys.path`` so ``from src.lib...`` imports resolve
regardless of the working directory or how pytest is invoked (``pytest``,
``python -m pytest``, IDE runners, or CI). Without this, the ``tests/`` directory
(which has no ``__init__.py``) gets prepended instead of the project root and the
``src`` package fails to import.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
