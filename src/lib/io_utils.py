"""
I/O utilities — centralised path resolution, universe loading, and JSON writing.

All paths are computed relative to this file's location so the project works
regardless of the working directory when scripts are invoked.

Directory layout assumed:
    stock-selection-dashboard/
        src/lib/io_utils.py   ← this file
        config/universe.csv
        data/raw/
        data/processed/
        app/data/
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Root and standard paths
# ---------------------------------------------------------------------------

# Two levels up from src/lib/ → project root
ROOT: Path = Path(__file__).resolve().parents[2]

DATA_RAW: Path = ROOT / "data" / "raw"
DATA_PROCESSED: Path = ROOT / "data" / "processed"
APP_DATA: Path = ROOT / "app" / "data"

UNIVERSE_PATH: Path = ROOT / "config" / "universe.csv"


def _ensure_dir(path: Path) -> None:
    """Create directory (and parents) if it does not exist."""
    path.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Universe
# ---------------------------------------------------------------------------

def read_universe(enabled_only: bool = True) -> pd.DataFrame:
    """
    Load config/universe.csv into a DataFrame.

    Expected columns (at minimum):
        ticker   — stock symbol, e.g. "AAPL"
        cik      — SEC CIK number (int or zero-padded string)
        sector   — GICS sector string
        name     — company display name
        enabled  — 1 to include in pipeline, 0 to skip

    Parameters
    ----------
    enabled_only : bool
        If True (default) return only rows where ``enabled == 1``.

    Returns
    -------
    pd.DataFrame
        Universe with normalised column types. CIK is stored as a zero-padded
        10-character string for direct use with SEC endpoints.

    Raises
    ------
    FileNotFoundError
        If config/universe.csv does not exist.
    ValueError
        If required columns are missing.
    """
    if not UNIVERSE_PATH.exists():
        raise FileNotFoundError(f"Universe file not found: {UNIVERSE_PATH}")

    df = pd.read_csv(UNIVERSE_PATH, dtype=str)
    df.columns = [c.strip().lower() for c in df.columns]

    # Normalise: universe.csv may use 'symbol' as header; pipeline expects 'ticker'
    if "symbol" in df.columns and "ticker" not in df.columns:
        df = df.rename(columns={"symbol": "ticker"})

    required = {"ticker", "cik", "sector", "name", "enabled"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"universe.csv is missing columns: {missing}")

    # Normalise types
    df["ticker"] = df["ticker"].str.strip().str.upper()
    df["cik"] = df["cik"].str.strip().str.zfill(10)
    df["enabled"] = pd.to_numeric(df["enabled"], errors="coerce").fillna(0).astype(int)
    df["name"] = df["name"].str.strip()
    df["sector"] = df["sector"].str.strip()

    if enabled_only:
        df = df[df["enabled"] == 1].reset_index(drop=True)
        logger.debug("Universe loaded: %d enabled tickers", len(df))
    else:
        logger.debug("Universe loaded: %d total tickers", len(df))

    return df


# ---------------------------------------------------------------------------
# JSON I/O
# ---------------------------------------------------------------------------

def write_json(obj: Any, path: Path | str, indent: int = 2) -> None:
    """
    Serialise *obj* to JSON and write it to *path*.

    Creates parent directories automatically.
    Uses a compact but human-readable format (indent=2 by default).

    Parameters
    ----------
    obj : Any
        JSON-serialisable object (dict, list, etc.).
    path : Path | str
        Destination file path.
    indent : int
        JSON indentation level. Pass 0 or None for minified output.
    """
    path = Path(path)
    _ensure_dir(path.parent)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=indent or None, ensure_ascii=False, default=_json_default)

    logger.debug("Wrote %s (%.1f KB)", path.name, path.stat().st_size / 1024)


def read_json(path: Path | str) -> Any:
    """
    Read and parse a JSON file.

    Parameters
    ----------
    path : Path | str
        Source file path.

    Returns
    -------
    Any
        Parsed JSON content.

    Raises
    ------
    FileNotFoundError
        If the file does not exist.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"JSON file not found: {path}")
    with open(path, encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _json_default(obj: Any) -> Any:
    """
    Fallback serialiser for types not natively supported by json.dump.
    Handles pandas Timestamp, numpy scalars, and Path objects.
    """
    # pandas / numpy
    try:
        import numpy as np  # noqa: PLC0415

        if isinstance(obj, (np.integer,)):
            return int(obj)
        if isinstance(obj, (np.floating,)):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
    except ImportError:
        pass

    try:
        import pandas as pd  # noqa: PLC0415

        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        if isinstance(obj, pd.NA.__class__):
            return None
    except ImportError:
        pass

    if isinstance(obj, Path):
        return str(obj)

    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serialisable")
