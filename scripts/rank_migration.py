#!/usr/bin/env python3
"""D-gate (INIT-22 M1): rank-migration report, old methodology vs new.

Read-only. Compares two ranked_stocks.json snapshots by ticker and prints, for a
HUMAN reviewer (there is no ground-truth ranking in M1), how much the ranking
moved: Spearman rank correlation, top-N churn (in/out), the biggest movers, and
the old vs new top-10. Not a pass/fail gate — the artifact you read before the
one-shot regime flip.

Usage:
    python scripts/rank_migration.py OLD.json NEW.json [top_n]
"""
from __future__ import annotations

import json
import sys
from pathlib import Path


def _load(path: str) -> dict[str, int]:
    rows = json.loads(Path(path).read_text(encoding="utf-8"))
    return {r["ticker"]: r["rank"] for r in rows}


def _spearman(old: dict[str, int], new: dict[str, int], common: list[str]) -> float:
    # Spearman = Pearson on the rank values themselves (they are already ranks).
    o = [old[t] for t in common]
    n = [new[t] for t in common]
    k = len(common)
    mo, mn = sum(o) / k, sum(n) / k
    cov = sum((a - mo) * (b - mn) for a, b in zip(o, n))
    so = sum((a - mo) ** 2 for a in o) ** 0.5
    sn = sum((b - mn) ** 2 for b in n) ** 0.5
    return cov / (so * sn) if so and sn else float("nan")


def main(old_path: str, new_path: str, top_n: int = 20) -> int:
    old, new = _load(old_path), _load(new_path)
    common = sorted(set(old) & set(new))
    print(f"universe: old={len(old)} new={len(new)} common={len(common)}")
    print(f"Spearman rank corr (composite): {_spearman(old, new, common):.4f}")

    old_top = {t for t, r in old.items() if r <= top_n}
    new_top = {t for t, r in new.items() if r <= top_n}
    entered = sorted(new_top - old_top, key=lambda t: new[t])
    left = sorted(old_top - new_top, key=lambda t: old[t])
    print(f"\ntop-{top_n} churn: {len(new_top & old_top)}/{top_n} stayed, "
          f"{len(entered)} entered, {len(left)} left")
    print(f"  entered: {', '.join(f'{t}(#{new[t]})' for t in entered)}")
    print(f"  left   : {', '.join(f'{t}(old #{old[t]})' for t in left)}")

    movers = sorted(common, key=lambda t: new[t] - old[t])
    def fmt(t: str) -> str:
        return f"{t}: {old[t]} -> {new[t]} ({new[t] - old[t]:+d})"
    print("\nbiggest RISERS (up the ranking):")
    for t in movers[:10]:
        print("  " + fmt(t))
    print("biggest FALLERS:")
    for t in movers[-10:][::-1]:
        print("  " + fmt(t))

    inv_old = {r: t for t, r in old.items()}
    inv_new = {r: t for t, r in new.items()}
    print("\ntop-10   OLD           NEW")
    for i in range(1, 11):
        print(f"  #{i:<3d}   {inv_old.get(i, '—'):<12s}  {inv_new.get(i, '—')}")
    return 0


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("usage: rank_migration.py OLD.json NEW.json [top_n]", file=sys.stderr)
        raise SystemExit(2)
    raise SystemExit(main(sys.argv[1], sys.argv[2],
                          int(sys.argv[3]) if len(sys.argv) > 3 else 20))
