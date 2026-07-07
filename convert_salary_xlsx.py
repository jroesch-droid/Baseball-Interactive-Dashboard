"""
convert_salary_xlsx.py
======================
One-shot converter: reads a Padres contract spreadsheet (the
Padres_salary.xlsx layout — header on row 4, per-year columns with
values like "$25.09M" / "$900k" / "FA" / blank) and writes the
salaries.json the nightly pipeline consumes.

Usage:
    python convert_salary_xlsx.py Padres_salary.xlsx [season]

Rules:
  * Rows with an explicit dollar value for the season are written as-is.
  * "FA" rows are skipped (not on this season's payroll).
  * Blank rows inside the player block are pre-arbitration players with
    no listed figure; they are written at PRE_ARB_ESTIMATE_USD and their
    names recorded under "_pre_arb_estimated" for transparency.
    VERIFY that constant against the current CBA league minimum.
  * Footer/legend rows (no Name) are ignored.
"""
from __future__ import annotations

import json
import re
import sys
from datetime import date

import pandas as pd

# I believe the 2026 MLB league minimum under the 2022–26 CBA is
# $780,000, but VERIFY before treating pre-arb figures as decision-grade.
PRE_ARB_ESTIMATE_USD = 780_000

MONEY_RE = re.compile(r"^\$([\d.,]+)\s*([MmKk])")


def parse_money(text) -> int | None:
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return None
    m = MONEY_RE.match(str(text).strip())
    if not m:
        return None
    val = float(m.group(1).replace(",", ""))
    return int(round(val * (1e6 if m.group(2).lower() == "m" else 1e3)))


def main() -> int:
    xlsx = sys.argv[1] if len(sys.argv) > 1 else "Padres_salary.xlsx"
    season = sys.argv[2] if len(sys.argv) > 2 else str(date.today().year)
    df = pd.read_excel(xlsx, sheet_name=0, header=3)
    df.columns = [str(c).strip() for c in df.columns]
    if season not in df.columns:
        print(f"ERROR: no '{season}' column; found {list(df.columns)}")
        return 1

    players: dict[str, int] = {}
    pre_arb: list[str] = []
    skipped_fa: list[str] = []
    skipped_non_player: list[str] = []
    for _, row in df.iterrows():
        name = row.get("Name")
        if not isinstance(name, str) or not name.strip():
            continue                                   # footer / legend rows
        name = name.strip()
        # Real player rows carry numeric service time; the footer legend
        # ("Signed", "Payroll (options)", ...) does not.
        if pd.isna(pd.to_numeric(row.get("SrvTm"), errors="coerce")):
            skipped_non_player.append(name)
            continue
        cell = row.get(season)
        usd = parse_money(cell)
        if usd is not None:
            players[name] = usd
        elif isinstance(cell, str) and cell.strip().upper() in ("FA", "ARB"):
            skipped_fa.append(name)
        else:                                          # blank → pre-arb
            players[name] = PRE_ARB_ESTIMATE_USD
            pre_arb.append(name)

    out = {
        "source": f"manual — {xlsx} (front-office contract sheet)",
        "as_of": date.today().isoformat(),
        "season": int(season),
        "_pre_arb_estimated": pre_arb,
        "_pre_arb_note": (f"Players listed above had no {season} figure in the "
                          f"sheet and were written at ${PRE_ARB_ESTIMATE_USD:,} "
                          "(league-minimum estimate — verify against the CBA)."),
        "_skipped_not_on_payroll": skipped_fa,
        "_skipped_non_player_rows": skipped_non_player,
        "players": players,
    }
    with open("salaries.json", "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
    total = sum(players.values()) / 1e6
    print(f"salaries.json written: {len(players)} players "
          f"({len(players) - len(pre_arb)} explicit, {len(pre_arb)} pre-arb "
          f"estimated, {len(skipped_fa)} skipped as FA/Arb) · "
          f"{season} total ${total:.1f}M")
    return 0


if __name__ == "__main__":
    sys.exit(main())
