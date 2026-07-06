"""
update_padres_stats.py
======================
Nightly data pipeline for a San Diego Padres dashboard.

Fetches (for the *current* season, resolved dynamically):
  1. NL West standings          -> pybaseball.standings()
  2. Padres team batting stats  -> pybaseball.team_batting()   (FanGraphs)
  3. Padres team pitching stats -> pybaseball.team_pitching()  (FanGraphs)
  4. Player-level Statcast logs -> pybaseball.statcast_batter() / statcast_pitcher()

Cleans everything, then atomically overwrites `padres_live_data.json`
so the dashboard always reads a complete, fresh payload.

NOTE ON PLAYERS: Roster moves happen. Edit KEY_PLAYERS below to match the
current roster. (Dylan Cease signed with Toronto before the 2026 season,
so he is intentionally NOT in the default list.)
"""

from __future__ import annotations

import json
import logging
import os
import sys
import tempfile
from datetime import date, datetime, timezone

import numpy as np
import pandas as pd
from pybaseball import (
    cache,
    playerid_lookup,
    standings,
    statcast_batter,
    statcast_pitcher,
    team_batting,
    team_pitching,
)

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
TEAM_NAME_BREF = "San Diego Padres"   # Name used by Baseball-Reference standings
TEAM_ABBREV_FG = "SDP"                # Abbreviation used by FanGraphs team tables
OUTPUT_FILE = "padres_live_data.json"

# (first_name, last_name, role) — role is "batter" or "pitcher".
# EDIT THIS LIST whenever the roster changes.
KEY_PLAYERS: list[tuple[str, str, str]] = [
    ("Manny", "Machado", "batter"),
    ("Fernando", "Tatis", "batter"),   # accent stripped for the lookup index
    ("Nick", "Pivetta", "pitcher"),    # verify vs. current rotation
]

# Statcast columns worth shipping to a dashboard (raw feed has 90+ columns).
STATCAST_KEEP_COLS = [
    "game_date", "player_name", "pitch_type", "release_speed", "release_spin_rate",
    "events", "description", "launch_speed", "launch_angle", "hit_distance_sc",
    "estimated_ba_using_speedangle", "estimated_woba_using_speedangle",
    "bb_type", "balls", "strikes", "outs_when_up", "inning",
    "home_team", "away_team", "zone", "p_throws", "stand",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("padres-pipeline")

# Cache pybaseball requests locally — keeps us polite to the data sources
# and speeds up re-runs on failure/retry.
cache.enable()


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def current_season() -> int:
    """Resolve the season year dynamically (no hard-coded 2026)."""
    today = date.today()
    # Before March there is no current-season data yet; fall back to last year.
    return today.year if today.month >= 3 else today.year - 1


def season_start(year: int) -> str:
    """Conservative season start date for Statcast queries (covers early openers)."""
    return f"{year}-03-01"


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standard cleaning applied to every dataset:
      * strip whitespace from column names
      * convert numeric-looking object columns to numbers
      * replace inf with NaN
      * NaN -> None happens later during JSON serialization
    """
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    for col in df.columns:
        if df[col].dtype == object:
            converted = pd.to_numeric(df[col], errors="coerce")
            # Only adopt the numeric version if it didn't destroy real text data
            if converted.notna().sum() >= df[col].notna().sum():
                df[col] = converted

    df = df.replace([np.inf, -np.inf], np.nan)
    return df


def df_to_records(df: pd.DataFrame) -> list[dict]:
    """DataFrame -> list of dicts with JSON-safe values (NaN -> None, dates -> ISO)."""
    if df is None or df.empty:
        return []
    df = df.copy()
    for col in df.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns:
        df[col] = df[col].dt.strftime("%Y-%m-%d")
    # NaN/NaT -> None so json.dump emits null instead of crashing on NaN
    df = df.astype(object).where(pd.notna(df), None)
    return df.to_dict(orient="records")


# --------------------------------------------------------------------------- #
# Fetchers
# --------------------------------------------------------------------------- #
def fetch_standings(year: int) -> list[dict]:
    """Return the division table that contains the Padres (NL West)."""
    log.info("Fetching %s standings ...", year)
    try:
        all_divisions = standings(year)  # list of DataFrames, one per division
    except Exception as exc:  # noqa: BLE001 — keep pipeline alive per-section
        log.error("Standings fetch failed: %s", exc)
        return []

    for division_df in all_divisions:
        div = clean_dataframe(division_df)
        team_col = div.columns[0] if len(div.columns) else None
        if team_col and div[team_col].astype(str).str.contains("Padres", case=False).any():
            log.info("Found Padres division table (%d teams).", len(div))
            return df_to_records(div)

    log.warning("Padres not found in any standings table.")
    return []


def fetch_team_stats(year: int) -> tuple[list[dict], list[dict]]:
    """Padres team batting + pitching rows from the FanGraphs leaderboards."""
    batting_records: list[dict] = []
    pitching_records: list[dict] = []

    log.info("Fetching %s team batting ...", year)
    try:
        bat = clean_dataframe(team_batting(year))
        if "Team" in bat.columns:
            bat = bat[bat["Team"].astype(str).str.upper() == TEAM_ABBREV_FG]
        batting_records = df_to_records(bat)
    except Exception as exc:  # noqa: BLE001
        log.error("Team batting fetch failed: %s", exc)

    log.info("Fetching %s team pitching ...", year)
    try:
        pit = clean_dataframe(team_pitching(year))
        if "Team" in pit.columns:
            pit = pit[pit["Team"].astype(str).str.upper() == TEAM_ABBREV_FG]
        pitching_records = df_to_records(pit)
    except Exception as exc:  # noqa: BLE001
        log.error("Team pitching fetch failed: %s", exc)

    return batting_records, pitching_records


def lookup_mlbam_id(first: str, last: str) -> int | None:
    """Resolve a player's MLBAM id via the Chadwick register (fuzzy fallback)."""
    try:
        result = playerid_lookup(last, first, fuzzy=True)
        if result.empty:
            return None
        # Prefer the most recent active player when multiple rows match
        result = result.sort_values("mlb_played_last", ascending=False)
        mlbam = result.iloc[0]["key_mlbam"]
        return int(mlbam) if pd.notna(mlbam) else None
    except Exception as exc:  # noqa: BLE001
        log.error("ID lookup failed for %s %s: %s", first, last, exc)
        return None


def summarize_batter(df: pd.DataFrame) -> dict:
    """Small aggregate block so the dashboard doesn't have to recompute."""
    batted = df.dropna(subset=["launch_speed"]) if "launch_speed" in df else pd.DataFrame()
    return {
        "pitches_seen": int(len(df)),
        "batted_ball_events": int(len(batted)),
        "avg_exit_velocity": round(float(batted["launch_speed"].mean()), 2) if len(batted) else None,
        "max_exit_velocity": round(float(batted["launch_speed"].max()), 2) if len(batted) else None,
        "avg_launch_angle": round(float(batted["launch_angle"].mean()), 2)
        if len(batted) and "launch_angle" in batted else None,
        "home_runs": int((df["events"] == "home_run").sum()) if "events" in df else 0,
    }


def summarize_pitcher(df: pd.DataFrame) -> dict:
    speeds = df["release_speed"].dropna() if "release_speed" in df else pd.Series(dtype=float)
    spins = df["release_spin_rate"].dropna() if "release_spin_rate" in df else pd.Series(dtype=float)
    return {
        "pitches_thrown": int(len(df)),
        "avg_velocity": round(float(speeds.mean()), 2) if len(speeds) else None,
        "max_velocity": round(float(speeds.max()), 2) if len(speeds) else None,
        "avg_spin_rate": round(float(spins.mean()), 1) if len(spins) else None,
        "strikeouts": int((df["events"] == "strikeout").sum()) if "events" in df else 0,
        "pitch_mix": df["pitch_type"].value_counts().to_dict() if "pitch_type" in df else {},
    }


def fetch_player_statcast(year: int) -> dict:
    """Statcast logs + summaries for every player in KEY_PLAYERS."""
    start, end = season_start(year), date.today().isoformat()
    players_payload: dict = {}

    for first, last, role in KEY_PLAYERS:
        display_name = f"{first} {last}"
        log.info("Statcast: %s (%s) ...", display_name, role)

        mlbam_id = lookup_mlbam_id(first, last)
        if mlbam_id is None:
            log.warning("No MLBAM id for %s — skipping.", display_name)
            players_payload[display_name] = {"error": "player_id_not_found", "role": role}
            continue

        try:
            raw = (
                statcast_batter(start, end, mlbam_id)
                if role == "batter"
                else statcast_pitcher(start, end, mlbam_id)
            )
        except Exception as exc:  # noqa: BLE001
            log.error("Statcast fetch failed for %s: %s", display_name, exc)
            players_payload[display_name] = {"error": "statcast_fetch_failed", "role": role}
            continue

        df = clean_dataframe(raw)
        if df.empty:
            players_payload[display_name] = {
                "role": role, "mlbam_id": mlbam_id,
                "summary": {}, "recent_events": [],
                "note": "no statcast data in range",
            }
            continue

        keep = [c for c in STATCAST_KEEP_COLS if c in df.columns]
        df = df[keep]
        if "game_date" in df.columns:
            df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")
            df = df.sort_values("game_date", ascending=False)

        summary = summarize_batter(df) if role == "batter" else summarize_pitcher(df)

        players_payload[display_name] = {
            "role": role,
            "mlbam_id": mlbam_id,
            "summary": summary,
            # Full logs can be tens of thousands of rows; cap what we ship.
            "recent_events": df_to_records(df.head(500)),
        }

    return players_payload


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main() -> int:
    year = current_season()
    log.info("=== Padres pipeline starting | season=%s ===", year)

    standings_records = fetch_standings(year)
    batting_records, pitching_records = fetch_team_stats(year)
    players_payload = fetch_player_statcast(year)

    payload = {
        "meta": {
            "team": TEAM_NAME_BREF,
            "season": year,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "sources": ["pybaseball (Baseball-Reference, FanGraphs, Baseball Savant)"],
        },
        "standings_nl_west": standings_records,
        "team_batting": batting_records,
        "team_pitching": pitching_records,
        "players": players_payload,
    }

    # Atomic overwrite: write to a temp file in the same directory, then
    # os.replace() it over the old file. The dashboard can never read a
    # half-written JSON, and the old data is always fully discarded.
    out_dir = os.path.dirname(os.path.abspath(OUTPUT_FILE)) or "."
    fd, tmp_path = tempfile.mkstemp(dir=out_dir, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False, default=str)
        os.replace(tmp_path, OUTPUT_FILE)
    except Exception:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise

    log.info(
        "Wrote %s | standings=%d rows, batting=%d, pitching=%d, players=%d",
        OUTPUT_FILE, len(standings_records), len(batting_records),
        len(pitching_records), len(players_payload),
    )

    # Fail the CI job loudly if literally nothing came back (bad night / API change)
    if not any([standings_records, batting_records, pitching_records]) and not players_payload:
        log.error("All fetches returned empty — failing so the workflow alerts you.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
