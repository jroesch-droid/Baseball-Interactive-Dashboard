"""
update_padres_stats.py  (v2 — full active roster)
==================================================
Nightly data pipeline for a San Diego Padres dashboard.

What changed vs. v1
-------------------
* No more hard-coded KEY_PLAYERS list. The script dynamically pulls the
  Padres' current 26-man ACTIVE roster every run, so trades, IL moves,
  and call-ups are picked up automatically the next night.
* Every rostered player gets a full profile:
    - true per-game season game logs (MLB Stats API `gameLog` splits)
    - season aggregate stats (MLB Stats API `season` splits)
    - Statcast pitch/hit-level data + a compact summary
      (exit velo / launch angle for hitters, velo / spin / pitch mix
       for pitchers; two-way players get both)
* Output JSON restructured into a relational shape:
    {
      "meta":             {...},
      "team_stats":       { "standings_nl_west", "batting", "pitching" },
      "roster":           [ 26 players, basic info + season stat line ],
      "player_deep_dive": { "Player Name": { game_logs, statcast, ... } }
    }

Data sources
------------
* Active roster + game logs + season lines: MLB Stats API
  (https://statsapi.mlb.com — free, no key required). pybaseball has no
  active-roster function, so we hit this API directly with stdlib urllib.
* Statcast events: pybaseball.statcast_batter / statcast_pitcher
  (Baseball Savant).
* Standings / team tables: pybaseball (Baseball-Reference, FanGraphs).

Efficiency notes
----------------
* The roster endpoint returns each player's MLBAM id directly, so the
  fuzzy Chadwick `playerid_lookup` step from v1 is gone entirely
  (removes one lookup per player per run).
* pybaseball's local HTTP cache is enabled — retries and re-runs are
  nearly free.
* A short politeness delay is inserted between Baseball Savant queries.
* Event rows shipped per player are capped (EVENT_ROW_CAP) so the JSON
  stays dashboard-sized even with 26 full profiles. Summaries are always
  computed from the FULL dataset before capping.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import tempfile
import time
import urllib.parse
import urllib.request
from datetime import date, datetime, timezone

import numpy as np
import pandas as pd
from pybaseball import (
    cache,
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
PADRES_MLBAM_TEAM_ID = 135            # MLB Stats API team id for San Diego
OUTPUT_FILE = "padres_live_data.json"

STATSAPI_BASE = "https://statsapi.mlb.com/api/v1"
HTTP_TIMEOUT_S = 30
SAVANT_DELAY_S = 1.0                  # politeness pause between Statcast pulls

# Cap on raw Statcast event rows shipped per player (a full season can be
# 2,000+ rows per regular; 26 players x thousands of rows would bloat the
# JSON). Raise this if your dashboard needs deeper raw logs.
EVENT_ROW_CAP = 300

# Statcast columns worth shipping to a dashboard (raw feed has 90+ columns).
BATTER_EVENT_COLS = [
    "game_date", "player_name", "pitch_type", "events", "description",
    "launch_speed", "launch_angle", "hit_distance_sc", "bb_type",
    "estimated_ba_using_speedangle", "estimated_woba_using_speedangle",
    "balls", "strikes", "outs_when_up", "inning", "home_team", "away_team",
    "zone", "p_throws", "stand",
]
PITCHER_EVENT_COLS = [
    "game_date", "player_name", "pitch_type", "release_speed",
    "release_spin_rate", "events", "description", "launch_speed",
    "launch_angle", "balls", "strikes", "outs_when_up", "inning",
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
# Generic helpers
# --------------------------------------------------------------------------- #
def current_season() -> int:
    """Resolve the season year dynamically (no hard-coded year)."""
    today = date.today()
    # Before March there is no current-season data yet; fall back to last year.
    return today.year if today.month >= 3 else today.year - 1


def season_start(year: int) -> str:
    """Conservative season start date for Statcast queries (covers early openers)."""
    return f"{year}-03-01"


def http_get_json(url: str, retries: int = 3, backoff_s: float = 2.0) -> dict:
    """Small stdlib JSON GET with retry — used for the MLB Stats API."""
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "padres-dashboard-pipeline/2.0"}
            )
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_S) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            log.warning("GET %s failed (attempt %d/%d): %s", url, attempt, retries, exc)
            time.sleep(backoff_s * attempt)
    raise RuntimeError(f"GET {url} failed after {retries} attempts: {last_exc}")


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standard cleaning applied to every dataset:
      * strip whitespace from column names
      * convert numeric-looking object columns to numbers
      * replace inf with NaN (NaN -> None happens during serialization)
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

    return df.replace([np.inf, -np.inf], np.nan)


def df_to_records(df: pd.DataFrame) -> list[dict]:
    """DataFrame -> list of dicts with JSON-safe values (NaN -> None, dates -> ISO)."""
    if df is None or df.empty:
        return []
    df = df.copy()
    for col in df.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns:
        df[col] = df[col].dt.strftime("%Y-%m-%d")
    df = df.astype(object).where(pd.notna(df), None)
    return df.to_dict(orient="records")


# --------------------------------------------------------------------------- #
# Roster (MLB Stats API)
# --------------------------------------------------------------------------- #
def fetch_active_roster(year: int) -> list[dict]:
    """
    Pull the current 26-man active roster. Returns a list of dicts:
      { name, mlbam_id, position, position_type, jersey_number, roles }
    where roles is ["batter"], ["pitcher"], or both for two-way players.
    """
    url = (
        f"{STATSAPI_BASE}/teams/{PADRES_MLBAM_TEAM_ID}/roster"
        f"?rosterType=active&season={year}"
    )
    log.info("Fetching active roster from MLB Stats API ...")
    data = http_get_json(url)

    roster: list[dict] = []
    for entry in data.get("roster", []):
        person = entry.get("person") or {}
        position = entry.get("position") or {}
        pid = person.get("id")
        name = person.get("fullName")
        if not pid or not name:
            continue

        pos_type = position.get("type", "")  # "Pitcher", "Infielder", ...
        if pos_type == "Pitcher":
            roles = ["pitcher"]
        elif "Two-Way" in pos_type:
            roles = ["batter", "pitcher"]
        else:
            roles = ["batter"]

        roster.append({
            "name": name,
            "mlbam_id": int(pid),
            "position": position.get("abbreviation"),
            "position_type": pos_type,
            "jersey_number": entry.get("jerseyNumber"),
            "roles": roles,
        })

    log.info("Active roster: %d players.", len(roster))
    return roster


def _stats_url(player_id: int, year: int, stat_type: str, group: str) -> str:
    qs = urllib.parse.urlencode(
        {"stats": stat_type, "season": year, "group": group, "gameType": "R"}
    )
    return f"{STATSAPI_BASE}/people/{player_id}/stats?{qs}"


def fetch_season_line(player_id: int, year: int, group: str) -> dict:
    """Season aggregate stat line (AVG/OPS/HR or ERA/WHIP/K, etc.)."""
    try:
        data = http_get_json(_stats_url(player_id, year, "season", group))
        for block in data.get("stats", []):
            for split in block.get("splits", []):
                return split.get("stat") or {}
    except Exception as exc:  # noqa: BLE001
        log.warning("Season line fetch failed (id=%s, %s): %s", player_id, group, exc)
    return {}


def fetch_game_logs(player_id: int, year: int, group: str) -> list[dict]:
    """True per-game season game logs from the MLB Stats API."""
    logs: list[dict] = []
    try:
        data = http_get_json(_stats_url(player_id, year, "gameLog", group))
        for block in data.get("stats", []):
            for split in block.get("splits", []):
                logs.append({
                    "date": split.get("date"),
                    "opponent": (split.get("opponent") or {}).get("name"),
                    "is_home": split.get("isHome"),
                    "stats": split.get("stat") or {},
                })
    except Exception as exc:  # noqa: BLE001
        log.warning("Game log fetch failed (id=%s, %s): %s", player_id, group, exc)
    return logs


# --------------------------------------------------------------------------- #
# Team-level fetchers (unchanged data sources from v1)
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


# --------------------------------------------------------------------------- #
# Statcast per player
# --------------------------------------------------------------------------- #
def summarize_batter(df: pd.DataFrame) -> dict:
    """Compact aggregate block so the dashboard doesn't have to recompute."""
    batted = df.dropna(subset=["launch_speed"]) if "launch_speed" in df else pd.DataFrame()
    return {
        "pitches_seen": int(len(df)),
        "batted_ball_events": int(len(batted)),
        "avg_exit_velocity": round(float(batted["launch_speed"].mean()), 2) if len(batted) else None,
        "max_exit_velocity": round(float(batted["launch_speed"].max()), 2) if len(batted) else None,
        "avg_launch_angle": round(float(batted["launch_angle"].mean()), 2)
        if len(batted) and "launch_angle" in batted else None,
        "hard_hit_events": int((batted["launch_speed"] >= 95).sum()) if len(batted) else 0,
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


def fetch_statcast_block(player: dict, role: str, start: str, end: str) -> dict:
    """One role's Statcast payload for one player: summary + capped event rows."""
    name, mlbam_id = player["name"], player["mlbam_id"]
    try:
        raw = (
            statcast_batter(start, end, mlbam_id)
            if role == "batter"
            else statcast_pitcher(start, end, mlbam_id)
        )
    except Exception as exc:  # noqa: BLE001
        log.error("Statcast fetch failed for %s (%s): %s", name, role, exc)
        return {"error": "statcast_fetch_failed"}

    df = clean_dataframe(raw)
    if df.empty:
        return {"summary": {}, "events": [], "note": "no statcast data in range"}

    keep_cols = BATTER_EVENT_COLS if role == "batter" else PITCHER_EVENT_COLS
    df = df[[c for c in keep_cols if c in df.columns]]
    if "game_date" in df.columns:
        df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")
        df = df.sort_values("game_date", ascending=False)

    # Summaries use the FULL dataset; only the shipped raw rows are capped.
    summary = summarize_batter(df) if role == "batter" else summarize_pitcher(df)
    return {"summary": summary, "events": df_to_records(df.head(EVENT_ROW_CAP))}


def build_player_profiles(roster: list[dict], year: int) -> tuple[list[dict], dict]:
    """
    Loop the ENTIRE active roster and build:
      * roster_out — array of basic per-player rows (the "roster" key)
      * deep_dive  — {"Player Name": full profile} (the "player_deep_dive" key)
    Any single player failing never kills the run — errors are recorded
    inside that player's profile instead.
    """
    start, end = season_start(year), date.today().isoformat()
    roster_out: list[dict] = []
    deep_dive: dict = {}

    for i, player in enumerate(roster, 1):
        name = player["name"]
        log.info("[%d/%d] Building profile: %s (%s) ...",
                 i, len(roster), name, "/".join(player["roles"]))

        profile: dict = {
            "mlbam_id": player["mlbam_id"],
            "position": player["position"],
            "position_type": player["position_type"],
            "roles": player["roles"],
            "game_logs": {},
            "statcast": {},
        }
        season_lines: dict = {}

        for role in player["roles"]:
            group = "hitting" if role == "batter" else "pitching"
            season_lines[group] = fetch_season_line(player["mlbam_id"], year, group)
            profile["game_logs"][group] = fetch_game_logs(player["mlbam_id"], year, group)
            profile["statcast"][role] = fetch_statcast_block(player, role, start, end)
            time.sleep(SAVANT_DELAY_S)  # be polite to Baseball Savant

        roster_out.append({
            "name": name,
            "mlbam_id": player["mlbam_id"],
            "position": player["position"],
            "position_type": player["position_type"],
            "jersey_number": player["jersey_number"],
            "roles": player["roles"],
            "season_stats": season_lines,
        })
        deep_dive[name] = profile

    return roster_out, deep_dive


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main() -> int:
    year = current_season()
    log.info("=== Padres pipeline v2 starting | season=%s ===", year)

    # 1) Dynamic roster — if this fails, we cannot build player data at all.
    try:
        roster = fetch_active_roster(year)
    except Exception as exc:  # noqa: BLE001
        log.error("Active roster fetch failed — aborting: %s", exc)
        return 1
    if not roster:
        log.error("Active roster came back empty — aborting.")
        return 1

    # 2) Team-level context.
    standings_records = fetch_standings(year)
    batting_records, pitching_records = fetch_team_stats(year)

    # 3) Full-roster player profiles.
    roster_records, deep_dive = build_player_profiles(roster, year)

    payload = {
        "meta": {
            "team": TEAM_NAME_BREF,
            "season": year,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "roster_size": len(roster_records),
            "event_row_cap_per_player": EVENT_ROW_CAP,
            "schema_version": 2,
            "sources": [
                "MLB Stats API (roster, game logs, season lines)",
                "pybaseball (Baseball-Reference, FanGraphs, Baseball Savant)",
            ],
        },
        "team_stats": {
            "standings_nl_west": standings_records,
            "batting": batting_records,
            "pitching": pitching_records,
        },
        "roster": roster_records,
        "player_deep_dive": deep_dive,
    }

    # Atomic overwrite: write to a temp file in the same directory, then
    # os.replace() it over the old file so readers never see a partial JSON.
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
        "Wrote %s | standings=%d, team_bat=%d, team_pit=%d, roster=%d, deep_dive=%d",
        OUTPUT_FILE, len(standings_records), len(batting_records),
        len(pitching_records), len(roster_records), len(deep_dive),
    )

    # Fail the CI job loudly if literally nothing useful came back.
    if not deep_dive and not any([standings_records, batting_records, pitching_records]):
        log.error("All fetches returned empty — failing so the workflow alerts you.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
