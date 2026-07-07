"""
update_padres_stats.py  (v3 — real analytics feed)
===================================================
Nightly data pipeline for the Padres front-office dashboard.

What changed vs. v2
-------------------
The frontend's placeholder MODELS are replaced by real fetched inputs:

1. SALARIES — scraped from the Spotrac Padres payroll page with
   requests + BeautifulSoup. Parsed defensively (Spotrac redesigns
   periodically); any failure falls back to the last known values.
   NOTE: verify Spotrac's terms of service permit automated access,
   and treat their figures as estimates — confirm decision-grade
   numbers against official/primary sources.

2. STUFF+ / LOCATION+ / PITCHING+ — from the FanGraphs leaderboards,
   fetched in three tiers (most robust first):
     T1: pybaseball.pitching_stats() — FG leaderboard via library
     T2: FanGraphs' own JSON API (the endpoint their site calls)
     T3: Playwright headless-browser scrape (heavy; last resort;
         requires `playwright install chromium` in CI)
   Column/field names are matched flexibly because FG's exact API
   field names for the Stuff+ model are not guaranteed stable.

3. ZONE DAMAGE (xwOBA − wOBA by strike-zone cell × pitch class) —
   computed from the Baseball Savant per-pitch data this pipeline
   already downloads per hitter. We do NOT call any "statcastZones"
   endpoint: I could not verify such an array exists on the MLB
   StatsAPI. Savant's per-pitch feed carries the verifiable inputs
   (`zone` 1–14, `estimated_woba_using_speedangle`, `woba_value`,
   `woba_denom`), so the zone grid is aggregated locally from real
   pitch-level data.

Reliability
-----------
* LAST-KNOWN-GOOD: the previous padres_live_data.json is loaded at
  startup. Any analytics section whose fresh fetch fails or returns
  empty is replaced by the prior section, stamped `"stale": true`
  with its original `as_of` timestamp, so the dashboard never loses
  a data point it once had.
* Every section is fetched independently; one failure never kills
  the run.

Output schema (v3) — additions the frontend reads natively
-----------------------------------------------------------
{
  "meta": {...},
  "team_stats": {...},                      # unchanged from v2
  "roster": [...],                          # unchanged from v2
  "player_deep_dive": {
      "<Name>": {
          ...v2 fields...,
          "zone_damage": {                  # NEW — real, per hitter
              "ff_high": { "zones": { "1": {"n":.., "woba":..,
                            "xwoba":.., "gap_x1000":..}, ... } },
              "sl_sweep": {...}, "ch_split": {...}, "cb": {...},
              "si": {...}
          }
      }
  },
  "analytics": {                            # NEW top-level block
      "salaries": {
          "source": "spotrac", "as_of": "...", "stale": false,
          "players": { "<Name>": <base_salary_usd_int> }
      },
      "pitching_models": {
          "source": "fangraphs", "as_of": "...", "stale": false,
          "players": { "<Name>": { "stuff_plus": .., 
                       "location_plus": .., "pitching_plus": .. } }
      },
      "assumptions": { "dollars_per_war_musd": 8.0,
                       "high_spin_ff_rpm": 2400 }
  }
}

Zone key: MLB Gameday `zone` codes — 1–9 are the 3×3 rulebook zone
(1 = up-and-in to a RHH from catcher's view), 11–14 the shadow
corners outside it.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import tempfile
import time
import unicodedata
import urllib.parse
import urllib.request
from datetime import date, datetime, timezone

import numpy as np
import pandas as pd

try:
    import requests
    from bs4 import BeautifulSoup
    HAVE_SCRAPE_LIBS = True
except ImportError:  # keep the core pipeline alive without them
    HAVE_SCRAPE_LIBS = False

from pybaseball import (
    cache,
    standings,
    statcast_batter,
    statcast_pitcher,
    team_batting,
    team_pitching,
)
try:
    from pybaseball import pitching_stats  # FG leaderboards (tier 1)
    HAVE_PITCHING_STATS = True
except ImportError:
    HAVE_PITCHING_STATS = False

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
TEAM_NAME_BREF = "San Diego Padres"
TEAM_ABBREV_FG = "SDP"
PADRES_MLBAM_TEAM_ID = 135
OUTPUT_FILE = "padres_live_data.json"

STATSAPI_BASE = "https://statsapi.mlb.com/api/v1"
HTTP_TIMEOUT_S = 30
SAVANT_DELAY_S = 1.0
EVENT_ROW_CAP = 300

# Spotrac page. VERIFY: URL pattern has changed across site redesigns;
# adjust here if the scrape logs an empty result.
SPOTRAC_URL = "https://www.spotrac.com/mlb/san-diego-padres/payroll/"
SCRAPE_UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
             "(KHTML, like Gecko) Chrome/126.0 Safari/537.36 "
             "padres-dashboard-pipeline/3.0 (nightly stats refresh)")

# FanGraphs JSON API (tier 2). Field names for the Stuff+ model are
# matched flexibly downstream; `type=36` targets the Pitching+ page
# but is NOT guaranteed stable — the code logs what it receives.
FANGRAPHS_API = "https://www.fangraphs.com/api/leaders/major-league/data"

DOLLARS_PER_WAR_MUSD = 8.0     # market-rate assumption shipped to frontend
HIGH_SPIN_FF_RPM = 2400        # OUR convention for "high-spin four-seam";
                               # not an official MLB definition.

# Pitch-class buckets matching the frontend's selector values.
PITCH_CLASS_MAP = {
    "sl_sweep": {"SL", "ST", "SV"},
    "ch_split": {"CH", "FS", "FO", "SC"},
    "cb":       {"CU", "KC", "CS"},
    "si":       {"SI", "FT"},
    # "ff_high" is FF/FA filtered by spin at aggregation time
}

BATTER_EVENT_COLS = [
    "game_date", "player_name", "pitch_type", "events", "description",
    "launch_speed", "launch_angle", "hit_distance_sc", "bb_type",
    "estimated_ba_using_speedangle", "estimated_woba_using_speedangle",
    "woba_value", "woba_denom", "release_spin_rate",
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
cache.enable()


# --------------------------------------------------------------------------- #
# Generic helpers
# --------------------------------------------------------------------------- #
def current_season() -> int:
    today = date.today()
    return today.year if today.month >= 3 else today.year - 1


def season_start(year: int) -> str:
    return f"{year}-03-01"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def http_get_json(url: str, retries: int = 3, backoff_s: float = 2.0,
                  headers: dict | None = None) -> dict:
    last_exc: Exception | None = None
    hdrs = {"User-Agent": SCRAPE_UA}
    if headers:
        hdrs.update(headers)
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers=hdrs)
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_S) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            log.warning("GET %s failed (attempt %d/%d): %s", url, attempt, retries, exc)
            time.sleep(backoff_s * attempt)
    raise RuntimeError(f"GET {url} failed after {retries} attempts: {last_exc}")


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    for col in df.columns:
        if df[col].dtype == object:
            converted = pd.to_numeric(df[col], errors="coerce")
            if converted.notna().sum() >= df[col].notna().sum():
                df[col] = converted
    return df.replace([np.inf, -np.inf], np.nan)


def df_to_records(df: pd.DataFrame) -> list[dict]:
    if df is None or df.empty:
        return []
    df = df.copy()
    for col in df.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns:
        df[col] = df[col].dt.strftime("%Y-%m-%d")
    df = df.astype(object).where(pd.notna(df), None)
    return df.to_dict(orient="records")


def normalize_name(name: str) -> str:
    """
    Canonical key for cross-source player matching:
    lowercase, accents stripped, punctuation removed, Jr/Sr/II/III dropped.
    'José Ramírez Jr.' -> 'jose ramirez'
    """
    if not name:
        return ""
    s = unicodedata.normalize("NFKD", str(name))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[.\'`’]", "", s.lower())
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"\s+(jr|sr|ii|iii|iv)$", "", s)
    return s


# --------------------------------------------------------------------------- #
# Last-known-good store
# --------------------------------------------------------------------------- #
def load_previous_payload() -> dict:
    """Read the prior output so failed sections can be preserved."""
    try:
        with open(OUTPUT_FILE, encoding="utf-8") as f:
            prev = json.load(f)
        log.info("Loaded previous payload (generated %s) for fallback.",
                 (prev.get("meta") or {}).get("generated_at_utc"))
        return prev
    except FileNotFoundError:
        log.info("No previous %s — first run, no fallback available.", OUTPUT_FILE)
        return {}
    except Exception as exc:  # noqa: BLE001
        log.warning("Previous payload unreadable (%s) — no fallback.", exc)
        return {}


def with_fallback(section: str, fresh: dict | None, prev_section: dict | None,
                  min_players: int = 1) -> dict:
    """
    Return the fresh analytics section if it has data; otherwise the previous
    one stamped stale. If neither exists, return an explicit empty section.
    """
    if fresh and len(fresh.get("players") or {}) >= min_players:
        fresh["stale"] = False
        return fresh
    if prev_section and (prev_section.get("players") or {}):
        old = dict(prev_section)
        old["stale"] = True
        log.warning("%s: fresh fetch empty/failed — preserving last known "
                    "values from %s.", section, old.get("as_of"))
        return old
    log.warning("%s: no fresh data and no previous data.", section)
    return {"source": None, "as_of": now_iso(), "stale": False, "players": {}}


# --------------------------------------------------------------------------- #
# 1) Salaries — Spotrac (requests + BeautifulSoup)
# --------------------------------------------------------------------------- #
_MONEY_RE = re.compile(r"\$[\d,]+(?:\.\d+)?")


def _parse_money(text: str) -> int | None:
    """'$12,345,678' -> 12345678. Ignores per-year suffixes."""
    m = _MONEY_RE.search(text or "")
    if not m:
        return None
    try:
        return int(float(m.group(0).replace("$", "").replace(",", "")))
    except ValueError:
        return None


def fetch_salaries_spotrac(year: int) -> dict:
    """
    Scrape base salaries from the Spotrac Padres payroll page.

    Parsing strategy (defensive — Spotrac's markup changes across
    redesigns): walk every <table>; a usable row has a player-profile
    link (href containing '/player/' or '/mlb/') in its first cells and
    at least one $-amount cell. We take the FIRST money value in the
    row, which on Spotrac payroll tables is the current-year figure.
    VERIFY the parsed numbers against the live page after any Spotrac
    redesign, and confirm their ToS permits automated access.
    """
    if not HAVE_SCRAPE_LIBS:
        log.error("salaries: requests/bs4 not installed — skipping scrape.")
        return {}
    url = SPOTRAC_URL
    log.info("salaries: fetching %s ...", url)
    try:
        resp = requests.get(url, headers={"User-Agent": SCRAPE_UA},
                            timeout=HTTP_TIMEOUT_S)
        resp.raise_for_status()
    except Exception as exc:  # noqa: BLE001
        log.error("salaries: request failed: %s", exc)
        return {}

    soup = BeautifulSoup(resp.text, "lxml")
    players: dict[str, int] = {}
    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            link = tr.find("a", href=re.compile(r"/(player|mlb)/", re.I))
            if not link:
                continue
            name = link.get_text(" ", strip=True)
            # Spotrac sometimes renders "Last, First" or appends position
            name = re.sub(r"\s{2,}.*$", "", name)
            if "," in name:
                last, _, first = name.partition(",")
                name = f"{first.strip()} {last.strip()}"
            if not re.search(r"[A-Za-z]{2,}\s+[A-Za-z]", name):
                continue
            money = None
            for td in tr.find_all(["td", "th"]):
                money = _parse_money(td.get_text(" ", strip=True))
                if money and money >= 100_000:      # skip $0 / bonus crumbs
                    break
            if money:
                key = normalize_name(name)
                # keep the largest figure if a player appears in several
                # tables (active vs. injured vs. retained sections)
                prev_best = (players.get(key) or {}).get("salary", 0)
                if money > prev_best:
                    players[key] = {"display": name, "salary": money}

    if not players:
        log.error("salaries: 0 rows parsed — Spotrac markup likely changed; "
                  "inspect %s and update the selector logic.", url)
        return {}
    log.info("salaries: parsed %d players.", len(players))
    return {
        "source": "spotrac",
        "source_url": url,
        "as_of": now_iso(),
        "season": year,
        "note": ("Scraped base-salary figures; Spotrac values are estimates — "
                 "verify decision-grade numbers against primary sources."),
        "players": {v["display"]: v["salary"] for v in players.values()},
        "_norm_index": {k: v["display"] for k, v in players.items()},
    }


# --------------------------------------------------------------------------- #
# 2) Stuff+ / Location+ / Pitching+ — FanGraphs, three tiers
# --------------------------------------------------------------------------- #
_STUFF_PATTERNS = {
    "stuff_plus":    re.compile(r"^(stuff\s*\+|sp_stuff|stf\+.*all)$", re.I),
    "location_plus": re.compile(r"^(location\s*\+|sp_location|loc\+.*all)$", re.I),
    "pitching_plus": re.compile(r"^(pitching\s*\+|sp_pitching|pit\+.*all)$", re.I),
}


def _extract_stuff_columns(df: pd.DataFrame, name_col: str,
                           team_col: str | None) -> dict:
    """Flexible column matching → {display_name: {metric: value}}."""
    colmap: dict[str, str] = {}
    for metric, pat in _STUFF_PATTERNS.items():
        for c in df.columns:
            if pat.match(str(c).strip()):
                colmap[metric] = c
                break
    if "stuff_plus" not in colmap:
        log.warning("fangraphs: no Stuff+ column found among: %s",
                    list(df.columns)[:40])
        return {}
    if team_col and team_col in df.columns:
        df = df[df[team_col].astype(str).str.upper().str.contains("SDP|PADRES", na=False)]
    out: dict[str, dict] = {}
    for _, row in df.iterrows():
        name = str(row.get(name_col, "")).strip()
        if not name:
            continue
        entry = {}
        for metric, col in colmap.items():
            v = pd.to_numeric(pd.Series([row.get(col)]), errors="coerce").iloc[0]
            entry[metric] = round(float(v), 1) if pd.notna(v) else None
        if entry.get("stuff_plus") is not None:
            out[name] = entry
    return out


def _stuff_tier1_pybaseball(year: int) -> dict:
    """Tier 1: pybaseball's FanGraphs leaderboard fetch (no browser)."""
    if not HAVE_PITCHING_STATS:
        return {}
    log.info("fangraphs: tier 1 (pybaseball.pitching_stats) ...")
    df = clean_dataframe(pitching_stats(year, qual=0))
    if df.empty:
        return {}
    name_col = "Name" if "Name" in df.columns else df.columns[0]
    team_col = "Team" if "Team" in df.columns else None
    return _extract_stuff_columns(df, name_col, team_col)


def _stuff_tier2_fg_api(year: int) -> dict:
    """
    Tier 2: FanGraphs' own JSON API. The `type=36` page id and the
    exact response field names are NOT guaranteed — everything is
    matched flexibly and logged so a drift degrades to tier 3.
    """
    log.info("fangraphs: tier 2 (JSON API) ...")
    qs = urllib.parse.urlencode({
        "age": "", "pos": "all", "stats": "pit", "lg": "all", "qual": 0,
        "season": year, "season1": year, "startdate": "", "enddate": "",
        "month": 0, "hand": "", "team": 0, "pageitems": 2000, "pagenum": 1,
        "ind": 0, "rost": 0, "players": "", "type": 36,
        "postseason": "", "sortdir": "default", "sortstat": "WAR",
    })
    try:
        data = http_get_json(f"{FANGRAPHS_API}?{qs}",
                             headers={"Accept": "application/json"})
    except Exception as exc:  # noqa: BLE001
        log.warning("fangraphs: tier 2 request failed: %s", exc)
        return {}
    rows = data.get("data") if isinstance(data, dict) else data
    if not isinstance(rows, list) or not rows:
        return {}
    df = pd.DataFrame(rows)
    # FG API name fields vary: PlayerName / Name (sometimes with HTML)
    name_col = next((c for c in ("PlayerName", "Name", "playerName")
                     if c in df.columns), None)
    if not name_col:
        return {}
    df[name_col] = df[name_col].astype(str).str.replace(r"<[^>]+>", "", regex=True)
    team_col = next((c for c in ("TeamName", "Team", "teamName")
                     if c in df.columns), None)
    return _extract_stuff_columns(df, name_col, team_col)


def _stuff_tier3_playwright(year: int) -> dict:
    """
    Tier 3 (last resort, per request): headless Chromium via Playwright.
    Requires `pip install playwright && playwright install chromium`.
    Renders the FG Pitching+ leaderboard and reads the table DOM.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log.warning("fangraphs: tier 3 skipped — playwright not installed.")
        return {}
    log.info("fangraphs: tier 3 (Playwright) ...")
    url = ("https://www.fangraphs.com/leaders/major-league?"
           f"pos=all&stats=pit&lg=all&qual=0&type=36&season={year}"
           f"&season1={year}&ind=0&team=0&pageitems=2000")
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            page = browser.new_page(user_agent=SCRAPE_UA)
            page.goto(url, timeout=60_000, wait_until="networkidle")
            page.wait_for_selector("table", timeout=30_000)
            html = page.content()
            browser.close()
    except Exception as exc:  # noqa: BLE001
        log.error("fangraphs: tier 3 browser run failed: %s", exc)
        return {}
    try:
        tables = pd.read_html(html)
    except ValueError:
        return {}
    for t in tables:
        t = clean_dataframe(t)
        if t.empty:
            continue
        name_col = next((c for c in t.columns if str(c).lower() == "name"), None)
        if not name_col:
            continue
        team_col = next((c for c in t.columns if str(c).lower() == "team"), None)
        got = _extract_stuff_columns(t, name_col, team_col)
        if got:
            return got
    return {}


def fetch_stuff_plus(year: int, roster_norm: set[str]) -> dict:
    """Run the tiers; filter to current Padres by normalized name."""
    players: dict[str, dict] = {}
    tier_used = None
    for tier_used, fn in (("pybaseball", _stuff_tier1_pybaseball),
                          ("fg_json_api", _stuff_tier2_fg_api),
                          ("playwright", _stuff_tier3_playwright)):
        try:
            players = fn(year)
        except Exception as exc:  # noqa: BLE001
            log.error("fangraphs: tier %s crashed: %s", tier_used, exc)
            players = {}
        if players:
            break
    if not players:
        return {}
    # Keep rows matching the active roster (team filters can miss
    # deadline movers), but keep everything if name-matching whiffs.
    matched = {n: v for n, v in players.items()
               if normalize_name(n) in roster_norm}
    if matched:
        players = matched
    log.info("fangraphs: %d Padres pitchers via tier '%s'.",
             len(players), tier_used)
    return {
        "source": "fangraphs",
        "tier": tier_used,
        "as_of": now_iso(),
        "season": year,
        "note": "Stuff+/Location+/Pitching+ scale: 100 = league average.",
        "players": players,
    }


# --------------------------------------------------------------------------- #
# 3) Zone damage — computed from Savant per-pitch data (real inputs)
# --------------------------------------------------------------------------- #
def classify_pitch_rows(df: pd.DataFrame) -> pd.Series:
    """Map each pitch row to a frontend pitch-class bucket (or None)."""
    pt = df.get("pitch_type")
    spin = pd.to_numeric(df.get("release_spin_rate"), errors="coerce")
    if pt is None:
        return pd.Series([None] * len(df), index=df.index)
    pt = pt.astype(str).str.upper().str.strip()
    out = pd.Series([None] * len(df), index=df.index, dtype=object)
    ff = pt.isin({"FF", "FA"})
    out[ff & (spin >= HIGH_SPIN_FF_RPM)] = "ff_high"
    for cls, codes in PITCH_CLASS_MAP.items():
        out[pt.isin(codes)] = cls
    return out


def compute_zone_damage(df: pd.DataFrame) -> dict:
    """
    Aggregate real per-pitch Savant data into
      {pitch_class: {"zones": {zone: {n, woba, xwoba, gap_x1000}}}}.

    wOBA per cell  = Σ woba_value / Σ woba_denom  (Savant's own fields)
    xwOBA per cell = Σ (estimated_woba_using_speedangle where present,
                        else woba_value) / Σ woba_denom
      — the standard approximation: batted balls use the expected value,
        strikeouts/walks keep their actual value. Cells with fewer than
        MIN_N wOBA-denominator events are shipped with the count but a
        null gap, so the frontend can gray them out instead of showing
        noise.
    """
    MIN_N = 5
    need = {"zone", "woba_value", "woba_denom"}
    if df is None or df.empty or not need.issubset(df.columns):
        return {}
    d = df.copy()
    d["zone"] = pd.to_numeric(d["zone"], errors="coerce")
    d = d[d["zone"].notna()]
    if d.empty:
        return {}
    d["zone"] = d["zone"].astype(int)
    d["_cls"] = classify_pitch_rows(d)
    d = d[d["_cls"].notna()]
    for c in ("woba_value", "woba_denom", "estimated_woba_using_speedangle"):
        d[c] = pd.to_numeric(d.get(c), errors="coerce")
    d["_xw"] = d["estimated_woba_using_speedangle"].fillna(d["woba_value"])

    out: dict[str, dict] = {}
    for (cls, zone), g in d.groupby(["_cls", "zone"]):
        denom = float(g["woba_denom"].sum(skipna=True) or 0)
        n = int(denom)
        cell: dict = {"n": n, "pitches": int(len(g))}
        if n >= MIN_N:
            woba = float(g["woba_value"].sum(skipna=True)) / denom
            xwoba = float(g["_xw"].sum(skipna=True)) / denom
            cell.update({
                "woba": round(woba, 3),
                "xwoba": round(xwoba, 3),
                "gap_x1000": int(round((xwoba - woba) * 1000)),
            })
        else:
            cell.update({"woba": None, "xwoba": None, "gap_x1000": None})
        out.setdefault(cls, {"zones": {}})["zones"][str(zone)] = cell
    return out


# --------------------------------------------------------------------------- #
# Roster / team-level fetchers (unchanged data sources from v2)
# --------------------------------------------------------------------------- #
def fetch_active_roster(year: int) -> list[dict]:
    url = (f"{STATSAPI_BASE}/teams/{PADRES_MLBAM_TEAM_ID}/roster"
           f"?rosterType=active&season={year}")
    log.info("Fetching active roster from MLB Stats API ...")
    data = http_get_json(url)
    roster: list[dict] = []
    for entry in data.get("roster", []):
        person = entry.get("person") or {}
        position = entry.get("position") or {}
        pid, name = person.get("id"), person.get("fullName")
        if not pid or not name:
            continue
        pos_type = position.get("type", "")
        if pos_type == "Pitcher":
            roles = ["pitcher"]
        elif "Two-Way" in pos_type:
            roles = ["batter", "pitcher"]
        else:
            roles = ["batter"]
        roster.append({
            "name": name, "mlbam_id": int(pid),
            "position": position.get("abbreviation"),
            "position_type": pos_type,
            "jersey_number": entry.get("jerseyNumber"),
            "roles": roles,
        })
    log.info("Active roster: %d players.", len(roster))
    return roster


def _stats_url(player_id: int, year: int, stat_type: str, group: str) -> str:
    qs = urllib.parse.urlencode(
        {"stats": stat_type, "season": year, "group": group, "gameType": "R"})
    return f"{STATSAPI_BASE}/people/{player_id}/stats?{qs}"


def fetch_season_line(player_id: int, year: int, group: str) -> dict:
    try:
        data = http_get_json(_stats_url(player_id, year, "season", group))
        for block in data.get("stats", []):
            for split in block.get("splits", []):
                return split.get("stat") or {}
    except Exception as exc:  # noqa: BLE001
        log.warning("Season line fetch failed (id=%s, %s): %s", player_id, group, exc)
    return {}


def fetch_game_logs(player_id: int, year: int, group: str) -> list[dict]:
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


def fetch_standings(year: int) -> list[dict]:
    log.info("Fetching %s standings ...", year)
    try:
        all_divisions = standings(year)
    except Exception as exc:  # noqa: BLE001
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
# Statcast per player (+ real zone damage for batters)
# --------------------------------------------------------------------------- #
def summarize_batter(df: pd.DataFrame) -> dict:
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
    """
    One role's Statcast payload: summary + capped rows, and for batters
    the real zone_damage grid computed from the FULL (uncapped) dataset.
    """
    name, mlbam_id = player["name"], player["mlbam_id"]
    try:
        raw = (statcast_batter(start, end, mlbam_id) if role == "batter"
               else statcast_pitcher(start, end, mlbam_id))
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

    summary = summarize_batter(df) if role == "batter" else summarize_pitcher(df)
    block = {"summary": summary, "events": df_to_records(df.head(EVENT_ROW_CAP))}
    if role == "batter":
        block["zone_damage"] = compute_zone_damage(df)   # full data, pre-cap
    return block


def build_player_profiles(roster: list[dict], year: int) -> tuple[list[dict], dict]:
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
            "game_logs": {}, "statcast": {},
        }
        season_lines: dict = {}
        for role in player["roles"]:
            group = "hitting" if role == "batter" else "pitching"
            season_lines[group] = fetch_season_line(player["mlbam_id"], year, group)
            profile["game_logs"][group] = fetch_game_logs(player["mlbam_id"], year, group)
            block = fetch_statcast_block(player, role, start, end)
            if role == "batter":
                profile["zone_damage"] = block.pop("zone_damage", {})
            profile["statcast"][role] = block
            time.sleep(SAVANT_DELAY_S)
        roster_out.append({
            "name": name, "mlbam_id": player["mlbam_id"],
            "position": player["position"],
            "position_type": player["position_type"],
            "jersey_number": player["jersey_number"],
            "roles": player["roles"],
            "season_stats": season_lines,
        })
        deep_dive[name] = profile
    return roster_out, deep_dive


# --------------------------------------------------------------------------- #
# Salary ↔ roster join
# --------------------------------------------------------------------------- #
def join_salaries_to_roster(salaries: dict, roster: list[dict]) -> dict:
    """
    Re-key scraped salaries to EXACT roster names so the frontend can do
    a direct `analytics.salaries.players[player.name]` lookup. Unmatched
    scraped names are kept under their original spelling and listed in
    `unmatched` for log visibility.
    """
    if not salaries.get("players"):
        return salaries
    norm_to_salary = {normalize_name(n): v
                      for n, v in salaries["players"].items()}
    joined: dict[str, int] = {}
    for p in roster:
        v = norm_to_salary.pop(normalize_name(p["name"]), None)
        if v is not None:
            joined[p["name"]] = v
    unmatched = {salaries["_norm_index"].get(k, k): v
                 for k, v in norm_to_salary.items()} if "_norm_index" in salaries else {}
    if unmatched:
        log.info("salaries: %d scraped names not on the active roster "
                 "(IL/minors/retained money are expected here).", len(unmatched))
    out = {k: v for k, v in salaries.items() if k != "_norm_index"}
    out["players"] = joined
    out["unmatched"] = unmatched
    out["matched_count"] = len(joined)
    return out


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main() -> int:
    year = current_season()
    log.info("=== Padres pipeline v3 starting | season=%s ===", year)
    previous = load_previous_payload()
    prev_analytics = previous.get("analytics") or {}

    # 1) Roster — required.
    try:
        roster = fetch_active_roster(year)
    except Exception as exc:  # noqa: BLE001
        log.error("Active roster fetch failed — aborting: %s", exc)
        return 1
    if not roster:
        log.error("Active roster came back empty — aborting.")
        return 1
    roster_norm = {normalize_name(p["name"]) for p in roster}

    # 2) Team-level context.
    standings_records = fetch_standings(year)
    batting_records, pitching_records = fetch_team_stats(year)

    # 3) Analytics feeds (independent; each falls back to last known good).
    try:
        fresh_sal = join_salaries_to_roster(fetch_salaries_spotrac(year), roster)
    except Exception as exc:  # noqa: BLE001
        log.error("salaries: crashed: %s", exc)
        fresh_sal = {}
    salaries = with_fallback("salaries", fresh_sal,
                             prev_analytics.get("salaries"))

    try:
        fresh_stuff = fetch_stuff_plus(year, roster_norm)
    except Exception as exc:  # noqa: BLE001
        log.error("fangraphs: crashed: %s", exc)
        fresh_stuff = {}
    pitching_models = with_fallback("pitching_models", fresh_stuff,
                                    prev_analytics.get("pitching_models"))

    # 4) Full-roster player profiles (includes real zone_damage per batter).
    roster_records, deep_dive = build_player_profiles(roster, year)

    # Preserve last-known zone grids for any batter whose fresh grid is empty.
    prev_dd = previous.get("player_deep_dive") or {}
    for name, prof in deep_dive.items():
        if "batter" in prof.get("roles", []) and not prof.get("zone_damage"):
            old = (prev_dd.get(name) or {}).get("zone_damage")
            if old:
                prof["zone_damage"] = old
                prof["zone_damage_stale"] = True
                log.warning("zone_damage: preserved last known grid for %s.", name)

    payload = {
        "meta": {
            "team": TEAM_NAME_BREF,
            "season": year,
            "generated_at_utc": now_iso(),
            "roster_size": len(roster_records),
            "event_row_cap_per_player": EVENT_ROW_CAP,
            "schema_version": 3,
            "sources": [
                "MLB Stats API (roster, game logs, season lines)",
                "pybaseball (Baseball-Reference, FanGraphs, Baseball Savant)",
                "Spotrac (salary estimates — scraped)",
                "FanGraphs (Stuff+/Location+/Pitching+)",
            ],
        },
        "team_stats": {
            "standings_nl_west": standings_records,
            "batting": batting_records,
            "pitching": pitching_records,
        },
        "roster": roster_records,
        "player_deep_dive": deep_dive,
        "analytics": {
            "salaries": salaries,
            "pitching_models": pitching_models,
            "assumptions": {
                "dollars_per_war_musd": DOLLARS_PER_WAR_MUSD,
                "high_spin_ff_rpm": HIGH_SPIN_FF_RPM,
                "zone_min_denominator": 5,
                "note": ("dollars_per_war is a market-rate convention, not a "
                         "fetched value; high_spin_ff_rpm is this pipeline's "
                         "own threshold for 'high-spin four-seam'."),
            },
        },
    }

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

    log.info("Wrote %s | standings=%d, roster=%d, salaries=%d (stale=%s), "
             "stuff+=%d (stale=%s)",
             OUTPUT_FILE, len(standings_records), len(roster_records),
             len(salaries.get("players") or {}), salaries.get("stale"),
             len(pitching_models.get("players") or {}),
             pitching_models.get("stale"))

    if not deep_dive and not any([standings_records, batting_records, pitching_records]):
        log.error("All fetches returned empty — failing so the workflow alerts you.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
