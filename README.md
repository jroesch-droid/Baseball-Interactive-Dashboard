# ⚾ Padres Live Analytics Dashboard

A self-updating San Diego Padres statistics dashboard. Every night, an automated pipeline pulls fresh data from public baseball data sources, and a live web dashboard visualizes it — no server required.

**🔗 Live dashboard:** https://jroesch-droid.github.io/Baseball-Interactive-Dashboard/

## What it shows

- **NL West standings** — the Padres' record, win percentage, division rank, and games back
- **Run differential** — team runs scored vs. runs allowed for the season
- **Launch angle × exit velocity** — a scatter plot of every tracked batted ball from key hitters, with the classic "barrel" launch-angle window highlighted
- **Pitching trends** — per-game average velocity and spin rate over the season for tracked pitchers
- **Head-to-head radar** — compare any two tracked players across normalized stat axes
- **Sortable roster table** — key Statcast summary stats for every tracked player, with the best value in each column highlighted

## How it works

```
┌──────────────────┐     nightly cron      ┌────────────────────────┐
│  GitHub Actions  │ ───────────────────►  │ update_padres_stats.py │
└──────────────────┘                       └───────────┬────────────┘
                                                       │ fetches via pybaseball
                                                       ▼
                                    Baseball-Reference · FanGraphs · Baseball Savant
                                                       │
                                                       ▼
                                          padres_live_data.json (committed to repo)
                                                       │
                                                       ▼
                                     GitHub Pages serves index.html + JSON
                                     (dashboard re-checks for new data every 30 min)
```

1. **`update_padres_stats.py`** runs every night via GitHub Actions (see `.github/workflows/update_stats.yml`). It fetches standings, team batting/pitching stats, and player-level Statcast logs using the [pybaseball](https://github.com/jldbc/pybaseball) library, then writes everything to `padres_live_data.json`.
2. The workflow **commits the refreshed JSON back to this repo**, which triggers a GitHub Pages redeploy.
3. **`index.html`** (the dashboard) is a single static page that fetches the JSON from the same folder and renders everything client-side with Chart.js. It automatically re-checks for new data every 30 minutes.

## Data sources

All data comes from public sources via pybaseball:

- **Baseball-Reference** — standings
- **FanGraphs** — team batting and pitching leaderboards
- **Baseball Savant (Statcast)** — pitch-level and batted-ball data for individual players

## Tracked players

Player-level Statcast tracking is configured in the `KEY_PLAYERS` list at the top of `update_padres_stats.py`. To add or remove a player, edit that list (first name, last name, and `"batter"` or `"pitcher"`) and the dashboard will pick them up after the next pipeline run. Roster moves happen — keep the list current.

## Repo structure

| File | Purpose |
|---|---|
| `index.html` | The dashboard (static page, Chart.js visualizations) |
| `update_padres_stats.py` | Nightly data pipeline |
| `padres_live_data.json` | Auto-generated data file (committed by the bot — don't edit by hand) |
| `.github/workflows/update_stats.yml` | GitHub Actions schedule + auto-commit |
| `requirements.txt` | Python dependencies for the pipeline |

## Running it yourself

Want your own copy (or a version for a different team)?

1. **Fork this repo.**
2. In **Settings → Actions → General**, set Workflow permissions to **Read and write**.
3. In **Settings → Pages**, deploy from the `main` branch, root folder.
4. Go to the **Actions** tab and manually trigger **Update Padres Dashboard Data** to generate the first data file.
5. Your dashboard will be live at `https://<your-username>.github.io/<repo-name>/`.

To adapt it for another team, change `TEAM_NAME_BREF` and `TEAM_ABBREV_FG` in `update_padres_stats.py` and update `KEY_PLAYERS`.

## Notes & limitations

- Data refreshes **once per night** (~11:30 PM Pacific), not in real time. The "live" in the name means the page picks up new data automatically — it's not a play-by-play scoreboard.
- GitHub's scheduled workflows can run several minutes late, and GitHub may pause schedules on repos with no recent activity (re-enabling takes one click).
- pybaseball scrapes public websites, so a source site redesign can occasionally break a fetch until the library updates. The pipeline is built to fail loudly in the Actions tab when that happens.

---

*Not affiliated with the San Diego Padres or MLB. Built for fun with public data.*
