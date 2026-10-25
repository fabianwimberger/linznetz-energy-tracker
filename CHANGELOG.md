# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v1.7.0] - 2026-10-25

The status bar remembers how the last LinzNetz fetch went.

### Features

- After a reload or a restart the status bar shows the last fetch with its time, for example `LinzNetz fetched 3 days, 25 Oct, 15:05`, `LinzNetz up to date, 25 Oct, 15:05` or `LinzNetz fetch failed, 25 Oct, 15:05`. Until the first fetch it reads `LinzNetz idle`.
- `/api/stats` reports `last_fetch_at`, `last_fetch_outcome` and `last_fetch_days`.

### Documentation & Links

- [README](https://github.com/fabianwimberger/linznetz-energy-tracker#readme)
- [Full changelog](https://github.com/fabianwimberger/linznetz-energy-tracker/compare/v1.6.2...v1.7.0)

## [v1.6.2] - 2026-10-25

Tidies the chart toolbar on phones and corrects the status text.

### Fixes

- On phones the range buttons take their own row, so `Reset zoom` sits beside the zoom controls instead of alone.
- The status bar reads `LinzNetz idle` until a fetch has run, instead of `not fetched yet` after a reload.

### Documentation & Links

- [README](https://github.com/fabianwimberger/linznetz-energy-tracker#readme)
- [Full changelog](https://github.com/fabianwimberger/linznetz-energy-tracker/compare/v1.6.1...v1.6.2)

## [v1.6.1] - 2026-10-24

Gives the chart more room and evens out the panels around it.

### Features

- The chart legend is gone: the plot area takes the height it used, and last reading and date moved from their own strip into the status bar.
- On phones the status bar leaves out range and date, which the time axis already shows.

### Fixes

- The side panel is as tall as the chart card. The drop area fills the leftover height, so `Fetch latest from LinzNetz` sits above the export divider.
- Aggregation shows three options over two centred ones, with the same label spacing as the import and export groups.
- The zoom buttons share the container style of the range switch.

### Documentation & Links

- [README](https://github.com/fabianwimberger/linznetz-energy-tracker#readme)
- [Full changelog](https://github.com/fabianwimberger/linznetz-energy-tracker/compare/v1.6.0...v1.6.1)

## [v1.6.0] - 2026-10-23

Redesigns the web UI around the consumption chart.

### Features

- New web UI: the chart fills the main area with the range control above it, aggregation, import and export sit in a side panel, and a status bar shows days, readings and the LinzNetz fetch status. It follows the system light or dark theme and works on phones.

### Dependencies

- FastAPI 0.142.2 → 0.161.1
- Pydantic 2.13.5 → 2.14.1
- SQLAlchemy 2.1.2 → 2.1.6
- Chart.js 4.5.1 → 4.6.1
- Ruff 0.16.10 → 0.17.2
- Remove the unused python-dateutil dependency.

### Documentation & Links

- [README](https://github.com/fabianwimberger/linznetz-energy-tracker#readme)
- [Full changelog](https://github.com/fabianwimberger/linznetz-energy-tracker/compare/v1.5.5...v1.6.0)

## [v1.5.5] - 2026-10-03

Consolidates Python dependencies and gives local development, CI, and Docker a consistent installation.

### Fixes

- Remove duplicate runtime and development dependency lists.
- Use virtual environments for local commands, CI, and the container runtime.

### Features

- Add a multi-stage container build that installs the application package and preserves offline frontend assets.

### Documentation & Links

- [README](https://github.com/fabianwimberger/linznetz-energy-tracker#readme)
- [Full changelog](https://github.com/fabianwimberger/linznetz-energy-tracker/compare/v1.5.4...v1.5.5)

## [v1.5.4] - 2026-10-02

Updates the API server, asynchronous database dependencies, and development tools.

### Dependencies

- Bump FastAPI from 0.141.1 to 0.142.2.
- Bump SQLAlchemy from 2.0.54 to 2.1.2, keeping the `asyncio` extra.
- Bump Uvicorn from 0.53.0 to 0.54.0.
- Bump Ruff from 0.16.8 to 0.16.10.
- Bump mypy from 2.3.1 to 2.4.0.

### Documentation & Links

- [README](https://github.com/fabianwimberger/linznetz-energy-tracker#readme)
- [Full changelog](https://github.com/fabianwimberger/linznetz-energy-tracker/compare/v1.5.3...v1.5.4)

## [v1.5.3] - 2026-10-01

Installs SQLAlchemy's asyncio dependencies explicitly and removes redundant code commentary.

### Dependencies

- Install SQLAlchemy with the `asyncio` extra so greenlet is available.

### Documentation & Links

- [README](https://github.com/fabianwimberger/linznetz-energy-tracker#readme)
- [Full changelog](https://github.com/fabianwimberger/linznetz-energy-tracker/compare/v1.5.2...v1.5.3)

## [v1.5.2] - 2026-09-19

Hardens the vendor asset download, single-sources the application version, and stops the API docs from being exposed.

### Fixes

- Verify the SHA-256 hash of every downloaded vendor asset and pin the expected version, so a tampered or unexpected file fails the build instead of shipping
- Derive the version reported by the app from a single `version.py`, removing the drift between the API metadata and `pyproject.toml`
- Disable the OpenAPI schema and docs endpoints (`/openapi.json`, `/docs`, `/redoc`) — they exposed the full API surface to any network client

### Dependencies

- Bump ruff from 0.16.5 to 0.16.7

### Documentation & Links

- [README](https://github.com/fabianwimberger/linznetz-energy-tracker#readme)
- [Container image](https://github.com/fabianwimberger/linznetz-energy-tracker/pkgs/container/energy-tracker)

## [v1.5.1] - 2026-09-01

Fixes a broken daily CSV import after LinzNetz changed the consumption portal's calendar widget.

### Fixes

- Restore daily imports after LinzNetz removed the `assignToDate` calendar widget from the consumption portal — dates are now set the same way the portal's own JavaScript does

### Dependencies

- Bump ruff from 0.16.3 to 0.16.5
- Bump mypy from 2.3.0 to 2.3.1
- Bump uvicorn from 0.52.3 to 0.52.4
- Bump pydantic from 2.13.4 to 2.13.5

### Documentation & Links

- [README](https://github.com/fabianwimberger/linznetz-energy-tracker#readme)

## [v1.5.0] - 2026-08-14

Adds a CSV export endpoint and fixes a Docker image publishing bug that could serve a single-arch image under multi-arch tags.

### Features

- Add a CSV export endpoint for downloading energy readings

### Fixes

- Derive export end time instead of trusting the stored `reading_date_to` value
- Publish real multi-platform Docker manifests — `latest` and versioned tags could previously point at a single-arch image depending on which platform build finished last, crashing with `exec format error` on the other architecture

### Dependencies

- Bump fastapi from 0.139.0 to 0.141.1
- Bump uvicorn from 0.51.0 to 0.52.3
- Bump sqlalchemy from 2.0.51 to 2.0.52
- Bump ruff from 0.15.21 to 0.16.3
- Bump actions/setup-python from 6 to 7
- Drop the pinned `python:3.14-alpine3.24` base image in favor of the rolling `python:3.14-alpine` tag

### Documentation & Links

- [README](https://github.com/fabianwimberger/linznetz-energy-tracker#readme)

## [v1.4.0] - 2026-07-18

A visual redesign of the dashboard, plus a chart interaction fix.

### Features

- Redesigned the dashboard with a warm, meter-inspired palette — copper for consumption, cyan for the moving average, gold for forecasts — replacing the generic dark-slate look.
- Self-hosted Space Grotesk and IBM Plex Mono typefaces, a custom gauge icon replacing the emoji header icon, and a designed empty state for when there's no data yet.

### Fixes

- Fixed the `<`/`>` chart pan buttons causing visible stutter on datasets with meaningful history.
- Fixed the entry-count badge staying on "Loading..." when the database has zero readings.

### Documentation & Links

- Refreshed the dashboard screenshot to show the new interface.
- Third-party license table updated for the newly self-hosted fonts.

## [v1.3.0] - 2026-05-14

Adds direct chart navigation controls for exploring energy history from the dashboard.

### Features

- Added chart buttons for zooming in, zooming out, panning left, panning right, and resetting the view.
- Refreshed the dashboard screenshot to show the current interface.

### Documentation & Links

- Source: GitHub release archive.

## [v1.2.1] - 2026-05-09

LinzNetz Energy Tracker now has release metadata aligned with the current package version and repository contents.

### Fixes

- Align package version metadata with the current release line
- Remove the repository changelog in favor of GitHub release notes

### Documentation & Links

- [README](https://github.com/fabianwimberger/linznetz-energy-tracker#readme)

## [v1.2.0] - 2026-05-09

DST collision and timezone correctness fixes.

### Fixes
- UTC storage with local date/time columns (schema v5) — eliminates DST duplicate-key collisions
- DST fold detection via prev_local_naive comparison
- ISO week calculation fixed for year-boundary dates
- Hourly pattern refresh optimization
- CORS hardening, /healthz endpoint, rate limiter scoping
- Modernized project config, ruff/mypy compatibility
- Dockerfile cleanup, fetcher regex fixes

### Documentation & Links
- https://github.com/fabianwimberger/linznetz-energy-tracker

## [v1.1.0] - 2026-05-03

Adds automated downloading of quarter-hourly consumption CSVs directly from the LinzNetz portal, eliminating the need to manually download and upload files.

### Features

- **LinzNetz Portal Auto-Fetch** — new sidebar button and `POST /api/fetch` endpoint that logs into the LinzNetz consumption portal, downloads missing quarter-hour CSVs for the configured lookback window, and imports them automatically.
- **Partial-day handling** — days that the portal has only published partially are re-fetched on subsequent presses until all 96 quarter-hour slots are present.
- **Standalone fetcher CLI** — `linznetz_fetcher.py` can be used independently to download CSVs for a date range.
- **Optional integration** — the fetch button is only shown when `LINZNETZ_USERNAME` and `LINZNETZ_PASSWORD` are configured; without credentials the app works exactly as before.

### Configuration

New environment variables (all optional):

| Variable | Default | Purpose |
|----------|---------|---------|
| `LINZNETZ_USERNAME` | unset | Portal username |
| `LINZNETZ_PASSWORD` | unset | Portal password |
| `LINZNETZ_LOOKBACK_DAYS` | `7` | How many past days to check for missing data |

Copy `.env.example` to `.env` and fill in your credentials to enable the feature.

### Dependencies

- `httpx` moved from dev to runtime dependencies (required for portal client).

### Documentation & Links

- [README](https://github.com/fabianwimberger/linznetz-energy-tracker/blob/main/README.md)
- [CHANGELOG](https://github.com/fabianwimberger/linznetz-energy-tracker/blob/main/CHANGELOG.md)
- [Docker Image](https://github.com/fabianwimberger/linznetz-energy-tracker/pkgs/container/energy-tracker)

## [v1.0.0] - 2026-04-25

First official release. Self-hosted dashboard for visualizing electricity consumption from Austrian smart meter (Smart Meter / Intelligenter Zähler) CSV exports — typically from the LinzNetz Netzbetreiber portal or any Austrian grid operator that provides quarter-hourly or daily CSV files.

For live polling instead of CSV imports, see the sister project [sma-energy-tracker](https://github.com/fabianwimberger/sma-energy-tracker).

### Features

- **Quarter-hourly raw view** with your average daily load pattern overlaid
- **Daily / weekly / monthly / yearly** aggregations with moving averages
- **Simple linear forecast** for the current week, month, or year
- **Duplicate import detection** via SHA-256 hashing
- **Handles both Austrian formats** — `Datum von / Datum bis / Energiemenge in kWh` (quarter-hourly) and `Datum / Energiemenge in kWh` (daily)
- **SQLite** — no external database required

### Quick Start

```bash
docker run -d \
  --name linznetz-energy-tracker \
  --restart unless-stopped \
  -p 8000:8000 \
  -v linznetz-energy-tracker-data:/app/data \
  -e TZ=Europe/Vienna \
  ghcr.io/fabianwimberger/linznetz-energy-tracker:1.0.0
```

Open the UI at **http://localhost:8000**, then import your CSV file.

### Documentation & Links

- [README](https://github.com/fabianwimberger/linznetz-energy-tracker#readme)
- [Container image](https://github.com/fabianwimberger/linznetz-energy-tracker/pkgs/container/energy-tracker)
