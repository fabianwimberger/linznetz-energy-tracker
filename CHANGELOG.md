# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - 2026-05-03

### Added

- **Auto-fetch from LinzNetz portal** — new "Fetch latest from LinzNetz" button in the sidebar and `POST /api/fetch` endpoint. When `LINZNETZ_USERNAME` and `LINZNETZ_PASSWORD` are configured, the app logs into the LinzNetz consumption portal, downloads quarter-hour CSVs for any missing days in the lookback window, and imports them automatically.
- `linznetz_fetcher.py` — standalone async scraper for the LinzNetz JSF/PrimeFaces portal with CLI support.
- Environment variables `LINZNETZ_USERNAME`, `LINZNETZ_PASSWORD`, and `LINZNETZ_LOOKBACK_DAYS` (default 7) for portal credentials.
- `.env.example` with documented configuration options.

### Changed

- `httpx` moved from dev to runtime dependencies (required for portal client).

## [1.0.0] - 2026-04-24

### Added

- Initial release
- Quarter-hourly raw view with average daily load pattern overlaid
- Daily, weekly, monthly, and yearly aggregations with moving averages
- Simple linear forecast for current week, month, and year
- CSV import with duplicate detection via SHA-256 hashing
- Support for both Austrian smart-meter CSV formats (quarter-hourly and daily)
- SQLite backend with WAL mode and automatic schema migrations
- Docker and Docker Compose deployment with multi-architecture support (AMD64/ARM64)
