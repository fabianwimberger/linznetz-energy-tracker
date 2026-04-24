# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
