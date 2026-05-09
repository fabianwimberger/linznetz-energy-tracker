#!/usr/bin/env python3
"""Database schema initialization and migrations."""

import logging
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 5


def _is_dst_transition_day(d: date) -> bool:
    """Return True if *d* is the European DST transition day (last Sunday
    of March or last Sunday of October)."""
    if d.month not in (3, 10):
        return False
    # Last Sunday of the month
    next_month = d.replace(day=28) + timedelta(days=4)
    last_sunday = next_month - timedelta(days=next_month.weekday() + 1)
    return d == last_sunday


async def init_database(engine: AsyncEngine):
    async with engine.connect() as conn:
        await conn.execute(text("PRAGMA journal_mode=WAL"))
        await conn.execute(text("PRAGMA busy_timeout=5000"))
        await conn.execute(text("PRAGMA synchronous=NORMAL"))
        await conn.execute(text("PRAGMA cache_size=-64000"))
        await conn.execute(text("PRAGMA temp_store=MEMORY"))

        # Check schema version
        await conn.execute(
            text("""
            CREATE TABLE IF NOT EXISTS schema_version (
                version INTEGER PRIMARY KEY,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        )

        result = await conn.execute(text("SELECT MAX(version) as v FROM schema_version"))
        current_version = result.scalar() or 0

        if current_version < SCHEMA_VERSION:
            logger.info(f"Upgrading schema from {current_version} to {SCHEMA_VERSION}")
            await apply_migrations(conn, current_version)
            await conn.execute(
                text("INSERT OR IGNORE INTO schema_version (version) VALUES (:v)"),
                {"v": SCHEMA_VERSION},
            )
            await conn.commit()


async def apply_migrations(conn, current_version):
    if current_version < 1:
        await conn.execute(
            text("""
            CREATE TABLE IF NOT EXISTS energy_readings (
                reading_date_from TIMESTAMP PRIMARY KEY,
                reading_date_to TIMESTAMP NOT NULL,
                energy_kwh REAL NOT NULL CHECK(energy_kwh >= 0 AND energy_kwh <= 100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        )

        await conn.execute(
            text("""
            CREATE TABLE IF NOT EXISTS daily_energy_summary (
                date DATE PRIMARY KEY,
                total_energy_kwh REAL NOT NULL,
                reading_count INTEGER DEFAULT 0,
                min_quarter_hour_kwh REAL,
                max_quarter_hour_kwh REAL,
                avg_quarter_hour_kwh REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        )

        await conn.execute(
            text("""
            CREATE TABLE IF NOT EXISTS import_log (
                id TEXT PRIMARY KEY,
                filename TEXT NOT NULL,
                file_hash TEXT NOT NULL UNIQUE,
                processing_status TEXT NOT NULL CHECK(processing_status IN ('processing', 'completed', 'failed')),
                records_processed INTEGER DEFAULT 0,
                error_message TEXT,
                started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP
            )
        """)
        )

    if current_version < 2:
        await conn.execute(
            text("""
            CREATE INDEX IF NOT EXISTS idx_readings_date
            ON energy_readings(DATE(reading_date_from))
        """)
        )

        await conn.execute(
            text("""
            CREATE INDEX IF NOT EXISTS idx_readings_hour_minute
            ON energy_readings(strftime('%H:%M', reading_date_from))
        """)
        )

        await conn.execute(
            text("""
            CREATE INDEX IF NOT EXISTS idx_daily_date_desc
            ON daily_energy_summary(date DESC)
        """)
        )

        await conn.execute(
            text("""
            CREATE INDEX IF NOT EXISTS idx_import_log_hash
            ON import_log(file_hash)
        """)
        )

        # updated_at triggers
        await conn.execute(
            text("""
            CREATE TRIGGER IF NOT EXISTS update_energy_readings_timestamp
            AFTER UPDATE ON energy_readings
            BEGIN
                UPDATE energy_readings SET updated_at = CURRENT_TIMESTAMP
                WHERE reading_date_from = NEW.reading_date_from;
            END
        """)
        )

        await conn.execute(
            text("""
            CREATE TRIGGER IF NOT EXISTS update_daily_summary_timestamp
            AFTER UPDATE ON daily_energy_summary
            BEGIN
                UPDATE daily_energy_summary SET updated_at = CURRENT_TIMESTAMP
                WHERE date = NEW.date;
            END
        """)
        )

    if current_version < 3:
        await conn.execute(
            text("""
            CREATE INDEX IF NOT EXISTS idx_daily_year_week
            ON daily_energy_summary(strftime('%Y-%W', date))
        """)
        )

        await conn.execute(
            text("""
            CREATE INDEX IF NOT EXISTS idx_daily_year_month
            ON daily_energy_summary(strftime('%Y-%m', date))
        """)
        )

        # Analyze for query optimizer
        await conn.execute(text("ANALYZE energy_readings"))
        await conn.execute(text("ANALYZE daily_energy_summary"))

    if current_version < 4:
        await conn.execute(
            text("""
            CREATE TABLE IF NOT EXISTS hourly_pattern (
                time_slot TEXT PRIMARY KEY,
                avg_power_w REAL NOT NULL,
                sample_count INTEGER NOT NULL
            )
        """)
        )

        await conn.execute(text("DROP INDEX IF EXISTS idx_readings_hour_minute"))
        try:
            await conn.execute(text("ALTER TABLE energy_readings DROP COLUMN raw_data"))
        except Exception as e:
            logger.warning(f"Could not drop raw_data column: {e}")
        await conn.execute(
            text("""
            INSERT OR REPLACE INTO hourly_pattern (time_slot, avg_power_w, sample_count)
            SELECT
                strftime('%H:%M', reading_date_from) as time_slot,
                AVG(energy_kwh * 4 * 1000) as avg_power_w,
                COUNT(*) as sample_count
            FROM energy_readings
            GROUP BY strftime('%H:%M', reading_date_from)
            HAVING COUNT(*) >= 5
        """)
        )

        await conn.execute(text("ANALYZE"))

    if current_version < 5:
        # Add local date/time columns for DST-safe grouping and querying
        await conn.execute(text("ALTER TABLE energy_readings ADD COLUMN date_local DATE;"))
        await conn.execute(text("ALTER TABLE energy_readings ADD COLUMN time_slot_local TEXT;"))

        # Backfill local columns from existing naive timestamps.
        # The stored values are already local time, so we extract directly
        # without the 'localtime' modifier (which would wrongly treat them
        # as UTC and shift near-midnight slots by one day).
        await conn.execute(
            text("""
                UPDATE energy_readings SET
                    date_local = DATE(reading_date_from),
                    time_slot_local = strftime('%H:%M', reading_date_from);
            """)
        )

        # Convert existing naive timestamps to UTC ISO strings so future
        # re-imports replace rather than duplicate rows.
        tz = ZoneInfo("Europe/Vienna")
        utc = ZoneInfo("UTC")
        result = await conn.execute(text("SELECT reading_date_from FROM energy_readings"))
        rows = result.fetchall()
        for (naive_str,) in rows:
            try:
                naive_dt = datetime.fromisoformat(naive_str)
                local_dt = naive_dt.replace(tzinfo=tz)
                utc_dt = local_dt.astimezone(utc)
                utc_str = utc_dt.isoformat()
                await conn.execute(
                    text("""
                        UPDATE energy_readings
                        SET reading_date_from = :utc
                        WHERE reading_date_from = :naive
                    """),
                    {"utc": utc_str, "naive": naive_str},
                )
            except (ValueError, TypeError) as e:
                logger.warning(f"Could not convert timestamp {naive_str!r}: {e}")

        # Replace expression indexes with column indexes
        await conn.execute(text("DROP INDEX IF EXISTS idx_readings_date"))
        await conn.execute(text("DROP INDEX IF EXISTS idx_readings_hour_minute"))
        await conn.execute(
            text("""
                CREATE INDEX IF NOT EXISTS idx_readings_date_local
                ON energy_readings(date_local);
            """)
        )
        await conn.execute(
            text("""
                CREATE INDEX IF NOT EXISTS idx_readings_time_slot
                ON energy_readings(time_slot_local);
            """)
        )
        # Drop incorrect year-week index; ISO weeks are computed in application code
        await conn.execute(text("DROP INDEX IF EXISTS idx_daily_year_week"))

        # Rebuild hourly_pattern to use the new column
        await conn.execute(text("DELETE FROM hourly_pattern"))
        await conn.execute(
            text("""
                INSERT INTO hourly_pattern (time_slot, avg_power_w, sample_count)
                SELECT
                    time_slot_local as time_slot,
                    AVG(energy_kwh * 4 * 1000) as avg_power_w,
                    COUNT(*) as sample_count
                FROM energy_readings
                GROUP BY time_slot_local
                HAVING COUNT(*) >= 5
            """)
        )

        # Warn about DST transition days that may have incorrect data due to
        # old naive-timestamp primary-key collisions.
        result = await conn.execute(text("SELECT DISTINCT date_local FROM energy_readings"))
        for (date_str,) in result.fetchall():
            d = datetime.strptime(date_str, "%Y-%m-%d").date()
            if _is_dst_transition_day(d):
                start = datetime(d.year, d.month, d.day, tzinfo=tz)
                end = start + timedelta(days=1)
                expected = int((end - start).total_seconds() / 900)
                cnt_res = await conn.execute(
                    text("SELECT COUNT(*) FROM energy_readings WHERE date_local = :d"),
                    {"d": date_str},
                )
                actual = cnt_res.scalar() or 0
                if actual != expected:
                    logger.warning(
                        f"DST transition day {d} has {actual} readings "
                        f"(expected {expected}). Data may have been lost during "
                        f"previous naive-timestamp imports. Consider re-importing "
                        f"this day for accurate totals."
                    )

        await conn.execute(text("ANALYZE"))
