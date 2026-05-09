#!/usr/bin/env python3
"""CSV import for Austrian smart-meter CSV exports."""

import csv
import hashlib
import logging
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

logger = logging.getLogger(__name__)
BATCH_SIZE = 1000

VIENNA_TZ = ZoneInfo("Europe/Vienna")
UTC_TZ = ZoneInfo("UTC")


class CSVImportError(Exception):
    """Custom exception for errors during CSV import."""

    pass


class CSVProcessor:
    """Processes quarter-hourly and daily CSV exports."""

    def __init__(self, engine: AsyncEngine):
        self.engine = engine
        self.expected_headers_quarter_hourly = [
            "datum von",
            "datum bis",
            "energiemenge in kwh",
        ]
        self.expected_headers_daily = ["datum", "energiemenge in kwh"]

    @staticmethod
    def calculate_file_hash(file_path: str) -> str:
        """SHA-256 hash for duplicate detection."""
        sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256.update(chunk)
        return sha256.hexdigest()

    @staticmethod
    def parse_german_datetime(
        value: str | None, *, fold: int = 0
    ) -> datetime | None:
        """Parse German-format datetime and convert to UTC.

        Args:
            value: German-format datetime string, e.g. "31.10.2024 02:00".
            fold: 0 for the first occurrence of an ambiguous local time
                (autumn DST, e.g. CEST / UTC+2), 1 for the second
                occurrence (CET / UTC+1).
        """
        if not value:
            return None
        try:
            naive = datetime.strptime(value.strip(), "%d.%m.%Y %H:%M")
            local_dt = naive.replace(fold=fold, tzinfo=VIENNA_TZ)
            return local_dt.astimezone(UTC_TZ)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def parse_any_daily_date(value: str | None) -> date | None:
        if not value:
            return None
        stripped_value = value.strip()
        for fmt in ("%d.%m.%Y", "%y-%m-%d", "%Y-%m-%d"):
            try:
                return datetime.strptime(stripped_value, fmt).date()
            except ValueError:
                continue
        return None

    @staticmethod
    def parse_german_decimal(value: str | None) -> Decimal | None:
        if not value:
            return None
        try:
            return Decimal(value.strip().replace(",", "."))
        except (InvalidOperation, TypeError):
            return None

    @staticmethod
    def validate_energy_value(value: Decimal) -> bool:
        return 0 <= value <= 100

    @staticmethod
    def validate_date_sequence(date_from: datetime, date_to: datetime) -> bool:
        return date_to - date_from == timedelta(minutes=15)

    async def _batch_insert_readings(self, conn, readings: list[dict]) -> int:
        if not readings:
            return 0

        insert_sql = text("""
            INSERT OR REPLACE INTO energy_readings
                (reading_date_from, reading_date_to, energy_kwh, date_local, time_slot_local)
            VALUES
                (:reading_date_from, :reading_date_to, :energy_kwh, :date_local, :time_slot_local)
        """)

        processed = 0
        for i in range(0, len(readings), BATCH_SIZE):
            batch = readings[i : i + BATCH_SIZE]
            await conn.execute(insert_sql, batch)
            processed += len(batch)

        return processed

    async def _refresh_daily_summaries(self, conn, dates: set[date]) -> None:
        if not dates:
            return

        start_date = min(dates)
        end_date = max(dates)
        logger.info(f"Refreshing daily summaries from {start_date} to {end_date}")
        refresh_sql = text("""
            INSERT OR REPLACE INTO daily_energy_summary
                (date, total_energy_kwh, reading_count,
                 min_quarter_hour_kwh, max_quarter_hour_kwh, avg_quarter_hour_kwh)
            SELECT
                date_local as date,
                SUM(energy_kwh) as total_energy_kwh,
                COUNT(*) as reading_count,
                MIN(energy_kwh) as min_quarter_hour_kwh,
                MAX(energy_kwh) as max_quarter_hour_kwh,
                AVG(energy_kwh) as avg_quarter_hour_kwh
            FROM energy_readings
            WHERE date_local BETWEEN :start_date AND :end_date
            GROUP BY date_local
        """)

        await conn.execute(
            refresh_sql,
            {"start_date": start_date.isoformat(), "end_date": end_date.isoformat()},
        )

    async def _refresh_hourly_pattern(self, conn) -> None:
        """Rebuild the daily-pattern cache (96 rows for 15-min data)."""
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

    async def _process_quarter_hourly_file(
        self, conn, file_path: str, header: list[str], dialect: Any
    ) -> int:
        readings: list[dict] = []
        affected_dates: set[date] = set()
        skipped_rows = 0

        with open(file_path, "r", encoding="utf-8-sig") as f:
            reader = csv.reader(f, dialect)
            next(reader)  # Skip header

            # Track local datetimes already seen in this file so that the
            # second occurrence of any slot during autumn DST gets fold=1.
            seen_local: set[datetime] = set()

            for row_num, row in enumerate(reader, 2):
                if len(row) < len(header):
                    logger.debug(f"Row {row_num}: Incomplete data, skipping")
                    skipped_rows += 1
                    continue

                row_data = {h.lower(): val for h, val in zip(header, row)}

                raw_from = row_data.get("datum von")
                raw_to = row_data.get("datum bis")
                if not raw_from or not raw_to:
                    logger.debug(f"Row {row_num}: Missing date columns")
                    skipped_rows += 1
                    continue

                naive_from = datetime.strptime(raw_from.strip(), "%d.%m.%Y %H:%M")
                naive_to = datetime.strptime(raw_to.strip(), "%d.%m.%Y %H:%M")

                # Detect DST fold: if this exact local datetime was already
                # seen in this file, it's the second pass (autumn DST).
                fold_from = 1 if naive_from in seen_local else 0
                fold_to = 1 if naive_to in seen_local else 0
                seen_local.add(naive_from)
                seen_local.add(naive_to)

                date_from = self.parse_german_datetime(raw_from, fold=fold_from)
                date_to = self.parse_german_datetime(raw_to, fold=fold_to)
                energy_kwh = self.parse_german_decimal(
                    row_data.get("energiemenge in kwh")
                )

                # Validation
                if date_from is None or date_to is None or energy_kwh is None:
                    logger.debug(f"Row {row_num}: Missing essential data")
                    skipped_rows += 1
                    continue

                if not self.validate_date_sequence(date_from, date_to):
                    logger.debug(f"Row {row_num}: Invalid 15-minute interval")
                    skipped_rows += 1
                    continue

                if not self.validate_energy_value(energy_kwh):
                    logger.warning(
                        f"Row {row_num}: Energy value {energy_kwh} out of bounds"
                    )
                    skipped_rows += 1
                    continue

                local_dt = date_from.astimezone(VIENNA_TZ)
                date_local = local_dt.date()
                time_slot_local = local_dt.strftime("%H:%M")

                affected_dates.add(date_local)
                readings.append(
                    {
                        "reading_date_from": date_from.isoformat(),
                        "reading_date_to": date_to.isoformat(),
                        "energy_kwh": float(energy_kwh),
                        "date_local": date_local.isoformat(),
                        "time_slot_local": time_slot_local,
                    }
                )

        if not readings:
            raise CSVImportError(
                f"No valid data found. Skipped {skipped_rows} invalid rows."
            )

        # Batch insert and refresh derived tables
        inserted = await self._batch_insert_readings(conn, readings)
        await self._refresh_daily_summaries(conn, affected_dates)

        logger.info(
            f"Processed {inserted} readings, skipped {skipped_rows} invalid rows"
        )
        return inserted

    async def _process_daily_summary_file(
        self, conn, file_path: str, header: list[str], dialect: Any
    ) -> int:
        summaries: list[dict] = []
        invalid_count = 0

        with open(file_path, "r", encoding="utf-8-sig") as f:
            reader = csv.reader(f, dialect)
            next(reader)  # Skip header

            for row_num, row in enumerate(reader, 2):
                if len(row) < len(header):
                    invalid_count += 1
                    continue

                row_data = {h.lower(): val for h, val in zip(header, row)}

                date_val = self.parse_any_daily_date(row_data.get("datum"))
                energy_val = self.parse_german_decimal(
                    row_data.get("energiemenge in kwh")
                )

                if date_val and energy_val is not None:
                    summaries.append(
                        {
                            "date": date_val,
                            "total_energy_kwh": float(energy_val),
                        }
                    )
                else:
                    invalid_count += 1
                    logger.debug(f"Row {row_num}: Invalid date or energy value")

        if not summaries:
            raise CSVImportError(
                f"No valid daily data found. {invalid_count} invalid rows."
            )

        # Insert daily summaries without overwriting existing quarter-hour data.
        insert_sql = text("""
            INSERT OR IGNORE INTO daily_energy_summary
                (date, total_energy_kwh)
            VALUES
                (:date, :total_energy_kwh)
        """)

        result = await conn.execute(insert_sql, summaries)
        inserted = result.rowcount

        # Refresh summaries for any dates that already have quarter-hour readings
        # so the accurate aggregate (with reading_count > 0) is preserved.
        await self._refresh_daily_summaries(
            conn, {s["date"] for s in summaries}
        )

        logger.info(
            f"Imported {inserted} daily summaries, skipped {len(summaries) - inserted} duplicates"
        )
        return inserted

    async def process_csv_file(
        self, file_path: str, *, refresh_pattern: bool = True
    ) -> dict[str, Any]:
        filename = Path(file_path).name
        logger.info(f"Processing: {filename}")
        file_hash = self.calculate_file_hash(file_path)

        async with self.engine.begin() as conn:
            # Check if already imported
            result = await conn.execute(
                text("SELECT id FROM import_log WHERE file_hash = :hash"),
                {"hash": file_hash},
            )
            if result.scalar_one_or_none():
                logger.info(f"File already imported: {filename}")
                return {
                    "status": "skipped",
                    "filename": filename,
                    "message": "File already imported (hash match)",
                }

            # Log import start
            log_id = str(uuid4())
            await conn.execute(
                text("""
                    INSERT INTO import_log
                        (id, filename, file_hash, processing_status, started_at)
                    VALUES
                        (:id, :filename, :hash, 'processing', datetime('now'))
                """),
                {"id": log_id, "filename": filename, "hash": file_hash},
            )

            try:
                # Detect CSV format
                with open(file_path, "r", encoding="utf-8-sig") as f:
                    sample = f.read(2048)
                    f.seek(0)

                    # Try to detect dialect
                    try:
                        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
                    except csv.Error:
                        dialect = csv.excel()  # type: ignore[assignment]

                    reader = csv.reader(f, dialect)
                    header_raw = [h.strip() for h in next(reader)]

                header_lower = [h.lower() for h in header_raw]
                records_processed = 0

                # Process based on detected format
                if "datum von" in header_lower and "datum bis" in header_lower:
                    logger.info("Detected quarter-hourly format")
                    records_processed = await self._process_quarter_hourly_file(
                        conn, file_path, header_raw, dialect
                    )
                elif "datum" in header_lower and "energiemenge in kwh" in header_lower:
                    logger.info("Detected daily summary format")
                    records_processed = await self._process_daily_summary_file(
                        conn, file_path, header_raw, dialect
                    )
                else:
                    raise CSVImportError(f"Unknown CSV format. Headers: {header_raw}")

                # Refresh hourly pattern once per file if requested
                if refresh_pattern:
                    await self._refresh_hourly_pattern(conn)

                # Update import log
                await conn.execute(
                    text("""
                        UPDATE import_log
                        SET processing_status = 'completed',
                            records_processed = :records,
                            completed_at = datetime('now')
                        WHERE id = :id
                    """),
                    {"records": records_processed, "id": log_id},
                )

                logger.info(
                    f"Successfully processed {filename}: {records_processed} records"
                )
                return {
                    "status": "success",
                    "filename": filename,
                    "records_processed": records_processed,
                }

            except Exception as e:
                # Log failure
                await conn.execute(
                    text("""
                        UPDATE import_log
                        SET processing_status = 'failed',
                            error_message = :error,
                            completed_at = datetime('now')
                        WHERE id = :id
                    """),
                    {"error": str(e), "id": log_id},
                )

                if isinstance(e, CSVImportError):
                    raise
                raise CSVImportError(f"Processing failed: {e}") from e
