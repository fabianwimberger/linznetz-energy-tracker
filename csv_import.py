#!/usr/bin/env python3
"""CSV import for Austrian smart-meter CSV exports."""

import csv
import hashlib
import logging
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

logger = logging.getLogger(__name__)
BATCH_SIZE = 1000


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
    def parse_german_datetime(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            return datetime.strptime(value.strip(), "%d.%m.%Y %H:%M")
        except (ValueError, TypeError):
            return None

    @staticmethod
    def parse_any_daily_date(value: Optional[str]) -> Optional[date]:
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
    def parse_german_decimal(value: Optional[str]) -> Optional[Decimal]:
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

    async def _batch_insert_readings(self, conn, readings: List[Dict]) -> int:
        if not readings:
            return 0

        insert_sql = text("""
            INSERT OR REPLACE INTO energy_readings
                (reading_date_from, reading_date_to, energy_kwh)
            VALUES
                (:reading_date_from, :reading_date_to, :energy_kwh)
        """)

        processed = 0
        for i in range(0, len(readings), BATCH_SIZE):
            batch = readings[i : i + BATCH_SIZE]
            await conn.execute(insert_sql, batch)
            processed += len(batch)

        return processed

    async def _refresh_daily_summaries(self, conn, dates: set) -> None:
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
                DATE(reading_date_from) as date,
                SUM(energy_kwh) as total_energy_kwh,
                COUNT(*) as reading_count,
                MIN(energy_kwh) as min_quarter_hour_kwh,
                MAX(energy_kwh) as max_quarter_hour_kwh,
                AVG(energy_kwh) as avg_quarter_hour_kwh
            FROM energy_readings
            WHERE DATE(reading_date_from) BETWEEN :start_date AND :end_date
            GROUP BY DATE(reading_date_from)
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
                strftime('%H:%M', reading_date_from) as time_slot,
                AVG(energy_kwh * 4 * 1000) as avg_power_w,
                COUNT(*) as sample_count
            FROM energy_readings
            GROUP BY strftime('%H:%M', reading_date_from)
            HAVING COUNT(*) >= 5
        """)
        )

    async def _process_quarter_hourly_file(
        self, conn, file_path: str, header: List[str], dialect: Any
    ) -> int:
        readings = []
        affected_dates = set()
        skipped_rows = 0

        with open(file_path, "r", encoding="utf-8-sig") as f:
            reader = csv.reader(f, dialect)
            next(reader)  # Skip header

            for row_num, row in enumerate(reader, 2):
                if len(row) < len(header):
                    logger.debug(f"Row {row_num}: Incomplete data, skipping")
                    skipped_rows += 1
                    continue

                row_data = {h.lower(): val for h, val in zip(header, row)}

                date_from = self.parse_german_datetime(row_data.get("datum von"))
                date_to = self.parse_german_datetime(row_data.get("datum bis"))
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

                affected_dates.add(date_from.date())
                readings.append(
                    {
                        "reading_date_from": date_from,
                        "reading_date_to": date_to,
                        "energy_kwh": float(energy_kwh),
                    }
                )

        if not readings:
            raise CSVImportError(
                f"No valid data found. Skipped {skipped_rows} invalid rows."
            )

        # Batch insert and refresh derived tables
        inserted = await self._batch_insert_readings(conn, readings)
        await self._refresh_daily_summaries(conn, affected_dates)
        await self._refresh_hourly_pattern(conn)

        logger.info(
            f"Processed {inserted} readings, skipped {skipped_rows} invalid rows"
        )
        return inserted

    async def _process_daily_summary_file(
        self, conn, file_path: str, header: List[str], dialect: Any
    ) -> int:
        summaries = []
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

        # Simplified insert for daily summaries
        insert_sql = text("""
            INSERT OR IGNORE INTO daily_energy_summary
                (date, total_energy_kwh, reading_count)
            VALUES
                (:date, :total_energy_kwh, 0)
        """)

        result = await conn.execute(insert_sql, summaries)
        inserted = result.rowcount

        logger.info(
            f"Imported {inserted} daily summaries, skipped {len(summaries) - inserted} duplicates"
        )
        return inserted

    async def process_csv_file(self, file_path: str) -> Dict[str, Any]:
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
                raise CSVImportError(f"Processing failed: {e}")
