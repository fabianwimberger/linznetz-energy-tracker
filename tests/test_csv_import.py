"""Tests for CSV import functionality."""

from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from csv_import import CSVImportError, CSVProcessor

UTC = ZoneInfo("UTC")
VIENNA = ZoneInfo("Europe/Vienna")


class TestParseGermanDatetime:
    def test_valid_datetime(self):
        result = CSVProcessor.parse_german_datetime("01.01.2025 00:00")
        # January is CET (UTC+1)
        assert result == datetime(2024, 12, 31, 23, 0, tzinfo=UTC)

    def test_valid_datetime_different_time(self):
        result = CSVProcessor.parse_german_datetime("15.06.2023 14:30")
        # June is CEST (UTC+2)
        assert result == datetime(2023, 6, 15, 12, 30, tzinfo=UTC)

    def test_empty_string(self):
        assert CSVProcessor.parse_german_datetime("") is None

    def test_none_input(self):
        assert CSVProcessor.parse_german_datetime(None) is None

    def test_invalid_format(self):
        assert CSVProcessor.parse_german_datetime("2025-01-01 00:00") is None

    def test_dst_fold_second_occurrence(self):
        """Autumn DST: second 02:00 gets fold=1 and maps to 01:00 UTC (CET)."""
        result = CSVProcessor.parse_german_datetime("27.10.2024 02:00", fold=1)
        # fold=1 => CET (UTC+1)
        assert result == datetime(2024, 10, 27, 1, 0, tzinfo=UTC)

    def test_dst_fold_first_occurrence(self):
        """Autumn DST: first 02:00 gets fold=0 and maps to 00:00 UTC (CEST)."""
        result = CSVProcessor.parse_german_datetime("27.10.2024 02:00", fold=0)
        # fold=0 => CEST (UTC+2)
        assert result == datetime(2024, 10, 27, 0, 0, tzinfo=UTC)


class TestParseAnyDailyDate:
    def test_german_format(self):
        result = CSVProcessor.parse_any_daily_date("01.01.2025")
        assert result == date(2025, 1, 1)

    def test_iso_format(self):
        result = CSVProcessor.parse_any_daily_date("2025-01-01")
        assert result == date(2025, 1, 1)

    def test_two_digit_year_format(self):
        result = CSVProcessor.parse_any_daily_date("25-01-01")
        assert result == date(2025, 1, 1)

    def test_empty_string(self):
        assert CSVProcessor.parse_any_daily_date("") is None

    def test_invalid_format(self):
        assert CSVProcessor.parse_any_daily_date("not a date") is None


class TestParseGermanDecimal:
    def test_comma_decimal(self):
        result = CSVProcessor.parse_german_decimal("12,345")
        assert result == Decimal("12.345")

    def test_whitespace_stripping(self):
        result = CSVProcessor.parse_german_decimal("  0,123  ")
        assert result == Decimal("0.123")

    def test_empty_string(self):
        assert CSVProcessor.parse_german_decimal("") is None

    def test_none_input(self):
        assert CSVProcessor.parse_german_decimal(None) is None

    def test_invalid_value(self):
        assert CSVProcessor.parse_german_decimal("abc") is None


class TestValidateEnergyValue:
    def test_valid_value(self):
        assert CSVProcessor.validate_energy_value(Decimal("10.5")) is True

    def test_zero_value(self):
        assert CSVProcessor.validate_energy_value(Decimal("0")) is True

    def test_max_boundary(self):
        assert CSVProcessor.validate_energy_value(Decimal("100")) is True

    def test_negative_value(self):
        assert CSVProcessor.validate_energy_value(Decimal("-1")) is False

    def test_above_max(self):
        assert CSVProcessor.validate_energy_value(Decimal("100.1")) is False


class TestValidateDateSequence:
    def test_valid_15_minute_interval(self):
        dt_from = datetime(2025, 1, 1, 23, 0, tzinfo=UTC)   # 00:00 CET
        dt_to = datetime(2025, 1, 1, 23, 15, tzinfo=UTC)    # 00:15 CET
        assert CSVProcessor.validate_date_sequence(dt_from, dt_to) is True

    def test_invalid_interval(self):
        dt_from = datetime(2025, 1, 1, 23, 0, tzinfo=UTC)
        dt_to = datetime(2025, 1, 1, 23, 30, tzinfo=UTC)
        assert CSVProcessor.validate_date_sequence(dt_from, dt_to) is False

    def test_negative_interval(self):
        dt_from = datetime(2025, 1, 1, 23, 15, tzinfo=UTC)
        dt_to = datetime(2025, 1, 1, 23, 0, tzinfo=UTC)
        assert CSVProcessor.validate_date_sequence(dt_from, dt_to) is False


class TestCalculateFileHash:
    def test_consistent_hash(self, tmp_path: Path):
        file_path = tmp_path / "test.csv"
        file_path.write_text("test content")

        hash1 = CSVProcessor.calculate_file_hash(str(file_path))
        hash2 = CSVProcessor.calculate_file_hash(str(file_path))

        assert hash1 == hash2
        assert len(hash1) == 64  # SHA-256 hex length

    def test_different_content_different_hash(self, tmp_path: Path):
        file1 = tmp_path / "a.csv"
        file2 = tmp_path / "b.csv"
        file1.write_text("content a")
        file2.write_text("content b")

        hash1 = CSVProcessor.calculate_file_hash(str(file1))
        hash2 = CSVProcessor.calculate_file_hash(str(file2))

        assert hash1 != hash2


class TestProcessCSVFile:
    @pytest.fixture
    def processor(self, test_engine):
        return CSVProcessor(test_engine)

    @pytest.mark.asyncio
    async def test_quarter_hourly_csv(self, processor: CSVProcessor, tmp_path: Path):
        csv_file = tmp_path / "quarter_hourly.csv"
        csv_file.write_text(
            "Datum von;Datum bis;Energiemenge in kWh\n"
            "01.01.2025 00:00;01.01.2025 00:15;0,123\n"
            "01.01.2025 00:15;01.01.2025 00:30;0,456\n"
        )

        result = await processor.process_csv_file(str(csv_file))

        assert result["status"] == "success"
        assert result["filename"] == "quarter_hourly.csv"
        assert result["records_processed"] == 2

    @pytest.mark.asyncio
    async def test_daily_summary_csv(self, processor: CSVProcessor, tmp_path: Path):
        csv_file = tmp_path / "daily.csv"
        csv_file.write_text(
            "Datum;Energiemenge in kWh\n01.01.2025;12,345\n02.01.2025;10,000\n"
        )

        result = await processor.process_csv_file(str(csv_file))

        assert result["status"] == "success"
        assert result["filename"] == "daily.csv"
        assert result["records_processed"] == 2

    @pytest.mark.asyncio
    async def test_duplicate_detection(self, processor: CSVProcessor, tmp_path: Path):
        csv_file = tmp_path / "duplicate.csv"
        csv_file.write_text("Datum;Energiemenge in kWh\n01.01.2025;12,345\n")

        result1 = await processor.process_csv_file(str(csv_file))
        assert result1["status"] == "success"

        result2 = await processor.process_csv_file(str(csv_file))
        assert result2["status"] == "skipped"
        assert "hash match" in result2["message"]

    @pytest.mark.asyncio
    async def test_unknown_format(self, processor: CSVProcessor, tmp_path: Path):
        csv_file = tmp_path / "unknown.csv"
        csv_file.write_text("Column A;Column B;Column C\n1;2;3\n")

        with pytest.raises(CSVImportError, match="Unknown CSV format"):
            await processor.process_csv_file(str(csv_file))

    @pytest.mark.asyncio
    async def test_empty_file(self, processor: CSVProcessor, tmp_path: Path):
        csv_file = tmp_path / "empty.csv"
        csv_file.write_text("Datum;Energiemenge in kWh\n")

        with pytest.raises(CSVImportError, match="No valid daily data found"):
            await processor.process_csv_file(str(csv_file))

    @pytest.mark.asyncio
    async def test_invalid_energy_value(self, processor: CSVProcessor, tmp_path: Path):
        csv_file = tmp_path / "invalid.csv"
        csv_file.write_text(
            "Datum von;Datum bis;Energiemenge in kWh\n"
            "01.01.2025 00:00;01.01.2025 00:15;200,0\n"
        )

        with pytest.raises(CSVImportError, match="No valid data found"):
            await processor.process_csv_file(str(csv_file))
