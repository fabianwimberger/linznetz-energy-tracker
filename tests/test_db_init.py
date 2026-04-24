"""Tests for database initialization and migrations."""

import tempfile
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from db_init import SCHEMA_VERSION, apply_migrations, init_database


class TestInitDatabase:
    @pytest.mark.asyncio
    async def test_creates_schema_version_table(self):
        db_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        db_file.close()
        database_url = f"sqlite+aiosqlite:///{db_file.name}"

        engine = create_async_engine(database_url, pool_pre_ping=True)
        await init_database(engine)

        async with engine.connect() as conn:
            result = await conn.execute(
                text(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version'"
                )
            )
            assert result.scalar_one_or_none() == "schema_version"

        await engine.dispose()
        Path(db_file.name).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_sets_pragmas(self):
        db_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        db_file.close()
        database_url = f"sqlite+aiosqlite:///{db_file.name}"

        engine = create_async_engine(database_url, pool_pre_ping=True)
        await init_database(engine)

        async with engine.connect() as conn:
            result = await conn.execute(text("PRAGMA journal_mode"))
            assert result.scalar_one() == "wal"

        await engine.dispose()
        Path(db_file.name).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_creates_core_tables(self):
        db_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        db_file.close()
        database_url = f"sqlite+aiosqlite:///{db_file.name}"

        engine = create_async_engine(database_url, pool_pre_ping=True)
        await init_database(engine)

        async with engine.connect() as conn:
            for table in (
                "energy_readings",
                "daily_energy_summary",
                "import_log",
                "hourly_pattern",
            ):
                result = await conn.execute(
                    text(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name=:name"
                    ),
                    {"name": table},
                )
                assert result.scalar_one_or_none() == table

        await engine.dispose()
        Path(db_file.name).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_records_schema_version(self):
        db_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        db_file.close()
        database_url = f"sqlite+aiosqlite:///{db_file.name}"

        engine = create_async_engine(database_url, pool_pre_ping=True)
        await init_database(engine)

        async with engine.connect() as conn:
            result = await conn.execute(
                text("SELECT MAX(version) as v FROM schema_version")
            )
            assert result.scalar_one() == SCHEMA_VERSION

        await engine.dispose()
        Path(db_file.name).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_idempotent_runs(self):
        db_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        db_file.close()
        database_url = f"sqlite+aiosqlite:///{db_file.name}"

        engine = create_async_engine(database_url, pool_pre_ping=True)
        await init_database(engine)
        await init_database(engine)

        async with engine.connect() as conn:
            result = await conn.execute(
                text("SELECT MAX(version) as v FROM schema_version")
            )
            assert result.scalar_one() == SCHEMA_VERSION

        await engine.dispose()
        Path(db_file.name).unlink(missing_ok=True)


class TestApplyMigrations:
    @pytest.mark.asyncio
    async def test_v0_to_v1_creates_tables(self):
        db_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        db_file.close()
        database_url = f"sqlite+aiosqlite:///{db_file.name}"

        engine = create_async_engine(database_url, pool_pre_ping=True)

        async with engine.begin() as conn:
            await conn.execute(
                text("""
                    CREATE TABLE schema_version (
                        version INTEGER PRIMARY KEY,
                        applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
            )
            await apply_migrations(conn, 0)

        async with engine.connect() as conn:
            result = await conn.execute(
                text(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='energy_readings'"
                )
            )
            assert result.scalar_one_or_none() == "energy_readings"

        await engine.dispose()
        Path(db_file.name).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_v1_to_v2_creates_indexes(self):
        db_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        db_file.close()
        database_url = f"sqlite+aiosqlite:///{db_file.name}"

        engine = create_async_engine(database_url, pool_pre_ping=True)
        await init_database(engine)

        async with engine.connect() as conn:
            result = await conn.execute(
                text(
                    "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_readings_date'"
                )
            )
            assert result.scalar_one_or_none() == "idx_readings_date"

        await engine.dispose()
        Path(db_file.name).unlink(missing_ok=True)
