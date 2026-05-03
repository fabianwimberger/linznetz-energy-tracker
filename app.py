#!/usr/bin/env python3
"""Energy Consumption Tracker"""

import os
import logging
from pathlib import Path
from typing import List, Optional, Literal, Dict, Any, MutableMapping
from datetime import date, datetime, timedelta
from contextlib import asynccontextmanager
from collections import defaultdict

import aiofiles  # type: ignore[import-untyped]
import uvicorn
from fastapi import FastAPI, HTTPException, Query, File, UploadFile, APIRouter, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

from csv_import import CSVProcessor, CSVImportError
from db_init import init_database
from linznetz_fetcher import FetchError, LinzNetzFetcher, NoDataError

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Configuration
DATA_DIR = Path(os.getenv("DATA_DIR", "/app/data"))
DATABASE_URL = os.getenv(
    "DATABASE_URL", f"sqlite+aiosqlite:///{DATA_DIR}/energy_data.db"
)
UPLOAD_DIR = DATA_DIR / "csv_uploads"
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB limit
STATIC_DIR = Path(os.getenv("STATIC_DIR", "/app/static"))

# Comma-separated origins; "*" allows any (useful for local dev).
CORS_ORIGINS = [
    o.strip() for o in os.getenv("CORS_ORIGINS", "*").split(",") if o.strip()
]

LINZNETZ_USERNAME = os.getenv("LINZNETZ_USERNAME")
LINZNETZ_PASSWORD = os.getenv("LINZNETZ_PASSWORD")
LINZNETZ_LOOKBACK_DAYS = int(os.getenv("LINZNETZ_LOOKBACK_DAYS", "7"))

db_context: Dict[str, Any] = {}
upload_tracker: MutableMapping[str, List[datetime]] = defaultdict(list)


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)
        await init_database(engine)

        db_context["engine"] = engine
        db_context["csv_processor"] = CSVProcessor(engine)

        logger.info("Database engine created and CSV processor initialized.")
        yield
    finally:
        if "engine" in db_context:
            await db_context["engine"].dispose()
            logger.info("Database engine disposed.")


app = FastAPI(
    title="Energy Analysis",
    description="Web application for graphical analysis of energy consumption.",
    version="1.1.0",
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

app.add_middleware(GZipMiddleware, minimum_size=500)


@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    return response


app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
api_router = APIRouter(prefix="/api")


class ImportResult(BaseModel):
    status: str
    filename: str
    records_processed: Optional[int] = None
    error: Optional[str] = None


class ChartData(BaseModel):
    labels: List[str]
    data: List[float]
    moving_average: Optional[List[Optional[float]]] = None
    daily_average_pattern: Optional[List[float]] = None
    forecast: Optional[List[Optional[float]]] = None


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def get_frontend():
    try:
        async with aiofiles.open(STATIC_DIR / "index.html", "r", encoding="utf-8") as f:
            content = await f.read()
            return HTMLResponse(content=content)
    except FileNotFoundError:
        return HTMLResponse(
            content="<h1>Error: index.html not found.</h1>", status_code=404
        )


@api_router.post("/import", response_model=List[ImportResult])
async def upload_and_import_csv(request: Request, files: List[UploadFile] = File(...)):
    """Import CSV files."""
    client_ip = request.client.host if request.client else "unknown"
    now = datetime.now()

    # Clean up old entries and check rate limit
    upload_tracker[client_ip] = [
        t for t in upload_tracker[client_ip] if now - t < timedelta(hours=1)
    ]

    if len(upload_tracker[client_ip]) >= 50:
        raise HTTPException(
            status_code=429, detail="Too many uploads. Please try again later."
        )

    upload_tracker[client_ip].append(now)

    # Clean up empty entries to prevent memory leak
    if not upload_tracker[client_ip]:
        del upload_tracker[client_ip]

    results = []
    for file in files:
        if not file.filename or not file.filename.endswith(".csv"):
            results.append(
                ImportResult(
                    status="error",
                    filename=file.filename or "unknown",
                    error="Only .csv files are allowed.",
                )
            )
            continue

        if file.size and file.size > MAX_FILE_SIZE:
            results.append(
                ImportResult(
                    status="error",
                    filename=file.filename,
                    error=f"File size exceeds {MAX_FILE_SIZE // (1024 * 1024)}MB limit.",
                )
            )
            continue

        safe_filename = Path(file.filename).name
        file_path = UPLOAD_DIR / safe_filename

        try:
            async with aiofiles.open(file_path, "wb") as buffer:
                content = await file.read()
                await buffer.write(content)

            result_dict = await db_context["csv_processor"].process_csv_file(
                str(file_path)
            )
            results.append(ImportResult(**result_dict))

        except CSVImportError as e:
            results.append(
                ImportResult(status="error", filename=file.filename, error=str(e))
            )
        except Exception as e:
            logger.error(
                f"Critical error processing {file.filename}: {e}", exc_info=True
            )
            results.append(
                ImportResult(
                    status="error",
                    filename=file.filename,
                    error="An internal server error has occurred.",
                )
            )
        finally:
            if file_path.exists():
                file_path.unlink(missing_ok=True)

    # Clean up tracker periodically
    if len(upload_tracker) > 100:
        cutoff = now - timedelta(hours=1)
        for ip in list(upload_tracker.keys()):
            upload_tracker[ip] = [t for t in upload_tracker[ip] if t > cutoff]
            if not upload_tracker[ip]:
                del upload_tracker[ip]

    return results


@api_router.post("/fetch", response_model=List[ImportResult])
async def fetch_from_linznetz(request: Request):
    """Pull missing quarter-hour data for the last LINZNETZ_LOOKBACK_DAYS days."""
    if not LINZNETZ_USERNAME or not LINZNETZ_PASSWORD:
        raise HTTPException(
            status_code=503,
            detail="LinzNetz credentials not configured (set LINZNETZ_USERNAME and LINZNETZ_PASSWORD)",
        )

    client_ip = request.client.host if request.client else "unknown"
    now = datetime.now()
    upload_tracker[client_ip] = [
        t for t in upload_tracker[client_ip] if now - t < timedelta(hours=1)
    ]
    if len(upload_tracker[client_ip]) >= 50:
        raise HTTPException(
            status_code=429, detail="Too many requests. Please try again later."
        )
    upload_tracker[client_ip].append(now)

    today = date.today()
    candidates = sorted(
        today - timedelta(days=i) for i in range(1, LINZNETZ_LOOKBACK_DAYS + 1)
    )
    # LinzNetz often pushes partial days, so a day is only "complete" when
    # all 96 quarter-hour slots are present. Anything below gets re-fetched.
    rows = await _fetch_data(
        db_context["engine"],
        """
        SELECT DATE(reading_date_from) AS d
        FROM energy_readings
        WHERE DATE(reading_date_from) >= :start
        GROUP BY DATE(reading_date_from)
        HAVING COUNT(*) >= 96
        """,
        {"start": candidates[0].isoformat()},
    )
    complete = {row["d"] for row in rows}
    missing = [d for d in candidates if d.isoformat() not in complete]

    if not missing:
        return [
            ImportResult(
                status="skipped",
                filename=f"last {LINZNETZ_LOOKBACK_DAYS} days",
                error="All days already imported.",
            )
        ]

    results: List[ImportResult] = []
    async with LinzNetzFetcher(LINZNETZ_USERNAME, LINZNETZ_PASSWORD) as fetcher:
        for day in missing:
            day_str = day.isoformat()
            try:
                body, server_name = await fetcher.fetch(
                    day, day, granularity="quarter", unit="KWH"
                )
            except NoDataError:
                results.append(
                    ImportResult(
                        status="skipped",
                        filename=day_str,
                        error="No data available yet.",
                    )
                )
                continue
            except FetchError as e:
                logger.warning("LinzNetz fetch failed for %s: %s", day_str, e)
                results.append(
                    ImportResult(status="error", filename=day_str, error=str(e))
                )
                continue
            except Exception as e:
                logger.error("LinzNetz fetch errored for %s", day_str, exc_info=True)
                results.append(
                    ImportResult(
                        status="error",
                        filename=day_str,
                        error=f"Unexpected error: {e}",
                    )
                )
                continue

            safe_name = Path(server_name).name
            file_path = UPLOAD_DIR / safe_name
            try:
                async with aiofiles.open(file_path, "wb") as f:
                    await f.write(body)
                result_dict = await db_context["csv_processor"].process_csv_file(
                    str(file_path)
                )
                results.append(ImportResult(**result_dict))
            except CSVImportError as e:
                results.append(
                    ImportResult(status="error", filename=safe_name, error=str(e))
                )
            except Exception as e:
                logger.error("Import failed for %s: %s", safe_name, e, exc_info=True)
                results.append(
                    ImportResult(
                        status="error",
                        filename=safe_name,
                        error="An internal server error has occurred.",
                    )
                )
            finally:
                if file_path.exists():
                    file_path.unlink(missing_ok=True)

    return results


async def _fetch_data(engine, query: str, params: Optional[Dict[str, Any]] = None):
    """Fetch data from the database."""
    async with engine.connect() as conn:
        result = await conn.execute(text(query), params or {})
        return result.mappings().fetchall()


@api_router.get("/chart-data", response_model=ChartData)
async def get_chart_data(
    aggregation: Literal["raw", "daily", "weekly", "monthly", "yearly"] = Query(
        "daily"
    ),
    day: Optional[date] = None,
):
    try:
        if aggregation == "raw":
            if not day:
                raise HTTPException(
                    status_code=400,
                    detail="A 'day' parameter is required for raw aggregation.",
                )

            # Daily raw view with average pattern overlay
            daily_query = """
                SELECT strftime('%H:%M', reading_date_from) as label, 
                       (energy_kwh * 4 * 1000) as value
                FROM energy_readings
                WHERE DATE(reading_date_from) = :day
                ORDER BY reading_date_from
            """

            # Precomputed daily pattern avoids full-table scans.
            pattern_query = """
                SELECT time_slot, avg_power_w
                FROM hourly_pattern
                ORDER BY time_slot
            """

            daily_rows = await _fetch_data(
                db_context["engine"], daily_query, {"day": day}
            )
            pattern_rows = await _fetch_data(db_context["engine"], pattern_query)

            if not daily_rows:
                return ChartData(labels=[], data=[], daily_average_pattern=[])

            pattern_map = {
                row["time_slot"]: float(row["avg_power_w"]) for row in pattern_rows
            }

            return ChartData(
                labels=[row["label"] for row in daily_rows],
                data=[float(row["value"]) for row in daily_rows],
                daily_average_pattern=[
                    pattern_map.get(row["label"], 0) for row in daily_rows
                ],
            )

        elif aggregation == "daily":
            query = """
                SELECT date as label,
                       total_energy_kwh as value,
                       AVG(total_energy_kwh) OVER (
                           ORDER BY date 
                           ROWS BETWEEN 45 PRECEDING AND 44 FOLLOWING
                       ) as moving_average
                FROM daily_energy_summary
                ORDER BY date
            """

            rows = await _fetch_data(db_context["engine"], query)

            if not rows:
                return ChartData(labels=[], data=[], moving_average=[])

            return ChartData(
                labels=[row["label"] for row in rows],
                data=[float(row["value"]) for row in rows],
                moving_average=[
                    float(row["moving_average"]) if row["moving_average"] else None
                    for row in rows
                ],
            )

        elif aggregation == "weekly":
            # Weekly aggregation with forecast
            query = """
                SELECT strftime('%Y-W%W', date) as label,
                       strftime('%Y-%W', date) as sort_key,
                       SUM(total_energy_kwh) as value,
                       COUNT(*) as day_count,
                       AVG(SUM(total_energy_kwh)) OVER (
                           ORDER BY strftime('%Y-%W', date)
                           ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
                       ) as moving_average
                FROM daily_energy_summary
                GROUP BY strftime('%Y-%W', date)
                ORDER BY strftime('%Y-%W', date)
            """

            rows = await _fetch_data(db_context["engine"], query)

            # Forecast current week
            current_date = datetime.now().date()
            current_week = current_date.strftime("%Y-%W")

            forecast_values: List[Optional[float]] = []
            for row in rows:
                if row["sort_key"] == current_week:
                    days_in_week = 7
                    actual_days = int(row["day_count"])
                    if actual_days < days_in_week:
                        avg_per_day = float(row["value"]) / actual_days
                        forecast = avg_per_day * days_in_week
                        forecast_values.append(forecast)
                    else:
                        forecast_values.append(None)
                else:
                    forecast_values.append(None)

            return ChartData(
                labels=[row["label"] for row in rows],
                data=[float(row["value"]) for row in rows],
                moving_average=[
                    float(row["moving_average"]) if row["moving_average"] else None
                    for row in rows
                ],
                forecast=forecast_values,
            )

        elif aggregation == "monthly":
            # Monthly aggregation with forecast
            query = """
                SELECT strftime('%Y-%m', date) as label,
                       SUM(total_energy_kwh) as value,
                       COUNT(*) as day_count,
                       AVG(SUM(total_energy_kwh)) OVER (
                           ORDER BY strftime('%Y-%m', date)
                           ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
                       ) as moving_average
                FROM daily_energy_summary
                GROUP BY strftime('%Y-%m', date)
                ORDER BY strftime('%Y-%m', date)
            """

            rows = await _fetch_data(db_context["engine"], query)

            # Forecast current month
            current_date = datetime.now().date()
            current_month = current_date.strftime("%Y-%m")

            forecast_values = []
            for row in rows:
                if row["label"] == current_month:
                    # Days in current month
                    year, month = map(int, row["label"].split("-"))
                    if month == 12:
                        next_month_date = datetime(year + 1, 1, 1).date()
                    else:
                        next_month_date = datetime(year, month + 1, 1).date()
                    days_in_month = (
                        next_month_date - datetime(year, month, 1).date()
                    ).days

                    actual_days = int(row["day_count"])
                    if actual_days < days_in_month:
                        avg_per_day = float(row["value"]) / actual_days
                        forecast = avg_per_day * days_in_month
                        forecast_values.append(forecast)
                    else:
                        forecast_values.append(None)
                else:
                    forecast_values.append(None)

            return ChartData(
                labels=[row["label"] for row in rows],
                data=[float(row["value"]) for row in rows],
                moving_average=[
                    float(row["moving_average"]) if row["moving_average"] else None
                    for row in rows
                ],
                forecast=forecast_values,
            )

        else:  # yearly
            # Yearly aggregation with forecast
            query = """
                SELECT strftime('%Y', date) as label,
                       SUM(total_energy_kwh) as value,
                       COUNT(*) as day_count,
                       AVG(SUM(total_energy_kwh)) OVER (
                           ORDER BY strftime('%Y', date)
                           ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
                       ) as moving_average
                FROM daily_energy_summary
                GROUP BY strftime('%Y', date)
                ORDER BY strftime('%Y', date)
            """

            rows = await _fetch_data(db_context["engine"], query)

            # Forecast current year
            current_date = datetime.now().date()
            current_year = current_date.strftime("%Y")

            forecast_values = []
            for row in rows:
                if row["label"] == current_year:
                    # Days in current year
                    year = int(row["label"])
                    days_in_year = (
                        366
                        if (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0))
                        else 365
                    )

                    actual_days = int(row["day_count"])
                    if actual_days < days_in_year:
                        avg_per_day = float(row["value"]) / actual_days
                        forecast = avg_per_day * days_in_year
                        forecast_values.append(forecast)
                    else:
                        forecast_values.append(None)
                else:
                    forecast_values.append(None)

            return ChartData(
                labels=[row["label"] for row in rows],
                data=[float(row["value"]) for row in rows],
                moving_average=[
                    float(row["moving_average"]) if row["moving_average"] else None
                    for row in rows
                ],
                forecast=forecast_values,
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Database error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Database error")


@api_router.get("/latest-date")
async def get_latest_data_date():
    """Returns the date of the most recent reading in the database."""
    query = "SELECT DATE(MAX(reading_date_from)) as latest_date FROM energy_readings"
    rows = await _fetch_data(db_context["engine"], query)
    data = rows[0] if rows else None
    return {
        "latest_date": data["latest_date"] if data and data["latest_date"] else None
    }


@api_router.get("/stats")
async def get_database_stats():
    """Get database statistics for monitoring"""
    query = """
        SELECT 
            (SELECT COUNT(*) FROM energy_readings) as total_readings,
            (SELECT COUNT(*) FROM daily_energy_summary) as total_days,
            (SELECT MIN(date) FROM daily_energy_summary) as first_date,
            (SELECT MAX(date) FROM daily_energy_summary) as last_date,
            (SELECT COUNT(*) FROM import_log WHERE processing_status = 'completed') as successful_imports
    """
    rows = await _fetch_data(db_context["engine"], query)
    return rows[0] if rows else {}


app.include_router(api_router)

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
