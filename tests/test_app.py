"""Tests for FastAPI application endpoints."""

from datetime import date, timedelta
from pathlib import Path

import app as app_module
from linznetz_fetcher import FetchError, NoDataError


class TestRootEndpoint:
    def test_returns_html(self, client):
        response = client.get("/")
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]

    def test_security_headers_present(self, client):
        response = client.get("/")
        assert response.headers["X-Content-Type-Options"] == "nosniff"
        assert response.headers["X-Frame-Options"] == "DENY"


class TestLatestDateEndpoint:
    def test_returns_json(self, client):
        response = client.get("/api/latest-date")
        assert response.status_code == 200
        assert response.json() == {"latest_date": None}


class TestStatsEndpoint:
    def test_returns_zero_stats_for_empty_db(self, client):
        response = client.get("/api/stats")
        assert response.status_code == 200
        data = response.json()
        assert data["total_readings"] == 0
        assert data["total_days"] == 0
        assert data["successful_imports"] == 0


class TestChartDataEndpoint:
    def test_daily_aggregation_empty(self, client):
        response = client.get("/api/chart-data?aggregation=daily")
        assert response.status_code == 200
        data = response.json()
        assert data["labels"] == []
        assert data["data"] == []

    def test_raw_requires_day(self, client):
        response = client.get("/api/chart-data?aggregation=raw")
        assert response.status_code == 400
        assert "day" in response.json()["detail"].lower()

    def test_invalid_aggregation(self, client):
        response = client.get("/api/chart-data?aggregation=invalid")
        assert response.status_code == 422

    def test_raw_with_day_empty(self, client):
        response = client.get("/api/chart-data?aggregation=raw&day=2025-01-01")
        assert response.status_code == 200
        data = response.json()
        assert data["labels"] == []
        assert data["data"] == []

    def test_weekly_aggregation_empty(self, client):
        response = client.get("/api/chart-data?aggregation=weekly")
        assert response.status_code == 200
        data = response.json()
        assert data["labels"] == []
        assert data["data"] == []

    def test_monthly_aggregation_empty(self, client):
        response = client.get("/api/chart-data?aggregation=monthly")
        assert response.status_code == 200
        data = response.json()
        assert data["labels"] == []
        assert data["data"] == []

    def test_yearly_aggregation_empty(self, client):
        response = client.get("/api/chart-data?aggregation=yearly")
        assert response.status_code == 200
        data = response.json()
        assert data["labels"] == []
        assert data["data"] == []


class TestImportEndpoint:
    def test_upload_csv_file(self, client, tmp_path: Path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("Datum;Energiemenge in kWh\n01.01.2025;12,345\n")

        with csv_file.open("rb") as f:
            response = client.post(
                "/api/import",
                files={"files": ("test.csv", f, "text/csv")},
            )

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["status"] == "success"
        assert data[0]["records_processed"] == 1

    def test_rejects_non_csv(self, client, tmp_path: Path):
        txt_file = tmp_path / "test.txt"
        txt_file.write_text("not a csv")

        with txt_file.open("rb") as f:
            response = client.post(
                "/api/import",
                files={"files": ("test.txt", f, "text/plain")},
            )

        assert response.status_code == 200
        data = response.json()
        assert data[0]["status"] == "error"
        assert ".csv" in data[0]["error"]

    def test_duplicate_upload(self, client, tmp_path: Path):
        csv_file = tmp_path / "duplicate.csv"
        csv_file.write_text("Datum;Energiemenge in kWh\n01.01.2025;12,345\n")

        with csv_file.open("rb") as f:
            response1 = client.post(
                "/api/import",
                files={"files": ("duplicate.csv", f, "text/csv")},
            )
        assert response1.status_code == 200
        assert response1.json()[0]["status"] == "success"

        with csv_file.open("rb") as f:
            response2 = client.post(
                "/api/import",
                files={"files": ("duplicate.csv", f, "text/csv")},
            )
        assert response2.status_code == 200
        assert response2.json()[0]["status"] == "skipped"

    def test_rate_limiting(self, client, tmp_path: Path):
        csv_file = tmp_path / "rate.csv"
        csv_file.write_text("Datum;Energiemenge in kWh\n01.01.2025;12,345\n")

        # Upload many times to trigger rate limit
        for _ in range(51):
            with csv_file.open("rb") as f:
                response = client.post(
                    "/api/import",
                    files={"files": ("rate.csv", f, "text/csv")},
                )

        # The 51st upload should be rate limited
        assert response.status_code == 429
        assert "too many" in response.json()["detail"].lower()

    def test_upload_quarter_hourly(self, client, tmp_path: Path):
        csv_file = tmp_path / "quarter.csv"
        csv_file.write_text(
            "Datum von;Datum bis;Energiemenge in kWh\n"
            "01.01.2025 00:00;01.01.2025 00:15;0,123\n"
            "01.01.2025 00:15;01.01.2025 00:30;0,456\n"
        )

        with csv_file.open("rb") as f:
            response = client.post(
                "/api/import",
                files={"files": ("quarter.csv", f, "text/csv")},
            )

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["status"] == "success"
        assert data[0]["records_processed"] == 2

    def test_import_updates_stats(self, client, tmp_path: Path):
        csv_file = tmp_path / "stats.csv"
        csv_file.write_text("Datum;Energiemenge in kWh\n01.01.2025;12,345\n")

        with csv_file.open("rb") as f:
            client.post(
                "/api/import",
                files={"files": ("stats.csv", f, "text/csv")},
            )

        response = client.get("/api/stats")
        assert response.status_code == 200
        data = response.json()
        assert data["total_days"] == 1
        assert data["successful_imports"] == 1


def _quarter_hour_csv_for(day: date, kwh: float = 0.123) -> bytes:
    rows = ["Datum von;Datum bis;Energiemenge in kWh"]
    fmt = day.strftime("%d.%m.%Y")
    next_fmt = (day + timedelta(days=1)).strftime("%d.%m.%Y")
    kwh_de = f"{kwh:.3f}".replace(".", ",")
    for hour in range(24):
        for q in range(4):
            start = f"{fmt} {hour:02d}:{q * 15:02d}"
            if q < 3:
                end = f"{fmt} {hour:02d}:{(q + 1) * 15:02d}"
            elif hour < 23:
                end = f"{fmt} {hour + 1:02d}:00"
            else:
                end = f"{next_fmt} 00:00"
            rows.append(f"{start};{end};{kwh_de}")
    return ("\n".join(rows) + "\n").encode("utf-8")


class FakeFetcher:
    """Async-context-manager replacement for LinzNetzFetcher used in tests."""

    def __init__(self, plan):
        self._plan = plan
        self.calls = []

    def __call__(self, *_, **__):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def fetch(self, date_from, date_to, *, granularity="quarter", unit="KWH"):
        self.calls.append((date_from, date_to, granularity))
        action = self._plan.get(date_from)
        if action is None:
            raise NoDataError(f"no data for {date_from}")
        if isinstance(action, Exception):
            raise action
        return action, f"AT_QH_{date_from.strftime('%Y%m%d')}.csv"


class TestFetchEndpoint:
    def test_503_when_creds_missing(self, client, monkeypatch):
        monkeypatch.setattr(app_module, "LINZNETZ_USERNAME", None)
        monkeypatch.setattr(app_module, "LINZNETZ_PASSWORD", None)
        response = client.post("/api/fetch")
        assert response.status_code == 503
        assert "credentials" in response.json()["detail"].lower()

    def test_all_days_present_returns_skipped(self, client, monkeypatch, tmp_path):
        # Pre-populate full quarter-hour data (incl. the 23:45 slot) for the
        # whole lookback window so the endpoint sees nothing as missing.
        today = date.today()
        for i in range(1, 8):
            d = today - timedelta(days=i)
            csv = tmp_path / f"qh_{d}.csv"
            csv.write_bytes(_quarter_hour_csv_for(d))
            with csv.open("rb") as f:
                client.post("/api/import", files={"files": (csv.name, f, "text/csv")})

        monkeypatch.setattr(app_module, "LINZNETZ_USERNAME", "u")
        monkeypatch.setattr(app_module, "LINZNETZ_PASSWORD", "p")
        # If something tries to call the real fetcher, fail loudly.
        monkeypatch.setattr(app_module, "LinzNetzFetcher", FakeFetcher({}))

        response = client.post("/api/fetch")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["status"] == "skipped"
        assert "already imported" in (data[0]["error"] or "").lower()

    def test_partial_day_is_refetched(self, client, monkeypatch, tmp_path):
        # A day below the 96-slot threshold must be re-fetched — the
        # criterion is "every 15-min slot present", not just "any data".
        target_day = date.today() - timedelta(days=1)
        partial = (
            "Datum von;Datum bis;Energiemenge in kWh\n"
            f"{target_day.strftime('%d.%m.%Y')} 00:00;{target_day.strftime('%d.%m.%Y')} 00:15;0,123\n"
        )
        csv = tmp_path / "partial.csv"
        csv.write_text(partial)
        with csv.open("rb") as f:
            client.post("/api/import", files={"files": (csv.name, f, "text/csv")})

        monkeypatch.setattr(app_module, "LINZNETZ_USERNAME", "u")
        monkeypatch.setattr(app_module, "LINZNETZ_PASSWORD", "p")
        plan = {target_day: _quarter_hour_csv_for(target_day)}
        monkeypatch.setattr(app_module, "LinzNetzFetcher", FakeFetcher(plan))

        response = client.post("/api/fetch")
        assert response.status_code == 200
        data = response.json()
        successes = [r for r in data if r["status"] == "success"]
        assert len(successes) == 1
        assert successes[0]["records_processed"] == 96

    def test_happy_path_imports_missing_day(self, client, monkeypatch):
        monkeypatch.setattr(app_module, "LINZNETZ_USERNAME", "u")
        monkeypatch.setattr(app_module, "LINZNETZ_PASSWORD", "p")
        target_day = date.today() - timedelta(days=1)
        plan = {target_day: _quarter_hour_csv_for(target_day)}
        # Other days raise NoDataError → "skipped".
        fake = FakeFetcher(plan)
        monkeypatch.setattr(app_module, "LinzNetzFetcher", fake)

        response = client.post("/api/fetch")
        assert response.status_code == 200
        data = response.json()
        # One success for target_day, six "skipped" entries for the rest.
        successes = [r for r in data if r["status"] == "success"]
        skipped = [r for r in data if r["status"] == "skipped"]
        assert len(successes) == 1
        assert successes[0]["records_processed"] == 96
        assert len(skipped) == 6
        assert all("no data available" in r["error"].lower() for r in skipped)

    def test_fetch_error_surfaces_per_day(self, client, monkeypatch):
        monkeypatch.setattr(app_module, "LINZNETZ_USERNAME", "u")
        monkeypatch.setattr(app_module, "LINZNETZ_PASSWORD", "p")
        target_day = date.today() - timedelta(days=1)
        plan = {target_day: FetchError("boom")}
        monkeypatch.setattr(app_module, "LinzNetzFetcher", FakeFetcher(plan))

        response = client.post("/api/fetch")
        assert response.status_code == 200
        data = response.json()
        errors = [r for r in data if r["status"] == "error"]
        assert len(errors) == 1
        assert errors[0]["error"] == "boom"
