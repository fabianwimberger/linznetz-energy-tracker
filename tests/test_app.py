"""Tests for FastAPI application endpoints."""

from pathlib import Path


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
