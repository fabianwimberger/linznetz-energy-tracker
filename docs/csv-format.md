# CSV Format

The importer auto-detects two formats based on headers:

**Quarter-hourly** (one row per 15-minute interval)

```
Datum von;Datum bis;Energiemenge in kWh
01.01.2025 00:00;01.01.2025 00:15;0,123
...
```

**Daily summary** (one row per day)

```
Datum;Energiemenge in kWh
01.01.2025;12,345
...
```

Decimal values use a comma as separator. The delimiter (`;` / `,` / tab) is
auto-detected.

## Export

`GET /api/export` downloads your data in the same format shown above, so the
file can be re-imported (e.g. into a fresh instance) without any conversion.

| Parameter | Values | Description |
|---|---|---|
| `granularity` | `raw` (default), `daily` | Quarter-hourly readings or the daily summary |
| `from`, `to` | `YYYY-MM-DD` | Optional inclusive date range |

Download links are also available in the sidebar under **Data Export**.
