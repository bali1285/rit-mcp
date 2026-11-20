# rit-mcp

One MCP server for Ritchies data. Ask your AI (Copilot, Claude) plain
questions and it calls these tools. Read-only.

Today it has one source: **Auckland Transport realtime** (`at_` tools). More
sources, such as the data warehouse, slot into `sources/` the same way.

Runs on Azure Functions (Flex Consumption) as a self-hosted MCP custom handler.
That hosting option is in public preview.

## Layout

| Path | What it is |
| --- | --- |
| `server.py` | The server: auth, `/health`, `/mcp`. Loads every source. |
| `sources/at_realtime.py` | AT tools over the data lake (realtime and daily history) |
| `sources/brand.py` | `rit_brand`: Ritchies logo, colours and report rules |
| `assets/` | Logo files served by `rit_brand` |
| `sources/__init__.py` | The list of sources |
| `host.json` | Tells Functions to run `server.py` as an MCP custom handler |
| `requirements.txt` | Python packages, installed by Functions on deploy |
| `.vscode/mcp.json` | Connects VS Code to the deployed server and to a local run |
| `local.settings.json` | Local-only settings (not committed) |

## Tools

| Tool | What it answers |
| --- | --- |
| `rit_brand` | Ritchies logo (as data URIs), colours, fonts, chart and writing rules. AIs call it before making a report |
| `at_whats_running` | What's running now, split by operator |
| `at_late_trips` | Late trips now, by route and/or operator |
| `at_vehicle_positions` | Where vehicles are, by route and/or operator |
| `at_alerts` | Service alerts active now: headline, routes, stops affected, active times |
| `at_cancellations` | Cancelled trips today (or a day), split by operator |
| `at_on_time_summary` | On time, delayed and major delay today (or a day), plus early and late starts. Group by operator, route or Ritchies depot |
| `at_query_feed` | Anything else about today: read-only SQL over the `feed` view |
| `at_performance` | Any date range (yesterday and earlier): missed, cancelled, delayed, major delay, punctuality, reliability, vehicles used. By operator, area, route, day, mode or Ritchies depot |
| `at_query_history` | Anything else over a date range: read-only SQL over the `trips`, `scheduled` and `missed` views |
| `at_alerts_history` | Alerts over a date range: counts by effect and cause, most affected routes, the alerts themselves |
| `at_describe_feed` | Latest snapshot, operators, and the `feed` columns |
| `ecan_whats_running` | Christchurch (Metro): buses running now, by operator (Ritchies, Go Bus) and route |
| `ecan_vehicle_positions` | Christchurch: where buses are now, by route and/or operator |
| `ecan_query` | Christchurch: read-only SQL over the `vehicles` view, for now, today or one single date |

Data:

- Realtime: `ritadlsdevpublic/aucklandtransport/combinefeed/YYYY/MM/DD/HH/combinefeed_MMSS.parquet` (Auckland time, about one a minute). Used for today.
- Daily: `combinefeed/YYYY/MM/trip_updates_DD.parquet` (one row per trip, final state) and `alerts_DD.parquet` (one row per alert), written the next morning. Used for yesterday and earlier, so date ranges are cheap.
- Operators: AT static GTFS in `aucklandtransport/GTFS/`. The server uses the newest `at_gtfs_routes_YYYYMMDD.parquet`, the `at_gtfs_agency_` file of the same date and the newest `at_gtfs_stops_` file, reloaded every 6 hours. Routes it can't match fall back to vehicle label letters (RT, NB, GB).
- Area (Central, North Shore, West, East, South, Far South, Far North, Islands) is approximate, from stop or vehicle positions.
- Vehicle positions outside the Auckland region are treated as junk GPS and blanked (`position_valid = false`). About a quarter of reporting vehicles do this, mostly parked ones.
- Definitions match the Command Centre (all env-overridable): delayed is more than 2 min late, major delay more than 5. Punctual is no more than 5 min late at the final stop, reliable no more than 15. Early start is leaving the first stop more than 1 min early, late start more than 5 min late. Arriving early at the end of a route is normal and counts as on time.
- Missed trips: scheduled in the GTFS timetable (trips, stop_times, calendar, calendar_dates) but never in the feed, not even as cancelled. A trip counts as seen if its trip_id turned up, or another variant of the same slot (route, start time, direction) did. Trips after midnight are only judged once the next day's file exists. Days outside the timetable on file get no missed figure, and the tool says so.
- Depots: Ritchies trips get a depot and division from `assets/route-depot-unit.csv` (same file as the Command Centre), matched on the middle part of the trip_id. Other operators have no depot. Update the CSV when the route allocation changes.
- ECan (Christchurch): `ecan/VehicleMonitoringDelivery/YYYY/MM/DD/HH/VehicleMonitoringDelivery_MMSS.parquet`, Metro's SIRI feed, about one a minute. One day at a time only: there's no daily roll-up, so ranges of days are refused as too large. No lateness: the feed's scheduled times are mostly just the snapshot time, so delays from it would be made up. Occupancy is always 0 so far. Set `ECAN_LOCAL_DATA_DIR` to test against a local copy.
- Alert text arrives as Python-style text; the server parses it into plain headlines, route lists and times.
- `at_query_feed` and `at_query_history` run in an in-memory DuckDB with file and network access switched off and settings locked.

## Brand kit

`rit_brand` serves `assets/ritchies-logo.png` and `assets/ritchies-icon.png` as data URIs, plus the colours (blue `#045293`, red `#EE3224`, grey `#9FB3C8` for competitors) and house rules. Change the files or `sources/brand.py` to update it for everyone.

## Azure setup (Portal)

Function App `rit-mcp`, Flex Consumption, Python 3.12 or 3.13, resource group `RIT-Data-Dev`.

1. **Identity:** system-assigned identity on.
2. **Data access:** the identity needs **Storage Blob Data Reader** on the `aucklandtransport` container in `ritadlsdevpublic`. (Assigned by the MSP.)
3. **Environment variables (App settings):**

   | Name | Value |
   | --- | --- |
   | `APP_API_TOKEN` | Random string, 32+ characters. Clients send it as a bearer token. |
   | `AzureWebJobsFeatureFlags` | `EnableMcpCustomHandlerPreview` |

   Optional: `AT_STORAGE_ACCOUNT`, `AT_CONTAINER` (defaults `ritadlsdevpublic`, `aucklandtransport`).
4. **Deploy:** in VS Code, Azure Functions extension > Deploy to Function App > `rit-mcp`. Functions installs `requirements.txt` during the deploy.
5. **Check:** open `https://<app-host>/health`. You should see `{"status": "ok"}`.

## Connect

Open this folder in VS Code. `.vscode/mcp.json` already has the server.
Check the URL matches the Function App's default domain on its Overview page.
Start `rit-mcp` from Copilot Chat's tools list (Agent mode). VS Code asks for
the token once and keeps it out of the file.

Any other MCP client: URL `https://<app-host>/mcp`, header
`Authorization: Bearer <APP_API_TOKEN>`.

## Run locally

Your own account needs Storage Blob Data Reader on the container too.

```
python -m venv .venv
.venv\Scripts\activate          (Windows)
pip install -r requirements.txt
az login
python server.py --describe     # checks the feed and GTFS load
```

Then either `func start` (uses `local.settings.json`, serves on port 7071) or
`python server.py` (port 8000, set `APP_API_TOKEN` first). The `rit-mcp-local`
entry in `.vscode/mcp.json` points at `func start`.

## Adding a source

1. Add `sources/<name>.py` with an `INSTRUCTIONS` string and a `register(mcp)`
   function that adds tools named `<prefix>_...`.
2. Add it to `SOURCES` in `sources/__init__.py`.

Before adding anything sensitive (the warehouse), move from the shared token to
Entra sign-in, connect as a read-only database role that only sees views, and
turn on VNet integration so the app can reach Postgres.
