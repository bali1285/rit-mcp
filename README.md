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
| `sources/at_realtime.py` | AT realtime tools over the data lake |
| `sources/__init__.py` | The list of sources |
| `host.json` | Tells Functions to run `server.py` as an MCP custom handler |
| `requirements.txt` | Python packages, installed by Functions on deploy |
| `.vscode/mcp.json` | Connects VS Code to the deployed server and to a local run |
| `local.settings.json` | Local-only settings (not committed) |

## AT tools

| Tool | What it answers |
| --- | --- |
| `at_whats_running` | What's running now, split by operator |
| `at_late_trips` | Late trips now, by route and/or operator |
| `at_vehicle_positions` | Where vehicles are, by route and/or operator |
| `at_alerts` | Current service alerts, by route or effect |
| `at_cancellations` | Cancelled trips for a day, split by operator |
| `at_on_time_summary` | Punctuality for a day, by route or operator |
| `at_query_feed` | Anything else: read-only SQL over the `feed` view |
| `at_describe_feed` | Latest snapshot, operators, and the `feed` columns |

Data:

- Realtime: `ritadlsdevpublic/aucklandtransport/combinefeed/YYYY/MM/DD/HH/combinefeed_MMSS.parquet` (Auckland time, about one a minute).
- Operators: AT static GTFS in `aucklandtransport/GTFS/`. The server uses the newest `at_gtfs_routes_YYYYMMDD.parquet` and the `at_gtfs_agency_` file of the same date, reloaded every 6 hours. Routes it can't match fall back to vehicle label letters (RT, NB, GB).
- Day tools (`at_cancellations`, `at_on_time_summary`) read one snapshot every 15 minutes by default, so they're estimates.
- `at_query_feed` runs in an in-memory DuckDB with file and network access switched off and settings locked.

## Azure setup (Portal)

Function App `rit-mcp`, Flex Consumption, Python 3.12 or 3.13, resource group `RIT-Data-Dev`.

1. **Identity:** system-assigned identity on.
2. **Data access:** the identity needs **Storage Blob Data Reader** on the `ritadlsdevpublic` storage account. (Assigned by the IT help desk.)
3. **Environment variables (App settings):**

   | Name | Value |
   | --- | --- |
   | `APP_API_TOKEN` | Random string, 32+ characters. Clients send it as a bearer token. |
   | `AzureWebJobsFeatureFlags` | `EnableMcpCustomHandlerPreview` |
   | `PYTHONPATH` | `/home/site/wwwroot/.python_packages/lib/site-packages` |

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

Your own account needs Storage Blob Data Reader on the storage account too.

```
python -m venv .venv
.venv\Scripts\activate          (Windows)
pip install -r requirements.txt
az login
python server.py --describe     # checks the feed and GTFS load
```

Then either `func start` (uses `local.settings.json`, serves on port 7071) or
`python server.py` (port 8000, or set `MCP_PORT`; set `APP_API_TOKEN` first). The `rit-mcp-local`
entry in `.vscode/mcp.json` points at `func start`.

## Adding a source

1. Add `sources/<name>.py` with an `INSTRUCTIONS` string and a `register(mcp)`
   function that adds tools named `<prefix>_...`.
2. Add it to `SOURCES` in `sources/__init__.py`.

Before adding anything sensitive (the warehouse), move from the shared token to
Entra sign-in, connect as a read-only database role that only sees views, and
turn on VNet integration so the app can reach Postgres.
