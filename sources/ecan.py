"""Environment Canterbury (Metro, Christchurch) source.

Read-only tools over the Metro SIRI VehicleMonitoring snapshots saved as
Parquet in the `ecan` container:

    <container>/VehicleMonitoringDelivery/YYYY/MM/DD/HH/VehicleMonitoringDelivery_MMSS.parquet

Folder times are Auckland/Christchurch local time. One snapshot is roughly one
minute. There's no daily roll-up yet, so these tools only cover one day at a
time. They also don't report lateness: the feed's "aimed" time is usually just
the snapshot time, so any delay worked out from it would be made up.

All tools are registered with an `ecan_` prefix by `register()`.
"""

from __future__ import annotations

import logging
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from functools import lru_cache
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from mcp.server.mcpserver.exceptions import ToolError

from .at_realtime import QUERY_TIMEOUT_S, STORAGE_ACCOUNT, TZ, _explained, _rows, _str_col

CONTAINER = os.getenv("ECAN_CONTAINER", "ecan")
VM_PREFIX = os.getenv("ECAN_VM_PREFIX", "VehicleMonitoringDelivery")
LOCAL_DATA_DIR = os.getenv("ECAN_LOCAL_DATA_DIR")  # optional: a local copy of the container
MAX_FILES = int(os.getenv("ECAN_MAX_FILES_PER_CALL", "200"))

TOO_BIG = ("ECan data covers one day at a time. It's stored as one file a minute with no "
           "daily roll-up, so a range of days is too large to read on the fly. Ask about "
           "right now, today, or a single date instead.")

# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------

_container_client = None


def _container():
    global _container_client
    if _container_client is None:
        from azure.identity import DefaultAzureCredential
        from azure.storage.blob import ContainerClient

        _container_client = ContainerClient(
            account_url=f"https://{STORAGE_ACCOUNT}.blob.core.windows.net",
            container_name=CONTAINER,
            credential=DefaultAzureCredential(),
        )
    return _container_client


SNAPSHOT_RE = re.compile(r"/\d{4}/\d{2}/\d{2}/\d{2}/[^/]*_\d{4}\.parquet$")


def _list(prefix: str) -> list[str]:
    if LOCAL_DATA_DIR:
        root = Path(LOCAL_DATA_DIR)
        base = root / prefix
        if not base.exists():
            return []
        names = (p.relative_to(root).as_posix() for p in base.rglob("*.parquet"))
    else:
        names = (b.name for b in _container().list_blobs(name_starts_with=prefix))
    # Only real snapshots: the container also has stray files beside the folders.
    return sorted(n for n in names if SNAPSHOT_RE.search(n))


@lru_cache(maxsize=int(os.getenv("CACHE_FILES", "256")))
def _read(name: str) -> pa.Table:
    if LOCAL_DATA_DIR:
        return pq.read_table(Path(LOCAL_DATA_DIR) / name)
    return pq.read_table(pa.BufferReader(_container().download_blob(name).readall()))


def _snapshot_time(name: str) -> datetime:
    parts = name.split("/")
    year, month, day, hour = (int(p) for p in parts[-5:-1])
    mmss = Path(parts[-1]).stem.split("_")[-1]
    return datetime(year, month, day, hour, int(mmss[:2]), int(mmss[2:4]), tzinfo=TZ)


def _latest_name() -> str:
    now = datetime.now(TZ)
    for back in range(4):
        names = _list(f"{VM_PREFIX}/{now - timedelta(hours=back):%Y/%m/%d/%H}/")
        if names:
            return names[-1]
    raise ToolError("No ECan snapshots in the last 3 hours. The ECan feed may have stopped.")


def _parse_day(day: str) -> datetime:
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}\s*(to|-|\.\.|,)\s*\d{4}-\d{2}-\d{2}", day.strip()):
        raise ToolError(TOO_BIG)
    try:
        return datetime.strptime(day.strip(), "%Y-%m-%d").replace(tzinfo=TZ)
    except ValueError:
        raise ToolError("scope must be 'latest', 'today' or one date as YYYY-MM-DD. " + TOO_BIG) from None


def _scope_names(scope: str, every_minutes: int) -> list[str]:
    scope = (scope or "latest").strip().lower()
    if scope == "latest":
        return [_latest_name()]
    d = datetime.now(TZ) if scope == "today" else _parse_day(scope)
    names = _list(f"{VM_PREFIX}/{d:%Y/%m/%d}/")
    every = max(1, every_minutes)
    picked, last = [], None
    for n in names:
        t = _snapshot_time(n)
        slot = (t.hour * 60 + t.minute) // every
        if slot != last:
            picked.append(n)
            last = slot
    if len(picked) > MAX_FILES:
        raise ToolError(f"That needs {len(picked)} snapshots (limit {MAX_FILES}). Use a bigger every_minutes.")
    return picked


def _load(names: list[str]) -> pa.Table:
    with ThreadPoolExecutor(max_workers=8) as pool:
        tables = list(pool.map(_read, names))
    out = []
    for name, t in zip(names, tables):
        t = pa.table({c: _str_col(t, c) for c in t.column_names})
        ts = _snapshot_time(name).isoformat(timespec="seconds")
        out.append(t.append_column("_snapshot", pa.array([ts] * t.num_rows, pa.string())))
    return pa.concat_tables(out, promote_options="default")


# ---------------------------------------------------------------------------
# Query layer: one clean `vehicles` view
# ---------------------------------------------------------------------------

J = "MonitoredVehicleJourney."
RAW = {
    "response_ts": "ResponseTimestamp", "recorded_at": "RecordedAt",
    "vehicle_ref": "VehicleMonitoringRef", "vehicle_ref2": J + "VehicleRef",
    "line_ref": J + "LineRef", "direction": J + "DirectionRef", "variant": J + "VariantRef",
    "trip_code": J + "TripImportCode", "service_date": J + "FramedVehicleJourneyRef.DataFrameRef",
    "journey_ref": J + "FramedVehicleJourneyRef.DatedVehicleJourneyRef",
    "route": J + "PublishedLineName", "operator": J + "OperatorRef",
    "origin_name": J + "OriginName", "destination_name": J + "DestinationName",
    "monitored": J + "Monitored", "lon": J + "VehicleLocation.Longitude",
    "lat": J + "VehicleLocation.Latitude", "occupancy_pct": J + "OccupancyPercent",
    "passengers": J + "PassengerCountOnBus", "at_stop": J + "MonitoredCall.VehicleAtStop",
    "stop_name": J + "MonitoredCall.StopPointName",
}

VIEW_SQL = r"""
CREATE MACRO nz(x) AS nullif(nullif(nullif(trim(CAST(x AS VARCHAR)), 'nan'), 'None'), '');
CREATE VIEW vehicles AS
WITH b AS (
  SELECT
    -- Local NZ time, kept without an offset (same as the AT tools).
    CAST(substr(_snapshot, 1, 19) AS TIMESTAMP) AS snapshot_time,
    TRY_CAST(substr(nz(recorded_at), 1, 19) AS TIMESTAMP) AS recorded_at,
    coalesce(nz(vehicle_ref), nz(vehicle_ref2)) AS vehicle_id,
    nz(route) AS route, nz(line_ref) AS line_ref, nz(variant) AS variant,
    nz(direction) AS direction, nz(origin_name) AS origin_name, nz(destination_name) AS destination_name,
    nz(trip_code) AS trip_code, nz(journey_ref) AS journey_ref,
    TRY_CAST(nz(service_date) AS DATE) AS service_date,
    nz(operator) AS operator_raw,
    CASE WHEN nz(operator) ILIKE '%ritchies%' THEN 'Ritchies'
         WHEN nz(operator) ILIKE '%go%bus%' THEN 'Go Bus'
         ELSE nz(operator) END AS operator,
    TRY_CAST(nz(lat) AS DOUBLE) AS lat0, TRY_CAST(nz(lon) AS DOUBLE) AS lon0,
    TRY_CAST(nz(occupancy_pct) AS DOUBLE) AS occupancy_pct,
    TRY_CAST(TRY_CAST(nz(passengers) AS DOUBLE) AS INTEGER) AS passengers,
    lower(nz(monitored)) = 'true' AS monitored,
    lower(nz(at_stop)) = 'true' AS at_stop,
    nz(stop_name) AS stop_name
  FROM raw
)
SELECT * EXCLUDE (lat0, lon0, rn),
  CASE WHEN position_valid THEN lat0 END AS lat,
  CASE WHEN position_valid THEN lon0 END AS lon
FROM (
  SELECT *, (lat0 BETWEEN -44.6 AND -42.5 AND lon0 BETWEEN 171.0 AND 173.6) AS position_valid,
         row_number() OVER (PARTITION BY snapshot_time, vehicle_id ORDER BY recorded_at DESC NULLS LAST) AS rn
  FROM b
) WHERE rn = 1;
"""

COLUMNS_HELP = """\
vehicles (ecan_query): one row per bus per snapshot, from Metro's SIRI VehicleMonitoring.
  snapshot_time, recorded_at   Christchurch (NZ) local time
  vehicle_id, route (public route number, e.g. '95', 'Oa'), line_ref, variant
  direction, origin_name, destination_name, stop_name, at_stop (BOOLEAN)
  operator        'Ritchies' | 'Go Bus' | other (operator_raw keeps Metro's text, e.g. 'Ritchies - CHC')
  trip_code, journey_ref, service_date
  lat, lon, position_valid   (lat/lon NULL when outside Canterbury)
  occupancy_pct, passengers, monitored
Notes: no delay or lateness columns on purpose. Count DISTINCT vehicle_id."""


def _conform(t: pa.Table) -> pa.Table:
    cols = {k: _str_col(t, v) for k, v in RAW.items()}
    cols["_snapshot"] = _str_col(t, "_snapshot")
    return pa.table(cols)


def _connect(table: pa.Table) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET threads = 4")
    con.execute(f"SET memory_limit = '{os.getenv('DUCKDB_MEMORY_LIMIT', '1GB')}'")
    con.register("raw", _conform(table))
    con.execute(VIEW_SQL)
    con.execute("SET enable_external_access = false")
    con.execute("SET lock_configuration = true")
    return con


def _ts(name: str) -> str:
    return _snapshot_time(name).isoformat(timespec="seconds")


# ---------------------------------------------------------------------------
# MCP tools
# ---------------------------------------------------------------------------

def whats_running(operator: str | None = None) -> dict:
    """Buses running in Christchurch right now (latest snapshot): totals by
    operator and by route. operator: part of a name, e.g. 'Ritchies' or 'Go'.
    No lateness: the ECan feed's scheduled times aren't reliable."""
    name = _latest_name()
    con = _connect(_load([name]))
    where = "WHERE (? IS NULL OR operator ILIKE '%' || ? || '%' OR operator_raw ILIKE '%' || ? || '%')"
    p = [operator] * 3
    by_op = _rows(con, f"""SELECT operator, count(DISTINCT vehicle_id) AS buses,
                                  count(DISTINCT route) AS routes
                           FROM vehicles {where} GROUP BY 1 ORDER BY buses DESC""", p)
    by_route = _rows(con, f"""SELECT route, operator, count(DISTINCT vehicle_id) AS buses
                              FROM vehicles {where} GROUP BY 1, 2 ORDER BY buses DESC, route""", p)
    return {"snapshot": _ts(name), "operator_filter": operator,
            "total_buses": sum(r["buses"] for r in by_op), "by_operator": by_op, "by_route": by_route}


def vehicle_positions(route: str | None = None, operator: str | None = None, limit: int = 100) -> dict:
    """Where Christchurch buses are now (latest snapshot), by route and/or
    operator. Positions outside Canterbury are dropped as junk GPS."""
    name = _latest_name()
    con = _connect(_load([name]))
    cap = max(1, min(limit, 500))
    rows = _rows(con, f"""
        SELECT vehicle_id, route, operator, destination_name, stop_name, at_stop,
               lat, lon, occupancy_pct, recorded_at
        FROM vehicles
        WHERE position_valid
          AND (? IS NULL OR lower(route) = lower(?))
          AND (? IS NULL OR operator ILIKE '%' || ? || '%' OR operator_raw ILIKE '%' || ? || '%')
        ORDER BY route, vehicle_id LIMIT {cap}""", [route, route, operator, operator, operator])
    return {"snapshot": _ts(name), "count": len(rows), "vehicles": rows}


def query(sql: str, scope: str = "latest", every_minutes: int = 15, max_rows: int = 200) -> dict:
    """Run your own read-only SQL (DuckDB dialect) against the `vehicles` view
    for Christchurch (Metro). scope: 'latest', 'today' or ONE date 'YYYY-MM-DD'
    (one snapshot every N minutes). Only one day at a time: ECan has no daily
    roll-up, so ranges of days are too large and are refused. Only a single
    SELECT or WITH query. There are no delay columns on purpose."""
    text = sql.strip().rstrip(";").strip()
    if ";" in text or not text.lower().startswith(("select", "with")):
        raise ToolError("Only one SELECT or WITH query is allowed.")
    names = _scope_names(scope, every_minutes)
    if not names:
        return {"scope": scope, "snapshots_read": 0, "rows": [],
                "note": "No ECan snapshots for that day."}
    con = _connect(_load(names))
    cap = max(1, min(max_rows, 1000))
    timer = threading.Timer(QUERY_TIMEOUT_S, con.interrupt)
    timer.start()
    try:
        rows = _rows(con, f"SELECT * FROM ({text}) AS q LIMIT {cap + 1}")
    except duckdb.Error as exc:
        raise ToolError(f"{type(exc).__name__}: {exc}") from exc
    finally:
        timer.cancel()
    return {"scope": scope, "snapshots_read": len(names),
            "first_snapshot": _ts(names[0]), "last_snapshot": _ts(names[-1]),
            "truncated": len(rows) > cap, "rows": rows[:cap]}


INSTRUCTIONS = (
    "ecan_* tools: Environment Canterbury / Metro (Christchurch) buses, from Metro's SIRI "
    "VehicleMonitoring feed, captured about once a minute. Operators are 'Ritchies' (Metro "
    "calls it 'Ritchies - CHC') and 'Go Bus'. What these tools can answer: what's running, "
    "where buses are, and the Ritchies vs Go Bus split, for right now, today or one single "
    "date. What they can't: (1) lateness, punctuality or delays, because the feed's scheduled "
    "times aren't reliable, so never work out a delay from it; (2) more than one day. ECan data "
    "is one file a minute with no daily roll-up, so if someone asks about a week, a month or "
    "any range of days, tell them that's too large for ECan right now and offer a single day "
    "instead. Don't loop over days to get around it.\n\n" + COLUMNS_HELP
)

TOOLS = {
    "ecan_whats_running": whats_running,
    "ecan_vehicle_positions": vehicle_positions,
    "ecan_query": query,
}


def register(mcp) -> None:
    for name, fn in TOOLS.items():
        mcp.tool(name=name)(_explained(fn))
