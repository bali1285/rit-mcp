"""Auckland Transport realtime source.

Read-only tools over the AT realtime "combined feed" snapshots saved as
Parquet in ADLS Gen2, plus AT's static GTFS for operator names:

    <container>/combinefeed/YYYY/MM/DD/HH/combinefeed_MMSS.parquet
    <container>/GTFS/at_gtfs_<file>_YYYYMMDD.parquet

Folder times are Auckland local time. One snapshot is roughly one minute.
All tools are registered with an `at_` prefix by `register()`.
"""

from __future__ import annotations

import functools
import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from functools import lru_cache
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import pyarrow as pa
import pyarrow.compute  # noqa: F401  (pa.compute)
import pyarrow.parquet as pq
from mcp.server.mcpserver.exceptions import ToolError

# ---------------------------------------------------------------------------
# Settings (all from environment variables, so nothing changes per host)
# ---------------------------------------------------------------------------

STORAGE_ACCOUNT = os.getenv("AT_STORAGE_ACCOUNT", "ritadlsdevpublic")
CONTAINER = os.getenv("AT_CONTAINER", "aucklandtransport")
FEED_PREFIX = os.getenv("AT_FEED_PREFIX", "combinefeed")
GTFS_PREFIX = os.getenv("AT_GTFS_PREFIX", "GTFS/")
GTFS_REFRESH_S = int(os.getenv("GTFS_REFRESH_S", "21600"))  # reload static GTFS every 6 hours
LOCAL_DATA_DIR = os.getenv("LOCAL_DATA_DIR")  # optional: read a local copy instead of the lake
TZ = ZoneInfo(os.getenv("AT_TIMEZONE", "Pacific/Auckland"))


MAX_FILES_PER_CALL = int(os.getenv("MAX_FILES_PER_CALL", "200"))
CACHE_FILES = int(os.getenv("CACHE_FILES", "256"))
QUERY_TIMEOUT_S = float(os.getenv("QUERY_TIMEOUT_S", "30"))

# Punctuality window, in minutes. Defaults: up to 1 min early, up to 5 min late.
EARLY_LIMIT_MIN = float(os.getenv("ON_TIME_EARLY_MIN", "1"))
LATE_LIMIT_MIN = float(os.getenv("ON_TIME_LATE_MIN", "5"))

# ---------------------------------------------------------------------------
# Storage: list and read snapshot files (lake, or a local copy for testing)
# ---------------------------------------------------------------------------

_container_client = None


def _container():
    global _container_client
    if _container_client is None:
        from azure.identity import DefaultAzureCredential
        from azure.storage.blob import ContainerClient

        # Uses managed identity in Azure (set AZURE_CLIENT_ID for a
        # user-assigned one) and your `az login` locally.
        _container_client = ContainerClient(
            account_url=f"https://{STORAGE_ACCOUNT}.blob.core.windows.net",
            container_name=CONTAINER,
            credential=DefaultAzureCredential(),
        )
    return _container_client


def _list(prefix: str) -> list[str]:
    """Blob names (or relative local paths) of Parquet files under a prefix."""
    if LOCAL_DATA_DIR:
        root = Path(LOCAL_DATA_DIR)
        base = root / prefix
        if not base.exists():
            return []
        return sorted(p.relative_to(root).as_posix() for p in base.rglob("*.parquet"))
    return sorted(
        b.name for b in _container().list_blobs(name_starts_with=prefix)
        if b.name.endswith(".parquet")
    )


@lru_cache(maxsize=CACHE_FILES)
def _read(name: str) -> pa.Table:
    """Read one snapshot. Snapshots never change once written, so cache them."""
    if LOCAL_DATA_DIR:
        return pq.read_table(Path(LOCAL_DATA_DIR) / name)
    data = _container().download_blob(name).readall()
    return pq.read_table(pa.BufferReader(data))


_gtfs_cache: dict = {"loaded": 0.0, "table": None, "source": None}
_GTFS_EMPTY = pa.table({c: pa.array([], pa.string()) for c in
                        ["route_id", "route_short_name", "agency_id", "agency_name", "route_type"]})


def _read_any(name: str) -> pa.Table:
    if LOCAL_DATA_DIR:
        return pq.read_table(Path(LOCAL_DATA_DIR) / name)
    return pq.read_table(pa.BufferReader(_container().download_blob(name).readall()))


def _str_col(t: pa.Table, col: str) -> pa.Array:
    if col not in t.column_names:
        return pa.nulls(t.num_rows, pa.string())
    return pa.compute.cast(t[col], pa.string())


def _gtfs_routes() -> pa.Table:
    """Static GTFS routes joined to agency names, cached for GTFS_REFRESH_S.

    Uses the newest dated at_gtfs_routes_YYYYMMDD.parquet and the agency file
    from the same date. Falls back to routes.parquet. If anything fails the
    server keeps going on vehicle-label fleet codes alone.
    """
    if _gtfs_cache["table"] is not None and time.time() - _gtfs_cache["loaded"] < GTFS_REFRESH_S:
        return _gtfs_cache["table"]
    table, source = _GTFS_EMPTY, None
    try:
        names = _list(GTFS_PREFIX)
        dated = sorted(n for n in names if Path(n).name.startswith("at_gtfs_routes_"))
        routes_name = dated[-1] if dated else next((n for n in names if Path(n).name == "routes.parquet"), None)
        if routes_name:
            routes = _read_any(routes_name)
            agency_ids = _str_col(routes, "agency_id")
            agency_names = agency_ids
            agency_name_file = routes_name.replace("at_gtfs_routes_", "at_gtfs_agency_")
            if agency_name_file != routes_name and agency_name_file in names:
                agency = _read_any(agency_name_file)
                lookup = dict(zip(_str_col(agency, "agency_id").to_pylist(),
                                  _str_col(agency, "agency_name").to_pylist()))
                agency_names = pa.array([lookup.get(a, a) for a in agency_ids.to_pylist()], pa.string())
            table = pa.table({
                "route_id": _str_col(routes, "route_id"),
                "route_short_name": _str_col(routes, "route_short_name"),
                "agency_id": agency_ids,
                "agency_name": agency_names,
                "route_type": _str_col(routes, "route_type"),
            })
            source = routes_name
    except Exception:  # keep serving without GTFS rather than failing every call
        logging.exception("Could not load static GTFS from %s", GTFS_PREFIX)
    _gtfs_cache.update(loaded=time.time(), table=table, source=source)
    return table


def _snapshot_time(name: str) -> datetime:
    """combinefeed/2026/11/19/13/combinefeed_0100.parquet -> 2026-11-19 13:01:00 NZ."""
    parts = name.split("/")
    year, month, day, hour = (int(p) for p in parts[-5:-1])
    mmss = Path(parts[-1]).stem.split("_")[-1]
    return datetime(year, month, day, hour, int(mmss[:2]), int(mmss[2:4]), tzinfo=TZ)


def _hour_prefix(t: datetime) -> str:
    return f"{FEED_PREFIX}/{t:%Y/%m/%d/%H}/"


def _latest_name() -> str:
    """Newest snapshot, looking back up to 3 hours."""
    now = datetime.now(TZ)
    for back in range(4):
        names = _list(_hour_prefix(now - timedelta(hours=back)))
        if names:
            return names[-1]
    raise RuntimeError("No snapshots found in the last 3 hours.")


def _day_names(day: str | None, every_minutes: int) -> list[str]:
    """Snapshots for one Auckland day, thinned to one every N minutes."""
    d = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=TZ) if day else datetime.now(TZ)
    names = _list(f"{FEED_PREFIX}/{d:%Y/%m/%d}/")
    every = max(1, every_minutes)
    picked, last_slot = [], None
    for n in names:
        t = _snapshot_time(n)
        slot = (t.hour * 60 + t.minute) // every
        if slot != last_slot:  # first snapshot in each slot
            picked.append(n)
            last_slot = slot
    if len(picked) > MAX_FILES_PER_CALL:
        raise ToolError(
            f"That needs {len(picked)} snapshots (limit {MAX_FILES_PER_CALL}). "
            "Use a bigger every_minutes."
        )
    return picked


def _load(names: list[str]) -> pa.Table:
    """Read snapshots in parallel and stack them, tagging each row with its snapshot time."""
    with ThreadPoolExecutor(max_workers=8) as pool:
        tables = list(pool.map(_read, names))
    tagged = []
    for name, t in zip(names, tables):
        # Snapshots don't always agree on column types (a column that's empty in
        # one file can come through as a different type). Everything is cleaned
        # and typed in the feed view anyway, so read every column as text.
        t = pa.table({c: _str_col(t, c) for c in t.column_names})
        ts = _snapshot_time(name).isoformat(timespec="seconds")
        tagged.append(t.append_column("_snapshot", pa.array([ts] * t.num_rows, pa.string())))
    return pa.concat_tables(tagged, promote_options="default")


# ---------------------------------------------------------------------------
# Query layer: turn the raw snapshot columns into one clean `feed` view
# ---------------------------------------------------------------------------

# Raw columns the clean view reads. Missing ones are added as empty, so an
# older or newer file with a slightly different schema still loads.
RAW_COLUMNS = [
    "id", "is_deleted",
    "trip_update_trip_trip_id", "trip_update_trip_start_time", "trip_update_trip_start_date",
    "trip_update_trip_schedule_relationship", "trip_update_trip_route_id", "trip_update_trip_direction_id",
    "trip_update_stop_time_update_stop_sequence", "trip_update_stop_time_update_stop_id",
    "trip_update_stop_time_update_arrival_delay", "trip_update_stop_time_update_departure_delay",
    "trip_update_vehicle_id", "trip_update_vehicle_label", "trip_update_timestamp", "trip_update_delay",
    "vehicle_position_latitude", "vehicle_position_longitude", "vehicle_position_bearing",
    "vehicle_position_speed", "vehicle_timestamp", "vehicle_vehicle_id", "vehicle_vehicle_label",
    "vehicle_trip_trip_id", "vehicle_trip_start_time", "vehicle_trip_start_date",
    "vehicle_trip_schedule_relationship", "vehicle_trip_route_id", "vehicle_trip_direction_id",
    "vehicle_occupancy_status",
    "alert_cause", "alert_effect", "alert_severity_level", "alert_header_text_translation",
    "alert_description_text_translation", "alert_active_period", "alert_informed_entity",
]

# Fleet code = the letters at the start of a vehicle label ("RT1402", "NB 4323").
# Names here are best guesses: confirm them, or override the whole map with
# the AT_FLEET_OPERATORS app setting as JSON, e.g. {"RT": "Ritchies"}.
FLEET_OPERATORS: dict[str, str] = {
    # Fallback only, for routes the static GTFS can't match. Names match
    # agency_name in AT's agency.txt so both sources group together.
    "RT": "Ritchies Transport",
    "NB": "New Zealand Bus",
    "GB": "Go Bus",
    "HE": "Howick and Eastern",
    "TR": "Tranzit Group Ltd",
    "BA": "Bayes Coachlines",
    "WB": "Waiheke Bus Company",
    "AMP": "AT Metro",
}
if os.getenv("AT_FLEET_OPERATORS"):
    FLEET_OPERATORS = json.loads(os.environ["AT_FLEET_OPERATORS"])

FEED_SQL = r"""
CREATE MACRO nz(x) AS nullif(nullif(trim(CAST(x AS VARCHAR)), 'nan'), '');
CREATE MACRO num(x) AS TRY_CAST(nz(x) AS DOUBLE);
CREATE MACRO trip_rel(x) AS CASE CAST(num(x) AS INTEGER)
    WHEN 0 THEN 'SCHEDULED' WHEN 1 THEN 'ADDED' WHEN 2 THEN 'UNSCHEDULED' WHEN 3 THEN 'CANCELLED'
    WHEN 5 THEN 'REPLACEMENT' WHEN 6 THEN 'DUPLICATED' WHEN 7 THEN 'DELETED' END;

CREATE VIEW base AS
SELECT
    CAST(left(_snapshot, 19) AS TIMESTAMP) AS snapshot_time,
    CASE
        WHEN nz(alert_cause) IS NOT NULL OR nz(alert_effect) IS NOT NULL THEN 'alert'
        WHEN nz(vehicle_vehicle_id) IS NOT NULL OR nz(vehicle_position_latitude) IS NOT NULL THEN 'vehicle'
        WHEN nz(trip_update_trip_trip_id) IS NOT NULL THEN 'trip_update'
    END AS entity_type,
    nz(id) AS entity_id,
    coalesce(nz(trip_update_trip_trip_id), nz(vehicle_trip_trip_id)) AS trip_id,
    coalesce(nz(trip_update_trip_route_id), nz(vehicle_trip_route_id)) AS route_id,
    CAST(coalesce(num(trip_update_trip_direction_id), num(vehicle_trip_direction_id)) AS INTEGER) AS direction_id,
    coalesce(nz(trip_update_trip_start_date), nz(vehicle_trip_start_date)) AS start_date,
    coalesce(nz(trip_update_trip_start_time), nz(vehicle_trip_start_time)) AS start_time,
    coalesce(trip_rel(trip_update_trip_schedule_relationship), trip_rel(vehicle_trip_schedule_relationship)) AS trip_status,
    nz(trip_update_stop_time_update_stop_id) AS stop_id,
    CAST(num(trip_update_stop_time_update_stop_sequence) AS INTEGER) AS stop_sequence,
    coalesce(num(trip_update_delay), num(trip_update_stop_time_update_departure_delay),
             num(trip_update_stop_time_update_arrival_delay)) AS delay_s,
    coalesce(nz(vehicle_vehicle_id), nz(trip_update_vehicle_id)) AS vehicle_id,
    regexp_replace(coalesce(nz(vehicle_vehicle_label), nz(trip_update_vehicle_label)), '\s+', ' ', 'g') AS vehicle_label,
    nullif(regexp_extract(coalesce(nz(vehicle_vehicle_label), nz(trip_update_vehicle_label)),
                          '^([A-Z]{2,4})\s*\d+$', 1), '') AS fleet_code,
    num(vehicle_position_latitude) AS lat,
    num(vehicle_position_longitude) AS lon,
    num(vehicle_position_bearing) AS bearing,
    num(vehicle_position_speed) AS speed_kmh,  -- AT sends km/h despite the GTFS-RT spec saying m/s
    CASE CAST(num(vehicle_occupancy_status) AS INTEGER)
        WHEN 0 THEN 'EMPTY' WHEN 1 THEN 'MANY_SEATS_AVAILABLE' WHEN 2 THEN 'FEW_SEATS_AVAILABLE'
        WHEN 3 THEN 'STANDING_ROOM_ONLY' WHEN 4 THEN 'CRUSHED_STANDING_ROOM_ONLY' WHEN 5 THEN 'FULL'
        WHEN 6 THEN 'NOT_ACCEPTING_PASSENGERS' END AS occupancy,
    CAST(coalesce(num(vehicle_timestamp), num(trip_update_timestamp)) AS BIGINT) AS event_epoch,
    nz(alert_cause) AS alert_cause,
    nz(alert_effect) AS alert_effect,
    nz(alert_severity_level) AS alert_severity,
    nz(alert_header_text_translation) AS alert_header,
    nz(alert_description_text_translation) AS alert_description,
    nz(alert_active_period) AS alert_active_period,
    nz(alert_informed_entity) AS alert_informed_entity
FROM raw;

-- Cancelled trips carry no vehicle, so their operator comes from the fleet
-- code seen most on the same route in the loaded snapshots.
CREATE VIEW route_operator AS
SELECT route_id, arg_max(fleet_code, n) AS fleet_code
FROM (SELECT route_id, fleet_code, count(DISTINCT vehicle_id) AS n
      FROM base WHERE route_id IS NOT NULL AND fleet_code IS NOT NULL GROUP BY ALL)
GROUP BY route_id;

-- Static GTFS lookup by route_id, or by short name when the route_id's
-- version suffix (the "-203" in "NX1-203") doesn't match the static file.
CREATE VIEW gtfs_by_short AS
SELECT route_short_name, any_value(agency_id) AS agency_id, any_value(agency_name) AS agency_name,
       any_value(route_type) AS route_type
FROM gtfs_routes WHERE route_short_name IS NOT NULL
GROUP BY route_short_name HAVING count(DISTINCT agency_id) = 1;

CREATE VIEW feed AS
SELECT b.*,
       coalesce(g.agency_id, gs.agency_id, b.fleet_code, r.fleet_code) AS operator_code,
       coalesce(g.agency_name, gs.agency_name, o.name, b.fleet_code, r.fleet_code) AS operator,
       CASE WHEN g.agency_id IS NOT NULL THEN 'gtfs route'
            WHEN gs.agency_id IS NOT NULL THEN 'gtfs short name'
            WHEN b.fleet_code IS NOT NULL THEN 'vehicle label'
            WHEN r.fleet_code IS NOT NULL THEN 'inferred from route' END AS operator_source,
       CASE CAST(TRY_CAST(coalesce(g.route_type, gs.route_type) AS DOUBLE) AS INTEGER)
            WHEN 0 THEN 'tram' WHEN 1 THEN 'metro' WHEN 2 THEN 'rail' WHEN 3 THEN 'bus'
            WHEN 4 THEN 'ferry' WHEN 712 THEN 'school bus' END AS mode
FROM base b
LEFT JOIN gtfs_routes g ON g.route_id = b.route_id
LEFT JOIN gtfs_by_short gs ON g.route_id IS NULL AND gs.route_short_name = split_part(b.route_id, '-', 1)
LEFT JOIN route_operator r ON r.route_id = b.route_id
LEFT JOIN operators o ON o.code = coalesce(b.fleet_code, r.fleet_code);
"""

FEED_COLUMNS_HELP = """\
feed: one row per feed entity per snapshot.
  snapshot_time   TIMESTAMP    when the snapshot was taken, Auckland local time
  entity_type     'trip_update' | 'vehicle' | 'alert'
  trip_id, route_id, direction_id, start_date (YYYYMMDD), start_time (HH:MM:SS)
  trip_status     SCHEDULED | ADDED | UNSCHEDULED | CANCELLED | REPLACEMENT | DUPLICATED | DELETED
  stop_id, stop_sequence
  delay_s         seconds, positive = late (trip_update rows)
  vehicle_id, vehicle_label, fleet_code (label letters, e.g. RT, NB)
  operator_code, operator   agency from static GTFS routes; falls back to vehicle fleet code
  operator_source  'gtfs route' | 'gtfs short name' | 'vehicle label' | 'inferred from route'
  mode            bus | rail | ferry | school bus ... (from static GTFS route_type)
  lat, lon, bearing, speed_kmh, occupancy   (vehicle rows)
  event_epoch     feed timestamp, unix seconds
  alert_cause, alert_effect, alert_severity, alert_header, alert_description,
  alert_active_period, alert_informed_entity   (alert rows; text fields hold the raw JSON-ish value)
Notes: a trip usually has a trip_update row and a vehicle row in the same snapshot,
so count DISTINCT ids. Cancelled trips have no vehicle."""


def _conform(table: pa.Table) -> pa.Table:
    """Add any expected column that's missing, as all-null strings."""
    for col in RAW_COLUMNS:
        if col not in table.column_names:
            table = table.append_column(col, pa.nulls(table.num_rows, pa.string()))
    return table


def _connect(table: pa.Table) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET threads = 4")
    con.execute(f"SET memory_limit = '{os.getenv('DUCKDB_MEMORY_LIMIT', '1GB')}'")
    con.register("raw", _conform(table))
    con.register("operators", pa.table({"code": list(FLEET_OPERATORS), "name": list(FLEET_OPERATORS.values())}))
    con.register("gtfs_routes", _gtfs_routes())
    con.execute(FEED_SQL)
    # From here the connection can only read what's registered: no files,
    # no network, no extensions, and the settings can't be changed back.
    con.execute("SET enable_external_access = false")
    con.execute("SET lock_configuration = true")
    return con


def _plain(v):
    if v is None or isinstance(v, (str, int, float, bool)):
        return v
    return str(v)


def _rows(con: duckdb.DuckDBPyConnection, sql: str, params: list | None = None) -> list[dict]:
    cur = con.execute(sql, params or [])
    names = [d[0] for d in cur.description]
    return [{n: _plain(v) for n, v in zip(names, r)} for r in cur.fetchall()]


ROUTE_FILTER = "(? IS NULL OR route_id = ? OR route_id ILIKE ? || '-%')"


def _route_params(route: str | None) -> list:
    """AT route_ids look like 'NX1-203'; let people ask for 'NX1'."""
    return [route, route, route]


def _scope_names(scope: str, every_minutes: int) -> list[str]:
    if scope == "latest":
        return [_latest_name()]
    return _day_names(None if scope == "today" else scope, every_minutes)


def _ts(name: str) -> str:
    return _snapshot_time(name).isoformat(timespec="seconds")


# ---------------------------------------------------------------------------
# MCP tools
# ---------------------------------------------------------------------------

def describe_feed() -> dict:
    """Show the latest snapshot: when it was taken, row counts by entity type,
    fleet codes seen, and the columns of the `feed` view that at_query_feed uses."""
    name = _latest_name()
    con = _connect(_load([name]))
    return {
        "latest_file": name,
        "snapshot_time": _ts(name),
        "rows_by_entity": _rows(con, "SELECT entity_type, count(*) AS rows FROM feed GROUP BY 1 ORDER BY 2 DESC"),
        "fleet_codes": _rows(con, """
            SELECT fleet_code, any_value(operator) AS operator, count(DISTINCT vehicle_id) AS vehicles
            FROM feed WHERE fleet_code IS NOT NULL GROUP BY 1 ORDER BY 3 DESC"""),
        "gtfs_routes_file": _gtfs_cache["source"],
        "operators": _rows(con, """
            SELECT operator, operator_source, count(DISTINCT vehicle_id) AS vehicles,
                   count(DISTINCT trip_id) AS trips
            FROM feed WHERE entity_type <> 'alert' GROUP BY 1, 2 ORDER BY 4 DESC"""),
        "feed_columns": _rows(con, "SELECT column_name, column_type FROM (DESCRIBE feed)"),
        "help": FEED_COLUMNS_HELP,
    }


def get_late_trips(min_delay_minutes: float = 5, route: str | None = None,
                   operator: str | None = None, limit: int = 25) -> dict:
    """Trips running late right now (latest snapshot), worst first.
    Optional filters: route ('NX1', '70') and operator (code or part of a name)."""
    name = _latest_name()
    con = _connect(_load([name]))
    rows = _rows(
        con,
        f"""
        SELECT trip_id, any_value(route_id) AS route_id, any_value(operator) AS operator,
               any_value(vehicle_label) AS vehicle_label, any_value(stop_id) AS stop_id,
               round(max(delay_s) / 60.0, 1) AS delay_minutes
        FROM feed
        WHERE entity_type = 'trip_update' AND delay_s IS NOT NULL
          AND coalesce(trip_status, '') <> 'CANCELLED'
          AND {ROUTE_FILTER} AND (? IS NULL OR operator_code ILIKE ? OR operator ILIKE '%' || ? || '%')
        GROUP BY trip_id
        HAVING max(delay_s) >= ? * 60
        ORDER BY delay_minutes DESC
        LIMIT ?
        """,
        _route_params(route) + [operator, operator, operator, min_delay_minutes, max(1, min(limit, 200))],
    )
    return {"snapshot_time": _ts(name), "late_trips": rows}


def count_active_routes() -> dict:
    """What's running right now (latest snapshot): totals, a breakdown by
    operator (vehicles, trips, routes, cancelled trips) and the busiest routes."""
    name = _latest_name()
    con = _connect(_load([name]))
    totals = _rows(con, """
        SELECT count(DISTINCT route_id) AS routes,
               count(DISTINCT trip_id) FILTER (WHERE coalesce(trip_status, '') <> 'CANCELLED') AS trips,
               count(DISTINCT trip_id) FILTER (WHERE trip_status = 'CANCELLED') AS cancelled_trips,
               count(DISTINCT vehicle_id) FILTER (WHERE entity_type = 'vehicle') AS vehicles_reporting
        FROM feed WHERE entity_type <> 'alert'""")[0]
    by_operator = _rows(con, """
        SELECT coalesce(operator, 'unknown') AS operator,
               count(DISTINCT vehicle_id) FILTER (WHERE entity_type = 'vehicle') AS vehicles,
               count(DISTINCT trip_id) FILTER (WHERE coalesce(trip_status, '') <> 'CANCELLED') AS trips,
               count(DISTINCT route_id) AS routes,
               count(DISTINCT trip_id) FILTER (WHERE trip_status = 'CANCELLED') AS cancelled_trips
        FROM feed WHERE entity_type <> 'alert'
        GROUP BY 1 ORDER BY vehicles DESC""")
    busiest = _rows(con, """
        SELECT route_id, any_value(operator) AS operator, count(DISTINCT vehicle_id) AS vehicles
        FROM feed WHERE entity_type = 'vehicle' AND route_id IS NOT NULL
        GROUP BY route_id ORDER BY vehicles DESC, route_id LIMIT 10""")
    return {"snapshot_time": _ts(name), **totals, "by_operator": by_operator, "busiest_routes": busiest}


def get_vehicle_positions(route: str | None = None, operator: str | None = None, limit: int = 50) -> dict:
    """Where vehicles are right now (latest snapshot): lat/lon, bearing,
    speed, occupancy, trip and delay. Filter by route and/or operator (code or part of a name)."""
    name = _latest_name()
    con = _connect(_load([name]))
    rows = _rows(
        con,
        f"""
        WITH v AS (SELECT * FROM feed WHERE entity_type = 'vehicle'
                   AND {ROUTE_FILTER} AND (? IS NULL OR operator_code ILIKE ? OR operator ILIKE '%' || ? || '%')),
             d AS (SELECT trip_id, max(delay_s) AS delay_s FROM feed
                   WHERE entity_type = 'trip_update' GROUP BY trip_id)
        SELECT v.vehicle_label, v.operator, v.route_id, v.trip_id, v.lat, v.lon, v.bearing,
               v.speed_kmh, v.occupancy,
               round(d.delay_s / 60.0, 1) AS delay_minutes
        FROM v LEFT JOIN d USING (trip_id)
        ORDER BY v.route_id, v.vehicle_label
        LIMIT ?
        """,
        _route_params(route) + [operator, operator, operator, max(1, min(limit, 500))],
    )
    return {"snapshot_time": _ts(name), "vehicles": rows}


def get_cancellations(date: str | None = None, route: str | None = None,
                      operator: str | None = None, every_minutes: int = 15) -> dict:
    """Trips marked cancelled on a day (Auckland time, YYYY-MM-DD, default
    today), with a breakdown by operator. Operator comes from AT's static
    GTFS for the route (cancelled trips have no vehicle). Samples one
    snapshot every N minutes, so short-lived cancellations can be missed."""
    names = _day_names(date, every_minutes)
    day = date or datetime.now(TZ).date().isoformat()
    if not names:
        return {"date": day, "snapshots_read": 0, "count": 0, "by_operator": [], "cancelled_trips": []}
    con = _connect(_load(names))
    where = f"trip_status = 'CANCELLED' AND {ROUTE_FILTER} AND (? IS NULL OR operator_code ILIKE ? OR operator ILIKE '%' || ? || '%')"
    params = _route_params(route) + [operator, operator, operator]
    trips = _rows(con, f"""
        SELECT trip_id, any_value(route_id) AS route_id, any_value(start_time) AS start_time,
               any_value(operator) AS operator, any_value(operator_source) AS operator_source,
               min(snapshot_time) AS first_seen, max(snapshot_time) AS last_seen
        FROM feed WHERE {where} GROUP BY trip_id ORDER BY first_seen""", params)
    by_operator = _rows(con, f"""
        SELECT coalesce(operator, 'unknown') AS operator, count(DISTINCT trip_id) AS cancelled_trips,
               count(DISTINCT route_id) AS routes
        FROM feed WHERE {where} GROUP BY 1 ORDER BY 2 DESC""", params)
    return {"date": day, "snapshots_read": len(names), "count": len(trips),
            "by_operator": by_operator, "cancelled_trips": trips}


def on_time_summary(route: str | None = None, operator: str | None = None, date: str | None = None,
                    every_minutes: int = 15, group_by_operator: bool = False) -> dict:
    """Punctuality for a day (Auckland time, YYYY-MM-DD, default today),
    optionally for one route or operator, or split by operator. Uses each
    trip's last observed delay. On time = no more than ON_TIME_EARLY_MIN early
    and ON_TIME_LATE_MIN late (defaults 1 and 5 minutes). An estimate from
    sampled snapshots, not an official AT figure."""
    names = _day_names(date, every_minutes)
    day = date or datetime.now(TZ).date().isoformat()
    if not names:
        return {"date": day, "snapshots_read": 0, "trips": 0}
    con = _connect(_load(names))
    group = "operator" if group_by_operator else "NULL"
    rows = _rows(
        con,
        f"""
        WITH last_delay AS (
            SELECT trip_id, any_value(operator) AS operator, arg_max(delay_s, snapshot_time) AS delay_s
            FROM feed
            WHERE entity_type = 'trip_update' AND delay_s IS NOT NULL
              AND coalesce(trip_status, '') <> 'CANCELLED'
              AND {ROUTE_FILTER} AND (? IS NULL OR operator_code ILIKE ? OR operator ILIKE '%' || ? || '%')
            GROUP BY trip_id
        )
        SELECT {group} AS operator, count(*) AS trips,
               count(*) FILTER (WHERE delay_s BETWEEN -? * 60 AND ? * 60) AS on_time,
               count(*) FILTER (WHERE delay_s > ? * 60) AS late,
               count(*) FILTER (WHERE delay_s < -? * 60) AS early,
               round(100.0 * count(*) FILTER (WHERE delay_s BETWEEN -? * 60 AND ? * 60) / nullif(count(*), 0), 1) AS on_time_pct,
               round(median(delay_s) / 60.0, 1) AS median_delay_minutes
        FROM last_delay
        GROUP BY 1 ORDER BY trips DESC
        """,
        _route_params(route) + [operator, operator, operator,
                                EARLY_LIMIT_MIN, LATE_LIMIT_MIN, LATE_LIMIT_MIN,
                                EARLY_LIMIT_MIN, EARLY_LIMIT_MIN, LATE_LIMIT_MIN],
    )
    head = {"date": day, "route": route, "operator": operator, "snapshots_read": len(names),
            "on_time_window_minutes": {"early": EARLY_LIMIT_MIN, "late": LATE_LIMIT_MIN}}
    if group_by_operator:
        return {**head, "by_operator": rows}
    return {**head, **(rows[0] if rows else {"trips": 0})}


def get_alerts(route: str | None = None, effect: str | None = None, limit: int = 50) -> dict:
    """Service alerts in the latest snapshot (cause, effect, header). Filter by
    route (matched inside the alert's informed entities) or effect, e.g.
    'NO_SERVICE', 'DETOUR', 'STOP_MOVED'."""
    name = _latest_name()
    con = _connect(_load([name]))
    rows = _rows(con, """
        SELECT entity_id, alert_cause AS cause, alert_effect AS effect, alert_severity AS severity,
               alert_header AS header, alert_informed_entity AS informed_entity
        FROM feed
        WHERE entity_type = 'alert'
          AND (? IS NULL OR alert_informed_entity ILIKE '%''' || ? || '%')
          AND (? IS NULL OR alert_effect = upper(?))
        ORDER BY effect, cause
        LIMIT ?""", [route, route, effect, effect, max(1, min(limit, 500))])
    summary = _rows(con, """
        SELECT alert_effect AS effect, count(*) AS alerts FROM feed
        WHERE entity_type = 'alert' GROUP BY 1 ORDER BY 2 DESC""")
    return {"snapshot_time": _ts(name), "by_effect": summary, "alerts": rows}


def query_feed(sql: str, scope: str = "latest", every_minutes: int = 15, max_rows: int = 200) -> dict:
    """Run your own read-only SQL (DuckDB dialect) against the `feed` view, for
    questions the other tools don't cover. scope: 'latest' (one snapshot),
    'today', or a date 'YYYY-MM-DD' (one snapshot every N minutes). Only a
    single SELECT or WITH query is allowed. Columns are listed in the server
    instructions and in at_describe_feed. Count DISTINCT trip_id / vehicle_id,
    because a trip appears in several rows."""
    text = sql.strip().rstrip(";").strip()
    if ";" in text or not text.lower().startswith(("select", "with")):
        raise ToolError("Only one SELECT or WITH query is allowed.")
    names = _scope_names(scope, every_minutes)
    if not names:
        return {"scope": scope, "snapshots_read": 0, "rows": []}
    con = _connect(_load(names))
    cap = max(1, min(max_rows, 1000))
    timer = threading.Timer(QUERY_TIMEOUT_S, con.interrupt)
    timer.start()
    try:
        rows = _rows(con, f"SELECT * FROM ({text}) AS q LIMIT {cap + 1}")
    except duckdb.Error as exc:
        # Hand the database's message back so the AI can fix its SQL.
        raise ToolError(f"{type(exc).__name__}: {exc}") from exc
    finally:
        timer.cancel()
    return {"scope": scope, "snapshots_read": len(names),
            "first_snapshot": _ts(names[0]), "last_snapshot": _ts(names[-1]),
            "truncated": len(rows) > cap, "rows": rows[:cap]}


# ---------------------------------------------------------------------------
# History: the daily combined files (one row per trip per service day)
#
#   combinefeed/YYYY/MM/trip_updates_DD.parquet   written the next morning
#
# Each row is a trip's last known state that day: final delay, last stop,
# cancelled or not. A month is about 30 files of ~1 MB, so date ranges are
# cheap. Today isn't there yet: use the realtime at_ tools for today.
# ---------------------------------------------------------------------------

MAX_HISTORY_DAYS = int(os.getenv("MAX_HISTORY_DAYS", "400"))

TRIP_COLUMNS = [
    "trip_update_trip_trip_id", "trip_update_trip_start_time", "trip_update_trip_start_date",
    "trip_update_trip_schedule_relationship", "trip_update_trip_route_id", "trip_update_trip_direction_id",
    "trip_update_stop_time_update_stop_sequence", "trip_update_stop_time_update_stop_id",
    "trip_update_stop_time_update_departure_delay", "trip_update_stop_time_update_arrival_delay",
    "trip_update_delay", "trip_update_vehicle_id", "trip_update_vehicle_label", "trip_update_timestamp",
]

_stops_cache: dict = {"loaded": 0.0, "table": None, "source": None}
_STOPS_EMPTY = pa.table({c: pa.array([], pa.string()) for c in ["stop_id", "stop_name", "stop_lat", "stop_lon"]})


def _gtfs_stops() -> pa.Table:
    """Static GTFS stops (newest dated at_gtfs_stops_ file), cached like routes."""
    if _stops_cache["table"] is not None and time.time() - _stops_cache["loaded"] < GTFS_REFRESH_S:
        return _stops_cache["table"]
    table, source = _STOPS_EMPTY, None
    try:
        names = _list(GTFS_PREFIX)
        dated = sorted(n for n in names if Path(n).name.startswith("at_gtfs_stops_"))
        name = dated[-1] if dated else next((n for n in names if Path(n).name == "stops.parquet"), None)
        if name:
            s = _read_any(name)
            table = pa.table({c: _str_col(s, c) for c in ["stop_id", "stop_name", "stop_lat", "stop_lon"]})
            source = name
    except Exception:
        logging.exception("Could not load GTFS stops from %s", GTFS_PREFIX)
    _stops_cache.update(loaded=time.time(), table=table, source=source)
    return table


def _history_names(start_date: str, end_date: str | None) -> list[tuple[str, str]]:
    """(service_date, blob name) for each daily trip_updates file in the range."""
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else start
    if end < start:
        raise ToolError("end_date is before start_date.")
    if (end - start).days + 1 > MAX_HISTORY_DAYS:
        raise ToolError(f"That's more than {MAX_HISTORY_DAYS} days. Use a shorter range.")
    picked, months = [], {}
    day = start
    while day <= end:
        prefix = f"{FEED_PREFIX}/{day:%Y/%m}/"
        if prefix not in months:  # one listing per month, not per day
            months[prefix] = set(_list(prefix))
        name = f"{prefix}trip_updates_{day:%d}.parquet"
        if name in months[prefix]:
            picked.append((day.isoformat(), name))
        day += timedelta(days=1)
    return picked


def _load_history(pairs: list[tuple[str, str]]) -> pa.Table:
    with ThreadPoolExecutor(max_workers=8) as pool:
        tables = list(pool.map(_read, [n for _, n in pairs]))
    parts = []
    for (service_date, _), t in zip(pairs, tables):
        cols = {c: _str_col(t, c) for c in TRIP_COLUMNS}
        cols["service_date"] = pa.array([service_date] * t.num_rows, pa.string())
        parts.append(pa.table(cols))
    if not parts:
        return pa.table({c: pa.array([], pa.string()) for c in TRIP_COLUMNS + ["service_date"]})
    return pa.concat_tables(parts)


TRIPS_SQL = r"""
CREATE MACRO nz(x) AS nullif(nullif(trim(CAST(x AS VARCHAR)), 'nan'), '');
CREATE MACRO num(x) AS TRY_CAST(nz(x) AS DOUBLE);

-- Approximate area from a stop's position. Rough boxes, good enough to
-- compare parts of Auckland, not to draw boundaries.
CREATE MACRO area_of(lat, lon) AS CASE
    WHEN lat IS NULL OR lon IS NULL THEN NULL
    WHEN lon > 174.95 AND lat > -36.90 THEN 'Islands'
    WHEN lat > -36.40 THEN 'Far North'
    WHEN lon < 174.69 THEN 'West'
    WHEN lat > -36.835 THEN 'North Shore'
    WHEN lat < -37.10 THEN 'Far South'
    WHEN lat < -36.93 THEN 'South'
    WHEN lon > 174.83 THEN 'East'
    ELSE 'Central' END;

CREATE VIEW trips_raw AS
SELECT
    CAST(service_date AS DATE) AS service_date,
    nz(trip_update_trip_trip_id) AS trip_id,
    nz(trip_update_trip_route_id) AS route_id,
    CAST(num(trip_update_trip_direction_id) AS INTEGER) AS direction_id,
    nz(trip_update_trip_start_date) AS start_date,
    nz(trip_update_trip_start_time) AS start_time,
    CAST(num(trip_update_trip_schedule_relationship) AS INTEGER) = 3 AS cancelled,
    coalesce(num(trip_update_delay), num(trip_update_stop_time_update_departure_delay),
             num(trip_update_stop_time_update_arrival_delay)) AS delay_s,
    nz(trip_update_stop_time_update_stop_id) AS last_stop_id,
    CAST(num(trip_update_stop_time_update_stop_sequence) AS INTEGER) AS last_stop_sequence,
    nz(trip_update_vehicle_id) AS vehicle_id,
    regexp_replace(nz(trip_update_vehicle_label), '\s+', ' ', 'g') AS vehicle_label,
    nullif(regexp_extract(nz(trip_update_vehicle_label), '^([A-Z]{2,4})\s*\d+$', 1), '') AS fleet_code,
    CAST(num(trip_update_timestamp) AS BIGINT) AS event_epoch
FROM raw_trips
WHERE nz(trip_update_trip_trip_id) IS NOT NULL;

-- The daily file can hold a trip twice: keep its latest update.
CREATE VIEW trips_dedup AS
SELECT * FROM trips_raw
QUALIFY row_number() OVER (PARTITION BY service_date, trip_id ORDER BY event_epoch DESC NULLS LAST) = 1;

CREATE VIEW gtfs_by_short AS
SELECT route_short_name, any_value(agency_id) AS agency_id, any_value(agency_name) AS agency_name,
       any_value(route_type) AS route_type
FROM gtfs_routes WHERE route_short_name IS NOT NULL
GROUP BY route_short_name HAVING count(DISTINCT agency_id) = 1;

CREATE VIEW trips AS
SELECT t.*,
       split_part(t.route_id, '-', 1) AS route_short_name,
       coalesce(g.agency_name, gs.agency_name, o.name, t.fleet_code) AS operator,
       coalesce(g.agency_id, gs.agency_id, t.fleet_code) AS operator_code,
       CASE CAST(TRY_CAST(coalesce(g.route_type, gs.route_type) AS DOUBLE) AS INTEGER)
            WHEN 0 THEN 'tram' WHEN 1 THEN 'metro' WHEN 2 THEN 'rail' WHEN 3 THEN 'bus'
            WHEN 4 THEN 'ferry' WHEN 712 THEN 'school bus' END AS mode,
       s.stop_name AS last_stop_name,
       area_of(TRY_CAST(s.stop_lat AS DOUBLE), TRY_CAST(s.stop_lon AS DOUBLE)) AS area,
       CASE WHEN t.cancelled THEN 'cancelled'
            WHEN t.delay_s IS NULL THEN 'unknown'
            WHEN t.delay_s > ? * 60 THEN 'late'
            WHEN t.delay_s < -? * 60 THEN 'early'
            ELSE 'on time' END AS outcome
FROM trips_dedup t
LEFT JOIN gtfs_routes g ON g.route_id = t.route_id
LEFT JOIN gtfs_by_short gs ON g.route_id IS NULL AND gs.route_short_name = split_part(t.route_id, '-', 1)
LEFT JOIN operators o ON o.code = t.fleet_code
LEFT JOIN gtfs_stops s ON s.stop_id = t.last_stop_id;
"""

TRIPS_COLUMNS_HELP = """\
trips: one row per trip per service day, from the daily files (yesterday and earlier).
  service_date    DATE         the day the file covers
  trip_id, route_id, route_short_name, direction_id, start_date, start_time
  cancelled       BOOLEAN
  delay_s         final known delay in seconds, positive = late
  outcome         'on time' | 'late' | 'early' | 'cancelled' | 'unknown'
                  (late = more than ON_TIME_LATE_MIN late, early = more than ON_TIME_EARLY_MIN early)
                  Delay is at the trip's last reported stop, usually near the end of the route,
                  where arriving early is normal. Treat 'early' as not late, not as a failure.
  operator, operator_code, mode (bus | rail | ferry ...)
  area            approximate, from the last stop's position: Central, North Shore, West,
                  East, South, Far South, Far North, Islands (NULL for cancelled trips)
  last_stop_id, last_stop_name, last_stop_sequence, vehicle_id, vehicle_label, fleet_code
Notes: percentages should use trips with a known outcome; cancelled trips have no area."""


def _connect_history(table: pa.Table) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET threads = 4")
    con.execute(f"SET memory_limit = '{os.getenv('DUCKDB_MEMORY_LIMIT', '1GB')}'")
    con.register("raw_trips", table)
    con.register("operators", pa.table({"code": list(FLEET_OPERATORS), "name": list(FLEET_OPERATORS.values())}))
    con.register("gtfs_routes", _gtfs_routes())
    con.register("gtfs_stops", _gtfs_stops())
    # The thresholds are baked into the view so every query uses the same definition.
    sql = TRIPS_SQL.replace("? * 60 THEN 'late'", f"{float(LATE_LIMIT_MIN)} * 60 THEN 'late'")
    sql = sql.replace("-? * 60 THEN 'early'", f"-{float(EARLY_LIMIT_MIN)} * 60 THEN 'early'")
    con.execute(sql)
    con.execute("SET enable_external_access = false")
    con.execute("SET lock_configuration = true")
    return con


GROUPINGS = {
    "operator": "operator",
    "area": "area",
    "operator_area": "operator, area",
    "route": "route_short_name, operator",
    "day": "service_date",
    "operator_day": "operator, service_date",
    "mode": "mode",
}


def performance(start_date: str, end_date: str | None = None, group_by: str = "operator",
                operator: str | None = None, route: str | None = None, area: str | None = None,
                limit: int = 100) -> dict:
    """Punctuality and cancellations over a date range from the daily files
    (YYYY-MM-DD; yesterday and earlier, today isn't combined yet). group_by:
    operator, area, operator_area, route, day, operator_day or mode. Returns
    trips, cancelled %, late %, on-time %, early % and median delay for each
    group. Delay is measured at each trip's last reported stop, usually near
    the end of the route, where arriving early is normal: on_time_pct counts
    every trip that wasn't late, early_pct is shown for info only. Late,
    on-time and early % are of trips that ran and reported a delay. Filters:
    operator (code or part of a name), route ('NX1'), area ('North Shore')."""
    if group_by not in GROUPINGS:
        raise ToolError(f"group_by must be one of: {', '.join(GROUPINGS)}")
    pairs = _history_names(start_date, end_date)
    if not pairs:
        return {"start_date": start_date, "end_date": end_date or start_date, "days_read": 0, "rows": []}
    con = _connect_history(_load_history(pairs))
    keys = GROUPINGS[group_by]
    rows = _rows(
        con,
        f"""
        SELECT {keys},
               count(*) AS trips,
               count(*) FILTER (WHERE outcome = 'cancelled') AS cancelled,
               round(100.0 * count(*) FILTER (WHERE outcome = 'cancelled') / count(*), 1) AS cancelled_pct,
               count(*) FILTER (WHERE outcome IN ('on time', 'late', 'early')) AS measured,
               round(100.0 * count(*) FILTER (WHERE outcome = 'late')
                     / nullif(count(*) FILTER (WHERE outcome IN ('on time', 'late', 'early')), 0), 1) AS late_pct,
               round(100.0 * count(*) FILTER (WHERE outcome IN ('on time', 'early'))
                     / nullif(count(*) FILTER (WHERE outcome IN ('on time', 'late', 'early')), 0), 1) AS on_time_pct,
               round(100.0 * count(*) FILTER (WHERE outcome = 'early')
                     / nullif(count(*) FILTER (WHERE outcome IN ('on time', 'late', 'early')), 0), 1) AS early_pct,
               round(median(delay_s) FILTER (WHERE NOT cancelled) / 60.0, 1) AS median_delay_minutes
        FROM trips
        WHERE {ROUTE_FILTER}
          AND (? IS NULL OR operator_code ILIKE ? OR operator ILIKE '%' || ? || '%')
          AND (? IS NULL OR area ILIKE ?)
        GROUP BY {keys}
        ORDER BY {'service_date' if group_by == 'day' else 'trips DESC'}
        LIMIT ?
        """,
        _route_params(route) + [operator, operator, operator, area, area, max(1, min(limit, 1000))],
    )
    return {"start_date": pairs[0][0], "end_date": pairs[-1][0], "days_read": len(pairs),
            "days_missing": ((datetime.strptime(end_date or start_date, "%Y-%m-%d").date()
                              - datetime.strptime(start_date, "%Y-%m-%d").date()).days + 1 - len(pairs)),
            "thresholds_minutes": {"late_over": LATE_LIMIT_MIN, "early_over": EARLY_LIMIT_MIN},
            "group_by": group_by, "rows": rows}


def query_history(sql: str, start_date: str, end_date: str | None = None, max_rows: int = 500) -> dict:
    """Run your own read-only SQL (DuckDB dialect) against the `trips` view
    (one row per trip per day, from the daily files) for a date range
    (YYYY-MM-DD, yesterday and earlier). Only a single SELECT or WITH query.
    Columns are in the server instructions."""
    text = sql.strip().rstrip(";").strip()
    if ";" in text or not text.lower().startswith(("select", "with")):
        raise ToolError("Only one SELECT or WITH query is allowed.")
    pairs = _history_names(start_date, end_date)
    if not pairs:
        return {"days_read": 0, "rows": []}
    con = _connect_history(_load_history(pairs))
    cap = max(1, min(max_rows, 2000))
    timer = threading.Timer(QUERY_TIMEOUT_S, con.interrupt)
    timer.start()
    try:
        rows = _rows(con, f"SELECT * FROM ({text}) AS q LIMIT {cap + 1}")
    except duckdb.Error as exc:
        raise ToolError(f"{type(exc).__name__}: {exc}") from exc
    finally:
        timer.cancel()
    return {"start_date": pairs[0][0], "end_date": pairs[-1][0], "days_read": len(pairs),
            "truncated": len(rows) > cap, "rows": rows[:cap]}


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------

INSTRUCTIONS = (
    "at_* tools: Auckland Transport realtime data (trip updates, vehicle positions and "
    "alerts), captured about once a minute. Times are Auckland local. Delays are in "
    "minutes unless a field says seconds; positive means late. Route can be a short name "
    "like 'NX1' or a full route_id. Operators come from AT's static GTFS (route agency), "
    "falling back to vehicle label fleet codes (RT, NB, GB...). Operator filters accept a "
    "code or part of a name. For today and right now use the realtime tools (at_late_trips, "
    "at_whats_running, at_query_feed...). For yesterday and earlier, and any date range "
    "(this week, this month), use at_performance and at_query_history: they read one "
    "combined file per day, so they're fast.\n\n" + FEED_COLUMNS_HELP + "\n\n" + TRIPS_COLUMNS_HELP
)

TOOLS = {
    "at_describe_feed": describe_feed,
    "at_late_trips": get_late_trips,
    "at_whats_running": count_active_routes,
    "at_vehicle_positions": get_vehicle_positions,
    "at_cancellations": get_cancellations,
    "at_on_time_summary": on_time_summary,
    "at_alerts": get_alerts,
    "at_query_feed": query_feed,
    "at_performance": performance,
    "at_query_history": query_history,
}


def _explained(fn):
    """Turn unexpected errors into a ToolError with the real message, so the AI
    (and whoever's debugging) sees what went wrong instead of a generic error."""

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except ToolError:
            raise
        except Exception as exc:
            logging.exception("%s failed", fn.__name__)
            text = str(exc)
            if "AuthorizationPermissionMismatch" in text or "AuthorizationFailure" in text:
                raise ToolError("rit-mcp can't read the data lake (no Storage Blob Data Reader role).") from exc
            raise ToolError(f"{type(exc).__name__}: {text[:500]}") from exc

    return wrapper


def register(mcp) -> None:
    """Add this source's tools to the shared MCP server."""
    for name, fn in TOOLS.items():
        mcp.tool(name=name)(_explained(fn))
