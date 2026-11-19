"""Auckland Transport realtime source.

Read-only tools over the AT realtime "combined feed" snapshots saved as
Parquet in ADLS Gen2, plus AT's static GTFS for operator names:

    <container>/combinefeed/YYYY/MM/DD/HH/combinefeed_MMSS.parquet
    <container>/GTFS/at_gtfs_<file>_YYYYMMDD.parquet

Folder times are Auckland local time. One snapshot is roughly one minute.
All tools are registered with an `at_` prefix by `register()`.
"""

from __future__ import annotations

import ast
import csv
import functools
import json
import logging
import os
import re
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

# Definitions, in minutes, from the Command Centre's OpsCentre (LOGIC.md):
DELAYED_MIN = float(os.getenv("DELAYED_MIN", "2"))            # delayed: more than 2 min late
MAJOR_DELAY_MIN = float(os.getenv("MAJOR_DELAY_MIN", "5"))    # major delay: more than 5 min late
PUNCTUALITY_MIN = float(os.getenv("PUNCTUALITY_MIN", "5"))    # punctual: at the last stop within 5 min
RELIABILITY_MIN = float(os.getenv("RELIABILITY_MIN", "15"))   # reliable: at the last stop within 15 min
EARLY_START_MIN = float(os.getenv("EARLY_START_MIN", "1"))    # early start: left the first stop over 1 min early
LATE_START_MIN = float(os.getenv("LATE_START_MIN", "5"))      # late start: left the first stop over 5 min late

ASSETS = Path(__file__).resolve().parent.parent / "assets"

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


# A minute snapshot: .../YYYY/MM/DD/HH/combinefeed_MMSS.parquet. Anything else
# in those folders (daily rollups, folder markers) is ignored.
SNAPSHOT_RE = re.compile(r"/\d{4}/\d{2}/\d{2}/\d{2}/[^/]*_\d{4}\.parquet$")


def _snapshot_names(prefix: str) -> list[str]:
    return [n for n in _list(prefix) if SNAPSHOT_RE.search(n)]


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
        names = _snapshot_names(_hour_prefix(now - timedelta(hours=back)))
        if names:
            return names[-1]
    raise RuntimeError("No snapshots found in the last 3 hours.")


def _day_names(day: str | None, every_minutes: int) -> list[str]:
    """Snapshots for one Auckland day, thinned to one every N minutes."""
    d = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=TZ) if day else datetime.now(TZ)
    names = _snapshot_names(f"{FEED_PREFIX}/{d:%Y/%m/%d}/")
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
CREATE MACRO in_region(lat, lon) AS coalesce(lat BETWEEN -37.6 AND -35.8 AND lon BETWEEN 174.0 AND 175.6, false);
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
    -- Positions outside the Auckland region are junk (parked or offline units report
    -- anything down to Antarctica), so they become NULL. position_valid says which.
    CASE WHEN in_region(num(vehicle_position_latitude), num(vehicle_position_longitude))
         THEN num(vehicle_position_latitude) END AS lat,
    CASE WHEN in_region(num(vehicle_position_latitude), num(vehicle_position_longitude))
         THEN num(vehicle_position_longitude) END AS lon,
    in_region(num(vehicle_position_latitude), num(vehicle_position_longitude)) AS position_valid,
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
            WHEN 4 THEN 'ferry' WHEN 712 THEN 'school bus' END AS mode,
       CASE WHEN coalesce(g.agency_id, gs.agency_id) = 'RTH' THEN d.depot END AS depot,
       CASE WHEN coalesce(g.agency_id, gs.agency_id) = 'RTH' THEN d.division END AS division
FROM base b
LEFT JOIN depots d ON d.route_uid = ltrim(split_part(b.trip_id, '-', 2), '0')
LEFT JOIN gtfs_routes g ON g.route_id = b.route_id
LEFT JOIN gtfs_by_short gs ON g.route_id IS NULL AND gs.route_short_name = regexp_replace(b.route_id, '-[^-]*$', '')
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
  depot, division Ritchies trips only: depot and division from the Command Centre route list
  lat, lon, bearing, speed_kmh, occupancy   (vehicle rows; lat/lon NULL when the GPS is junk)
  position_valid  false when a vehicle reports a position outside the Auckland region
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
    con.register("depots", _depots())
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
                    every_minutes: int = 10, group_by: str = "none", depot: str | None = None) -> dict:
    """Punctuality for a day from the minute snapshots (Auckland time,
    YYYY-MM-DD, default today), using the Command Centre definitions:
    delayed = more than 2 min late, major delay = more than 5 min (each trip's
    latest delay), early start = left the first stop more than 1 min early
    (a compliance issue), late start = more than 5 min late leaving it.
    group_by: none, operator, depot or route. Filters: route, operator, depot
    (Ritchies). Reads one snapshot every N minutes, so starts are only caught
    when a snapshot lands while the bus is at its first stop (starts_seen says
    how many). An estimate, not an official AT figure."""
    groups = {"none": "NULL", "operator": "operator", "depot": "depot", "route": "route_short_name"}
    if group_by not in groups:
        raise ToolError(f"group_by must be one of: {', '.join(groups)}")
    names = _day_names(date, every_minutes)
    day = date or datetime.now(TZ).date().isoformat()
    if not names:
        return {"date": day, "snapshots_read": 0, "trips": 0}
    con = _connect(_load(names))
    g = groups[group_by]
    rows = _rows(
        con,
        f"""
        WITH f AS (
            SELECT *, regexp_replace(route_id, '-[^-]*$', '') AS route_short_name FROM feed
            WHERE entity_type = 'trip_update' AND coalesce(trip_status, '') <> 'CANCELLED'
              AND {ROUTE_FILTER} AND (? IS NULL OR operator_code ILIKE ? OR operator ILIKE '%' || ? || '%')
              AND (? IS NULL OR depot ILIKE ? OR division ILIKE ?)
              AND ({str(group_by == "depot").lower()} = false OR operator_code = 'RTH')
        ),
        last_delay AS (
            SELECT trip_id, any_value(operator) AS operator,
                   coalesce(any_value(depot), 'Unknown depot') AS depot,
                   any_value(route_short_name) AS route_short_name,
                   arg_max(delay_s, snapshot_time) AS delay_s
            FROM f WHERE delay_s IS NOT NULL GROUP BY trip_id
        ),
        first_stop AS (
            SELECT trip_id, arg_max(delay_s, snapshot_time) AS start_delay_s
            FROM f WHERE stop_sequence = 1 AND delay_s IS NOT NULL GROUP BY trip_id
        )
        SELECT {g} AS {group_by if group_by != "none" else "all_trips"},
               count(*) AS trips,
               count(*) FILTER (WHERE delay_s > {DELAYED_MIN} * 60) AS delayed,
               count(*) FILTER (WHERE delay_s > {MAJOR_DELAY_MIN} * 60) AS major_delay,
               round(100.0 * count(*) FILTER (WHERE delay_s <= {DELAYED_MIN} * 60) / count(*), 1) AS on_time_pct,
               round(100.0 * count(*) FILTER (WHERE delay_s > {MAJOR_DELAY_MIN} * 60) / count(*), 1) AS major_delay_pct,
               count(s.trip_id) AS starts_seen,
               count(*) FILTER (WHERE s.start_delay_s < -{EARLY_START_MIN} * 60) AS early_starts,
               count(*) FILTER (WHERE s.start_delay_s > {LATE_START_MIN} * 60) AS late_starts,
               round(median(delay_s) / 60.0, 1) AS median_delay_minutes
        FROM last_delay l LEFT JOIN first_stop s USING (trip_id)
        GROUP BY 1 ORDER BY trips DESC
        """,
        _route_params(route) + [operator, operator, operator, depot, depot, depot],
    )
    head = {"date": day, "route": route, "operator": operator, "depot": depot,
            "snapshots_read": len(names),
            "definitions_minutes": {"delayed_over": DELAYED_MIN, "major_delay_over": MAJOR_DELAY_MIN,
                                    "early_start_over": EARLY_START_MIN, "late_start_over": LATE_START_MIN}}
    if group_by != "none":
        return {**head, "group_by": group_by, "rows": rows}
    return {**head, **(rows[0] if rows else {"trips": 0})}


def _pyval(text) -> list:
    """AT alert fields arrive as Python-style text, e.g. "[{'text': 'Detour', 'language': 'en'}]".
    Parse that safely (literal values only) into a list."""
    if text is None:
        return []
    s = str(text).strip()
    if not s or s in ("nan", "None"):
        return []
    for parse in (ast.literal_eval, json.loads):
        try:
            v = parse(s)
            return v if isinstance(v, list) else [v]
        except Exception:
            continue
    return [s]


def _alert_text(raw) -> str | None:
    items = _pyval(raw)
    texts = [i.get("text") for i in items if isinstance(i, dict) and i.get("text")]
    english = [i.get("text") for i in items if isinstance(i, dict) and i.get("language") in ("en", None) and i.get("text")]
    if english or texts:
        return (english or texts)[0]
    return str(items[0]) if items else None


def _alert_entities(raw) -> tuple[list[str], int]:
    routes, stops = set(), set()
    for i in _pyval(raw):
        if not isinstance(i, dict):
            continue
        if i.get("route_id"):
            routes.add(str(i["route_id"]))
        trip = i.get("trip") if isinstance(i.get("trip"), dict) else {}
        if trip.get("route_id"):
            routes.add(str(trip["route_id"]))
        if i.get("stop_id"):
            stops.add(str(i["stop_id"]))
    return sorted(routes), len(stops)


def _alert_periods(raw) -> list[dict]:
    out = []
    for i in _pyval(raw):
        if not isinstance(i, dict):
            continue
        p = {}
        for k in ("start", "end"):
            try:
                if i.get(k):
                    p[k] = datetime.fromtimestamp(float(i[k]), TZ).isoformat(timespec="minutes")
            except (TypeError, ValueError):
                pass
        if p:
            out.append(p)
    return out


def _route_matches(routes: list[str], route: str | None) -> bool:
    if not route:
        return True
    r = route.strip().upper()
    return any(x.upper() == r or x.upper().rsplit("-", 1)[0] == r for x in routes)


def _clean_alert(row: dict, description: bool = True) -> dict:
    routes, stop_count = _alert_entities(row.get("informed_entity"))
    out = {
        "id": row.get("id"),
        "cause": row.get("cause"),
        "effect": row.get("effect"),
        "header": _alert_text(row.get("header")),
        "routes": [r.rsplit("-", 1)[0] for r in routes],
        "route_ids": routes,
        "stops_affected": stop_count,
        "active": _alert_periods(row.get("active_period")),
    }
    if description:
        text = _alert_text(row.get("description"))
        out["description"] = text[:400] + ("..." if text and len(text) > 400 else "") if text else None
    return out


def get_alerts(route: str | None = None, effect: str | None = None, limit: int = 50) -> dict:
    """Service alerts active right now (latest snapshot): cause, effect,
    headline, description, routes affected, number of stops affected and
    active times. Filter by route ('70' or '70-205') or effect, e.g.
    'NO_SERVICE', 'DETOUR', 'STOP_MOVED'. For past days use at_alerts_history."""
    name = _latest_name()
    con = _connect(_load([name]))
    raw = _rows(con, """
        SELECT entity_id AS id, alert_cause AS cause, alert_effect AS effect,
               alert_header AS header, alert_description AS description,
               alert_active_period AS active_period, alert_informed_entity AS informed_entity
        FROM feed
        WHERE entity_type = 'alert' AND (? IS NULL OR alert_effect = upper(?))
        ORDER BY effect, cause""", [effect, effect])
    cleaned = [_clean_alert(r) for r in raw]
    rows = [a for a in cleaned if _route_matches(a["route_ids"], route)][: max(1, min(limit, 500))]
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


def _history_names(start_date: str, end_date: str | None, kind: str = "trip_updates") -> list[tuple[str, str]]:
    """(service_date, blob name) for each daily file of a kind (trip_updates, alerts) in the range."""
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
        name = f"{prefix}{kind}_{day:%d}.parquet"
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


def _depots() -> pa.Table:
    """Ritchies route_uid -> depot, division, unit (from the Command Centre's
    route-depot-unit.csv). route_uid is the middle part of a trip_id, e.g.
    "14703" in "1001-14703-55500-2-49e674df", without leading zeros."""
    global _DEPOTS
    if _DEPOTS is None:
        rows = {"route_uid": [], "depot": [], "division": [], "unit_number": []}
        path = ASSETS / "route-depot-unit.csv"
        if path.is_file():
            with path.open(encoding="utf-8-sig", newline="") as f:
                for r in csv.DictReader(f):
                    uid = (r.get("route_uid") or "").strip().lstrip("0")
                    if uid:
                        rows["route_uid"].append(uid)
                        rows["depot"].append((r.get("depot") or "").strip() or None)
                        rows["division"].append((r.get("division") or "").strip() or None)
                        rows["unit_number"].append((r.get("unit_number") or "").strip() or None)
        _DEPOTS = pa.table({k: pa.array(v, pa.string()) for k, v in rows.items()})
    return _DEPOTS


_DEPOTS = None


def _gtfs_latest(names: list[str], kind: str) -> str | None:
    # Exact table name then a date, so 'calendar' doesn't pick up 'calendar_dates'.
    pat = re.compile(rf"at_gtfs_{re.escape(kind)}_\d{{8}}\.parquet")
    dated = sorted(n for n in names if pat.fullmatch(Path(n).name))
    if dated:
        return dated[-1]
    return next((n for n in names if Path(n).name == f"{kind}.parquet"), None)


def _read_cols(name: str, columns: list[str]) -> pa.Table:
    """Read only some columns of a GTFS file, as text."""
    if LOCAL_DATA_DIR:
        t = pq.read_table(Path(LOCAL_DATA_DIR) / name)
    else:
        t = pq.read_table(pa.BufferReader(_container().download_blob(name).readall()))
    return pa.table({c: _str_col(t, c) for c in columns})


_sched_cache: dict = {"loaded": 0.0, "data": None}


def _gtfs_schedule() -> dict:
    """The static timetable, boiled down to one row per scheduled trip:
    service_id, first departure, first and last stop, and the dates the feed
    covers. Built from the newest GTFS trips, stop_times, calendar and
    calendar_dates files, cached for GTFS_REFRESH_S."""
    if _sched_cache["data"] is not None and time.time() - _sched_cache["loaded"] < GTFS_REFRESH_S:
        return _sched_cache["data"]
    empty = {
        "trips": pa.table({c: pa.array([], pa.string()) for c in
                           ["trip_id", "route_id", "service_id", "direction_id", "start_time",
                            "first_stop_id", "final_stop_sequence", "final_stop_id", "end_time"]}),
        "calendar": pa.table({c: pa.array([], pa.string()) for c in
                              ["service_id", "monday", "tuesday", "wednesday", "thursday", "friday",
                               "saturday", "sunday", "start_date", "end_date"]}),
        "calendar_dates": pa.table({c: pa.array([], pa.string()) for c in ["service_id", "date", "exception_type"]}),
        "feed_start": None, "feed_end": None, "source": None,
    }
    data = empty
    try:
        names = _list(GTFS_PREFIX)
        trips_name = _gtfs_latest(names, "trips")
        st_name = _gtfs_latest(names, "stop_times")
        cal_name = _gtfs_latest(names, "calendar")
        cd_name = _gtfs_latest(names, "calendar_dates")
        if trips_name and st_name and cal_name:
            trips = _read_cols(trips_name, ["trip_id", "route_id", "service_id", "direction_id"])
            st = _read_cols(st_name, ["trip_id", "stop_sequence", "stop_id", "arrival_time", "departure_time"])
            cal = _read_cols(cal_name, empty["calendar"].column_names)
            cd = _read_cols(cd_name, ["service_id", "date", "exception_type"]) if cd_name else empty["calendar_dates"]
            con = duckdb.connect()
            con.register("trips", trips)
            con.register("st", st)
            sched = con.sql("""
                WITH s AS (SELECT trip_id, CAST(stop_sequence AS INTEGER) AS seq, stop_id,
                                  arrival_time, departure_time FROM st)
                SELECT t.trip_id, t.route_id, t.service_id, t.direction_id,
                       min_by(coalesce(s.departure_time, s.arrival_time), s.seq) AS start_time,
                       min_by(s.stop_id, s.seq) AS first_stop_id,
                       CAST(max(s.seq) AS VARCHAR) AS final_stop_sequence,
                       max_by(s.stop_id, s.seq) AS final_stop_id,
                       max_by(coalesce(s.arrival_time, s.departure_time), s.seq) AS end_time
                FROM trips t JOIN s USING (trip_id)
                GROUP BY ALL
            """).fetch_arrow_table()
            starts = [d for d in cal["start_date"].to_pylist() if d]
            ends = [d for d in cal["end_date"].to_pylist() if d]
            info_name = _gtfs_latest(names, "feed_info")
            feed_start, feed_end = (min(starts) if starts else None), (max(ends) if ends else None)
            if info_name:
                info = _read_cols(info_name, ["feed_start_date", "feed_end_date"]).to_pylist()
                if info and info[0]["feed_start_date"]:
                    feed_start, feed_end = info[0]["feed_start_date"], info[0]["feed_end_date"] or feed_end
            data = {"trips": sched, "calendar": cal, "calendar_dates": cd,
                    "feed_start": feed_start, "feed_end": feed_end, "source": trips_name}
    except Exception:
        logging.exception("Could not build the GTFS schedule from %s", GTFS_PREFIX)
    _sched_cache.update(loaded=time.time(), data=data)
    return data


TRIPS_SQL = r"""
CREATE MACRO nz(x) AS nullif(nullif(trim(CAST(x AS VARCHAR)), 'nan'), '');
CREATE MACRO num(x) AS TRY_CAST(nz(x) AS DOUBLE);
CREATE MACRO short_name(r) AS regexp_replace(r, '-[^-]*$', '');
CREATE MACRO uid_of(trip_id) AS ltrim(split_part(trip_id, '-', 2), '0');

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
    CAST(service_date AS DATE) AS file_date,
    nz(trip_update_trip_trip_id) AS trip_id,
    nz(trip_update_trip_route_id) AS route_id,
    CAST(num(trip_update_trip_direction_id) AS INTEGER) AS direction_id,
    TRY_CAST(strptime(nz(trip_update_trip_start_date), '%Y%m%d') AS DATE) AS start_date,
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

-- A trip belongs to its start_date (its service day), whichever daily file it
-- turned up in. Keep its latest update.
CREATE VIEW trips_dedup AS
SELECT * EXCLUDE (file_date), coalesce(start_date, file_date) AS service_date
FROM trips_raw
QUALIFY row_number() OVER (PARTITION BY coalesce(start_date, file_date), trip_id
                           ORDER BY event_epoch DESC NULLS LAST) = 1;

CREATE VIEW gtfs_by_short AS
SELECT route_short_name, any_value(agency_id) AS agency_id, any_value(agency_name) AS agency_name,
       any_value(route_type) AS route_type
FROM gtfs_routes WHERE route_short_name IS NOT NULL
GROUP BY route_short_name HAVING count(DISTINCT agency_id) = 1;

CREATE VIEW trips_all AS
SELECT t.*,
       short_name(t.route_id) AS route_short_name,
       coalesce(g.agency_name, gs.agency_name, o.name, t.fleet_code) AS operator,
       coalesce(g.agency_id, gs.agency_id, t.fleet_code) AS operator_code,
       CASE CAST(TRY_CAST(coalesce(g.route_type, gs.route_type) AS DOUBLE) AS INTEGER)
            WHEN 0 THEN 'tram' WHEN 1 THEN 'metro' WHEN 2 THEN 'rail' WHEN 3 THEN 'bus'
            WHEN 4 THEN 'ferry' WHEN 712 THEN 'school bus' END AS mode,
       CASE WHEN coalesce(g.agency_id, gs.agency_id) = 'RTH' THEN d.depot END AS depot,
       CASE WHEN coalesce(g.agency_id, gs.agency_id) = 'RTH' THEN d.division END AS division,
       s.stop_name AS last_stop_name,
       area_of(TRY_CAST(s.stop_lat AS DOUBLE), TRY_CAST(s.stop_lon AS DOUBLE)) AS area,
       TRY_CAST(st.final_stop_sequence AS INTEGER) AS final_stop_sequence,
       -- Reached the end: last reported stop is the final scheduled stop.
       t.last_stop_sequence >= TRY_CAST(st.final_stop_sequence AS INTEGER)
           OR (t.last_stop_id IS NOT NULL AND t.last_stop_id = st.final_stop_id) AS reached_end
FROM trips_dedup t
LEFT JOIN gtfs_routes g ON g.route_id = t.route_id
LEFT JOIN gtfs_by_short gs ON g.route_id IS NULL AND gs.route_short_name = short_name(t.route_id)
LEFT JOIN operators o ON o.code = t.fleet_code
LEFT JOIN depots d ON d.route_uid = uid_of(t.trip_id)
LEFT JOIN gtfs_stops s ON s.stop_id = t.last_stop_id
LEFT JOIN sched_trips st ON st.trip_id = t.trip_id;

CREATE VIEW trips_base AS
SELECT * FROM trips_all WHERE service_date BETWEEN range_start() AND range_end();

-- Scheduled trips for each day in range, from calendar + calendar_dates.
CREATE VIEW active_services AS
WITH days AS (SELECT CAST(d AS DATE) AS service_date FROM service_days),
weekly AS (
    SELECT d.service_date, c.service_id
    FROM days d JOIN calendar c
      ON d.service_date BETWEEN strptime(c.start_date, '%Y%m%d') AND strptime(c.end_date, '%Y%m%d')
     AND CASE dayofweek(d.service_date)
            WHEN 0 THEN c.sunday WHEN 1 THEN c.monday WHEN 2 THEN c.tuesday WHEN 3 THEN c.wednesday
            WHEN 4 THEN c.thursday WHEN 5 THEN c.friday WHEN 6 THEN c.saturday END = '1'
),
added AS (
    SELECT CAST(strptime(cd.date, '%Y%m%d') AS DATE) AS service_date, cd.service_id
    FROM calendar_dates cd JOIN days d ON CAST(strptime(cd.date, '%Y%m%d') AS DATE) = d.service_date
    WHERE cd.exception_type = '1'
),
removed AS (
    SELECT CAST(strptime(cd.date, '%Y%m%d') AS DATE) AS service_date, cd.service_id
    FROM calendar_dates cd WHERE cd.exception_type = '2'
)
SELECT * FROM (SELECT * FROM weekly UNION SELECT * FROM added)
EXCEPT SELECT * FROM removed;

CREATE VIEW scheduled AS
SELECT a.service_date, st.trip_id, st.route_id, short_name(st.route_id) AS route_short_name,
       TRY_CAST(st.direction_id AS INTEGER) AS direction_id, st.start_time,
       coalesce(g.agency_name, gs.agency_name) AS operator,
       coalesce(g.agency_id, gs.agency_id) AS operator_code,
       CASE CAST(TRY_CAST(coalesce(g.route_type, gs.route_type) AS DOUBLE) AS INTEGER)
            WHEN 0 THEN 'tram' WHEN 1 THEN 'metro' WHEN 2 THEN 'rail' WHEN 3 THEN 'bus'
            WHEN 4 THEN 'ferry' WHEN 712 THEN 'school bus' END AS mode,
       CASE WHEN coalesce(g.agency_id, gs.agency_id) = 'RTH' THEN d.depot END AS depot,
       CASE WHEN coalesce(g.agency_id, gs.agency_id) = 'RTH' THEN d.division END AS division,
       CAST(NULL AS VARCHAR) AS area
FROM active_services a
JOIN sched_trips st USING (service_id)
LEFT JOIN gtfs_routes g ON g.route_id = st.route_id
LEFT JOIN gtfs_by_short gs ON g.route_id IS NULL AND gs.route_short_name = short_name(st.route_id)
LEFT JOIN depots d ON d.route_uid = uid_of(st.trip_id);

-- Missed: scheduled but never in the feed, not even cancelled. A trip counts
-- as seen if its trip_id turned up that day, or another calendar variant of
-- the same slot (route, start time, direction) did (Command Centre slot-key
-- rule). Trips starting after midnight are only judged when the next day's
-- file has been read.
CREATE VIEW missed AS
SELECT s.*
FROM scheduled s
WHERE NOT EXISTS (SELECT 1 FROM trips_all t WHERE t.service_date = s.service_date AND t.trip_id = s.trip_id)
  AND NOT EXISTS (SELECT 1 FROM trips_all t
                  WHERE t.service_date = s.service_date AND t.route_short_name = s.route_short_name
                    AND t.start_time = s.start_time
                    AND coalesce(t.direction_id, -1) = coalesce(s.direction_id, -1))
  AND (s.start_time < '24:00:00' OR next_day_read(s.service_date));
"""

TRIPS_COLUMNS_HELP = """\
trips: one row per trip per service day, from the daily files (yesterday and earlier).
  service_date    DATE         the trip's service day (its start_date)
  trip_id, route_id, route_short_name, direction_id, start_time
  cancelled       BOOLEAN
  delay_s         final known delay in seconds, positive = late
  outcome         'on time' (no more than 2 min late) | 'minor delay' (2-5 min) |
                  'major delay' (more than 5 min) | 'cancelled' | 'unknown'
  reached_end     the last reported stop is the trip's final stop (from the GTFS timetable)
  punctual        reached_end and no more than 5 min late at the last stop
  reliable        reached_end and no more than 15 min late at the last stop
  operator, operator_code, mode (bus | rail | ferry ...)
  depot, division Ritchies trips only: depot (Albany, Ascot, Birkenhead, Oruarangi, Swanson,
                  Takanini, Westgate) and division code, from the Command Centre route list
  area            approximate, from the last stop's position: Central, North Shore, West,
                  East, South, Far South, Far North, Islands (NULL for cancelled trips)
  last_stop_id, last_stop_name, last_stop_sequence, final_stop_sequence,
  vehicle_id, vehicle_label, fleet_code
missed: one row per scheduled trip that never appeared in the feed (same columns as
  scheduled: service_date, trip_id, route_id, route_short_name, direction_id, start_time,
  operator, operator_code, mode, depot, division). Only for days the GTFS timetable covers.
scheduled: every trip the GTFS timetable says should run on each day.
Definitions follow the Command Centre (OpsCentre): delayed > 2 min, major > 5 min,
punctuality = arrive at the last stop within 5 min, reliability = within 15 min.
Delay is at the trip's last reported stop; arriving early at the end is normal and not a failure."""


def _connect_history(pairs: list[tuple[str, str]], table: pa.Table, next_day_read: set[str],
                     start: str, end: str):
    """Returns (connection, days the timetable covers so missed trips can be judged)."""
    sched = _gtfs_schedule()
    con = duckdb.connect()
    con.execute("SET threads = 4")
    con.execute(f"SET memory_limit = '{os.getenv('DUCKDB_MEMORY_LIMIT', '1GB')}'")
    con.register("raw_trips", table)
    con.register("operators", pa.table({"code": list(FLEET_OPERATORS), "name": list(FLEET_OPERATORS.values())}))
    con.register("gtfs_routes", _gtfs_routes())
    con.register("gtfs_stops", _gtfs_stops())
    con.register("depots", _depots())
    con.register("sched_trips", sched["trips"])
    con.register("calendar", sched["calendar"])
    con.register("calendar_dates", sched["calendar_dates"])
    # Only judge days the timetable covers.
    fs, fe = sched["feed_start"], sched["feed_end"]
    days = [d for d, _ in pairs
            if fs and fe and fs <= d.replace("-", "") <= fe]
    con.register("service_days", pa.table({"d": pa.array(days, pa.string())}))
    con.execute(f"CREATE MACRO range_start() AS DATE '{start}'")
    con.execute(f"CREATE MACRO range_end() AS DATE '{end}'")
    nd = ", ".join(f"DATE '{d}'" for d in sorted(next_day_read)) or "NULL"
    con.execute(f"CREATE MACRO next_day_read(d) AS d IN ({nd})")
    con.execute(TRIPS_SQL)
    con.execute(f"""
        CREATE VIEW trips_scored AS
        SELECT *,
               CASE WHEN cancelled THEN 'cancelled'
                    WHEN delay_s IS NULL THEN 'unknown'
                    WHEN delay_s > {MAJOR_DELAY_MIN} * 60 THEN 'major delay'
                    WHEN delay_s > {DELAYED_MIN} * 60 THEN 'minor delay'
                    ELSE 'on time' END AS outcome,
               coalesce(reached_end, false) AND NOT cancelled AND delay_s <= {PUNCTUALITY_MIN} * 60 AS punctual,
               coalesce(reached_end, false) AND NOT cancelled AND delay_s <= {RELIABILITY_MIN} * 60 AS reliable
        FROM trips_base
    """)
    con.execute("CREATE VIEW trips AS SELECT * FROM trips_scored")
    con.execute("SET enable_external_access = false")
    con.execute("SET lock_configuration = true")
    return con, days


def _load_range(start_date: str, end_date: str | None):
    """Daily trip files for the range, plus the day after (late-night trips of
    the last day land there) when it exists."""
    pairs = _history_names(start_date, end_date)
    last = datetime.strptime(end_date or start_date, "%Y-%m-%d").date()
    extra = _history_names((last + timedelta(days=1)).isoformat(), None)
    files = pairs + extra
    have = {d for d, _ in files}
    next_day_read = {d for d, _ in pairs
                     if (datetime.strptime(d, "%Y-%m-%d").date() + timedelta(days=1)).isoformat() in have}
    return pairs, _load_history(files), next_day_read


GROUPINGS = {
    "operator": ["operator"],
    "depot": ["depot", "division"],
    "depot_route": ["depot", "route_short_name"],
    "area": ["area"],
    "operator_area": ["operator", "area"],
    "route": ["route_short_name", "operator"],
    "day": ["service_date"],
    "operator_day": ["operator", "service_date"],
    "depot_day": ["depot", "service_date"],
    "mode": ["mode"],
}


def performance(start_date: str, end_date: str | None = None, group_by: str = "operator",
                operator: str | None = None, route: str | None = None, area: str | None = None,
                depot: str | None = None, limit: int = 100) -> dict:
    """Service delivery and punctuality over a date range from the daily files
    (YYYY-MM-DD; yesterday and earlier, today isn't combined yet), using the
    Command Centre definitions. group_by: operator, depot, depot_route, area,
    operator_area, route, day, operator_day, depot_day or mode. For each group:
    scheduled trips (GTFS), trips run, missed (scheduled but never in the
    feed), cancelled, delayed % (> 2 min late), major delay % (> 5 min),
    punctuality % (arrived at the last stop within 5 min) and reliability %
    (within 15 min), median delay and vehicles used. Depot is Ritchies only.
    Filters: operator (code or part of a name), route ('NX1'), area
    ('North Shore'), depot ('Takanini')."""
    if group_by not in GROUPINGS:
        raise ToolError(f"group_by must be one of: {', '.join(GROUPINGS)}")
    pairs, table, next_day_read = _load_range(start_date, end_date)
    if not pairs:
        return {"start_date": start_date, "end_date": end_date or start_date, "days_read": 0, "rows": []}
    start, end = pairs[0][0], pairs[-1][0]
    con, judged = _connect_history(pairs, table, next_day_read, start, end)
    keys = GROUPINGS[group_by]
    key_sql = ", ".join(keys)
    depot_only = "depot" in keys
    where = f"""{ROUTE_FILTER}
          AND (NOT {str(depot_only).lower()} OR operator_code = 'RTH')
          AND (? IS NULL OR operator_code ILIKE ? OR operator ILIKE '%' || ? || '%')
          AND (? IS NULL OR area ILIKE ?)
          AND (? IS NULL OR depot ILIKE ? OR division ILIKE ?)"""
    params = (_route_params(route) + [operator, operator, operator, area, area, depot, depot, depot])
    run = _rows(con, f"""
        SELECT {key_sql},
               count(*) AS trips_in_feed,
               count(*) FILTER (WHERE cancelled) AS cancelled,
               count(DISTINCT vehicle_id) FILTER (WHERE NOT cancelled) AS vehicles_used,
               count(*) FILTER (WHERE outcome IN ('on time', 'minor delay', 'major delay')) AS measured,
               round(100.0 * count(*) FILTER (WHERE outcome IN ('minor delay', 'major delay'))
                     / nullif(count(*) FILTER (WHERE outcome IN ('on time', 'minor delay', 'major delay')), 0), 1) AS delayed_pct,
               round(100.0 * count(*) FILTER (WHERE outcome = 'major delay')
                     / nullif(count(*) FILTER (WHERE outcome IN ('on time', 'minor delay', 'major delay')), 0), 1) AS major_delay_pct,
               count(*) FILTER (WHERE reached_end AND NOT cancelled AND delay_s IS NOT NULL) AS reached_end,
               round(100.0 * count(*) FILTER (WHERE punctual)
                     / nullif(count(*) FILTER (WHERE reached_end AND NOT cancelled AND delay_s IS NOT NULL), 0), 1) AS punctuality_pct,
               round(100.0 * count(*) FILTER (WHERE reliable)
                     / nullif(count(*) FILTER (WHERE reached_end AND NOT cancelled AND delay_s IS NOT NULL), 0), 1) AS reliability_pct,
               round(median(delay_s) FILTER (WHERE NOT cancelled) / 60.0, 1) AS median_delay_minutes
        FROM (SELECT * REPLACE (coalesce(depot, CASE WHEN operator_code = 'RTH' THEN 'Unknown depot' END) AS depot)
              FROM trips) WHERE {where}
        GROUP BY ALL""", params)
    sched = _rows(con, f"""
        SELECT {key_sql}, count(*) AS scheduled
        FROM (SELECT * REPLACE (coalesce(depot, CASE WHEN operator_code = 'RTH' THEN 'Unknown depot' END) AS depot)
              FROM scheduled) WHERE {where} GROUP BY ALL""", params)
    miss = _rows(con, f"""
        SELECT {key_sql}, count(*) AS missed
        FROM (SELECT * REPLACE (coalesce(depot, CASE WHEN operator_code = 'RTH' THEN 'Unknown depot' END) AS depot)
              FROM missed) WHERE {where} GROUP BY ALL""", params)

    def k(r):
        return tuple(r.get(x) for x in keys)

    merged: dict = {}
    for r in run:
        merged[k(r)] = r
    for r in sched:
        merged.setdefault(k(r), {x: r[x] for x in keys}).update(scheduled=r["scheduled"])
    for r in miss:
        merged.setdefault(k(r), {x: r[x] for x in keys}).update(missed=r["missed"])
    rows = []
    for r in merged.values():
        if judged:
            r.setdefault("scheduled", 0)
            r.setdefault("missed", 0)
            r["missed_pct"] = round(100.0 * r["missed"] / r["scheduled"], 2) if r["scheduled"] else None
        if r.get("trips_in_feed"):
            r["cancelled_pct"] = round(100.0 * r["cancelled"] / r["trips_in_feed"], 2)
        rows.append(r)
    order = "service_date" if keys[-1] == "service_date" else None
    rows.sort(key=lambda r: (str(r.get(order)) if order else -(r.get("trips_in_feed") or r.get("scheduled") or 0)))
    return {
        "start_date": start, "end_date": end, "days_read": len(pairs),
        "days_missing": ((datetime.strptime(end_date or start_date, "%Y-%m-%d").date()
                          - datetime.strptime(start_date, "%Y-%m-%d").date()).days + 1 - len(pairs)),
        "missed_trips_judged_for": f"{len(judged)} of {len(pairs)} days" + (
            "" if len(judged) == len(pairs) else
            " (the GTFS timetable on file doesn't cover the rest, so missed and scheduled are left out for them)"),
        "definitions_minutes": {"delayed_over": DELAYED_MIN, "major_delay_over": MAJOR_DELAY_MIN,
                                "punctual_within": PUNCTUALITY_MIN, "reliable_within": RELIABILITY_MIN},
        "group_by": group_by, "rows": rows[: max(1, min(limit, 1000))],
    }


def query_history(sql: str, start_date: str, end_date: str | None = None, max_rows: int = 500) -> dict:
    """Run your own read-only SQL (DuckDB dialect) over a date range
    (YYYY-MM-DD, yesterday and earlier) against the views `trips` (one row per
    trip per day), `scheduled` (GTFS timetable per day) and `missed`
    (scheduled trips never seen in the feed). Only a single SELECT or WITH
    query. Columns are in the server instructions."""
    text = sql.strip().rstrip(";").strip()
    if ";" in text or not text.lower().startswith(("select", "with")):
        raise ToolError("Only one SELECT or WITH query is allowed.")
    pairs, table, next_day_read = _load_range(start_date, end_date)
    if not pairs:
        return {"days_read": 0, "rows": []}
    con, judged = _connect_history(pairs, table, next_day_read, pairs[0][0], pairs[-1][0])
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
            "missed_judged_days": len(judged),
            "truncated": len(rows) > cap, "rows": rows[:cap]}


ALERT_COLUMNS = ["id", "alert_cause", "alert_effect", "alert_header_text_translation",
                 "alert_description_text_translation", "alert_active_period", "alert_informed_entity"]


def alerts_history(start_date: str, end_date: str | None = None, route: str | None = None,
                   effect: str | None = None, cause: str | None = None, search: str | None = None,
                   include_description: bool = False, limit: int = 100) -> dict:
    """Service alerts over a date range from the daily alerts files
    (YYYY-MM-DD; yesterday and earlier). Each alert appears once, with the
    days it was seen. Returns counts by effect and by cause, the most affected
    routes, and the alerts themselves (cause, effect, headline, routes, stops
    affected, active times). Filters: route ('70'), effect ('NO_SERVICE',
    'DETOUR', 'STOP_MOVED', 'MODIFIED_SERVICE'...), cause ('CONSTRUCTION',
    'MAINTENANCE'...), search (words in the headline or description)."""
    pairs = _history_names(start_date, end_date, kind="alerts")
    if not pairs:
        return {"start_date": start_date, "end_date": end_date or start_date, "days_read": 0, "alerts": []}
    with ThreadPoolExecutor(max_workers=8) as pool:
        tables = list(pool.map(_read, [n for _, n in pairs]))
    merged: dict = {}
    for (day, _), t in zip(pairs, tables):
        cols = {c: _str_col(t, c).to_pylist() for c in ALERT_COLUMNS}
        for i in range(t.num_rows):
            row = {
                "id": cols["id"][i], "cause": cols["alert_cause"][i], "effect": cols["alert_effect"][i],
                "header": cols["alert_header_text_translation"][i],
                "description": cols["alert_description_text_translation"][i],
                "active_period": cols["alert_active_period"][i],
                "informed_entity": cols["alert_informed_entity"][i],
            }
            key = row["id"] or f"{day}-{i}"
            first = merged.get(key, {}).get("first_seen", day)
            merged[key] = {**row, "first_seen": first, "last_seen": day,
                           "days_seen": merged.get(key, {}).get("days_seen", 0) + 1}
    alerts = []
    words = (search or "").lower().split()
    for row in merged.values():
        a = _clean_alert(row, description=True)
        if effect and (a["effect"] or "").upper() != effect.upper():
            continue
        if cause and (a["cause"] or "").upper() != cause.upper():
            continue
        if not _route_matches(a["route_ids"], route):
            continue
        hay = f"{a['header'] or ''} {a.get('description') or ''}".lower()
        if words and not all(w in hay for w in words):
            continue
        a.update(first_seen=row["first_seen"], last_seen=row["last_seen"], days_seen=row["days_seen"])
        if not include_description:
            a.pop("description", None)
        alerts.append(a)
    alerts.sort(key=lambda a: (a["first_seen"], a["effect"] or ""), reverse=True)

    def counts(key):
        c: dict = {}
        for a in alerts:
            c[a[key] or "unknown"] = c.get(a[key] or "unknown", 0) + 1
        return [{key: k, "alerts": v} for k, v in sorted(c.items(), key=lambda kv: -kv[1])]

    route_counts: dict = {}
    for a in alerts:
        for r in set(a["routes"]):
            route_counts[r] = route_counts.get(r, 0) + 1
    top_routes = [{"route": r, "alerts": n} for r, n in sorted(route_counts.items(), key=lambda kv: -kv[1])[:15]]
    return {"start_date": pairs[0][0], "end_date": pairs[-1][0], "days_read": len(pairs),
            "alerts_found": len(alerts), "by_effect": counts("effect"), "by_cause": counts("cause"),
            "most_affected_routes": top_routes, "alerts": alerts[: max(1, min(limit, 500))]}


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------

INSTRUCTIONS = (
    "at_* tools: Auckland Transport realtime data (trip updates, vehicle positions and "
    "alerts), captured about once a minute. Times are Auckland local. Delays are in "
    "minutes unless a field says seconds; positive means late. Route can be a short name "
    "like 'NX1' or a full route_id. Operators come from AT's static GTFS (route agency), "
    "falling back to vehicle label fleet codes (RT, NB, GB...). Ritchies trips also carry "
    "their depot (Albany, Ascot, Birkenhead, Oruarangi, Swanson, Takanini, Westgate). "
    "Definitions follow the Ritchies Command Centre: delayed > 2 min, major delay > 5 min, "
    "punctuality = within 5 min at the last stop, reliability = within 15 min, early start = "
    "left the first stop > 1 min early, missed = scheduled but never in the feed. Operator filters accept a "
    "code or part of a name. For today and right now use the realtime tools (at_late_trips, "
    "at_whats_running, at_query_feed...). For yesterday and earlier, and any date range "
    "(this week, this month), use at_performance, at_query_history and at_alerts_history: "
    "they read one combined file per day, so they're fast. Before making a report or chart, "
    "call rit_brand for the Ritchies logo and colours.\n\n" + FEED_COLUMNS_HELP + "\n\n" + TRIPS_COLUMNS_HELP
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
    "at_alerts_history": alerts_history,
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
