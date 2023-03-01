"""Read flight updates from kafka and store them into the database"""

from collections import defaultdict
from datetime import datetime, timezone, timedelta
from itertools import zip_longest, takewhile, chain
import json
import os
import threading
import time
import traceback
import warnings
from typing import Optional, Iterator, Iterable
from abc import ABC, abstractmethod

from confluent_kafka import KafkaException, Consumer  # type: ignore
import sqlalchemy as sa  # type: ignore
from sqlalchemy.sql import func, select, bindparam, and_  # type: ignore

# SQLAlchemy doesn't properly understand when you use columns with a "key"
# property with PostgreSQL's on_conflict_do_update statement, so it prints a
# pointless warning that we can just ignore.
warnings.filterwarnings("ignore", message="Additional column names not matching.*")

UTC = timezone.utc
TIMESTAMP_TZ = lambda: sa.TIMESTAMP(timezone=True)
# pylint: disable=invalid-name
meta = sa.MetaData()
TABLE: str = os.environ["TABLE"]

if TABLE not in ["flights", "positions"]:
    raise ValueError(f"Invalid TABLE env variable: {TABLE} - must be 'flights' or 'positions'")

if TABLE == "flights":
    table = sa.Table(
        "flights",
        meta,
        sa.Column("id", sa.String, primary_key=True),
        sa.Column("added", TIMESTAMP_TZ(), nullable=False, server_default=func.now()),
        sa.Column(
            "changed",
            TIMESTAMP_TZ(),
            nullable=False,
            server_default=func.now(),
            onupdate=func.now(),
        ),
        sa.Column("flight_number", sa.String, key="ident"),
        sa.Column("registration", sa.String, key="reg"),
        sa.Column("atc_ident", sa.String, key="atcident"),
        sa.Column("hexid", sa.String),
        sa.Column("origin", sa.String, key="orig"),
        sa.Column("destination", sa.String, key="dest"),
        sa.Column("aircraft_type", sa.String, key="aircrafttype"),
        sa.Column("filed_ground_speed", sa.Integer, key="gs"),
        sa.Column("filed_speed", sa.Integer, key="speed"),
        sa.Column("filed_altitude", sa.Integer),
        sa.Column("cruising_altitude", sa.Integer),
        sa.Column("true_cancel", sa.Boolean, key="trueCancel"),
        # These come through as a very lengthy list, worth stringifying?
        # sa.Column("waypoints", sa.String),
        sa.Column("route", sa.String),
        sa.Column("status", sa.Enum("S", "F", "A", "X", "Y", "Z", name="flightstatus")),
        sa.Column("actual_arrival_gate", sa.String),
        sa.Column("estimated_arrival_gate", sa.String),
        sa.Column("actual_departure_gate", sa.String),
        sa.Column("estimated_departure_gate", sa.String),
        sa.Column("actual_arrival_terminal", sa.String),
        sa.Column("scheduled_arrival_terminal", sa.String),
        sa.Column("actual_departure_terminal", sa.String),
        sa.Column("scheduled_departure_terminal", sa.String),
        sa.Column("actual_runway_off", sa.String),
        sa.Column("actual_runway_on", sa.String),
        sa.Column("baggage_claim", sa.String),
        sa.Column("cancelled", TIMESTAMP_TZ()),
        sa.Column("actual_out", TIMESTAMP_TZ()),
        sa.Column("actual_off", TIMESTAMP_TZ(), key="adt"),
        sa.Column("actual_on", TIMESTAMP_TZ(), key="aat"),
        sa.Column("actual_in", TIMESTAMP_TZ()),
        sa.Column("estimated_out", TIMESTAMP_TZ()),
        sa.Column("estimated_off", TIMESTAMP_TZ(), key="edt"),
        sa.Column("estimated_on", TIMESTAMP_TZ(), key="eta"),
        sa.Column("estimated_in", TIMESTAMP_TZ()),
        sa.Column("scheduled_off", TIMESTAMP_TZ(), key="fdt"),
        sa.Column("scheduled_out", TIMESTAMP_TZ()),
        sa.Column("scheduled_in", TIMESTAMP_TZ()),
        sa.Column("predicted_out", TIMESTAMP_TZ()),
        sa.Column("predicted_off", TIMESTAMP_TZ()),
        sa.Column("predicted_on", TIMESTAMP_TZ()),
        sa.Column("predicted_in", TIMESTAMP_TZ()),
    )
    VALID_EVENTS = {"arrival", "cancellation", "departure", "flightplan", "onblock", "offblock", "extendedFlightInfo", "flifo"}
elif TABLE == "positions":
    table = sa.Table(
        "positions",
        meta,
        sa.Column("id", sa.String, primary_key=True),
        sa.Column("added", TIMESTAMP_TZ(), nullable=False, server_default=func.now()),
        sa.Column("time", TIMESTAMP_TZ(), nullable=False, key="clock", primary_key=True),
        sa.Column("latitude", sa.String, key="lat", primary_key=True),
        sa.Column("longitude", sa.String, key="lon", primary_key=True),
        sa.Column("facility_hash", sa.String),
        sa.Column("facility_name", sa.String),
        sa.Column("update_type", sa.String, key="updateType"),
        sa.Column("adsb_version", sa.Integer),
        sa.Column("aircraft_type", sa.String, key="aircrafttype"),
        sa.Column("altitude", sa.Integer, key="alt"),
        sa.Column("gnss_altitude", sa.Integer, key="alt_gnss"),
        sa.Column("altitude_change", sa.String, key="altChange"),
        sa.Column("atc_ident", sa.String, key="atcident"),
        sa.Column("registration", sa.String, key="reg"),
        sa.Column("origin", sa.String, key="orig"),
        sa.Column("destination", sa.String, key="dest"),
        sa.Column("estimated_departure_time", sa.Integer, key="edt"),
        sa.Column("estimated_arrival_time", sa.Integer, key="eta"),
        sa.Column("en_route_time", sa.Integer, key="ete"),
        sa.Column("filed_en_route_time", sa.Integer, key="filed_ete"),
        sa.Column("groundspeed", sa.Integer, key="gs"),
        sa.Column("heading", sa.String),
        sa.Column("magnetic_heading", sa.String, key="heading_magnetic"),
        sa.Column("true_heading", sa.String, key="heading_true"),
        sa.Column("hexid", sa.String),
        sa.Column("mach_number", sa.String, key="mach"),
        sa.Column("nac_p", sa.Integer),
        sa.Column("nac_v", sa.Integer),
        sa.Column("nav_altitude", sa.Integer),
        sa.Column("nav_heading", sa.String),
        sa.Column("nav_modes", sa.String),
        sa.Column("nav_altimeter_setting", sa.String, key="nav_qnh"),
        sa.Column("nic", sa.Integer),
        sa.Column("nic_baro", sa.Integer),
        sa.Column("radius_of_containment", sa.Integer, key="pos_rc"),
        sa.Column("sil", sa.Integer),
        sa.Column("sil_type", sa.String),
        sa.Column("route", sa.String),
        sa.Column("air_pressure", sa.Integer, key="pressure"),
        sa.Column("filed_cruising_speed", sa.Integer, key="speed"),
        sa.Column("indicated_airspeed", sa.Integer, key="speed_ias"),
        sa.Column("true_airspeed", sa.Integer, key="speed_tas"),
        sa.Column("squawk", sa.Integer),
        sa.Column("temperature", sa.Integer),
        sa.Column("temperature_quality", sa.Integer),
        sa.Column("waypoints", sa.String),
        sa.Column("vertical_rate", sa.Integer, key="vertRate"),
        sa.Column("geometric_vertical_rate", sa.Integer, key="vertRate_geom"),
        sa.Column("wind_direction", sa.Integer, key="wind_dir"),
        sa.Column("wind_quality", sa.Integer),
        sa.Column("wind_speed", sa.Integer),
    )
    VALID_EVENTS = {"position"}

engine_args: dict = {}
db_url: str = os.environ["DB_URL"]

if "postgresql" in db_url:
    # Improve psycopg2 insert performance by using "fast execution helpers".
    # Further tuning of the executemany_*_page_size parameters could improve
    # performance even more.
    # https://docs.sqlalchemy.org/en/13/dialects/postgresql.html#psycopg2-fast-execution-helpers
    engine_args["executemany_mode"] = "values"
elif "sqlite" in db_url:
    # isolation_level setting works around buggy Python sqlite driver behavior
    # https://docs.sqlalchemy.org/en/13/dialects/sqlite.html#serializable-isolation-savepoints-transactional-ddl
    # timeout setting ensures we don't encounter timeouts when multiple threads
    # are trying to write to the database
    engine_args["connect_args"] = {"timeout": 60, "isolation_level": None}

engine = sa.create_engine(db_url, **engine_args)
if TABLE == "positions" and engine.name not in ["sqlite", "postgresql"]:
    raise Exception("Positions are only supported by SQLite and PostgreSQL-based databases")

# Make a hypertable on TimescaleDB for positions
attempt_hypertable = engine.name == "postgresql" and TABLE == "positions"

# Make sure that we can connect to the DB
while True:
    try:
        engine.connect()
        break
    except sa.exc.OperationalError as error:
        print(f"Can't connect to the database ({error}), trying again in a few seconds")
        time.sleep(3)

# Columns in the table that we'll explicitly be setting
MSG_TABLE_COLS = {c for c in table.c if c.server_default is None}
# The keys in a message that we want in the table
MSG_TABLE_KEYS = {c.key for c in MSG_TABLE_COLS}

finished = threading.Event()
cache_lock = threading.Lock()
SQLITE_VAR_LIMIT = None

dest_history = dict()

class Cache(ABC):
    """A cache for accumulating flight or position information which can be flushed as necessary."""

    @abstractmethod
    def add(self, data: dict) -> None:
        """Insert new row into the cache"""

    @abstractmethod
    def flush(self, conn) -> None:
        """Flush cache to the database"""


class PositionCache(Cache):
    """Position Cache Operations"""

    # pylint: disable=redefined-outer-name
    def __init__(self, table):
        """Initialize cache as a list"""
        self.cache = []
        self.table = table

    def add(self, data: dict) -> None:
        """Insert new row into the cache"""
        insert_data = dict.fromkeys(MSG_TABLE_KEYS)
        insert_data.update(data)
        self.cache.append(insert_data)

    def flush(self, conn) -> None:
        """Flush cache to the database"""
        if not self.cache:
            return

        print(f"Flushing {len(self.cache)} new positions to database")
        assert engine.name in ["sqlite", "postgresql"], f"{engine.name} is unsupported"
        if engine.name == "postgresql":
            # pylint: disable=import-outside-toplevel
            from sqlalchemy.dialects.postgresql import insert  # type: ignore

            statement = insert(self.table).on_conflict_do_nothing()
        elif engine.name == "sqlite":
            # Can replace with on_conflict_do_nothing() in SQLAlchemy 1.4b2
            statement = self.table.insert().prefix_with("OR IGNORE")

        # Ignore conflicts, indicative of running on an old pitr against a
        # more recently updated table. Rows will just be stale until we catch
        # up.
        conn.execute(statement, *self.cache)
        self.cache.clear()


class FlightCache(Cache):
    """Flight Cache Operations"""

    # pylint: disable=redefined-outer-name
    def __init__(self, table):
        """Initialize cache as a dict"""
        self.cache = defaultdict(lambda: dict.fromkeys(MSG_TABLE_KEYS))  # type: dict
        self.table = table

    def add(self, data: dict) -> None:
        """Insert new row into the cache or update an existing row"""
        self.cache[data["id"]].update(data)

    def flush(self, conn) -> None:
        """Flush cache to the database"""
        if not self.cache:
            return

        print(f"Flushing {len(self.cache)} new/updated flights to database")
        if engine.name == "postgresql":
            # Use postgresql's "ON CONFLICT UPDATE" statement to simplify logic
            # pylint: disable=import-outside-toplevel
            from sqlalchemy.dialects.postgresql import insert  # type: ignore

            statement = insert(self.table)
            # This builds the "SET ?=?" part of the update statement,
            # making sure to keep the row's current values if they're
            # non-null and the new row's are null
            col_updates = {
                c.name: func.coalesce(statement.excluded[c.key], c) for c in MSG_TABLE_COLS
            }
            # on_conflict_do_update won't handle Columns with onupdate set.
            # Have to do it ourselves.
            # https://docs.sqlalchemy.org/en/13/dialects/postgresql.html#sqlalchemy.dialects.postgresql.Insert.on_conflict_do_update
            col_updates["changed"] = func.now()
            statement = statement.on_conflict_do_update(index_elements=["id"], set_=col_updates)
            conn.execute(statement, *self.cache.values())
        else:
            updates = []
            # Get all rows from database that will need updating. Parameter
            # list is chunked as needed to prevent overrunning sqlite
            # limits: https://www.sqlite.org/limits.html#max_variable_number
            id_chunks = chunk(self.cache.keys(), SQLITE_VAR_LIMIT)
            existing = chain.from_iterable(
                conn.execute(select([self.table]).where(self.table.c.id.in_(keys)))
                for keys in id_chunks
            )
            for flight in existing:
                cache_flight = self.cache.pop(flight["id"])
                for k in cache_flight:
                    if cache_flight[k] is None:
                        cache_flight[k] = flight[k]
                # SQLAlchemy reserves column names in bindparam (used
                # below) for itself, so we need to rename this
                cache_flight["_id"] = cache_flight.pop("id")
                updates.append(cache_flight)
            # We removed the to-be-updated flights from the cache, so
            # insert the rest
            inserts = self.cache.values()
            # pylint: disable=no-value-for-parameter
            if updates:
                conn.execute(
                    self.table.update().where(self.table.c.id == bindparam("_id")), *updates,
                )
            if inserts:
                conn.execute(self.table.insert(), *inserts)
        self.cache.clear()


cache: Cache
if TABLE == "flights":
    cache = FlightCache(table)
elif TABLE == "positions":
    cache = PositionCache(table)


def convert_msg_fields(msg: dict) -> dict:
    """Remove unneeded keys from message JSON and convert value types.

    Modifies msg in-place and returns it."""
    pitr = msg["pitr"]
    for key in msg.keys() - MSG_TABLE_KEYS:
        del msg[key]
    for key, val in list(msg.items()):
        column_type = str(table.c[key].type)
        try:
            if column_type == "TIMESTAMP":
                msg[key] = datetime.fromtimestamp(int(val), tz=UTC)
            elif column_type == "INTEGER":
                msg[key] = int(float(val))
            elif column_type == "BOOLEAN":
                msg[key] = bool(int(val))
        except Exception as e:
            print(f"Couldn't convert '{key}' field in message for flight_id '{msg['id']}' at '{pitr}'")
            raise
    return msg


@sa.event.listens_for(engine, "begin")
def do_begin(conn: sa.engine.Transaction) -> None:
    """emit our own BEGIN, and make it immediate so we don't get a SQLITE_BUSY
    error if we happen to expire entries in the table between the flush_cache thread's
    read and write (more info about how this can occur:
    https://www.sqlite.org/rescode.html#busy_snapshot)
    """
    if engine.name != "sqlite":
        return

    conn.execute("BEGIN IMMEDIATE")


def chunk(values: Iterable, chunk_size: Optional[int]) -> Iterator:
    """Splits a sequence into separate sequences of equal size (besides the last)

    values is the sequence that you want to split
    chunk_size is the length of each chunk. The last chunk may be smaller than this.
    """
    if chunk_size:
        # Structured after itertools grouper recipe
        args = [iter(values)] * chunk_size
        # Prevent padding behavior of zip_longest
        for group in zip_longest(*args):
            yield takewhile(lambda x: x is not None, group)
    else:
        yield from [values]


def add_to_cache(data: dict) -> None:
    """add entry to the cache"""
    converted = convert_msg_fields(data)
    with cache_lock:
        cache.add(converted)


def flush_cache() -> None:
    """Add info into the database table"""
    while not finished.is_set():
        finished.wait(2)
        with cache_lock, engine.begin() as conn:
            cache.flush(conn)


def expire_old_from_table() -> None:
    """Wrapper for _expire_old_from_table"""
    while not finished.is_set():
        finished.wait(60)
        _expire_old_from_table()


def _expire_old_from_table() -> None:
    """Remove entries from the database if they have not been updated in 48 hours"""
    dropoff = datetime.now(tz=UTC) - timedelta(hours=48)
    dtmin = datetime.min.replace(tzinfo=UTC)
    # pylint: disable=no-value-for-parameter
    statement = table.delete().where(
        and_(*(func.coalesce(c, dtmin) < dropoff for c in table.c if str(c.type) == "TIMESTAMP"))
    )
    result = engine.execute(statement)
    if result.rowcount:
        print(f"Expired {result.rowcount} entries from database")
    result.close()


def process_unknown_message(data: dict) -> None:
    """Unknown message type"""
    print(f"Don't know how to handle message with type {data['type']}")


def process_arrival_message(data: dict) -> None:
    """Arrival message type"""
    if "ident" in data:
        global dest_history
        dest_history.pop(data.get("ident"))
    return add_to_cache(data)


def process_cancellation_message(data: dict) -> None:
    """Cancel message type"""
    data["cancelled"] = data["pitr"]
    disambiguate_altitude(data)
    return add_to_cache(data)


def process_departure_message(data: dict) -> None:
    """Departure message type"""
    return add_to_cache(data)


def process_offblock_message(data: dict) -> None:
    """Offblock message type"""
    data["actual_out"] = data["clock"]
    return add_to_cache(data)


def process_onblock_message(data: dict) -> None:
    """Onblock message type"""
    if "ident" in data:
        global dest_history
        dest_history.pop(data.get("ident"))
    data["actual_in"] = data["clock"]
    return add_to_cache(data)


def process_flifo_message(data: dict) -> None:
    """flifo message type"""
    # flifo messages try to help us with saner names, but we already convert
    # field names at the sqlalchemy level, so we actually need to convert the
    # nice names to ugly names so they can be converted again later...
    field_map = {
        "actual_off": "adt",
        "actual_on": "aat",
        "estimated_off": "edt",
        "estimated_on": "eta",
        "filed_airspeed": "speed",
        "status": "flightstatus",
        "scheduled_off": "fdt",
        "filed_alt": "filed_altitude",
        "cruising_alt": "cruising_altitude",
    }
    for k, v in field_map.items():
        if k in data:
            data[v] = data.pop(k)
    return add_to_cache(data)

def check_for_diversions(data: dict) -> None:
    """ETMS message, check destination"""
    if "dest" in data and "ident" in data:
        global dest_history

        dest = data.get("dest")
        if dest == "":
            return

        ident = data.get("ident")
        if ident == "":
            return

        if ident in dest_history:
            orig_dest = dest_history.get(ident)
            if orig_dest != "":
                if orig_dest != dest:
                    diversion(ident, dest)

        dest_history[ident] = dest

    return

def process_extended_flight_info_message(data: dict) -> None:
    """extendedFlightInfo message type"""
    return add_to_cache(data)


def process_flightplan_message(data: dict) -> None:
    """Flightplan message type"""
    disambiguate_altitude(data)
    return add_to_cache(data)


def process_position_message(data: dict) -> None:
    """Position message type"""
    return add_to_cache(data)


def process_keepalive_message(data: dict) -> None:
    """Keepalive message type"""
    behind = datetime.now(tz=UTC) - datetime.fromtimestamp(int(data["pitr"]), tz=UTC)
    print(f'Based on keepalive["pitr"], we are {behind} behind realtime')


def disambiguate_altitude(data: dict):
    """Replaces the alt field in the passed dict with an unambiguous field name"""

    if "alt" in data:
        if data.get("flightstatus") in ["F", "S"]:
            data["filed_altitude"] = data.pop("alt")
        else:
            data["cruising_altitude"] = data.pop("alt")


def setup_sqlite() -> None:
    """Set proper sqlite configurations"""

    # WAL mode allows reading the db while it's being written to
    engine.scalar("PRAGMA journal_mode=WAL")
    # pylint: disable=global-statement
    global SQLITE_VAR_LIMIT
    # SQLite's default
    SQLITE_VAR_LIMIT = 999
    for option in engine.execute("PRAGMA compile_options").fetchall():
        if option[0].startswith("MAX_VARIABLE_NUMBER="):
            SQLITE_VAR_LIMIT = int(option[0].split("=")[1])
            break


def main():
    """Read flight updates from kafka and store them into the database"""
    exists = False
    if engine.name == "sqlite":
        setup_sqlite()
    if engine.has_table(TABLE):
        print(f"{TABLE} table already exists, clearing expired rows from {TABLE} before continuing")
        _expire_old_from_table()
        exists = True
    meta.create_all(engine)

    if attempt_hypertable and not exists:
        # pylint: disable=broad-except
        try:
            engine.execute("SELECT create_hypertable('positions', 'time')")
        except Exception as error:
            print(f"Could not create hypertable: {error}")

    processor_functions = {
        "arrival": process_arrival_message,
        "departure": process_departure_message,
        "cancellation": process_cancellation_message,
        "offblock": process_offblock_message,
        "onblock": process_onblock_message,
        "flifo": process_flifo_message,
        "extendedFlightInfo": process_extended_flight_info_message,
        "flightplan": process_flightplan_message,
        "keepalive": process_keepalive_message,
        "position": process_position_message,
        "etms_update": check_for_diversions,
        "etms_scheduled": check_for_diversions,
        "etms_filed": check_for_diversions,
    }

    consumer = None
    while True:
        try:
            # Handle case where we initialized the consumer but failed to
            # subscribe. Don't want to keep initializing.
            if consumer is None:
                consumer = Consumer(
                    {
                        "bootstrap.servers": "kafka:9092",
                        "group.id": os.environ["KAFKA_GROUP_NAME"],
                        "auto.offset.reset": "earliest",
                        # Consider committing manually upon writes to db
                        # true by default anyway
                        "enable.auto.commit": True,
                        "auto.commit.interval.ms": 1000,
                    }
                )
            consumer.subscribe([os.environ["KAFKA_TOPIC_NAME"]])
            break
        except (KafkaException, OSError) as error:
            print(f"Kafka isn't available ({error}), trying again in a few seconds")
            time.sleep(3)

    threading.Thread(target=flush_cache, name="flush_cache").start()
    threading.Thread(target=expire_old_from_table, name="expire").start()
    while True:
        # Polling will mask SIGINT, just fyi
        messagestr = consumer.poll(timeout=1.0)
        if messagestr is None:
            # poll timed out
            continue
        if messagestr.error():
            print(f"Encountered kafka error: {messagestr.error()}")
            # They continue in the examples, so let's do it as well
            continue
        message = json.loads(messagestr.value())
        event_type = message["type"]
        if event_type != "keepalive" and event_type not in VALID_EVENTS:
            continue
        processor_functions.get(message["type"], process_unknown_message)(message)


if __name__ == "__main__":
    # pylint: disable=broad-except
    try:
        main()
    except Exception as error:
        traceback.print_exc()
        print("\nQuitting due to exception, wait a moment for cache flush to database\n")
    finally:
        finished.set()
