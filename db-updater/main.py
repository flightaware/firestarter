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
from typing import Optional, Generator, KeysView
from kafka import KafkaConsumer  # type: ignore
from kafka.errors import NoBrokersAvailable  # type: ignore

import sqlalchemy as sa  # type: ignore
from sqlalchemy.sql import func, select, bindparam, and_  # type: ignore

# SQLAlchemy doesn't properly understand when you use columns with a "key"
# property with PostgreSQL's on_conflict_do_update statement, so it prints a
# pointless warning that we can just ignore.
warnings.filterwarnings("ignore", message="Additional column names not matching.*")

UTC = timezone.utc
TIMESTAMP_TZ = lambda: sa.TIMESTAMP(timezone=True)
META = sa.MetaData()
FLIGHTS = sa.Table(
    "flights",
    META,
    sa.Column("id", sa.String, primary_key=True),
    sa.Column("added", TIMESTAMP_TZ(), nullable=False, server_default=func.now()),
    sa.Column(
        "changed", TIMESTAMP_TZ(), nullable=False, server_default=func.now(), onupdate=func.now(),
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
    sa.Column("filed_altitude", sa.Integer, key="alt"),
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
    sa.Column("baggage_claim", sa.String),
    sa.Column("cancelled", TIMESTAMP_TZ()),
    sa.Column("filed_off", TIMESTAMP_TZ(), key="fdt"),
    sa.Column("actual_out", TIMESTAMP_TZ()),
    sa.Column("actual_off", TIMESTAMP_TZ(), key="adt"),
    sa.Column("actual_on", TIMESTAMP_TZ(), key="aat"),
    sa.Column("actual_in", TIMESTAMP_TZ()),
    sa.Column("estimated_out", TIMESTAMP_TZ()),
    sa.Column("estimated_off", TIMESTAMP_TZ(), key="edt"),
    sa.Column("estimated_on", TIMESTAMP_TZ(), key="eta"),
    sa.Column("estimated_in", TIMESTAMP_TZ()),
    sa.Column("scheduled_out", TIMESTAMP_TZ()),
    sa.Column("scheduled_in", TIMESTAMP_TZ()),
    sa.Column("predicted_out", TIMESTAMP_TZ()),
    sa.Column("predicted_off", TIMESTAMP_TZ()),
    sa.Column("predicted_on", TIMESTAMP_TZ()),
    sa.Column("predicted_in", TIMESTAMP_TZ()),
)

engine_args: dict = {}
db_url: str = os.getenv("DB_URL")  # type: ignore
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

ENGINE = sa.create_engine(db_url, **engine_args)

# Columns in the table that we'll explicitly be setting
MSG_TABLE_COLS = {c for c in FLIGHTS.c if c.server_default is None}
# The keys in a message that we want in the flights table
MSG_TABLE_KEYS = {c.key for c in MSG_TABLE_COLS}

FINISHED = threading.Event()
CACHE_LOCK = threading.Lock()
# Use a cache for accumulating flight information, flushing it to the database
# as necessary.  It should contain full versions of flight rows (rather than
# the sparse ones we might get if we just insert/update flights according to
# received data) to ensure proper behavior of executemany-style SQLAlchemy
# statements.
# https://docs.sqlalchemy.org/en/13/core/tutorial.html#executing-multiple-statements
CACHE = defaultdict(lambda: dict.fromkeys(MSG_TABLE_KEYS))  # type: dict

SQLITE_VAR_LIMIT = None


@sa.event.listens_for(ENGINE, "begin")
def do_begin(conn: sa.engine.Transaction) -> None:
    """emit our own BEGIN, and make it immediate so we don't get a SQLITE_BUSY
    error if we happen to expire flights between the flush_cache thread's
    read and write (more info about how this can occur:
    https://www.sqlite.org/rescode.html#busy_snapshot)
    """
    if ENGINE.name != "sqlite":
        return

    conn.execute("BEGIN IMMEDIATE")


def convert_msg_fields(msg: dict) -> dict:
    """Remove unneeded keys from message JSON and convert value types.

    Modifies msg in-place and returns it."""
    for key in msg.keys() - MSG_TABLE_KEYS:
        del msg[key]
    for key, val in msg.items():
        column_type = str(FLIGHTS.c[key].type)
        if column_type == "TIMESTAMP":
            msg[key] = datetime.fromtimestamp(int(val), tz=UTC)
        elif column_type == "INTEGER":
            msg[key] = int(val)
        elif column_type == "BOOLEAN":
            msg[key] = bool(int(val))
    return msg


def insert_or_update(data: dict) -> None:
    """Insert new row into the database or update an existing row"""
    converted = convert_msg_fields(data)
    with CACHE_LOCK:
        CACHE[converted["id"]].update(converted)


def chunk(values: KeysView, chunk_size: Optional[int]) -> Generator:
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
    return [values]


def flush_cache(engine: sa.engine) -> None:
    """Add flights into the database"""
    while not FINISHED.is_set():
        FINISHED.wait(2)
        with CACHE_LOCK, engine.begin() as conn:
            if not CACHE:
                continue
            print(f"Flushing {len(CACHE)} new/updated flights to database")
            if engine.name == "postgresql":
                # Use postgresql's "ON CONFLICT UPDATE" statement to simplify logic
                # pylint: disable=import-outside-toplevel
                from sqlalchemy.dialects.postgresql import insert  # type: ignore

                statement = insert(FLIGHTS)
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
                conn.execute(statement, *CACHE.values())
            else:
                updates = []
                # Get all rows from database that will need updating. Parameter
                # list is chunked as needed to prevent overrunning sqlite
                # limits: https://www.sqlite.org/limits.html#max_variable_number
                id_chunks = chunk(CACHE.keys(), SQLITE_VAR_LIMIT)
                existing = chain.from_iterable(
                    conn.execute(select([FLIGHTS]).where(FLIGHTS.c.id.in_(keys)))
                    for keys in id_chunks
                )
                for flight in existing:
                    cache_flight = CACHE.pop(flight["id"])
                    for k in cache_flight:
                        if cache_flight[k] is None:
                            cache_flight[k] = flight[k]
                    # SQLAlchemy reserves column names in bindparam (used
                    # below) for itself, so we need to rename this
                    cache_flight["_id"] = cache_flight.pop("id")
                    updates.append(cache_flight)
                # We removed the to-be-updated flights from the cache, so
                # insert the rest
                inserts = CACHE.values()
                # pylint: disable=no-value-for-parameter
                if updates:
                    conn.execute(
                        FLIGHTS.update().where(FLIGHTS.c.id == bindparam("_id")), *updates,
                    )
                if inserts:
                    conn.execute(FLIGHTS.insert(), *inserts)
            CACHE.clear()


def expire_old_flights(engine: sa.engine) -> None:
    """Wrapper for _expire_old_flights"""
    while not FINISHED.is_set():
        FINISHED.wait(60)
        _expire_old_flights(engine)


def _expire_old_flights(engine: sa.engine) -> None:
    """Remove flights from the database if they have not been updated in 48 hours"""
    dropoff = datetime.now(tz=UTC) - timedelta(hours=48)
    dtmin = datetime.min.replace(tzinfo=UTC)
    # pylint: disable=no-value-for-parameter
    statement = FLIGHTS.delete().where(
        and_(*(func.coalesce(c, dtmin) < dropoff for c in FLIGHTS.c if str(c.type) == "TIMESTAMP"))
    )
    result = engine.execute(statement)
    if result.rowcount:
        print(f"Expired {result.rowcount} flights from database")
    result.close()


def process_unknown_message(data: dict) -> None:
    """Unknown message type"""
    print(f"Don't know how to handle message with type {data['type']}")


def process_arrival_message(data: dict) -> None:
    """Arrival message type"""
    return insert_or_update(data)


def process_cancellation_message(data: dict) -> None:
    """Cancel message type"""
    data["cancelled"] = data["pitr"]
    return insert_or_update(data)


def process_departure_message(data: dict) -> None:
    """Departure message type"""
    return insert_or_update(data)


def process_offblock_message(data: dict) -> None:
    """Offblock message type"""
    data["actual_out"] = data["clock"]
    return insert_or_update(data)


def process_onblock_message(data: dict) -> None:
    """Onblock message type"""
    data["actual_in"] = data["clock"]
    return insert_or_update(data)


def process_extended_flight_info_message(data: dict) -> None:
    """extendedFlightInfo message type"""
    return insert_or_update(data)


def process_flightplan_message(data: dict) -> None:
    """Flightplan message type"""
    return insert_or_update(data)


def process_keepalive_message(data: dict) -> None:
    """Keepalive message type"""
    behind = datetime.now(tz=UTC) - datetime.fromtimestamp(int(data["pitr"]), tz=UTC)
    print(f'Based on keepalive["pitr"], we are {behind} behind realtime')


def setup_sqlite(engine: sa.engine) -> None:
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
    if ENGINE.name == "sqlite":
        setup_sqlite(ENGINE)
    if ENGINE.has_table("flights"):
        print("flights table already exists, clearing expired flights before continuing")
        _expire_old_flights(ENGINE)
    META.create_all(ENGINE)

    processor_functions = {
        "arrival": process_arrival_message,
        "departure": process_departure_message,
        "cancellation": process_cancellation_message,
        "offblock": process_offblock_message,
        "onblock": process_onblock_message,
        "extendedFlightInfo": process_extended_flight_info_message,
        "flightplan": process_flightplan_message,
        "keepalive": process_keepalive_message,
    }

    while True:
        try:
            consumer = KafkaConsumer(
                os.getenv("KAFKA_TOPIC_NAME"),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,
                bootstrap_servers=["kafka:9092"],
                group_id="feed1",
            )

            threading.Thread(target=flush_cache, name="flush_cache", args=(ENGINE,)).start()
            threading.Thread(target=expire_old_flights, name="expire", args=(ENGINE,)).start()
            for msg in consumer:
                message = json.loads(msg.value)
                processor_functions.get(message["type"], process_unknown_message)(message)
            print("Got EOF from kafka, quitting")
            break
        except (OSError, NoBrokersAvailable) as error:
            print(f"Kafka isn't available ({error}), trying again in a few seconds")
            time.sleep(3)


if __name__ == "__main__":
    # pylint: disable=broad-except
    try:
        main()
    except Exception as error:
        traceback.print_exc()
        print("\nQuitting due to exception, wait a moment for cache flush to database\n")
    finally:
        FINISHED.set()
