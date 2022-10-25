"""Read flight updates from kafka and store them into the database"""

from collections import defaultdict
from datetime import datetime, timezone, timedelta
from itertools import zip_longest, takewhile, chain
import json
import os
from queue import Empty, Queue
import sys
import threading
import time
import traceback
import warnings
from typing import Any, Dict, List, Optional, Iterator, Iterable
from abc import ABC, abstractmethod

from confluent_kafka import (
    KafkaError,
    KafkaException,
    Consumer,
    Producer,
    TopicPartition,
)  # type: ignore
import sqlalchemy as sa  # type: ignore
from sqlalchemy.sql import func, delete, select, bindparam, and_, or_  # type: ignore

# SQLAlchemy doesn't properly understand when you use columns with a "key"
# property with PostgreSQL's on_conflict_do_update statement, so it prints a
# pointless warning that we can just ignore.
warnings.filterwarnings("ignore", message="Additional column names not matching.*")

UTC = timezone.utc
TIMESTAMP_TZ = lambda: sa.TIMESTAMP(timezone=True)
# pylint: disable=invalid-name
meta = sa.MetaData()
TABLE = "flights"

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
    sa.Column("cancelled", sa.Integer),
    sa.Column("actual_out", sa.Integer),
    sa.Column("actual_off", sa.Integer, key="adt"),
    sa.Column("actual_on", sa.Integer, key="aat"),
    sa.Column("actual_in", sa.Integer),
    sa.Column("estimated_out", sa.Integer),
    sa.Column("estimated_off", sa.Integer, key="edt"),
    sa.Column("estimated_on", sa.Integer, key="eta"),
    sa.Column("estimated_in", sa.Integer),
    sa.Column("scheduled_off", sa.Integer, key="fdt"),
    sa.Column("scheduled_out", sa.Integer),
    sa.Column("scheduled_in", sa.Integer),
    sa.Column("predicted_out", sa.Integer),
    sa.Column("predicted_off", sa.Integer),
    sa.Column("predicted_on", sa.Integer),
    sa.Column("predicted_in", sa.Integer),
)
VALID_EVENTS = {
    "arrival",
    "cancellation",
    "departure",
    "flightplan",
    "onblock",
    "offblock",
    "extendedFlightInfo",
    "flifo",
}

engine_args: dict = {}
db_url: str = os.environ["DB_URL"]

# isolation_level setting works around buggy Python sqlite driver behavior
# https://docs.sqlalchemy.org/en/13/dialects/sqlite.html#serializable-isolation-savepoints-transactional-ddl
# timeout setting ensures we don't encounter timeouts when multiple threads
# are trying to write to the database
engine_args["connect_args"] = {"timeout": 60, "isolation_level": None}

engine = sa.create_engine(db_url, **engine_args)

# Make sure that we can connect to the DB
while True:
    try:
        engine.connect()
        break
    except sa.exc.OperationalError as op_error:
        print(f"Can't connect to the database ({op_error}), trying again in a few seconds")
        time.sleep(3)

# Columns in the table that we'll explicitly be setting
MSG_TABLE_COLS = {c for c in table.c if c.server_default is None}
# The keys in a message that we want in the table
MSG_TABLE_KEYS = {c.key for c in MSG_TABLE_COLS}

finished = threading.Event()
cache_lock = threading.Lock()
SQLITE_VAR_LIMIT = None


class Cache(ABC):
    """A cache for accumulating flight or position information which can be flushed as necessary."""

    @abstractmethod
    def add(self, data: dict, offset: int) -> None:
        """Insert new row into the cache"""

    @abstractmethod
    def flush(self, conn) -> Optional[int]:
        """Flush cache to the database"""



class FlightCache(Cache):
    """Flight Cache Operations"""

    # pylint: disable=redefined-outer-name
    def __init__(self, table):
        """Initialize cache as a dict"""
        self.cache = defaultdict(lambda: dict.fromkeys(MSG_TABLE_KEYS))  # type: dict
        self.table = table
        self._offset = None
        self._pitr = 0

    @property
    def pitr(self) -> int:
        """Return the highest PITR value seen so far"""
        return self._pitr

    def add(self, data: dict, offset: int, pitr: int) -> None:
        """Insert new row into the cache or update an existing row"""
        self.cache[data["id"]].update(data)
        self._offset = offset
        self._pitr = max(self._pitr, pitr)

    def flush(self, conn) -> Optional[int]:
        """Flush cache to the database"""
        if not self.cache:
            return self._offset

        print(f"Flushing {len(self.cache):,} new/updated flights to database")

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
                self.table.update().where(self.table.c.id == bindparam("_id")),
                *updates,
            )
        if inserts:
            conn.execute(self.table.insert(), *inserts)
    self.cache.clear()

    offset = self._offset
    self._offset = None
    return offset


cache: Cache = FlightCache(table)


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
        except Exception:
            print(
                f"Couldn't convert '{key}' field in message for flight_id '{msg['id']}' at '{pitr}'"
            )
            raise
    return msg


@sa.event.listens_for(engine, "begin")
def do_begin(conn: sa.engine.Transaction) -> None:
    """emit our own BEGIN, and make it immediate so we don't get a SQLITE_BUSY
    error if we happen to expire entries in the table between the flush_cache thread's
    read and write (more info about how this can occur:
    https://www.sqlite.org/rescode.html#busy_snapshot)
    """
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


def add_to_cache(data: dict, offset: int) -> None:
    """add entry to the cache"""
    pitr = int(data["pitr"])
    converted = convert_msg_fields(data)
    with cache_lock:
        cache.add(converted, offset, pitr)


def flush_cache(consumer: Consumer) -> None:
    """Add info into the database table"""
    while not finished.is_set():
        finished.wait(2)
        offset_to_commit = None
        with cache_lock, engine.begin() as conn:
            offset_to_commit = cache.flush(conn)

        if offset_to_commit:
            tp = TopicPartition(os.environ["KAFKA_TOPIC_NAME"], 0, offset_to_commit)
            consumer.commit(offsets=[tp], asynchronous=False)
            print(f"Committed offset {offset_to_commit}")


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
        print(f"Expired {result.rowcount:,} entries from database")
    result.close()


def create_producer() -> Producer:
    """Create a Kafka producer for summarized flight data"""
    producer = None
    while producer is None:
        try:
            producer = Producer(
                {
                    "bootstrap.servers": "kafka:9092",
                    "linger.ms": 500,
                    "transactional.id": "firestarter-summarizer",
                    "batch.size": 65536,
                }
            )

            producer.init_transactions()
            producer.begin_transaction()
            producer.produce(
                "test",
                key="noop",
                value="",
            )
            producer.abort_transaction()

            return producer
        except KafkaException as error:
            pass
        except KafkaError as error:
            if not error.args[0].retriable():
                sys.exit(f"Fatal Kafka error for init_transactions: {error}")

        print(f"Kafka isn't available ({error}), trying again in a few seconds")
        time.sleep(3)


def summarized_flight_rows(virtual_clock: int, cutoff: int = 21600) -> List[Dict[str, Any]]:
    """Query the flights table for summarized flights, i.e., flights that have
    been cancelled, have an actual on and in, or have only an on but the on
    value is more than a threshold amount of seconds in the past relative to a
    given virtual_clock epoch timestamp"""
    summarized_rows = []
    with engine.begin() as conn:
        result = conn.execute(
            select([table]).where(
                or_(
                    table.c.cancelled != sa.null(),
                    and_(
                        table.c.actual_in != sa.null(),
                        table.c.aat != sa.null(),
                    ),
                    and_(
                        table.c.aat != sa.null(),
                        table.c.aat < (virtual_clock - cutoff),
                        table.c.actual_in == sa.null(),
                    ),
                )
            )
        )

        for row in (dict(r) for r in result):
            row.pop("changed")
            row.pop("added")
            summarized_rows.append(row)

    return summarized_rows


def cache_pitr() -> int:
    """Read the largest PITR in the cache"""
    with cache_lock:
        return cache.pitr


def find_summarized_flights(producer: Producer, queue: Queue) -> None:
    """Find all flights that need to be summarized into Kafka records
    After finding them and producing them to Kafka, send them through the queue
    so they can be removed from the database"""

    producer_topic = os.environ["PRODUCER_TOPIC_NAME"]
    batch_size = int(os.getenv("BATCH_SIZE", "20000"))
    while not finished.is_set():
        finished.wait(10)

        summarized_rows = summarized_flight_rows(cache_pitr())
        print(f"Processing {len(summarized_rows):,} summarized flight rows")
        if not summarized_rows:
            continue

        cur_idx = 0
        total_rows = len(summarized_rows)

        while cur_idx < total_rows:
            try:
                producer.begin_transaction()
                summarized_batch = summarized_rows[cur_idx : cur_idx + batch_size]
                for row in summarized_batch:
                    producer.produce(topic=producer_topic, value=json.dumps(row))

                producer.commit_transaction()
                print(f"Sent {len(summarized_batch):,} rows to {producer_topic} topic")
                cur_idx += batch_size

                queue.put([row["id"] for row in summarized_batch])
            except BufferError:
                print("Producer buffer full, flushing and trying again...")
                producer.abort_transaction()
                still_in_queue = producer.flush()
                print(f"{still_in_queue:,} messages in queue after flushing")
            except (KafkaException, KafkaError) as error:
                if error.args[0].retriable():
                    print("Retriable Kafka error when producing summarized rows")
                    print(f"{error}")

                    producer.abort_transaction()
                else:
                    sys.exit(f"Fatal Kafka error for init_transactions: {error}")


def remove_summarized_flights_from_database(queue: Queue) -> None:
    """Read flight IDs from a queue and delete them from the database"""
    while not finished.is_set():
        timeout = 3
        try:
            finished.wait(timeout)

            flight_ids = queue.get(block=False)
            if not flight_ids:
                continue

            with engine.begin() as conn:
                id_chunks = chunk(flight_ids, SQLITE_VAR_LIMIT)
                for keys in id_chunks:
                    conn.execute(delete(table).where(table.c.id.in_(keys)))

            print(f"Deleted {len(flight_ids):,} summarized flights from database")

        except Empty:
            continue


def process_unknown_message(data: dict, _: int) -> None:
    """Unknown message type"""
    print(f"Don't know how to handle message with type {data['type']}")


def process_arrival_message(data: dict, offset: int) -> None:
    """Arrival message type"""
    return add_to_cache(data, offset)


def process_cancellation_message(data: dict, offset: int) -> None:
    """Cancel message type"""
    data["cancelled"] = data["pitr"]
    disambiguate_altitude(data)
    return add_to_cache(data, offset)


def process_departure_message(data: dict, offset: int) -> None:
    """Departure message type"""
    return add_to_cache(data, offset)


def process_offblock_message(data: dict, offset: int) -> None:
    """Offblock message type"""
    data["actual_out"] = data["clock"]
    return add_to_cache(data, offset)


def process_onblock_message(data: dict, offset: int) -> None:
    """Onblock message type"""
    data["actual_in"] = data["clock"]
    return add_to_cache(data, offset)


def process_flifo_message(data: dict, offset: int) -> None:
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
    return add_to_cache(data, offset)


def process_extended_flight_info_message(data: dict, offset: int) -> None:
    """extendedFlightInfo message type"""
    return add_to_cache(data, offset)


def process_flightplan_message(data: dict, offset: int) -> None:
    """Flightplan message type"""
    disambiguate_altitude(data)
    return add_to_cache(data, offset)


def process_position_message(data: dict, offset: int) -> None:
    """Position message type"""
    return add_to_cache(data, offset)


def process_keepalive_message(data: dict, _: int) -> None:
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
    setup_sqlite()
    if engine.has_table(TABLE):
        print(f"{TABLE} table already exists, clearing expired rows from {TABLE} before continuing")
        _expire_old_from_table()
        exists = True
    meta.create_all(engine)

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
    }

    topic = os.environ["KAFKA_TOPIC_NAME"]
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
                        "enable.auto.commit": False,
                    }
                )
            consumer.subscribe([topic])
            break
        except (KafkaException, OSError) as error:
            print(f"Kafka isn't available ({error}), trying again in a few seconds")
            time.sleep(3)

    threading.Thread(target=flush_cache, args=(consumer,), name="flush_cache").start()
    threading.Thread(target=expire_old_from_table, name="expire").start()

    queue = Queue()
    threading.Thread(
        target=find_summarized_flights, args=(create_producer(), queue), name="summary"
    ).start()
    threading.Thread(
        target=remove_summarized_flights_from_database, args=(queue,), name="summary_cleaner"
    ).start()

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
        offset = messagestr.offset()
        event_type = message["type"]
        if event_type != "keepalive" and event_type not in VALID_EVENTS:
            continue
        processor_functions.get(message["type"], process_unknown_message)(message, offset)


if __name__ == "__main__":
    # pylint: disable=broad-except
    try:
        main()
    except Exception as error:
        traceback.print_exc()
        print("\nQuitting due to exception, wait a moment for cache flush to database\n")
    finally:
        finished.set()
