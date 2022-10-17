"""The Firestarter Connector connects to Firehose and writes the output to a kafka topic
"""

import asyncio
import json
import os
import ssl
import time
import warnings
import zlib
from typing import Optional, Tuple
from confluent_kafka import Consumer, KafkaException, KafkaError, Producer, TopicPartition  # type: ignore

CONNECTION_ERROR_LIMIT = 3

USERNAME: str
APIKEY: str

COMPRESSION: str
INIT_CMD_ARGS: str
INIT_CMD_TIME: str
KEEPALIVE: int
KEEPALIVE_STALE_PITRS: int
KEEPALIVE_PRODUCE: bool
SERVERNAME: str
STATS_PERIOD: int

# pylint: disable=invalid-name
stats_lock: asyncio.Lock
finished: asyncio.Event
producer: Producer

lines_read: int = 0
bytes_read: int = 0
last_good_pitr: Optional[int]


class ZlibReaderProtocol(asyncio.StreamReaderProtocol):
    """asyncio Protocol that handles streaming decompression of Firehose data"""

    def __init__(self, mode: str, *args, **kwargs) -> None:
        self._z = None
        if mode == "deflate":  # no header, raw deflate stream
            self._z = zlib.decompressobj(-zlib.MAX_WBITS)
        elif mode == "compress":  # zlib header
            self._z = zlib.decompressobj(zlib.MAX_WBITS)
        elif mode == "gzip":  # gzip header
            self._z = zlib.decompressobj(16 | zlib.MAX_WBITS)
        super().__init__(*args, **kwargs)

    def data_received(self, data: bytes) -> None:
        if not self._z:
            super().data_received(data)
        else:
            super().data_received(self._z.decompress(data))


# pylint: disable=bad-continuation
async def open_connection(
    host: str = None, port: int = None, *, loop=None, limit=2 ** 16, **kwds
) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    """Coroutine copied from asyncio source with a tweak to use our custom ZlibReadProtocol
    for the returned StreamReader
    """

    if loop is None:
        loop = asyncio.get_event_loop()
    else:
        warnings.warn(
            "The loop argument is deprecated since Python 3.8, "
            "and scheduled for removal in Python 3.10.",
            DeprecationWarning,
            stacklevel=2,
        )
    reader = asyncio.StreamReader(limit=limit, loop=loop)
    read_protocol = ZlibReaderProtocol(COMPRESSION, reader, loop=loop)
    write_protocol = asyncio.StreamReaderProtocol(reader, loop=loop)
    transport, _ = await loop.create_connection(lambda: read_protocol, host, port, **kwds)
    writer = asyncio.StreamWriter(transport, write_protocol, reader, loop)
    return reader, writer


def build_init_cmd(time_mode: str) -> str:
    """Builds the init command based on the environment variables provided in docker-compose"""
    initiation_command = f"{time_mode} username {USERNAME} password {APIKEY} useragent firestarter"
    if COMPRESSION != "":
        initiation_command += f" compression {COMPRESSION}"
    if KEEPALIVE != "":
        initiation_command += f" keepalive {KEEPALIVE}"
    if INIT_CMD_ARGS != "":
        initiation_command += f" {INIT_CMD_ARGS}"
    initiation_command += "\n"

    return initiation_command


def parse_script_args() -> None:
    """Sets global variables based on the environment variables provided in docker-compose"""
    # pylint: disable=global-statement
    # pylint: disable=line-too-long
    global USERNAME, APIKEY, SERVERNAME, COMPRESSION, STATS_PERIOD, KEEPALIVE, KEEPALIVE_STALE_PITRS, KEEPALIVE_PRODUCE, INIT_CMD_TIME, INIT_CMD_ARGS

    # **** REQUIRED ****
    USERNAME = os.environ["FH_USERNAME"]
    APIKEY = os.environ["FH_APIKEY"]
    # **** NOT REQUIRED ****
    SERVERNAME = os.environ.get("SERVER", "firehose-test.flightaware.com")
    COMPRESSION = os.environ.get("COMPRESSION", "")
    STATS_PERIOD = int(os.environ.get("PRINT_STATS_PERIOD", "10"))
    KEEPALIVE = int(os.environ.get("KEEPALIVE", "60"))
    KEEPALIVE_STALE_PITRS = int(os.environ.get("KEEPALIVE_STALE_PITRS", "5"))
    KEEPALIVE_PRODUCE = os.environ.get("KEEPALIVE_PRODUCE", "").lower() == "true"
    INIT_CMD_TIME = os.environ.get("INIT_CMD_TIME", "live")
    if os.environ.get("RESUME_FROM_LAST_PITR", "false").lower() == "true":
        if (resumption_pitr := get_last_pitr_produced()) :
            print(f"Resuming Firehose reading from PITR {resumption_pitr}")
            INIT_CMD_TIME = f"pitr {resumption_pitr}"
    if INIT_CMD_TIME.split()[0] not in ["live", "pitr"]:
        raise ValueError(f'$INIT_CMD_TIME value is invalid, should be "live" or "pitr <pitr>"')
    INIT_CMD_ARGS = os.environ.get("INIT_CMD_ARGS", "")
    for command in ["live", "pitr", "compression", "keepalive", "username", "password"]:
        if command in INIT_CMD_ARGS.split():
            raise ValueError(
                f'$INIT_CMD_ARGS should not contain the "{command}" command. '
                "It belongs in its own variable."
            )


def get_last_pitr_produced() -> Optional[int]:
    """See what the last PITR produced was to use for resuming from Firehose"""
    timeout = float(os.environ.get("RESUME_FROM_LAST_PITR_TIMEOUT", "3"))
    try:
        consumer = Consumer(
            {
                "bootstrap.servers": "kafka:9092",
                "enable.auto.commit": False,
                "group.id": "firestarter-connector-pitr-resumption",
            }
        )

        topic_of_interest = os.environ["KAFKA_TOPIC_NAME"]
        cluster_metadata = consumer.list_topics()
        topic_metadata = cluster_metadata.topics.get(topic_of_interest)
        if not topic_metadata:
            print(f"No data found for topic {topic_of_interest}")
            return

        pitr = None
        print(
            f"Looking for resumption PITR in {len(topic_metadata.partitions)} partition(s) in {topic_of_interest}"
        )
        for partition_id in topic_metadata.partitions:
            topic_partition = TopicPartition(topic_of_interest, partition_id)
            watermarks = consumer.get_watermark_offsets(topic_partition, timeout)

            if watermarks is None:
                print(f"Failed to get watermarks for partition {partition_id}")
                continue

            # The returned watermarks are [low, high) so decrement by 1
            # to get the last offset produced for the given topic
            low, high = watermarks
            high -= 1

            # Means there aren't any records in the partition
            if low >= high:
                print(f"No records in partition {partition_id}")
                continue

            consumer.assign([TopicPartition(topic_of_interest, partition_id, high)])
            last_record = consumer.poll(timeout)
            if last_record is None:
                print(f"Timed out reading offset {high} from partition {partition_id}")
                continue

            try:
                last_record_payload = last_record.value()
                last_record_dict = json.loads(last_record_payload)
            except json.JSONDecodeError as error:
                print(
                    f"Failed to decode JSON with payload '{last_record_payload}' from "
                    f"topic {topic_of_interest} in partition {partition_id} at offset {high}"
                )
                continue

            last_record_pitr = last_record_dict["pitr"]
            pitr = last_record_pitr if pitr is None else max(pitr, last_record_pitr)

        return pitr

    except (KafkaException, KafkaError, OSError) as error:
        print(f"Could not get resumption PITR: {error}")


async def event_wait(event: asyncio.Event, timeout: int) -> bool:
    """Wait for event with timeout, return True if event was set, False if we timed out

    This is intended to behave like threading.Event.wait"""
    try:
        return await asyncio.wait_for(event.wait(), timeout)
    except asyncio.TimeoutError:
        return event.is_set()


async def print_stats(period: int) -> None:
    """Periodically print information about how much data is flowing from Firehose."""
    # pylint: disable=global-statement
    global lines_read, bytes_read

    total_lines = 0
    total_bytes = 0
    initial_seconds = time.monotonic()
    last_seconds = initial_seconds
    first_pitr = None
    catchup_rate = 0
    while not finished.is_set():
        await event_wait(finished, period)
        now = time.monotonic()
        total_seconds = now - initial_seconds
        period_seconds = now - last_seconds
        if first_pitr:
            if total_seconds:
                catchup_rate = (int(last_good_pitr) - int(first_pitr)) / total_seconds
            else:
                catchup_rate = 0
        else:
            first_pitr = last_good_pitr
        last_seconds = now
        async with stats_lock:
            total_lines += lines_read
            total_bytes += bytes_read
            if period_seconds:
                print(
                    f"Period messages/s {lines_read / period_seconds:>5.0f}, "
                    f"period bytes/s {bytes_read / period_seconds:>5.0f}"
                )
            if total_seconds:
                print(
                    f"Total  messages/s {total_lines / total_seconds:>5.0f}, "
                    f"total  bytes/s {total_bytes / total_seconds:>5.0f}"
                )
            if catchup_rate:
                print(f"Total catchup rate: {catchup_rate:.2f}x")
            print(f"Total messages received: {total_lines}")
            print()
            lines_read = 0
            bytes_read = 0


async def read_firehose(time_mode: str) -> Optional[str]:
    """Open a connection to Firehose and read from it forever, passing all
    messages along to our kafka queues.

    Any errors will result in the function returning a string pitr value that
    can be passed to the function on a future call to allow for a reconnection
    without missing any data. The returned value may also be None, meaning that
    an error occurred before any pitr value was received from the server.

    time_mode may be either the string "live" or a pitr string that looks like
    "pitr <pitr>" where <pitr> is a value previously returned by this function
    """
    # pylint: disable=global-statement
    # pylint: disable=too-many-statements
    global last_good_pitr, lines_read, bytes_read, producer

    context = ssl.create_default_context()
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    try:
        fh_reader, fh_writer = await open_connection(host=SERVERNAME, port=1501, ssl=context)
    except (AttributeError, OSError) as error:
        print("Initial connection failed:", error)
        return None
    print(f"Opened connection to Firehose at {SERVERNAME}:1501")

    initiation_command = build_init_cmd(time_mode)
    print(initiation_command.strip())
    fh_writer.write(initiation_command.encode())
    await fh_writer.drain()

    pitr = None
    num_keepalives, last_good_keepalive_pitr = 0, 0
    while True:
        timeout = (KEEPALIVE + 10) if KEEPALIVE else None
        try:
            line = await asyncio.wait_for(fh_reader.readline(), timeout)
        except asyncio.TimeoutError:
            print(f"Server connection looks idle (haven't received anything in {timeout} seconds)")
            break
        except (AttributeError, OSError) as error:
            print("Lost server connection:", error)
            break
        if line == b"":
            print("Got EOF from Firehose server, connection intentionally closed")
            break
        message = json.loads(line)
        if message["type"] == "error":
            print(f'Error: {message["error_msg"]}')
            break

        if message["type"] == "keepalive":
            # if the pitr is the same as the last keepalive pitr, keep track of how long this is happening
            if last_good_keepalive_pitr == message["pitr"]:
                num_keepalives += 1
            else:
                num_keepalives = 0
            if num_keepalives >= KEEPALIVE_STALE_PITRS:
                break
            last_good_keepalive_pitr = message["pitr"]
        else:
            num_keepalives = 0

        last_good_pitr = pitr = message["pitr"]

        async with stats_lock:
            lines_read += 1
            bytes_read += len(line)

        def delivery_report(err, _):
            if err is not None:
                # All we can really do is report it
                print(f"Error when delivering message: {err}")

        # If it's a keepalive, move on since we don't want these messages in
        # Kafka
        if not KEEPALIVE_PRODUCE and message["type"] == "keepalive":
            continue

        # FIXME: This makes keepalives a bit useless if they won't be showing
        # up in order with any other messages
        key = message.get("id", "").encode() or None
        try:
            producer.produce(
                os.environ["KAFKA_TOPIC_NAME"],
                key=key,
                value=line,
                callback=delivery_report,
            )
        except BufferError as e:
            print(f"Encountered full outgoing buffer, should resolve itself: {e}")
            time.sleep(1)
        except KafkaException as e:
            err = e.args[0]
            # INVALID_REPLICATION_FACTOR occurs when Kafka broker is in transient state
            # and the partition count is still 0 so there's no leader. Wait to retry.
            if err.code != KafkaError.INVALID_REPLICATION_FACTOR and not err.retriable():
                print(f"Kafka exception occurred that cannot be retried: {err}")
                raise
            print(
                f"Encountered retriable kafka error ({err}), " "waiting a moment and trying again"
            )
            time.sleep(1)
        producer.poll(0)

    # We'll only reach this point if something's wrong with the connection.
    producer.flush()
    return pitr


async def main():
    """Connect to Firehose and write the output to kafka"""
    # pylint: disable=global-statement
    global producer, stats_lock, finished, last_good_pitr

    producer = None
    while producer is None:
        try:
            producer = Producer({"bootstrap.servers": "kafka:9092", "linger.ms": 500})
            producer.produce(
                "test",
                key="noop",
                value="",
            )
        except KafkaException as error:
            producer = None
            print(f"Kafka isn't available ({error}), trying again in a few seconds")
            time.sleep(3)

    stats_lock = asyncio.Lock()
    finished = asyncio.Event()
    last_good_pitr = None

    parse_script_args()

    stats_task = None
    if STATS_PERIOD:
        stats_task = asyncio.create_task(print_stats(STATS_PERIOD))
    errors = 0
    time_mode = INIT_CMD_TIME
    while True:
        pitr = await read_firehose(time_mode)
        if pitr:
            time_mode = f"pitr {pitr}"
            print(f'Reconnecting with "{time_mode}"')
            errors = 0
        elif errors < CONNECTION_ERROR_LIMIT - 1:
            print(f'Previous connection never got a pitr, trying again with "{time_mode}"')
            errors += 1
        else:
            print(
                f"Connection failed {CONNECTION_ERROR_LIMIT} "
                "times before getting a non-error message, quitting"
            )
            break

    if stats_task:
        print("Dumping stats one last time...")
        finished.set()
        await stats_task


if __name__ == "__main__":
    asyncio.run(main())
