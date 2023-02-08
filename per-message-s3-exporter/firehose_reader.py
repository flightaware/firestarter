"""Contains the code for an async Firehose reader
"""

import asyncio
from datetime import datetime
import json
import logging
import os
from pathlib import Path
import ssl
import time
import warnings
import zlib
from typing import Dict, List, Optional, Tuple

import attr

FIREHOSE_MESSAGE_TYPES: List[str] = [
    "arrival",
    "cancellation",
    "departure",
    "error",
    "extendedFlightInfo",
    "flifo",
    "flightplan",
    "fmswx",
    "ground_position",
    "keepalive",
    "location_entry",
    "location_exit",
    "near_surface_position",
    "surface_offblock",
    "surface_onblock",
    "power_on",
    "position",
    "vehicle_position",
]


class ReadFirehoseErrorThreshold(Exception):
    """Raised when Firehose reading reaches a threshold"""


async def event_wait(event: asyncio.Event, timeout: int) -> bool:
    """Wait for event with timeout, return True if event was set, False if we timed out

    This is intended to behave like threading.Event.wait"""
    try:
        return await asyncio.wait_for(event.wait(), timeout)
    except asyncio.TimeoutError:
        return event.is_set()


def pitr_map_location() -> Path:
    """Grab the PITR map from the PITR_MAP environment variable with a default
    useful for running this in Docker"""
    return Path(os.getenv("PITR_MAP", "/home/firestarter/pitrs/.pitrs-map"))


def pitr_map_from_file(init_time: str) -> Dict[str, int]:
    """Load PITR map from file, filling in a PITR value based on the init_time
    when appropriate

    Returns an empty dict if init_time is from live or no PITR map is found on
    disk"""
    if init_time == "live":
        logging.info("Starting Firehose stream from live")
        return {}

    pitr_map_path = pitr_map_location()
    if not pitr_map_path.is_file():
        logging.info(f"No PITR map on disk at {pitr_map_path}")
        return {}

    start_pitr_from_env = int(init_time.split()[-1])
    pitr_map = {message_type: start_pitr_from_env for message_type in FIREHOSE_MESSAGE_TYPES}

    logging.info(f"Fetching start PITRs from {pitr_map_path}")
    with pitr_map_path.open(encoding="utf-8") as pitr_map_file:
        try:
            pitr_map.update(json.loads(pitr_map_file.read()))
        except json.decoder.JSONDecodeError:
            logging.warning("PITR map file is empty or corrupted")

    return pitr_map


# pylint: disable=too-many-instance-attributes
@attr.s(kw_only=True)
class FirehoseConfig:
    """Contains the configuration necessary for connecting to Firehose"""

    server: str = attr.ib()
    username: str = attr.ib()
    password: str = attr.ib()
    compression: str = attr.ib()
    keepalive: int = attr.ib()
    keepalive_stale_pitrs: int = attr.ib()
    init_time: str = attr.ib()
    init_args: str = attr.ib()
    pitr_map: Dict[str, int] = attr.ib()

    @classmethod
    def from_env(cls):
        """Create a FirehoseConfig object based on environment variables"""
        username = os.environ["FH_USERNAME"]
        apikey = os.environ["FH_APIKEY"]
        server = os.environ.get("SERVER", "firehose-test.flightaware.com")
        compression = os.environ.get("COMPRESSION", "")
        keepalive = int(os.environ.get("KEEPALIVE", "60"))
        keepalive_stale_pitrs = int(os.environ.get("KEEPALIVE_STALE_PITRS", "5"))
        init_time = os.environ.get("INIT_CMD_TIME", "live")

        if init_time.split()[0] not in ["live", "pitr"]:
            raise ValueError('$INIT_CMD_TIME value is invalid, should be "live" or "pitr <pitr>"')

        pitr_map = pitr_map_from_file(init_time)
        if pitr_map:
            min_pitr = min(pitr_map.values())
            logging.info(f"Using {min_pitr} as starting PITR value")
            init_time = f"pitr {min_pitr}"

        init_args = os.environ.get("INIT_CMD_ARGS", "")
        for command in ["live", "pitr", "compression", "keepalive", "username", "password"]:
            if command in init_args.split():
                raise ValueError(
                    f'$INIT_CMD_ARGS should not contain the "{command}" command. '
                    "It belongs in its own variable."
                )

        return cls(
            username=username,
            password=apikey,
            server=server,
            compression=compression,
            keepalive=keepalive,
            keepalive_stale_pitrs=keepalive_stale_pitrs,
            init_time=init_time,
            init_args=init_args,
            pitr_map=pitr_map,
        )

    def init_command(self, time_mode: str) -> str:
        """Builds the init command based on the time_mode passed in and the
        configuration contained in the object"""
        initiation_command = (
            f"{time_mode} username {self.username} password {self.password} useragent firestarter"
        )
        if self.compression != "":
            initiation_command += f" compression {self.compression}"
        if self.keepalive:
            initiation_command += f" keepalive {self.keepalive}"
        if self.init_args:
            initiation_command += f" {self.init_args}"
        initiation_command += "\n"

        return initiation_command

    def pitr_in_past(self, message_type: str, pitr: int) -> bool:
        """Whether the PITR for a message type is up-to-date or not based on
        the PITR map"""
        return pitr < self.pitr_map.get(message_type, 0)


@attr.s(kw_only=True)
class FirehoseStats:
    """FirehoseStats is used to periodically logging.info stats about lines read from
    Firehose"""

    _stats_lock: asyncio.Lock = attr.ib(init=False, factory=asyncio.Lock)
    _finished: asyncio.Event = attr.ib(init=False, factory=asyncio.Event)

    _last_good_pitr: Optional[int] = attr.ib(init=False)
    _lines_read: int = attr.ib(init=False, factory=int)
    _bytes_read: int = attr.ib(init=False, factory=int)

    def finish(self):
        """Mark the stats logging.infoing as finished"""
        self._finished.set()

    async def update_stats(self, pitr: Optional[int], bytes_read: int):
        """Update the stats based on the values passed in"""
        async with self._stats_lock:
            self._last_good_pitr = pitr
            if pitr is not None:
                self._bytes_read += bytes_read
                self._lines_read += 1

    async def print_stats(self, period: int) -> None:
        """Periodically logging.info information about how much data is flowing from Firehose."""
        total_lines = 0
        total_bytes = 0

        initial_seconds = time.monotonic()
        last_seconds = initial_seconds

        first_pitr = None
        catchup_rate = 0

        while not self._finished.is_set():
            await event_wait(self._finished, period)

            now = time.monotonic()
            total_seconds = now - initial_seconds
            period_seconds = now - last_seconds

            if first_pitr:
                if total_seconds:
                    catchup_rate = (int(self._last_good_pitr) - int(first_pitr)) / total_seconds
                else:
                    catchup_rate = 0
            else:
                first_pitr = self._last_good_pitr

            last_seconds = now

            async with self._stats_lock:
                total_lines += self._lines_read
                total_bytes += self._bytes_read
                if period_seconds:
                    logging.info(
                        f"Period messages/s {self._lines_read / period_seconds:>5.0f}, "
                        f"period bytes/s {self._bytes_read / period_seconds:>5.0f}"
                    )
                if total_seconds:
                    logging.info(
                        f"Total  messages/s {total_lines / total_seconds:>5.0f}, "
                        f"total  bytes/s {total_bytes / total_seconds:>5.0f}"
                    )

                if catchup_rate:
                    logging.info(f"Total catchup rate: {catchup_rate:.2f}x")

                logging.info(f"Total messages received: {total_lines:,}")

                last_pitr_ts = datetime.fromtimestamp(self._last_good_pitr).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                logging.info(f"Last good PITR: {self._last_good_pitr} ({last_pitr_ts})")

                self._lines_read = 0
                self._bytes_read = 0


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


@attr.s(kw_only=True)
class AsyncFirehoseReader:
    """Async reader of Firehose messages
    Every message read gets put onto an async Queue dedicated to a specific
    message type"""

    config: FirehoseConfig = attr.ib()
    message_queues: Dict[str, asyncio.Queue] = attr.ib()

    _stats: FirehoseStats = attr.ib(init=False, factory=FirehoseStats)

    @property
    def stats_period(self) -> int:
        """How often in seconds to logging.info stats"""
        return int(os.environ.get("PRINT_STATS_PERIOD", "10"))

    @property
    def connection_error_limit(self) -> int:
        """How many Firehose read errors before stopping"""
        return int(os.environ.get("CONNECTION_ERROR_LIMIT", "3"))

    async def read_firehose(self):
        """Read Firehose until a threshold number of errors occurs"""
        await self._stats.update_stats(None, 0)
        stats_task = asyncio.create_task(self._stats.print_stats(self.stats_period))

        error_limit = self.connection_error_limit
        errors = 0

        time_mode = self.config.init_time

        while True:
            pitr = await self._read_until_error(time_mode)
            if pitr:
                time_mode = f"pitr {pitr}"
                logging.info(f'Reconnecting with "{time_mode}"')
                errors = 0
            elif errors < error_limit - 1:
                logging.info(
                    f'Previous connection never got a pitr, trying again with "{time_mode}"'
                )
                errors += 1
            else:
                logging.info(
                    f"Connection failed {error_limit} "
                    "times before getting a non-error message, quitting"
                )
                break

        logging.info("Dumping stats one last time...")
        self._stats.finish()
        await asyncio.wait_for(stats_task, self.stats_period)

        raise ReadFirehoseErrorThreshold

    async def _open_connection(
        self, port: int = None, *, loop=None, limit=2**16, **kwds
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

        read_protocol = ZlibReaderProtocol(self.config.compression, reader, loop=loop)
        write_protocol = asyncio.StreamReaderProtocol(reader, loop=loop)

        transport, _ = await loop.create_connection(
            lambda: read_protocol, self.config.server, port, **kwds
        )
        writer = asyncio.StreamWriter(transport, write_protocol, reader, loop)

        return reader, writer

    async def _read_until_error(self, time_mode: str) -> Optional[str]:
        """Open a connection to Firehose and read from it until the messages
        run out, passing all lines along to the asyncio Queue in the records_queue.

        Any errors will result in the function returning a string pitr value that
        can be passed to the function on a future call to allow for a reconnection
        without missing any data. The returned value may also be None, meaning that
        an error occurred before any pitr value was received from the server.

        time_mode may be either the string "live" or a pitr string that looks like
        "pitr <pitr>" where <pitr> is a value previously returned by this function
        """

        context = ssl.create_default_context()
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        try:
            fh_reader, fh_writer = await self._open_connection(port=1501, ssl=context)
        except (AttributeError, OSError) as error:
            logging.error(f"Initial connection failed: {error}")
            return None
        logging.info(f"Opened connection to Firehose at {self.config.server}:1501")

        initiation_command = self.config.init_command(time_mode)
        logging.info(initiation_command.strip())

        fh_writer.write(initiation_command.encode())
        await fh_writer.drain()

        pitr = None
        num_keepalives, last_good_keepalive_pitr = 0, 0
        while True:
            timeout = (self.config.keepalive + 10) if self.config.keepalive else None
            try:
                line = await asyncio.wait_for(fh_reader.readline(), timeout)
            except asyncio.TimeoutError:
                logging.error(
                    f"Server connection looks idle (haven't received anything in {timeout} seconds)"
                )
                break
            except (AttributeError, OSError) as error:
                logging.error(f"Lost server connection: {error}")
                break

            if line == b"":
                logging.error("Got EOF from Firehose server, connection intentionally closed")
                break

            message = json.loads(line)
            if message["type"] == "error":
                logging.error(f'Error: {message["error_msg"]}')
                break

            if message["type"] == "keepalive":
                # if the pitr is the same as the last keepalive pitr,
                # keep track of how long this is happening
                if last_good_keepalive_pitr == message["pitr"]:
                    num_keepalives += 1
                else:
                    num_keepalives = 0
                if num_keepalives >= self.config.keepalive_stale_pitrs:
                    break
                last_good_keepalive_pitr = message["pitr"]
            else:
                num_keepalives = 0

            pitr = int(message["pitr"])
            await self._stats.update_stats(pitr, len(line))

            # Do not pass along keepalives to the queue or messages with a PITR
            # in the past
            is_keepalive = message["type"] == "keepalive"
            pitr_in_past = self.config.pitr_in_past(message["type"], pitr)
            if is_keepalive or pitr_in_past:
                continue

            await self.message_queues[message["type"]].put(line)

        return pitr
