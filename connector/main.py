import asyncio
import json
import os
import ssl
import time
import warnings
import zlib


class ZlibReaderProtocol(asyncio.StreamReaderProtocol):
    """asyncio Protocol that handles streaming decompression of Firehose data"""

    def __init__(self, mode, *args, **kwargs):
        if mode == "deflate":  # no header, raw deflate stream
            self._z = zlib.decompressobj(-zlib.MAX_WBITS)
        elif mode == "compress":  # zlib header
            self._z = zlib.decompressobj(zlib.MAX_WBITS)
        elif mode == "gzip":  # gzip header
            self._z = zlib.decompressobj(16 | zlib.MAX_WBITS)
        else:
            self._z = None
        super().__init__(*args, **kwargs)

    def data_received(self, data):
        if not self._z:
            super().data_received(data)
        else:
            super().data_received(self._z.decompress(data))


async def open_connection(host=None, port=None, *, loop=None, limit=2 ** 16, **kwds):
    """Coroutine copied from asyncio source with a tweak to use our custom ZlibReadProtocol for the returned StreamReader
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
    read_protocol = ZlibReaderProtocol(compression, reader, loop=loop)
    write_protocol = asyncio.StreamReaderProtocol(reader, loop=loop)
    transport, _ = await loop.create_connection(
        lambda: read_protocol, host, port, **kwds
    )
    writer = asyncio.StreamWriter(transport, write_protocol, reader, loop)
    return reader, writer


def build_init_cmd(time_mode):
    initiation_command = f"{time_mode} username {username} password {apikey}"
    if compression != "":
        initiation_command += f" compression {compression}"
    if keepalive != "":
        initiation_command += f" keepalive {keepalive}"
    if init_cmd_args != "":
        initiation_command += f" {init_cmd_args}"
    initiation_command += "\n"

    return initiation_command


writers_lock = None
writers = []
stats_lock = None
start_event = None
finished = None
lines_read = 0
bytes_read = 0


def parse_script_args():
    global username, apikey, servername, compression, stats_period, keepalive, init_cmd_time, init_cmd_args, start_immediately

    # **** REQUIRED ****
    username = os.getenv("FH_USERNAME")
    apikey = os.getenv("FH_APIKEY")
    # **** NOT REQUIRED ****
    servername = os.getenv("SERVER")
    compression = os.getenv("COMPRESSION")
    stats_period = int(os.getenv("PRINT_STATS_PERIOD"))
    keepalive = int(os.getenv("KEEPALIVE"))
    init_cmd_time = os.getenv("INIT_CMD_TIME")
    if init_cmd_time.split()[0] not in ["live", "pitr"]:
        raise ValueError(
            f'$INIT_CMD_TIME value is invalid, should be "live" or "pitr <pitr>"'
        )
    init_cmd_args = os.getenv("INIT_CMD_ARGS")
    for command in ["live", "pitr", "compression", "keepalive", "username", "password"]:
        if command in init_cmd_args.split():
            raise ValueError(
                f'$INIT_CMD_ARGS should not contain the "{command}" command. It belongs in its own variable.'
            )
    start_immediately = int(os.getenv("START_IMMEDIATELY"))


async def event_wait(event, timeout):
    """Wait for event with timeout, return True if event was set, False if we timed out

    This is intended to behave like threading.Event.wait"""
    try:
        return await asyncio.wait_for(event.wait(), timeout)
    except asyncio.TimeoutError:
        return event.is_set()


async def print_stats(period):
    """Periodically print information about how much data is flowing from Firehose."""
    global lines_read, bytes_read, finished, last_good_pitr
    await start_event.wait()
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
                    f"Period messages/s {lines_read / period_seconds:>5.0f}, period bytes/s {bytes_read / period_seconds:>5.0f}"
                )
            if total_seconds:
                print(
                    f"Total  messages/s {total_lines / total_seconds:>5.0f}, total  bytes/s {total_bytes / total_seconds:>5.0f}"
                )
            if catchup_rate:
                print(f"Total catchup rate: {catchup_rate:.2f}x")
            print(f"Total messages received: {total_lines}")
            print()
            lines_read = 0
            bytes_read = 0


async def read_firehose(time_mode):
    """Open a connection to Firehose and read from it forever, passing all messages along to all connected clients.

    Any errors will result in the function returning a string pitr value that
    can be passed to the function on a future call to allow for a reconnection
    without missing any data. The returned value may also be None, meaning that
    an error occurred before any pitr value was received from the server.

    time_mode may be either the string "live" or a pitr string that looks like
    "pitr <pitr>" where <pitr> is a value previously returned by this function
    """
    global lines_read, bytes_read, last_good_pitr
    context = ssl.create_default_context()
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    fh_reader, fh_writer = await open_connection(host=servername, port=1501, ssl=context)
    print(f"Opened connection to Firehose at {servername}:1501")

    initiation_command = build_init_cmd(time_mode)
    print(initiation_command.strip())
    fh_writer.write(initiation_command.encode())
    await fh_writer.drain()

    pitr = None
    while True:
        timeout = (keepalive + 10) if keepalive else None
        try:
            line = await asyncio.wait_for(fh_reader.readline(), timeout)
        except asyncio.TimeoutError:
            print(
                f"Server connection looks idle (haven't received anything in {timeout} seconds)"
            )
            break
        except (AttributeError, OSError) as e:
            print("Lost server connection:", e)
            break
        if line == b"":
            print("Got EOF from Firehose server, connection intentionally closed")
            break
        message = json.loads(line)
        if message["type"] == "error":
            print(f'Error: {message["error_msg"]}')
            break

        last_good_pitr = pitr = message["pitr"]

        async with stats_lock:
            lines_read += 1
            bytes_read += len(line)

        async with writers_lock:
            for writer in writers:
                try:
                    writer.write(line)
                    await writer.drain()
                except OSError as e:
                    writers.remove(writer)
                    client = writer.transport.get_extra_info("peername")
                    print(f"Lost client connection with {client}:", e)

    # We'll only reach this point if something's wrong with the connection.
    return pitr


async def client_connected(reader, writer):
    client = writer.transport.get_extra_info("peername")
    print("Got client connection from", client)
    async with writers_lock:
        writers.append(writer)
    start_event.set()


async def main():
    global writers_lock, stats_lock, start_event, finished, last_good_pitr
    writers_lock = asyncio.Lock()
    stats_lock = asyncio.Lock()
    start_event = asyncio.Event()
    finished = asyncio.Event()
    last_good_pitr = None
    LISTEN_PORT = 1601

    parse_script_args()
    server = await asyncio.start_server(client_connected, port=LISTEN_PORT)
    print(f"Server is listening on all interfaces at port {LISTEN_PORT}")

    stats_task = None
    if stats_period:
        stats_task = asyncio.create_task(print_stats(stats_period))
    CONNECTION_ERROR_LIMIT = 3
    errors = 0
    time_mode = init_cmd_time
    while True:
        if start_immediately or start_event.is_set():
            start_event.set()
        else:
            print("Waiting for first client connection to start Firehose connection")
            await start_event.wait()
        print()
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
                f"Connection failed {CONNECTION_ERROR_LIMIT} times before getting a non-error message, quitting"
            )
            break
    for writer in writers:
        if writer.can_write_eof():
            writer.write_eof()
        print(f"Closing connection to {writer.transport.get_extra_info('peername')}")
        writer.close()
        await writer.wait_closed()
    if stats_task:
        print("Dumping stats one last time...")
        finished.set()
        await stats_task


if __name__ == "__main__":
    asyncio.run(main())
