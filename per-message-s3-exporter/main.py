"""The S3-Exporter reads records from Firehose and writes them to flat files in
S3 where each file contains only a single Firehose message type
"""

import argparse as ap
import asyncio
import bz2
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import partial
import json
import logging
import os
from pathlib import Path
from signal import Signals, SIGINT, SIGTERM
import sys
from typing import Dict, List, Optional, Tuple

import aiofiles
import attr
import boto3
from codetiming import Timer
from dotenv import load_dotenv
from humanfriendly import format_size, parse_size
from setproctitle import setproctitle

from firehose_reader import (
    AsyncFirehoseReader,
    FirehoseConfig,
    FIREHOSE_MESSAGE_TYPES,
    pitr_map_location,
)


def log_levels() -> Tuple[str, ...]:
    """Tuple of available logging levels"""
    return ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")


def setup_logging(args: ap.Namespace) -> None:
    """Setup logging format and level"""
    log_format = "%(levelname)8s: (%(funcName)s) %(message)s"

    logging.basicConfig(
        level=args.log_level,
        format=log_format,
    )

    # Make other loggers quiet
    for _ in ("boto", "asyncio"):
        logging.getLogger(_).setLevel(logging.INFO)


def parse_args() -> ap.Namespace:
    """Parse command-line arguments, using environment variables to override
    defaults"""
    parser = ap.ArgumentParser(formatter_class=ap.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        "--s3-bucket", default=os.getenv("S3_BUCKET", ""), help="S3 bucket to write files into"
    )
    parser.add_argument(
        "--s3-bucket-folder",
        default=os.getenv("S3_BUCKET_FOLDER", ""),
        help="S3 folder to use as prefix",
    )
    parser.add_argument(
        "--asyncio-queue-max-size",
        default=int(os.getenv("ASYNCIO_QUEUE_MAX_SIZE", "0")),
        help="Maximum size of the asyncio Queues used",
    )
    parser.add_argument(
        "--batch-strategy",
        choices=("records", "bytes", "both"),
        default=os.getenv("BATCH_STRATEGY", "bytes").lower(),
        help="Whether to batch S3 files based on a threshold number of "
        "records, bytes, or both (where whichever threshold is hit "
        "first is the one that wins)",
    )
    parser.add_argument(
        "--records-per-file",
        default=int(os.getenv("RECORDS_PER_FILE", "15000")),
        help="Threshold number of records in a file before writing to S3",
    )
    parser.add_argument(
        "--records-per-file-common-message-types",
        default=int(os.getenv("RECORDS_PER_FILE_COMMON_MESSAGE_TYPES", "300000")),
        help="Threshold number of records in a file for common message types (if specified)",
    )
    parser.add_argument(
        "--bytes-per-file",
        default=parse_size(os.getenv("BYTES_PER_FILE", "8MB"), binary=True),
        help="Threshold number of uncompressed bytes in a file before writing to S3",
    )
    parser.add_argument(
        "--bytes-per-file-common-message-types",
        default=parse_size(os.getenv("BYTES_PER_FILE_COMMON_MESSAGE_TYPES", "128MB"), binary=True),
        help="Threshold number of uncompressed bytes for common message types (if specified)",
    )
    parser.add_argument(
        "--common-message-types",
        default=os.getenv("COMMON_MESSAGE_TYPES", ""),
        help="Most voluminous message types (have separate byte and record threshold)",
    )
    parser.add_argument(
        "--compression-type",
        choices=("none", "bzip"),
        default=os.getenv("COMPRESSION_TYPE", "none").lower(),
        help="Compression to use for files uploaded to S3",
    )
    parser.add_argument(
        "--log-level",
        choices=log_levels(),
        default=os.getenv("LOG_LEVEL", "INFO").upper(),
        help="Possible log levels",
    )
    parser.add_argument(
        "--pitr-map",
        default=pitr_map_location(),
        help="Path to the PITR map for resuming Firehose reading",
    )

    return parser.parse_args()


def setup_signal_handlers(loop: asyncio.events.AbstractEventLoop) -> None:
    """Exit immediately on SIGINT or SIGTERM"""
    for signal_name in (SIGINT, SIGTERM):
        loop.add_signal_handler(
            signal_name,
            partial(signal_exit, signal_name),
        )


def signal_exit(signal: Signals) -> None:
    """Log the signal received and exit the program"""
    logging.info(f"Exiting from a {signal.name}")
    sys.exit()


# pylint: disable=too-few-public-methods
@attr.s(kw_only=True, frozen=True)
class S3WriteObject:
    """Class used by the S3FileBatcher class to store data needed for putting
    Firehose data into S3"""

    key: str = attr.ib()
    body: bytes = attr.ib()
    message_type: str = attr.ib()
    end_pitr: int = attr.ib()


# pylint: disable=too-many-instance-attributes
@attr.s(kw_only=True)
class S3FileBatcher:
    """Class used to batch together records from Firehose and write them to a
    queue along with some metadata. Each instance of this class is assigned a
    single Firehose message type.

    The consumer of the queue then writes these batches to S3.

    A batch is written to the `s3_writer_queue` if a threshold number of
    Firehose messages or a threshold number of bytes has been reached or
    exceeded in the current batch's uncompressed format.
    """

    args: ap.Namespace = attr.ib()
    message_type: str = attr.ib()
    s3_writer_queue: asyncio.Queue = attr.ib()

    _current_batch: list = attr.ib(init=False, factory=list)
    _current_batch_bytes: int = attr.ib(init=False, factory=int)
    # starting and ending PITR values for the current batch
    _start_pitr: int = attr.ib(init=False, factory=int)
    _end_pitr: int = attr.ib(init=False)

    _timer: Timer = attr.ib(
        init=False,
        default=attr.Factory(
            lambda self: Timer(
                name=f"s3_batcher_{self.message_type}",
                text="{name}: Built a batch of Firehose messages in {seconds:.2f} s",
                logger=logging.info,
            ),
            takes_self=True,
        ),
    )

    @property
    def common_message_type(self) -> bool:
        """Whether we are dealing with a common message type, i.e., a more
        voluminous message type that can have its own byte and record
        threshold"""
        return self.message_type in self.args.common_message_types

    @property
    def records_per_file(self) -> int:
        """Max number of Firehose messages in an S3 file"""
        if self.common_message_type:
            return self.args.records_per_file_common_message_types

        return self.args.records_per_file

    @property
    def bytes_per_file(self) -> int:
        """Max number of bytes in an S3 file"""
        if self.common_message_type:
            return self.args.bytes_per_file_common_message_types

        return self.args.bytes_per_file

    @property
    def compression_type(self) -> str:
        """Compression type to use (if any) for the S3 files"""
        return self.args.compression_type

    @property
    def batch_length(self) -> int:
        """Length of the current batch"""
        return len(self._current_batch)

    @property
    def batch_strategy(self) -> str:
        """Which batch strategy to use"""
        return self.args.batch_strategy

    @property
    def batch_bytes(self) -> str:
        """Humanify friendly str for the number of bytes in the current
        batch"""
        return format_size(self._current_batch_bytes, binary=True)

    @property
    def records_hit(self) -> bool:
        """Whether the current batch exceeds the records threshold"""
        return self.batch_length >= self.records_per_file

    @property
    def bytes_hit(self) -> bool:
        """Whether the current batch exceeds the bytes threshold taking into
        account more voluminous message types"""
        return self._current_batch_bytes >= self.bytes_per_file

    @property
    def folder_prefix(self) -> str:
        """S3 bucket folder prefix to add to front of key"""
        if not self.args.s3_bucket_folder:
            return ""

        return f"{self.args.s3_bucket_folder}/"

    def record_pitr(self, record: Optional[str]) -> int:
        """Return the PITR for a Firehose message"""
        if record is None:
            record = self._current_batch[-1]
        return int(json.loads(record)["pitr"])

    async def ingest_record(self, record: Optional[str]):
        """Ingest a record from Firehose, adding it to the current batch and
        writing a file to the S3 writer queue if necessary
        """
        # An empty line is a signal that we need to shutdown
        shutdown = record is None

        # Even though we might exceed the max bytes, that's okay since it's
        # only a rough threshold that we strive to maintain here and the number
        # of records is more strictly adhered to
        if not shutdown:
            if not self._current_batch:
                self._start_pitr = self.record_pitr(record)
                self._timer.start()

            self._current_batch.append(record)
            self._current_batch_bytes += len(record)

        if (shutdown and self.batch_length > 0) or self.should_write_batch_to_file():
            self._end_pitr = self.record_pitr(record)
            await self.enqueue_batch_contents()

            self._timer.stop()
            logging.info(f"Current batch has {self.batch_length:,} records with {self.batch_bytes}")

            self._current_batch = []
            self._current_batch_bytes = 0

        # Propagate shutdown signal
        if shutdown:
            logging.info(f"Shutting down {self.message_type} queue: sending signal to S3 writer")
            await self.s3_writer_queue.put(None)

    def should_write_batch_to_file(self) -> bool:
        """Whether the current batch needs to be written to an S3 file
        In order to see less common message types, the bytes hit will be
        significantly lower than for positions
        """
        if self.batch_strategy == "records":
            return self.records_hit
        if self.batch_strategy == "bytes":
            return self.bytes_hit

        return self.records_hit or self.bytes_hit

    async def enqueue_batch_contents(self):
        """Write the current batch of records to the S3 writer's queue"""
        if self.batch_length == 0:
            logging.warning(f"Current batch for {self.message_type} is empty, skipping")
            return

        filename = self.batch_filename()

        file_contents = b"".join(self._current_batch)
        if self.compression_type == "bzip":
            file_contents = bz2.compress(file_contents)

        s3_object = S3WriteObject(
            key=filename,
            body=file_contents,
            message_type=self.message_type,
            end_pitr=self._end_pitr,
        )

        logging.info(f"Writing a batch to the S3 writer for {self.message_type}")
        await self.s3_writer_queue.put(s3_object)

    def _s3_bucket_folder(self) -> str:
        """YYYY/MM/DD folder name to write batch of messages into"""
        date_format = datetime.now().strftime("year=%Y/month=%m/day=%d")
        return f"{self.folder_prefix}{self.message_type}/{date_format}"

    def _s3_file_extension(self) -> str:
        """File extension to use for uploads to S3"""
        if self.compression_type == "none":
            return ""

        return ".bz2"

    def batch_filename(self) -> str:
        """Filename to use in S3 for writing out the Firehose messages"""
        folder = self._s3_bucket_folder()
        start_pitr = self._start_pitr
        end_pitr = self._end_pitr
        extension = self._s3_file_extension()

        return f"{folder}/{start_pitr}_{end_pitr}{extension}"


async def build_batch_of_records_from_firehose(
    args: ap.Namespace,
    message_type: str,
    firehose_queue: asyncio.Queue,
    s3_writer_queue: asyncio.Queue,
) -> None:
    """Read Firehose records from an async queue, building up batches of them to
    write to disk before they make their way into S3"""
    batcher = S3FileBatcher(
        args=args,
        message_type=message_type,
        s3_writer_queue=s3_writer_queue,
    )
    while True:
        # Use a "blocking" await on the queue with Firehose messages which will
        # wait indefinitely until data shows up in the queue
        firehose_message: Optional[str] = await firehose_queue.get()

        await batcher.ingest_record(firehose_message)
        firehose_queue.task_done()

        # We've reached the end of the PITR range and need to shutdown
        if firehose_message is None:
            break


async def load_pitr_map(pitr_map_path: Path) -> Dict[str, int]:
    """Load the PITR map from disk if available. Returns an empty dict if
    nothing can be found"""
    if not pitr_map_path.is_file():
        return {}

    async with aiofiles.open(pitr_map_path) as pitr_map_file:
        try:
            return json.loads(await pitr_map_file.read())
        except json.decoder.JSONDecodeError:
            return {}


async def write_files_to_s3(
    args: ap.Namespace,
    executor: ThreadPoolExecutor,
    s3_queue: asyncio.Queue,
):
    """Reads file contenst from the s3_queue and writes them to the s3 bucket
    specified in args. When successful, writes a new PITR map to disk with the
    latest value for the message type written"""
    loop = asyncio.get_running_loop()

    # Using the standard configuration options, e.g., 3 retries
    # Gets all configuration options from environment variables
    # We create a separate client for each coroutine since it'll
    # be running behind the scenes in a threadpool and clients are
    # threadsafe according to the boto3 docs
    session = boto3.session.Session()
    s3_client = session.client("s3")

    # Load the PITR map from disk so we can start writing it
    pitr_map: Dict[str, int] = await load_pitr_map(args.pitr_map)
    pitr_map = {message_type: int(pitr) for message_type, pitr in pitr_map.items()}

    # Keep track of how many shutdown signals we receive for the case where
    # we're only ingesting a range of values and not processing files
    # indefinitely
    total_shutdown_signals = len(FIREHOSE_MESSAGE_TYPES)
    shutdown_signals_recvd = 0

    while shutdown_signals_recvd < total_shutdown_signals:
        s3_write_object: Optional[S3WriteObject] = await s3_queue.get()

        # Check for a shutdown signal
        if s3_write_object is None:
            shutdown_signals_recvd += 1
            continue

        # Get some timing stats on how long it takes to write to S3
        timer = Timer(
            text=f"Wrote {s3_write_object.key} to S3 in {{seconds:.2f}} s",
            logger=logging.info,
        )

        # Write to S3 using the ThreadPoolExecutor since boto3 isn't async
        logging.info(f"Attempting to write {s3_write_object.key} to S3 bucket {args.s3_bucket}...")

        timer.start()
        partial_s3_put = partial(
            s3_client.put_object,
            Bucket=args.s3_bucket,
            Key=s3_write_object.key,
            Body=s3_write_object.body,
        )
        await loop.run_in_executor(executor, partial_s3_put)

        timer.stop()
        s3_queue.task_done()

        # When a file has been successfully written to S3, we can update the
        # PITR map on disk with the latest values
        pitr_map[s3_write_object.message_type] = s3_write_object.end_pitr
        async with aiofiles.open(args.pitr_map, "w") as pitr_map_file:
            await pitr_map_file.write(json.dumps(pitr_map))
            logging.info("Saved PITR map to disk")


async def main(args: ap.Namespace):
    """Setup all the tasks needed to read from Firehose and write to S3"""
    # Setup the queues we're gonna need
    max_queue_size = args.asyncio_queue_max_size
    total_queues = len(FIREHOSE_MESSAGE_TYPES)

    firehose_message_queues: List[asyncio.Queue] = [
        asyncio.Queue(max_queue_size) for _ in range(total_queues)
    ]
    s3_writer_queue: asyncio.Queue = asyncio.Queue(max_queue_size)

    # Need this to run boto3 operations
    executor = ThreadPoolExecutor()

    # Have a single task to read messages from Firehose
    firehose_config = FirehoseConfig.from_env()
    firehose_reader = AsyncFirehoseReader(
        config=firehose_config,
        message_queues={
            message_type: firehose_message_queues[i]
            for i, message_type in enumerate(FIREHOSE_MESSAGE_TYPES)
        },
    )

    tasks: List[asyncio.Task] = [firehose_reader.read_firehose()]
    for i, message_type in enumerate(FIREHOSE_MESSAGE_TYPES):
        # Each message type gets its own S3 file batcher
        batcher_coro = build_batch_of_records_from_firehose(
            args, message_type, firehose_message_queues[i], s3_writer_queue
        )
        tasks.append(asyncio.create_task(batcher_coro))

    # Use a single S3 file writer for all message types
    tasks.append(write_files_to_s3(args, executor, s3_writer_queue))

    # Run all the tasks in the event loop to completion
    await asyncio.gather(*tasks, return_exceptions=False)


def batch_logging_message(args: ap.Namespace, common: bool = False) -> str:
    """Generate a suitable logging message about the batch strategy and size"""
    if not common:
        records_msg = f"{args.records_per_file:,}"
        bytes_msg = format_size(ARGS.bytes_per_file, binary=True)
    else:
        records_msg = f"{args.records_per_file_common_message_types:,}"
        bytes_msg = format_size(ARGS.bytes_per_file_common_message_types, binary=True)

    if args.batch_strategy == "bytes":
        return bytes_msg

    if args.batch_strategy == "records":
        return records_msg

    return f"{bytes_msg} or {records_msg}"


if __name__ == "__main__":
    load_dotenv()
    ARGS = parse_args()
    setproctitle(f"per-message-s3-exporter-{ARGS.s3_bucket}")

    setup_logging(ARGS)
    logging.info(
        f"Each {'non-common-message-type ' if ARGS.common_message_types else ''}"
        f"file in S3 will have {batch_logging_message(ARGS)} "
        f"and will be compressed with {ARGS.compression_type} "
        f"using a batch strategy of {ARGS.batch_strategy} in bucket {ARGS.s3_bucket} "
        f"with an S3 folder prefix of {ARGS.s3_bucket_folder}"
    )

    if ARGS.common_message_types:
        logging.info(
            f'Treating "{ARGS.common_message_types}" as common message types '
            f"with {batch_logging_message(ARGS, True)}"
        )

    LOOP = asyncio.get_event_loop()

    setup_signal_handlers(LOOP)

    LOOP.run_until_complete(main(ARGS))
