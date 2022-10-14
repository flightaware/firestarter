"""The S3-Exporter reads records from Kafka and writes them to flat files in S3
"""

import argparse as ap
import asyncio
import bz2
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import os
from signal import Signals, SIGINT, SIGTERM
import sys
from typing import List

from aiokafka import AIOKafkaConsumer, ConsumerRecord, TopicPartition
from aiokafka.errors import KafkaConnectionError
import attr
import boto3
from codetiming import Timer


def parse_args() -> ap.Namespace:
    """Parse command-line arguments, using environment variables to override
    defaults"""
    parser = ap.ArgumentParser(formatter_class=ap.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        "--s3-bucket", default=os.getenv("S3_BUCKET", ""), help="S3 bucket to write files into"
    )
    parser.add_argument(
        "--kafka-brokers",
        default=os.getenv("KAFKA_BROKERS", "kafka:9092"),
        help="URI for the Kafka brokers",
    )
    parser.add_argument(
        "--kafka-topic",
        default=os.getenv("KAFKA_TOPIC", "events"),
        help="Kafka topic to read data from before writing to S3",
    )
    parser.add_argument(
        "--asyncio-queue-max-size",
        default=int(os.getenv("ASYNCIO_QUEUE_MAX_SIZE", "0")),
        help="Maximum size of the asyncio Queues used",
    )
    parser.add_argument(
        "--records-per-file",
        default=int(os.getenv("RECORDS_PER_FILE", "100000")),
        help="Threshold number of records in a file before writing to S3",
    )
    parser.add_argument(
        "--compression-type",
        choices=("none", "bzip"),
        default=os.getenv("COMPRESSION_TYPE", "none").lower(),
        help="Compression to use for files uploaded to S3",
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
    print(f"Exiting from a {signal.name}")
    sys.exit()


async def partitions_for_topic(bootstrap_servers: str, topic: str) -> List[int]:
    """Get a list of all the partitions for the topic specified in args"""
    while True:
        try:
            consumer = AIOKafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                enable_auto_commit=False,
            )
            await consumer.start()

            partitions = consumer.partitions_for_topic(topic)
            return sorted(partitions)
        except KafkaConnectionError:
            print("Kafka not yet available: trying again in a few seconds")
            await asyncio.sleep(3)
        finally:
            await consumer.stop()


async def consume_records_from_kafka(
    args: ap.Namespace, partition: int, records_queue: asyncio.Queue, offsets_queue: asyncio.Queue
) -> None:
    """Consume records from Kafka, putting them onto the queue as received,
    reading offsets from a separate queue and committing them as appropriate"""
    consumer = AIOKafkaConsumer(
        bootstrap_servers=args.kafka_brokers,
        group_id="firestarter-s3-exporter",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )

    try:
        await consumer.start()

        topic_partition_assignment = TopicPartition(args.kafka_topic, partition)
        consumer.assign([topic_partition_assignment])

        last_committed = await consumer.committed(topic_partition_assignment)
        print(f"Consuming partition {partition} at offset {last_committed}")

        async for msg in consumer:
            await records_queue.put(msg)

            try:
                offset = offsets_queue.get_nowait()
            except asyncio.QueueEmpty:
                continue

            # Save the offset + 1 as per the Kafka docs
            offset += 1

            topic_partition = TopicPartition(args.kafka_topic, partition)
            await consumer.commit({topic_partition: offset})

            print(f"Committed offset {offset} for partition {partition}")
            offsets_queue.task_done()

    finally:
        await consumer.stop()


# pylint: disable=too-few-public-methods
@attr.s(kw_only=True, frozen=True)
class S3WriteObject:
    """Class used by the S3FileBatcher class to store data needed for putting
    Firehose data into S3"""

    key: str = attr.ib()
    body: bytes = attr.ib()
    last_offset: int = attr.ib()


# pylint: disable=too-many-instance-attributes
@attr.s(kw_only=True)
class S3FileBatcher:
    """Class used to batch together records from Kafka and write them to a
    queue along with some metadata. Each instance of this class is assigned a
    single partition in the given Kafka topic.

    The consumer of the queue then writes these batches to S3.
    """

    kafka_topic: str = attr.ib()
    kafka_partition: int = attr.ib()
    records_per_file: int = attr.ib()
    compression_type: str = attr.ib()
    s3_writer_queue: asyncio.Queue = attr.ib()

    _current_batch: list = attr.ib(init=False, factory=list)
    _starting_offset: int = attr.ib(init=False)
    _last_offset_in_batch: int = attr.ib(init=False)
    _timer: Timer = attr.ib(
        init=False,
        default=attr.Factory(
            lambda self: Timer(
                name=f"s3_batcher_{self.kafka_partition}",
                text=f"{{name}}: Built a batch of {self.records_per_file} "
                f"Kafka records in {{seconds:.2f}} s",
            ),
            takes_self=True,
        ),
    )

    async def ingest_record(self, record: ConsumerRecord):
        """Ingest a record from Kafka, adding it to the current batch and
        writing a file to disk if necessary
        """
        if not self._current_batch:
            self._starting_offset = record.offset
            self._timer.start()

        self._current_batch.append(record.value)

        if len(self._current_batch) >= self.records_per_file:
            self._last_offset_in_batch = record.offset

            await self.enqueue_batch_contents()
            self._current_batch = []
            self._timer.stop()

    async def enqueue_batch_contents(self):
        """Write the current batch of records to the S3 writer's queue"""
        filename = self.batch_filename()

        file_contents = b"".join(self._current_batch)
        if self.compression_type == "bzip":
            file_contents = bz2.compress(file_contents)

        s3_object = S3WriteObject(
            key=filename,
            body=file_contents,
            last_offset=self._last_offset_in_batch,
        )

        await self.s3_writer_queue.put(s3_object)

    def batch_filename(self) -> str:
        """Filename to use in S3 for writing out the records from Kafka"""
        topic = self.kafka_topic
        partition = self.kafka_partition
        starting_offset = self._starting_offset
        extension = f"json{'' if self.compression_type == 'none' else '.bz2'}"
        return f"{topic}_{partition}_{starting_offset}.{extension}"


async def build_batch_of_records_from_kafka(
    args: ap.Namespace,
    kafka_partition: int,
    kafka_queue: asyncio.Queue,
    s3_writer_queue: asyncio.Queue,
) -> None:
    """Read Kafka records from an async queue, building up batches of them to
    write to disk before they make their way into S3"""
    batcher = S3FileBatcher(
        kafka_topic=args.kafka_topic,
        kafka_partition=kafka_partition,
        records_per_file=args.records_per_file,
        compression_type=args.compression_type,
        s3_writer_queue=s3_writer_queue,
    )
    while True:
        # Use a "blocking" await on the queue with Kafka records
        kafka_record = await kafka_queue.get()
        await batcher.ingest_record(kafka_record)
        kafka_queue.task_done()


async def write_files_to_s3(
    args: ap.Namespace,
    executor: ThreadPoolExecutor,
    s3_queue: asyncio.Queue,
    kafka_offset_queue: asyncio.Queue,
):
    """Reads file contenst from the s3_queue and writes them to the s3 bucket
    specified in args. When successful, sends an offset update to the Kafka
    consumer via the kafka_offset_queue"""
    loop = asyncio.get_running_loop()

    # Using the standard configuration options, e.g., 3 retries
    # Gets all configuration options from environment variables
    # We create a separate session for each coroutine since it'll
    # be running behind the scenes in a threadpool and this is the
    # recommended way of having thread safety with boto3
    session = boto3.session.Session()
    s3_client = session.resource("s3")

    while True:
        s3_write_object: S3WriteObject = await s3_queue.get()

        # Get some timing stats on how long it takes to write to S3
        timer = Timer(
            text=f"Successfully wrote {s3_write_object.key} to S3 "
            f"with {args.records_per_file:,} Kafka records in "
            f"{{seconds:.2f}} ms",
        )

        # Write to S3 using the ThreadPoolExecutor since boto3 isn't async
        print(f"Attempting to write {s3_write_object.key} to S3...")
        timer.start()
        partial_s3_put = partial(
            s3_client.put_object,
            Bucket=args.s3_bucket,
            Key=s3_write_object.key,
            Body=s3_write_object.body,
        )
        await loop.run_in_executor(executor, partial_s3_put)

        s3_queue.task_done()
        timer.stop()

        # Once we've put the object into S3 we can save the last offset we
        # read from Kafka
        await kafka_offset_queue.put(s3_write_object.last_offset)


async def main(args: ap.Namespace):
    """Setup all the tasks needed to read from Kafka and write to S3"""
    partitions = await partitions_for_topic(args.kafka_brokers, args.kafka_topic)
    num_partitions = len(partitions)
    print(
        f"Creating a Kafka consumer and S3 file builder for "
        f"{num_partitions} partition(s) in topic {args.kafka_topic}"
    )

    max_queue_size = args.asyncio_queue_max_size

    kafka_records_queues: List[asyncio.Queue] = [
        asyncio.Queue(max_queue_size) for _ in range(len(partitions))
    ]
    kafka_offsets_queues: List[asyncio.Queue] = [
        asyncio.Queue(max_queue_size) for _ in range(len(partitions))
    ]
    s3_writer_queues: List[asyncio.Queue] = [
        asyncio.Queue(max_queue_size) for _ in range(len(partitions))
    ]

    # Need this to run boto3 operations
    executor = ThreadPoolExecutor()

    tasks: List[asyncio.Task] = []
    for partition in partitions:
        # Each partition gets its own Kafka consumer
        consumer_coro = consume_records_from_kafka(
            args, partition, kafka_records_queues[partition], kafka_offsets_queues[partition]
        )
        tasks.append(asyncio.create_task(consumer_coro))

        # Each partition gets its own S3 file batcher
        batcher_coro = build_batch_of_records_from_kafka(
            args, partition, kafka_records_queues[partition], s3_writer_queues[partition]
        )
        tasks.append(asyncio.create_task(batcher_coro))

        # Each partition gets its own S3 file writer
        s3_writer_coro = write_files_to_s3(
            args, executor, s3_writer_queues[partition], kafka_offsets_queues[partition]
        )
        tasks.append(asyncio.create_task(s3_writer_coro))

    # Run all the tasks in the event loop
    await asyncio.gather(*tasks, return_exceptions=False)


if __name__ == "__main__":
    ARGS = parse_args()

    print(
        f"Exporting Kafka records from topic {ARGS.kafka_topic} to S3 bucket "
        f"{ARGS.s3_bucket} from {ARGS.kafka_brokers}"
    )
    print(
        f"Each file in S3 will have {ARGS.records_per_file:,} Kafka records "
        f"and will be compressed with {ARGS.compression_type}"
    )

    LOOP = asyncio.get_event_loop()

    setup_signal_handlers(LOOP)

    LOOP.run_until_complete(main(ARGS))
