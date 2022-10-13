"""The S3-Exporter reads records from Kafka and writes them to flat files in S3
"""

import argparse as ap
import asyncio
import bz2
from functools import partial
import os
from pathlib import Path
from signal import Signals, SIGINT, SIGTERM
import sys
from typing import Any, Optional

from aiobotocore.session import get_session
from aiokafka import AIOKafkaConsumer, ConsumerRecord, TopicPartition
import attr
from types_aiobotocore_s3.client import S3Client


def parse_args():
    """Parse command-line arguments, using environment variables to override
    defaults"""
    parser = ap.ArgumentParser(formatter_class=ap.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        "--s3-bucket", default=os.getenv("S3_BUCKET", ""), help="S3 bucket to write files into"
    )
    parser.add_argument(
        "--aws-secret-access-key",
        default=os.getenv("AWS_SECRET_ACCESS_KEY", ""),
        help="AWS secret access key",
    )
    parser.add_argument(
        "--aws-access-key-id", default=os.getenv("AWS_ACCESS_KEY_ID", ""), help="AWS access key ID"
    )
    parser.add_argument(
        "--aws-region-name",
        default=os.getenv("AWS_REGION_NAME", "us-east-1"),
        help="AWS region with the S3 bucket",
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
        "--records-per-file",
        default=int(os.getenv("RECORDS_PER_FILE", "1000")),
        help="Threshold number of records in a file before writing to S3",
    )
    parser.add_argument(
        "--compression-type",
        choices=("none", "bzip"),
        default=os.getenv("COMPRESSION_TYPE", "none"),
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


async def consume_records_from_kafka(
    args: ap.Namespace, records_queue: asyncio.Queue, offsets_queue: asyncio.Queue
) -> None:
    """Consume records from Kafka, putting them onto the queue as received,
    reading offsets from a separate queue and committing them as appropriate"""
    consumer = AIOKafkaConsumer(
        args.kafka_topic,
        bootstrap_servers=args.kafka_brokers,
        group_id="firestarter-s3-exporter",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )

    try:
        await consumer.start()
        async for msg in consumer:
            await records_queue.put(msg)

            try:
                offset = offsets_queue.get_nowait()
            except asyncio.QueueEmpty:
                continue

            # Save the offset + 1 as per the Kafka docs
            # TODO: Make the partition dynamic, although it is safe to hardcode
            #  it for now since there is only one partition used by the
            #  connector
            topic_partition = TopicPartition(msg.topic, msg.partition)
            await consumer.commit({topic_partition: offset + 1})

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


@attr.s(kw_only=True)
class S3FileBatcher:
    """Class used to batch together records from Kafka and write them to a
    queue along with some metadata

    Once written to a file, that file can be uploaded to S3
    """

    records_per_file: int = attr.ib()
    compression_type: str = attr.ib()
    s3_writer_queue: asyncio.Queue = attr.ib()

    _current_batch: list = attr.ib(init=False, factory=list)
    _starting_offset: int = attr.ib(init=False)
    _last_offset_in_batch: int = attr.ib(init=False)
    _kafka_topic: str = attr.ib(init=False)

    async def ingest_record(self, record: ConsumerRecord):
        """Ingest a record from Kafka, adding it to the current batch and
        writing a file to disk if necessary
        """
        if not self._current_batch:
            self._starting_offset = record.offset
            self._kafka_topic = record.topic

        self._current_batch.append(record.value)

        if len(self._current_batch) >= self.records_per_file:
            self._last_offset_in_batch = record.offset

            await self.enqueue_batch_contents()
            self._current_batch = []

    async def enqueue_batch_contents(self):
        """Write the current batch of records to the S3 writer's queue"""
        filename = self.batch_filename()

        file_contents = b"".join(self._current_batch)
        if self.compression_type == "bzip":
            file_contents = bz2.compress(file_contents)

        s3_object = S3WriteObject(
            key=filename, body=file_contents, last_offset=self._last_offset_in_batch
        )

        await self.s3_writer_queue.put(s3_object)

    def batch_filename(self) -> Path:
        """Filename to use in S3 for writing out the records from Kafka"""
        extension = f"json{'' if self.compression_type == 'none' else '.bz2'}"
        return Path(f"{self._kafka_topic}_{self._starting_offset}.{extension}")


async def build_batch_of_records_from_kafka(
    args: ap.Namespace, kafka_queue: asyncio.Queue, s3_writer_queue: asyncio.Queue
) -> None:
    """Read Kafka records from an async queue, building up batches of them to
    write to disk before they make their way into S3"""
    batcher = S3FileBatcher(
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
    args: ap.Namespace, s3_queue: asyncio.Queue, kafka_offset_queue: asyncio.Queue
):
    """Reads file contenst from the s3_queue and writes them to the s3 bucket
    specified in args. When successful, sends an offset update to the Kafka
    consumer via the kafka_offset_queue"""
    session = get_session()
    async with session.create_client(
        "s3",
        aws_secret_access_key=args.aws_secret_access_key,
        aws_access_key_id=args.aws_access_key_id,
        region_name=args.aws_region_name,
    ) as client:
        client: S3Client

        while True:
            s3_write_object: S3WriteObject = await s3_queue.get()

            # Write to S3
            await client.put_object(
                Bucket=args.s3_bucket, Key=s3_write_object.key, Body=s3_write_object.body
            )

            # Once we've put the object into S3 we can save the last offset we
            # read from Kafka
            await kafka_offset_queue.put(s3_write_object.last_offset)


if __name__ == "__main__":
    ARGS = parse_args()

    KAFKA_RECORDS_QUEUE = asyncio.Queue()
    S3_WRITER_QUEUE = asyncio.Queue()
    KAFKA_OFFSETS_QUEUE = asyncio.Queue()

    LOOP = asyncio.get_event_loop()

    setup_signal_handlers(LOOP)

    # TODO: create a list of tasks and gather them, catching any exceptions
    #  Can create a separate task for each of the partitions in the Kafka topic
    #  being read from
    LOOP.call_soon(
        asyncio.create_task,
        consume_records_from_kafka(ARGS, KAFKA_RECORDS_QUEUE, KAFKA_OFFSETS_QUEUE),
    )

    LOOP.call_soon(
        asyncio.create_task,
        build_batch_of_records_from_kafka(ARGS, KAFKA_RECORDS_QUEUE, S3_WRITER_QUEUE),
    )

    LOOP.call_soon(
        asyncio.create_task, write_files_to_s3(ARGS, S3_WRITER_QUEUE, KAFKA_OFFSETS_QUEUE)
    )

    LOOP.run_forever()
