import collections
import logging
import multiprocessing

try:
    import Queue
except ImportError:
    # Python 3
    import queue as Queue
import sys
import time

import boto3
import six

log = logging.getLogger()


def sizeof(obj, seen=None):
    """Recursively and fully calculate the size of an object"""
    obj_id = id(obj)
    try:
        if obj_id in seen:
            return 0
    except TypeError:
        seen = set()

    seen.add(obj_id)

    size = sys.getsizeof(obj)

    if isinstance(obj, six.string_types):
        return size
    elif isinstance(obj, collections.Mapping):
        return size + sum(
            sizeof(key, seen) + sizeof(val, seen)
            for key, val in six.iteritems(obj)
        )
    elif isinstance(obj, collections.Iterable):
        return size + sum(
            sizeof(item, seen)
            for item in obj
        )

    return size


def worker_main(queue, region_name, stream_name, buffer_time=0.5, max_count=1000, max_size=(1024 ** 2)):
    log.debug("Working")
    kinesis_client = boto3.session.Session(region_name=region_name).client('kinesis')
    records = []
    next_records = []
    alive = True
    records_size = 0
    records_count = 0

    while alive:
        timer_start = time.time()
        while (time.time() - timer_start) < buffer_time:
            log.debug("Start Loop")
            # we want our queue to block up until the end of this buffer cycle, so we set our timeout to the amount
            # remaining in buffer_time by substracting how long we spent so far during this cycle
            queue_timeout = buffer_time - (time.time() - timer_start)
            try:
                log.debug("Fetching from queue with timeout: %s", queue_timeout)
                data, explicit_hash_key, partition_key = queue.get(block=True, timeout=queue_timeout)
            except Queue.Empty:
                continue

            record = {
                'Data': data,
                'PartitionKey': partition_key or '{0}{1}'.format(time.clock(), time.time()),
            }
            if explicit_hash_key is not None:
                record['ExplicitHashKey'] = explicit_hash_key

            records_size += sizeof(record)
            if records_size >= max_size:
                log.debug("Records exceed MAX_SIZE (%s)!  Adding to next_records: %s", max_size, record)
                next_records = [record]
                break

            log.debug("Adding to records (%d bytes): %s", records_size, record)
            records.append(record)

            records_count += 1
            if records_count == max_count:
                log.debug("Records have reached MAX_COUNT (%s)!  Flushing records.", max_count)
                break

        records, next_records = flush_records(client=kinesis_client, stream_name=stream_name,
                                              records=records, next_records=next_records)
        # buffer_time = 0.25
        log.debug("End loop")
    log.debug("End worker")


def flush_records(client, stream_name, records, next_records):
    if records:
        log.debug("Flushing %d records", len(records))
        client.put_records(StreamName=stream_name, Records=records)

    records = next_records
    next_records = []
    return records, next_records


class AsyncKinesisProducer(object):
    """Kinesis stream via an AsyncProducer"""

    def __init__(self, stream_name, buffer_time=0.5, max_count=1000, max_size=(1024 ** 2), region_name='us-east-1',
                 processes=2):
        log.info("init Kinesis Producer")
        self.queue = multiprocessing.Queue()
        self.pool = multiprocessing.Pool(processes, worker_main, (self.queue, region_name, stream_name,
                                                                  buffer_time, max_count, max_size,))
        log.info("Kinesis Producer ready")

    def put(self, data, explicit_hash_key=None, partition_key=None):
        self.queue.put((data, explicit_hash_key, partition_key))

    def put_record(self, data, explicit_hash_key=None, partition_key=None):
        self.queue.put((data, explicit_hash_key, partition_key))
