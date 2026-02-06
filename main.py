import os
from multiprocessing import Queue
from dsbulk_reader import DsBulkReader
from file_seeker import FileSeeker

WORKING_DIRECTORY = os.getenv("PARTITION_KEYS_DIRECTORY", "/home/thingsboard/tb-test/clean-old/")
CASSANDRA_URL = os.getenv("CASSANDRA_URL", "127.0.0.1")
CASSANDRA_DC = os.getenv("CASSANDRA_DC", "ske")
WORKER_COUNT: int = int(os.getenv("WORKER_COUNT", 4))
PARTITIONS_DIRECTORY: str = os.path.join(os.path.dirname(WORKING_DIRECTORY), "partitions")


if __name__ == "__main__":
    bulk_reader = DsBulkReader(WORKING_DIRECTORY, PARTITIONS_DIRECTORY, CASSANDRA_URL, CASSANDRA_DC)
    bulk_reader.start()
    try:
        queues: "list[Queue[str]]" = []
        for worker in range(WORKER_COUNT):
            queue_worker: "Queue[str]" = Queue()
            queues.append(queue_worker)

        file_seeker_instance = FileSeeker(PARTITIONS_DIRECTORY, queues, threshold_timeout_seconds=20)
        file_seeker_instance.start_monitoring()
        while True:
            continue
    finally:
        print("Stop all processes")
        bulk_reader.stop_dsbulk()

print("Hello I am done")