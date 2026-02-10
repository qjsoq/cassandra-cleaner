import os
from multiprocessing import Queue
from dsbulk_reader import DsBulkReader
from file_seeker import FileSeeker
from row_analyzer import RowAnalyzer
import logging

logging.basicConfig(level=logging.INFO)
loger = logging.getLogger()

WORKING_DIRECTORY = os.getenv("PARTITION_KEYS_DIRECTORY", "/home/thingsboard/tb-test/clean-old/")
CASSANDRA_URL = os.getenv("CASSANDRA_URL", "127.0.0.1")
CASSANDRA_DC = os.getenv("CASSANDRA_DC", "ske")
WORKER_COUNT: int = int(os.getenv("WORKER_COUNT", 4))
PARTITIONS_DIRECTORY: str = os.path.join(os.path.dirname(WORKING_DIRECTORY), "partitions")


if __name__ == "__main__":
    bulk_reader = DsBulkReader(WORKING_DIRECTORY, PARTITIONS_DIRECTORY, CASSANDRA_URL, CASSANDRA_DC)
    bulk_reader.start()
    queues: "list[Queue[str]]" = []
    analyzers: list[RowAnalyzer] = []
    try:

        for worker in range(WORKER_COUNT):
            queue_worker: "Queue[str]" = Queue()
            queues.append(queue_worker)
            row_analyzer =  RowAnalyzer(queue_worker)
            analyzers.append(row_analyzer)

        file_seeker_instance = FileSeeker(PARTITIONS_DIRECTORY, queues, threshold_timeout_seconds=5)
        file_seeker_instance.start()
        
        for i in analyzers:
            i.start()
        while True:
            continue

    finally:
        loger.warning("Stop all processes")
        bulk_reader.stop_dsbulk()
        
        for analyzer in analyzers:
            analyzer.terminate()
        
        for analyzer in analyzers:
            analyzer.join()
