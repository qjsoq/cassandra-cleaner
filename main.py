import os
from multiprocessing import Queue
from dsbulk_reader import DsBulkReader
from file_seeker import FileSeeker
from row_analyzer import RowAnalyzer
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

WORKING_DIRECTORY = os.getenv("PARTITION_KEYS_DIRECTORY", "/home/thingsboard/tb-test/clean-old/")
CASSANDRA_URL = os.getenv("CASSANDRA_URL", "127.0.0.1")
CASSANDRA_DC = os.getenv("CASSANDRA_DC", "ske")
WORKER_COUNT: int = int(os.getenv("WORKER_COUNT", 4))
PARTITIONS_DIRECTORY: str = os.path.join(os.path.dirname(WORKING_DIRECTORY), "partitions")


if __name__ == "__main__":
    start_time: float = time.time()
    bulk_reader = DsBulkReader(WORKING_DIRECTORY, PARTITIONS_DIRECTORY, CASSANDRA_URL, CASSANDRA_DC)
    bulk_reader.start()
    queues: "list[Queue[str | None]]" = []
    analyzers: list[RowAnalyzer] = []
    was_directory_field: bool = False
    try:

        for worker in range(WORKER_COUNT):
            queue_worker: "Queue[str | None]" = Queue()
            queues.append(queue_worker)
            row_analyzer =  RowAnalyzer(queue_worker)
            analyzers.append(row_analyzer)

        file_seeker_instance = FileSeeker(PARTITIONS_DIRECTORY, queues, threshold_timeout_seconds=5)
        file_seeker_instance.start()
        
        for i in analyzers:
            i.start()
        while True:
            time.sleep(5)
            if not file_seeker_instance.is_alive():
                logger.info(f"FileSeeker has stopped")
                break
            
            dead_analyzers = [p for p in analyzers if not p.is_alive()]
            
            if dead_analyzers: 
                logger.error(f"RowAnalyzer not working")
                break
            
            if not bulk_reader.is_alive():
                 pending_files = os.listdir(PARTITIONS_DIRECTORY)
                 
                 if not pending_files:
                     logger.info(f"The task completed in {time.time() - start_time}")
                     logger.info("All tasks completed. performing graceful shutdown.")
                     
                     # Send poison pills to all workers
                     for q in queues:
                         q.put(None)
                     break
            
    finally:
        logger.warning("Stop all processes")
        bulk_reader.stop_dsbulk()
        
        for analyzer in analyzers:
            analyzer.join(timeout=10) # Give them time to finish
            if analyzer.is_alive():
                 logger.warning(f"Analyzer {analyzer.name} did not finish gracefully, terminating.")
                 analyzer.terminate()
        
        for analyzer in analyzers:
             if analyzer.is_alive():
                analyzer.join()
