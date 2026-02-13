import os
import argparse
import signal
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

_shutdown_requested = False


def _signal_handler(signum, frame):
    global _shutdown_requested
    logger.warning(f"Received signal {signal.Signals(signum).name}, shutting down...")
    _shutdown_requested = True

def main():
    parser = argparse.ArgumentParser(description="Script to delete obsolete telemetry")
    parser.add_argument("--dry-run", action="store_true", help="Simulate the run without actually deleting data from Cassandra")

    args = parser.parse_args()

    if args.dry_run:
        logger.warning("RUNNING IN DRY MODE")

    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    start_time: float = time.time()
    bulk_reader = DsBulkReader(WORKING_DIRECTORY, PARTITIONS_DIRECTORY, CASSANDRA_URL, CASSANDRA_DC)
    bulk_reader.start()
    queues: "list[Queue[str | None]]" = []
    analyzers: list[RowAnalyzer] = []
    file_seeker_instance: FileSeeker | None = None
    try:

        for worker in range(WORKER_COUNT):
            queue_worker: "Queue[str | None]" = Queue()
            queues.append(queue_worker)
            row_analyzer =  RowAnalyzer(queue_worker, args.dry_run)
            analyzers.append(row_analyzer)

        file_seeker_instance = FileSeeker(PARTITIONS_DIRECTORY, queues, threshold_timeout_seconds=60)
        file_seeker_instance.start()

        for i in analyzers:
            i.start()
        while not _shutdown_requested:
            time.sleep(5)
            if not file_seeker_instance.is_alive():
                logger.info("FileSeeker has stopped")
                break

            dead_analyzers = [p for p in analyzers if not p.is_alive()]

            if dead_analyzers:
                logger.error("RowAnalyzer not working")
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
        bulk_reader.join(20)

        if file_seeker_instance is not None:
            file_seeker_instance.stop()
            file_seeker_instance.join(20)

        for analyzer in analyzers:
            analyzer.join(timeout=10) # Give them time to finish
            if analyzer.is_alive():
                 logger.warning(f"Analyzer {analyzer.name} did not finish gracefully, terminating.")
                 analyzer.terminate()

        for analyzer in analyzers:
             if analyzer.is_alive():
                analyzer.join()

if __name__ == "__main__":
    main()