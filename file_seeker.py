import os
import glob
from multiprocessing import Queue
from typing import override
import threading
import logging
import time

logger = logging.getLogger(__name__)

class FileSeeker(threading.Thread):
    def __init__(self, path_to_partitions: str, path_queues: "list[Queue[str | None]]", threshold_timeout_seconds: int = 120):
        super().__init__()
        self.path_to_partitions = path_to_partitions
        self.path_queues = path_queues
        self.processed_files_set: set[str] = set()
        self.threshold_timeout_seconds = threshold_timeout_seconds
        self.queue_balancing_counter = 0
        self._stop_event = threading.Event()
        self.daemon = True

    def stop(self) -> None:
        self._stop_event.set()

    def get_file_to_process(self) -> None:
        candidate_files: list[str] = glob.glob(os.path.join(self.path_to_partitions, "*.csv"))

        for file in candidate_files:
            if file in self.processed_files_set:
                continue

            last_modification_time: float  = os.path.getmtime(file)

            if time.time() - last_modification_time > self.threshold_timeout_seconds:
                logger.info(f"Pushing {file} to the {self.queue_balancing_counter} queue")
                self.path_queues[self.queue_balancing_counter].put(file)
                logger.info(f"The current Queue is {self.queue_balancing_counter} and its content are {self.path_queues[self.queue_balancing_counter].qsize()}")
                self.processed_files_set.add(file)
                if self.queue_balancing_counter == len(self.path_queues) - 1:
                    self.queue_balancing_counter = 0
                else:
                    self.queue_balancing_counter += 1
            else:
                logger.warning(f"File is in active use {file}")
                continue

        self.processed_files_set.intersection_update(candidate_files)

        return None

    def start_monitoring(self):
        while not self._stop_event.is_set():
            self.get_file_to_process()
            self._stop_event.wait(timeout=30)

    @override
    def run(self) -> None:
        try:
            logger.info("FileSeeker thread started")
            return self.start_monitoring()
        except Exception:
            logger.exception(f"Caught exception in {self.name}")
