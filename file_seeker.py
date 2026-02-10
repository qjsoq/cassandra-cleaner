import os
import glob
from multiprocessing import Queue
from typing import override
import threading
import logging
import time

logging.basicConfig(level=logging.INFO)
loger = logging.getLogger(__name__)

class FileSeeker(threading.Thread):
    def __init__(self, path_to_partitions: str, path_queues: "list[Queue[str]]", threshold_timeout_seconds: int = 120):
        super().__init__()
        self.path_to_partitions = path_to_partitions
        self.path_queues = path_queues
        self.processed_files_set: set[str] = set()
        self.threshold_timeout_seconds = threshold_timeout_seconds
    
    def get_file_to_process(self) -> None:
        candidate_files: list[str] = glob.glob(os.path.join(self.path_to_partitions, "*.csv"))
        queue_balancing_counter: int = 0
        
        for file in candidate_files:
            if file in self.processed_files_set:
                continue
            
            last_modification_time: float  = os.path.getmtime(file)
            
            if time.time() - last_modification_time > self.threshold_timeout_seconds:
                loger.info(f"Pushing {file} to the {queue_balancing_counter} queue")
                self.path_queues[queue_balancing_counter].put(file)
                loger.info(f"The current Queue is {queue_balancing_counter} and its content are {self.path_queues[queue_balancing_counter].qsize()}")
                self.processed_files_set.add(file)
                if queue_balancing_counter == len(self.path_queues) - 1:
                    queue_balancing_counter = 0
                else:    
                    queue_balancing_counter += 1
            else:
                loger.warning(f"File is in actibe use {file}")
                continue
        return None
    
    def start_monitoring(self):
        while True:
            self.get_file_to_process()
            time.sleep(30)
            
    @override
    def run(self) -> None:
        return self.start_monitoring()