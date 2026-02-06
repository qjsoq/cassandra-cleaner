import os
import glob
from multiprocessing import Queue
import time

class FileSeeker:
    def __init__(self, path_to_partitions: str, path_queues: "list[Queue[str]]", threshold_timeout_seconds: int = 120):
        self.path_to_partitions = path_to_partitions
        self.path_queues = path_queues
        self.processed_files_set: set[str] = set()
        self.threshold_timeout_seconds = threshold_timeout_seconds
        return None
    
    def get_file_to_process(self) -> None:
        candidate_files: list[str] = glob.glob(os.path.join(self.path_to_partitions, "*.csv"))
        queue_balancing_counter: int = 0
        
        for file in candidate_files:
            print(f"Check {file}")
            if file in self.processed_files_set:
                continue
            
            last_modification_time: float  = os.path.getmtime(file)
            
            if time.time() - last_modification_time > self.threshold_timeout_seconds:
                print(f"Pushing {file} to the {queue_balancing_counter} queue")
                self.path_queues[queue_balancing_counter].put(file)
                print(f"The current Queue is {queue_balancing_counter} and its content are {self.path_queues[queue_balancing_counter].qsize()}")
                self.processed_files_set.add(file)
                if queue_balancing_counter == len(self.path_queues) - 1:
                    queue_balancing_counter = 0
                else:    
                    queue_balancing_counter += 1
            else:
                print(f"File is in actibe use {file}")
                continue
        return None
    
    def start_monitoring(self):
        while True:
            self.get_file_to_process()
            time.sleep(30)