import os
import subprocess
import glob
import threading
import logging
from typing import override
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DsBulkReader(threading.Thread):
    
    def __init__(self, path_to_working_directory: str, partitions_directory: str, cassandra_url: str, cassandra_dc: str):
        super().__init__()
        self.partitions_directory = partitions_directory
        self.cassandra_url = cassandra_url
        self.cassandra_dc = cassandra_dc
        self.log_dir = os.path.join(os.path.dirname(path_to_working_directory), "logs")
        self.process: subprocess.Popen[str] | None = None
        os.makedirs(path_to_working_directory, exist_ok=True)
        os.makedirs(partitions_directory, exist_ok=True)
        
        self.daemon = True

    def _find_latest_checkpoint(self):
        checkpoints: list[str] = glob.glob(os.path.join(self.log_dir, "UNLOAD_*/checkpoint.csv"))
        
        if not checkpoints:
            logger.info(f"No checkpoints found in {self.log_dir}")
            return None
        
        return max(checkpoints, key=os.path.getmtime)
    
    def get_partitions(self):
        dsbulk_command: list[str] = ["dsbulk", "unload",
                  "-h", self.cassandra_url,
                  "-url", self.partitions_directory,
                  "-logDir", self.log_dir,
                  "-dc", self.cassandra_dc,
                  "-query", "SELECT DISTINCT entity_type, entity_id, key, partition FROM tb.ts_kv_cf",
                  "--connector.csv.maxRecords", "5000",
                  "--executor.maxPerSecond", "1024"]
        while True:
            current_cmd: list[str] = dsbulk_command.copy()
            
            checkpoint: str | None = self._find_latest_checkpoint()

            if checkpoint:
                logger.info(f"Found this checkpoint {checkpoint}")
                current_cmd.append(f"--dsbulk.log.checkpoint.file={checkpoint}")
                current_cmd.append("--dsbulk.log.checkpoint.replayStrategy")
                current_cmd.append("resume")

            self.process = subprocess.Popen(current_cmd, text=True)
            
            self.process.wait()
            
            if self.process.returncode == 0:
                break
            
            logger.warning("The partition extraction failed, attempting to restart the process")
            time.sleep(15)

    @override
    def run(self):
        self.get_partitions()
        
    def stop_dsbulk(self):
        if self.process:
            self.process.terminate()