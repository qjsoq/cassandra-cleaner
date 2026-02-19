import os
import subprocess
import glob
import threading
import logging
from typing import override
import time

logger = logging.getLogger(__name__)

MAX_DSBULK_RETRIES = 45


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
        
        self._stop_event = threading.Event()
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
                  "-u", os.getenv('CASSANDRA_USERNAME', 'cassandra'),
                  "-p", os.getenv('CASSANDRA_PASSWORD', 'cassandra'),
                  "-query", "SELECT DISTINCT entity_type, entity_id, key, partition FROM tb.ts_kv_cf",
                  "--connector.csv.maxRecords", "5000",
                  "--driver.basic.request.page-size", os.getenv("DSBULK_REQUEST_PAGE_SIZE", "3000"),
                  "--executor.maxPerSecond", "8192",
                  "--executor.continuousPaging.enabled", "false",
                  "--schema.splits", os.getenv("DSBULK_SCHEMA_SPLITS", "10000"),
                  "--engine.maxConcurrentQueries", os.getenv("DSBULK_ENGINE_MAXCONCURRENT", "32"),
                  "--driver.advanced.protocol.compression", "lz4",
                  "--log.maxErrors", "999888",
                  "--log.verbosity", "normal",
                  "--log.maxQueryWarnings", "10",
                  "--driver.advanced.request-tracker.classes", "[RequestLogger]",
                  "--driver.advanced.request-tracker.logs.error.enabled", "true",
                  "--driver.advanced.request-tracker.logs.show-stack-traces", "true",
                  "--driver.advanced.connection.init-query-timeout", "60 seconds",
                  "--driver.advanced.connection.connect-timeout", "60 seconds",
                  "--driver.advanced.retry-policy-max-retries", "10",
                  "--driver.basic.request.consistency", "LOCAL_QUORUM"]
        for attempt in range(1, MAX_DSBULK_RETRIES + 1):
            current_cmd: list[str] = dsbulk_command.copy()

            checkpoint: str | None = self._find_latest_checkpoint()
            
            if self._stop_event.is_set():
                logger.info("DsBulkReader was intentionally terminated.")
                break
            
            if checkpoint:
                logger.info(f"Found this checkpoint {checkpoint}")
                current_cmd.append(f"--dsbulk.log.checkpoint.file={checkpoint}")
                current_cmd.append("--dsbulk.log.checkpoint.replayStrategy")
                current_cmd.append("resume")

            self.process = subprocess.Popen(current_cmd, text=True)

            self.process.wait()
            
            if self._stop_event.is_set():
                logger.info("DsBulkReader was intentionally terminated.")
                break
            
            if self.process.returncode == 0:
                logger.info("Finished successfully")
                break

            logger.warning(f"The partition extraction failed (attempt {attempt}/{MAX_DSBULK_RETRIES}), attempting to restart the process")
            
            if self._stop_event.wait(15):
                logger.warning("Terminate DSBulk unload operation")
                break
        else:
            logger.error(f"dsbulk failed after {MAX_DSBULK_RETRIES} attempts, giving up")

    @override
    def run(self):
        self.get_partitions()
        
    def stop_dsbulk(self):
        self._stop_event.set()
        if self.process:
            self.process.terminate()
            self.process.wait()