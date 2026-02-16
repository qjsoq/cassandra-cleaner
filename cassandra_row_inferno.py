import logging
import asyncio
import uuid
import pandas as pd
from asyncio import Future, Semaphore, AbstractEventLoop
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT, Session, ResponseFuture
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import PreparedStatement

logger = logging.getLogger(__name__)

class CassandraCleaner:
    def __init__(self, hosts: list[str], username: str, password: str, concurrency: int = 20):
        self.hosts: list[str] = hosts
        self.auth_provider: PlainTextAuthProvider = PlainTextAuthProvider(username=username, password=password)
        self.cluster: Cluster | None = None
        self.session: Session | None = None
        self.prepared_delete_stmt: PreparedStatement | None = None
        self.concurrency = concurrency
        self.semaphore: Semaphore | None = None

    def connect(self):
        if not self.session:
            logger.info(f"Connecting to Cassandra nodes: {self.hosts}")

            self.cluster = Cluster(
                contact_points=self.hosts,
                auth_provider=self.auth_provider,
                protocol_version=4,
                connect_timeout=10
            )
            
            try:
                self.session = self.cluster.connect()
                query: str = "DELETE FROM tb.ts_kv_cf WHERE entity_type=? AND entity_id=? AND key=? AND partition=?"
                self.prepared_delete_stmt = self.session.prepare(query)
                self.semaphore = asyncio.Semaphore(self.concurrency)
                logger.info("Cassandra connected and prepared statement ready.")
            except Exception as e:
                logger.error(f"CRITICAL: Could not connect to Cassandra cluster: {e}")
                raise

    def shutdown(self):
        if self.cluster:
            self.cluster.shutdown()
            logger.info("Cassandra connection closed.")
            
    async def __aenter__(self):
        self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()
        return False

    async def delete_partitions(self, rows: list[dict[str, str]]):

        if not rows:
            return
        
        if self.semaphore is None:
             raise RuntimeError("CassandraCleaner is not connected. Call connect() first.")

        loop: AbstractEventLoop = asyncio.get_running_loop()
        tasks = []
        
        # Pass the instance semaphore
        tasks = [self.delete_row_task(row=row, semaphore=self.semaphore, loop=loop) for row in rows]
        
        logger.info(f"Starting deletion of {len(tasks)} partitions with shared concurrency limit")
        results: list = await asyncio.gather(*tasks, return_exceptions=True)
        logger.info(f"Complete deletion of {len(tasks)} partitions")

        failures: list[tuple[dict, Exception]] = []
        for index, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Failed to delete the partition with this key {rows[index]} with exception {result}")
                failures.append((rows[index], result))

        if failures:
            raise RuntimeError(f"{len(failures)} out of {len(rows)} partition deletions failed")
                

                
    async def delete_row_task(self, row: dict[str, str], semaphore: Semaphore, loop: AbstractEventLoop):
        async with semaphore:
            future: Future = loop.create_future()

            try:
                p_entity_type = str(row['entity_type'])
                p_entity_id = uuid.UUID(str(row['entity_id']))
                p_key = str(row['key'])
                p_partition = int(row['partition'])

                def _set_result_safe(f, res):
                    if not f.done(): f.set_result(res)
                def _set_exception_safe(f, exc):
                    if not f.done(): f.set_exception(exc)
                def on_success(result):
                    loop.call_soon_threadsafe(_set_result_safe, future, result)
                def on_error(exception):
                    loop.call_soon_threadsafe(_set_exception_safe, future, exception)

                if not self.session:
                    raise RuntimeError("Cassandra session is missing")
                params = (p_entity_type, p_entity_id, p_key, p_partition)
                cassandra_future: ResponseFuture = self.session.execute_async(self.prepared_delete_stmt, params)
                cassandra_future.add_callbacks(on_success, on_error)

                return await future

            except Exception as e:
                logger.error(f"Error processing row {row}: {e}")
                return e