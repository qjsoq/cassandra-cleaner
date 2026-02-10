import logging
import asyncio
import uuid
import pandas as pd
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import WhiteListRoundRobinPolicy, TokenAwarePolicy
from cassandra.query import PreparedStatement

logging.basicConfig(level=logging.INFO)
loger = logging.getLogger(__name__)

class CassandraCleaner:
    def __init__(self, hosts: list[str], username: str, password: str, port: int = 9042):
        self.hosts = hosts
        self.port = port
        self.auth_provider = PlainTextAuthProvider(username=username, password=password)
        self.cluster = None
        self.session = None
        self.prepared_delete_stmt = None

    def connect(self):
        """Initializes the Cassandra connection."""
        if not self.session:
            loger.info(f"Connecting to Cassandra nodes: {self.hosts}")
            
            # FIX 1: Use WhiteListRoundRobinPolicy
            # This is CRITICAL when using port-forwarding. It prevents the driver
            # from trying to connect to internal cluster IPs (10.1.x.x) which causes timeouts.
            lb_policy = TokenAwarePolicy(WhiteListRoundRobinPolicy(self.hosts))

            profile = ExecutionProfile(
                load_balancing_policy=lb_policy,
                request_timeout=30
            )

            self.cluster = Cluster(
                contact_points=self.hosts,
                port=self.port,
                auth_provider=self.auth_provider,
                execution_profiles={EXEC_PROFILE_DEFAULT: profile},
                protocol_version=4,
                connect_timeout=10
            )
            
            try:
                self.session = self.cluster.connect()
                query = "DELETE FROM tb.ts_kv_cf WHERE entity_type=? AND entity_id=? AND key=? AND partition=?"
                self.prepared_delete_stmt = self.session.prepare(query)
                loger.info("Cassandra connected and prepared statement ready.")
            except Exception as e:
                loger.error(f"CRITICAL: Could not connect to Cassandra cluster: {e}")
                raise e

    def shutdown(self):
        if self.cluster:
            self.cluster.shutdown()
            loger.info("Cassandra connection closed.")

    async def delete_partitions(self, rows):
        """
        Takes a list of dicts (rows) and deletes them from Cassandra in parallel.
        """
        if not rows:
            return

        loop = asyncio.get_running_loop()
        futures = []

        for row in rows:
            try:
                # Prepare Params
                p_entity_type = str(row['entity_type'])
                p_entity_id = uuid.UUID(str(row['entity_id']))
                p_key = str(row['key'])
                p_partition = int(row['partition'])

                # Create Future
                future = loop.create_future()

                # FIX 2: Robust Callback Wrapper
                # We define the check INSIDE the function scheduled on the loop.
                # This prevents race conditions where the future finishes between the
                # driver thread check and the asyncio loop execution.
                def _set_result_safe(f, res):
                    if not f.done():
                        f.set_result(res)

                def _set_exception_safe(f, exc):
                    if not f.done():
                        f.set_exception(exc)

                def on_success(result):
                    loop.call_soon_threadsafe(_set_result_safe, future, result)

                def on_error(exception):
                    loop.call_soon_threadsafe(_set_exception_safe, future, exception)

                # Execute
                if self.session is None:
                    raise Exception("Cassandra session is not connected!")

                params = (p_entity_type, p_entity_id, p_key, p_partition)
                cassandra_future = self.session.execute_async(self.prepared_delete_stmt, params)
                cassandra_future.add_callbacks(on_success, on_error)
                
                futures.append(future)

            except Exception as e:
                loger.error(f"Error preparing delete for row {row}: {e}")

        # Wait for batch
        if futures:
            try:
                await asyncio.gather(*futures)
                loger.info(f"Successfully deleted {len(futures)} partitions.")
            except Exception as e:
                loger.error(f"Error during async deletion batch: {e}")