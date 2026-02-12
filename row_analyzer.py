from multiprocessing import Queue, Process
from cassandra_row_inferno import CassandraCleaner
from typing import override, Any, Coroutine, NoReturn
import urllib.parse
import time
import pandas as pd
import asyncio
import asyncpg
import logging
import csv
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
DATABASE_DSN = f"postgresql://{os.getenv('DATABASE_USERNAME', 'postgres')}:{urllib.parse.quote_plus(os.getenv('DATABASE_PASSWORD', 'postgres'))}@{os.getenv('DATABASE_HOST', "localhost")}:5432/thingsboard"
BATCH_SIZE = 500

ENTITY_TABLE_NAME_MAP = {
    "TENANT": "tenant",
    "CUSTOMER": "customer",
    "USER": "tb_user", 
    "DASHBOARD": "dashboard",
    "ASSET": "asset",
    "DEVICE": "device",
    "ALARM": "alarm",
    "ENTITY_GROUP": "entity_group",
    "CONVERTER": "converter",
    "INTEGRATION": "integration",
    "RULE_CHAIN": "rule_chain",
    "RULE_NODE": "rule_node",
    "SCHEDULER_EVENT": "scheduler_event",
    "BLOB_ENTITY": "blob_entity",
    "REPORT_TEMPLATE": "report_template",
    "REPORT": "report",
    "ENTITY_VIEW": "entity_view",
    "WIDGETS_BUNDLE": "widgets_bundle",
    "WIDGET_TYPE": "widget_type",
    "ROLE": "role",
    "GROUP_PERMISSION": "group_permission",
    "TENANT_PROFILE": "tenant_profile",
    "DEVICE_PROFILE": "device_profile",
    "ASSET_PROFILE": "asset_profile",
    "API_USAGE_STATE": "api_usage_state",
    "TB_RESOURCE": "resource", 
    "OTA_PACKAGE": "ota_package",
    "EDGE": "edge",
    "RPC": "rpc",
    "QUEUE": "queue",
    "NOTIFICATION_TARGET": "notification_target",
    "NOTIFICATION_TEMPLATE": "notification_template",
    "NOTIFICATION_REQUEST": "notification_request",
    "NOTIFICATION": "notification",
    "NOTIFICATION_RULE": "notification_rule",
    "QUEUE_STATS": "queue_stats",
    "OAUTH2_CLIENT": "oauth2_client",
    "DOMAIN": "domain",
    "MOBILE_APP": "mobile_app",
    "MOBILE_APP_BUNDLE": "mobile_app_bundle",
    "CALCULATED_FIELD": "calculated_field",
    "CALCULATED_FIELD_LINK": "calculated_field_link",
    "JOB": "job",
    "SECRET": "secret",
    "ADMIN_SETTINGS": "admin_settings",
    "AI_MODEL": "ai_model"
}
class RowAnalyzer(Process):
    

    def __init__(self, read_queue: "Queue[str | None]", dry_run: bool):
        super().__init__()
        self.read_queue = read_queue
        self.dry_run = dry_run
        self.cassandra_cleaner = CassandraCleaner(
            hosts=[os.getenv('CASSANDRA_HOST', 'localhost')],
            username=os.getenv('CASSANDRA_USERNAME', 'cassandra'),
            password=os.getenv('CASSANDRA_PASSWORD', 'cassandra')
        )
        
    
    async def check_rows_in_postgres(self, row_dict: pd.DataFrame, connection_pool: asyncpg.Pool, entity_type: str) -> None | pd.DataFrame:

        table_name: str | None = ENTITY_TABLE_NAME_MAP.get(entity_type)
        
        temp_list_entity_ids: list[str] = row_dict["entity_id"].astype(str).to_list()
        set_entity_ids = set(temp_list_entity_ids)
        
        if not table_name:
            logger.warning(f"Table name for this entity_type not found {entity_type}")
            return
        
        logger.info(f"Checking rows in {table_name} table for worker {self.name}")
        query: str = f"SELECT id from {table_name} where id = ANY($1::uuid[])"
        
        start_fetch_time: float = time.time()
        
        found_rows = None
        
        for attempt in range(10):
            try:    
                found_rows = await connection_pool.fetch(query, set_entity_ids)
                break
            except asyncpg.exceptions.SerializationError as exc:
                logger.warning(f"The query execution failed on {attempt+1} attempt on {self.name} with query {query} with following parameters {set_entity_ids} ")
                logger.warning(f"With the following error: {exc}")
                await asyncio.sleep(2)
        
        if found_rows is None:
            raise RuntimeError(f"Failed to fetch rows {self.name}")
        
        logger.info(msg=f"Found rows for {entity_type}: {len(found_rows)} it took {time.time() - start_fetch_time} seconds for {self.name}")
        found_ids: set[str] = {str(found_row["id"]) for found_row in found_rows}
        
        return row_dict[~row_dict["entity_id"].isin(found_ids)]
            
        
    async def analyzer_event_loop(self) -> None:
        if not self.dry_run:
            self.cassandra_cleaner.connect()
        
        async with asyncpg.create_pool(dsn=DATABASE_DSN, min_size=5, max_size=6) as postgres_pool:
            loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
            try:
                while True:

                    file_path: str | None = await loop.run_in_executor(None, self.read_queue.get)
                    
                    if file_path is None:
                        logger.info(f"Received poison pill. Shutting down analyzer. {self.name}")
                        break

                    await self.get_row_from_file(file_path, postgres_pool)
            finally:
                if not self.dry_run:
                    self.cassandra_cleaner.shutdown()
                
    
    async def get_row_from_file(self, file_path: str, connection_pool: asyncpg.Pool) -> None:
        tasks: list[Coroutine[Any, Any, Any]] = []
        start_time: float = time.time()
        logger.info(f"Start processing {file_path}")
        
        loop = asyncio.get_running_loop()
        logger.info(f"Load file {file_path}")
        csv_data_frame: pd.DataFrame = await loop.run_in_executor(None, pd.read_csv, file_path)
        
        group_by_entity = csv_data_frame.groupby("entity_type")
        logger.info(f"The size of grouping by entity is {group_by_entity.size()}")
        
        for entitye_type, group_data_frame in group_by_entity:
            logger.info(f"Appending tasks for {entitye_type} in {file_path} with {len(group_data_frame)}")
            tasks.append(
                self.check_rows_in_postgres(group_data_frame, connection_pool, str(entitye_type))
            )
        
        logger.info(f"Send request to Postgres for {file_path}")
        results: list[pd.DataFrame | None] = await asyncio.gather(*tasks)
        
        delete_tasks = []
        for obsolete_dataframe_per_type in results:
            if obsolete_dataframe_per_type is None or obsolete_dataframe_per_type.empty:
                continue
            rows: list[dict] = obsolete_dataframe_per_type.to_dict(orient="records")
            for row in rows:
                logger.info(f"Deleting this partition key entity_type: {row['entity_type']}, entity_id: {row['entity_id']}, key: {row['key']}, partition: {row['partition']}")
                
            if self.dry_run:
                continue
            else:
                delete_tasks.append(self.cassandra_cleaner.delete_partitions(rows=rows))
            
        if delete_tasks:
            await asyncio.gather(*delete_tasks)
        try:
            logger.info(f"Remove {file_path}")
            os.remove(file_path)
            logger.info(f"Complete processing {file_path} it took {time.time() - start_time} seconds  for {self.name}")
        except OSError:
            pass
            
            
    @override
    def run(self):
        try:
            asyncio.run(self.analyzer_event_loop())
        except Exception as e:
            logger.exception(f"Caught exception {self.name}")
            raise
