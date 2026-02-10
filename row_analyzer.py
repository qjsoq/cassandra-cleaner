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
loger = logging.getLogger(__name__)
DATABASE_DSN = f"postgresql://{os.getenv('DATABASE_USERNAME', 'thingsboard')}:{urllib.parse.quote_plus(os.getenv('DATABASE_PASSWORD', 'cr67SDQQ?fEvA>m6KX8X]:|C'))}@{os.getenv('DATABASE_HOST', "localhost")}:5432/thingsboard"
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
    

    def __init__(self, read_queue: "Queue[str]"):
        super().__init__()
        self.read_queue = read_queue
        self.cassandra_cleaner = CassandraCleaner(
            hosts=[os.getenv('CASSANDRA_HOST', 'localhost')],
            username=os.getenv('CASSANDRA_USERNAME', 'cassandra'),
            password=os.getenv('CASSANDRA_PASSWORD', 'cassandra'), 
            port=9042
        )
    
    async def check_rows_in_postgres(self, row_dict: pd.DataFrame, connection_pool: asyncpg.Pool, entity_type: str) -> None | pd.DataFrame:

        table_name: str | None = ENTITY_TABLE_NAME_MAP.get(entity_type)
        
        entity_ids: list[str] = row_dict["entity_id"].astype(str).to_list()
        if not table_name:
            loger.warning(f"Table name for this entity_type not found {entity_type}")
            return
        
        loger.info(f"Checking rows in {table_name}")
        query: str = f"SELECT id from {table_name} where id = ANY($1::uuid[])"
        
        found_rows = await connection_pool.fetch(query, entity_ids)
        
        loger.info(msg=f"Found rows for {entity_type}: {len(found_rows)}")
        found_ids: list[str] = [str(found_row["id"]) for found_row in found_rows]
        
        return row_dict[~row_dict["entity_id"].isin(found_ids)]        
            
        
    async def analyzer_event_loop(self) -> NoReturn:
        self.cassandra_cleaner.connect()
        
        async with asyncpg.create_pool(dsn=DATABASE_DSN, min_size=5, max_size=6) as postgres_pool:
            loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
            try:
                while True:
                    file_path: str = await loop.run_in_executor(None, self.read_queue.get)

                    await self.get_row_from_file(file_path, postgres_pool)
            finally:
                self.cassandra_cleaner.shutdown()
                
    
    async def get_row_from_file(self, file_path: str, connection_pool: asyncpg.Pool) -> None:
        tasks: list[Coroutine[Any, Any, Any]] = []
        start_time: float = time.time()
        loger.info(f"Start processing {file_path}")
        
        loop = asyncio.get_running_loop()
        loger.info(f"Load file {file_path}")
        csv_data_frame: pd.DataFrame = await loop.run_in_executor(None, pd.read_csv, file_path)
        
        group_by_entity = csv_data_frame.groupby("entity_type")
        
        for entitye_type, group_data_frame in group_by_entity:
            loger.info(f"Appendind tasks for {entitye_type} in {file_path}")
            tasks.append(
                self.check_rows_in_postgres(group_data_frame, connection_pool, str(entitye_type))
            )
        
        loger.info(f"Send request to Postgres for {file_path}")
        results: list[pd.DataFrame] = await asyncio.gather(*tasks)
        delete_tasks = []
        for obsolete_dataframe_per_type in results:
            rows = obsolete_dataframe_per_type.to_dict(orient="records")
            delete_tasks.append(self.cassandra_cleaner.delete_partitions(rows=rows))
        if delete_tasks:
            await asyncio.gather(*delete_tasks)
        try:
            loger.info(f"Remove {file_path}")
            os.remove(file_path)
            loger.info(f"Complete processing {file_path} it took {time.time() - start_time} seconds")
        except OSError:
            pass
            
            
    @override
    def run(self):
        try:
            asyncio.run(self.analyzer_event_loop())
        except Exception as e:
            loger.exception(f"Caught exception {e.with_traceback}")
