from abc import ABC, abstractmethod
import pickle
import types
import io
import cloudpickle

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from powerflow_pipeline.powerflow import Pipeline
from powerflow_pipeline.util import PowerflowConfig
from sce.sce_mc_mapping import SCECircuitDataTransformer
from snowflake.snowpark import Session

status_ok = "SUCCESS"
status_failed = "FAILURE"

class CircuitCacheManager(ABC):
    @abstractmethod
    def get_circuit_object(self, circuit_key):
        """Retrieve a pickle object given a circuit key."""
        pass

    @abstractmethod
    def put_circuit_object(self, circuit_key, obj):
        """Add a circuit pickle object to cache."""
        pass


class SnowflakeCircuitCacheManager(CircuitCacheManager):
    def __init__(self, session, pf_config):
        self.session = session
        self.pf_config: PowerflowConfig = pf_config

    def get_circuit_object(self, circuit_key):
        """Retrieve a pickle object given a circuit key."""
        pass
    
    def merge_sql(self):
        return f"""
            MERGE INTO {self.pf_config.get_database()}.{self.pf_config.get_database_schema()}.{self.pf_config.get_encoded_circuits_table()} AS target
            USING (
                SELECT ? AS CIRCUIT_KEY, ? AS STATUS, 
                    ? AS POWERFLOW_VERSION, ? AS PRELOAD_ENCODED_CA, FEEDER_MRID
                    from {self.pf_config.get_connectivity_table_database()}.{self.pf_config.get_connectivity_table_schema()}.{self.pf_config.get_connectivity_table()}
                    where CIRCUIT_KEY = ? limit 1
            ) AS source
            ON target.CIRCUIT_KEY = source.CIRCUIT_KEY
            WHEN MATCHED THEN
                UPDATE SET
                    target.STATUS = source.STATUS,
                    target.FEEDER_MRID = source.FEEDER_MRID,
                    target.POWERFLOW_VERSION = source.POWERFLOW_VERSION,
                    target.PRELOAD_ENCODED_CA = source.PRELOAD_ENCODED_CA,
                    target.EDW_MODIFIED_DATE = SYSDATE(),
                    target.EDW_MODIFIED_BY = CURRENT_USER(),
                    target.EDW_BATCH_ID = DATE_PART(epoch_microsecond, CURRENT_TIMESTAMP()),
                    target.EDW_BATCH_DETAIL_ID = DATE_PART(epoch_microsecond, CURRENT_TIMESTAMP())
            WHEN NOT MATCHED THEN
                INSERT (
                    CIRCUIT_KEY, STATUS, FEEDER_MRID, 
                    POWERFLOW_VERSION, PRELOAD_ENCODED_CA, STUDY_ID, EDW_CREATED_DATE, EDW_CREATED_BY, EDW_MODIFIED_DATE, EDW_MODIFIED_BY,
                    EDW_BATCH_ID, EDW_BATCH_DETAIL_ID, EDW_LAST_DML_CD
                ) VALUES (
                    source.CIRCUIT_KEY, source.STATUS, source.FEEDER_MRID,
                    source.POWERFLOW_VERSION, source.PRELOAD_ENCODED_CA, '-999999', SYSDATE(), CURRENT_USER(), SYSDATE(), CURRENT_USER(),
                    DATE_PART(epoch_microsecond, CURRENT_TIMESTAMP()), DATE_PART(epoch_microsecond, CURRENT_TIMESTAMP()), 'I'
                )
            """

    def put_circuit_object(
        self,
        circuit_key,
        pipeline,
    ):
        with self.session.connection.cursor() as cur:
            logger.info("Building network")
            pipeline.build_net()
            logger.info(f"Building network done: {circuit_key}")
            picked_obj = cloudpickle.dumps(pipeline)
            try:
                cur.execute(
                    self.merge_sql(),
                    (
                        circuit_key,
                        status_ok,
                        "1.0",
                        picked_obj,
                        circuit_key,
                    ),
                )

                self.session.connection.commit()
            except Exception as e:
                exception = cloudpickle.dumps(e)
                cur.execute(
                    self.merge_sql,
                    (
                        circuit_key,
                        status_failed,
                        "1.0",
                        exception,
                    ),
                )
                # raise e TODO: revisit

        return "SUCCESS"
