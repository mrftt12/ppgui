from abc import ABC, abstractmethod
from dataclasses import asdict
from datetime import datetime
import json
import tempfile
import traceback
import uuid
from snowflake.snowpark.functions import col, explode

import time
import os
import pickle
from pathlib import Path

import pandas as pd
import snowflake.snowpark

import logging
import multiconductor as mc

from pandapower.auxiliary import ControllerNotConverged
# from multiconductor.pycci.cci_powerflow import LoadflowNotConverged

from snowflake.snowpark.functions import pandas_udtf, PandasDataFrameType
from snowflake.snowpark.types import (
    StringType,
    DecimalType,
    TimestampType,
    LongType,
    PandasDataFrame,
    StructField,
    DateType,
    IntegerType,
    BinaryType,
    ArrayType,
    StructType,
    FloatType,
    TimestampTimeZone,
    DoubleType,
)

from powerflow_pipeline.powerflow import Pipeline
from powerflow_pipeline.util import PowerflowConfig


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CircuitBatchIterator(ABC):
    @abstractmethod
    def get_circuit_batches(self):
        """Return an iterator that yields batches (lists) of circuit IDs."""
        pass

class BaseDbCircuitBatchIterator(CircuitBatchIterator):
    @abstractmethod
    def select_circuits_sql(self):
        pass
    
    def get_circuit_batches(self):
        all_circuits_query = self.session.sql(self.select_circuits_sql())
        all_circuits = [(c[0], c[1]) for c in all_circuits_query.collect()]
        return all_circuits

class StaticCircuitBatchIterator(BaseDbCircuitBatchIterator):
    def __init__(self, session, pf_config, circuit_ids, circuit_seg):
        self.circuit_ids = [item.strip() for item in circuit_ids.split(',')] 
        # self.batch_size = batch_size
        self.session = session
        self.pf_config = pf_config
        self.circuit_seg = circuit_seg

    def select_circuits_sql(self):
        in_clause = ", ".join([f"'{s}'" for s in self.circuit_ids])
        filter = "" if self.circuit_seg[2] == float('inf') else f"AND bus_count < {self.circuit_seg[2]}"
        
        return f"""
        WITH Circuits AS (SELECT
            circuit_key,
            COUNT(DISTINCT replace(CONNECTIVITY_NODEID, 's', '')) as bus_count
        FROM
            {self.pf_config.get_connectivity_table_database()}.{self.pf_config.get_connectivity_table_schema()}.{self.pf_config.get_connectivity_table()}
        WHERE
            circuit_key in ({in_clause})
        GROUP BY
            circuit_key
        )
        SELECT * FROM Circuits WHERE bus_count >= {self.circuit_seg[1]} {filter} ORDER BY bus_count DESC
        """
        
class DbCircuitBatchIterator(BaseDbCircuitBatchIterator):
    def __init__(self, session, pf_config, circuit_seg):
        self.session: snowflake.snowpark.Session = session
        # self.batch_size = batch_size
        self.circuit_seg = circuit_seg
        self.pf_config: PowerflowConfig = pf_config
        
    def select_circuits_sql(self):
        filter = "" if self.circuit_seg[2] == float('inf') else f"AND bus_count < {self.circuit_seg[2]}"
        return f"""
        WITH Circuits AS ( SELECT
            circuit_key,
            COUNT(DISTINCT replace(CONNECTIVITY_NODEID, 's', '')) as bus_count
        FROM
            {self.pf_config.get_connectivity_table_database()}.{self.pf_config.get_connectivity_table_schema()}.{self.pf_config.get_connectivity_table()} where circuit_key is not null
        GROUP BY
            circuit_key)
        SELECT * FROM Circuits WHERE bus_count >= {self.circuit_seg[1]} {filter} ORDER BY bus_count DESC
        """

metric_fields  = [
    StructField("FUNCTION_NAME", StringType(), nullable=False),
    StructField("EXECUTION_TIME", FloatType(), nullable=False),
    StructField("RUNID", StringType(), nullable=False),
]

OUTPUT_FIELDS = [
    StructField("POWER_SYSTEM_RESOURCE_MRID", StringType(), nullable=True),
    StructField("POWER_SYSTEM_RESOURCE_TYPE", StringType(), nullable=False),
    StructField("CONDUCTING_EQUIPMENTID", StringType(), nullable=True),
    StructField("LINKED_EQUIPTYPE", StringType(), nullable=False),
    StructField("REPORTED_DTTM", TimestampType(), nullable=True),
    StructField("REPORTED_DT", DateType(), nullable=False),
    StructField("HOUR_ID", IntegerType(), nullable=False),
    StructField("SVVOLTAGE_V_A_PU", DecimalType(18, 6), nullable=False),
    StructField("SVVOLTAGE_A_ANGLE", DecimalType(18, 6), nullable=False),
    StructField("SVVOLTAGE_V_B_PU", DecimalType(18, 6), nullable=False),
    StructField("SVVOLTAGE_B_ANGLE", DecimalType(18, 6), nullable=False),
    StructField("SVVOLTAGE_V_C_PU", DecimalType(18, 6), nullable=False),
    StructField("SVVOLTAGE_C_ANGLE", DecimalType(18, 6), nullable=False),
    StructField("SVVOLTAGE_V_N_PU", DecimalType(18, 6), nullable=False),
    StructField("SVVOLTAGE_N_ANGLE", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_P_A_MW", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_Q_A_MVAR", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_P_B_MW", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_Q_B_MVAR", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_P_C_MW", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_Q_C_MVAR", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_P_N_MW", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_Q_N_MVAR", DecimalType(18, 6), nullable=False),
    StructField("SVCURRENT_CURRENT_A_KA", DecimalType(18, 6), nullable=False),
    StructField("SVCURRENT_A_ANGLE", DecimalType(18, 6), nullable=False),
    StructField("SVCURRENT_CURRENT_B_KA", DecimalType(18, 6), nullable=False),
    StructField("SVCURRENT_B_ANGLE", DecimalType(18, 6), nullable=False),
    StructField("SVCURRENT_CURRENT_C_KA", DecimalType(18, 6), nullable=False),
    StructField("SVCURRENT_C_ANGLE", DecimalType(18, 6), nullable=False),
    StructField("SVCURRENT_CURRENT_N_KA", DecimalType(18, 6), nullable=False),
    StructField("SVCURRENT_N_ANGLE", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_P_LOAD_A_MW", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_Q_LOAD_A_MVAR", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_P_LOAD_B_MW", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_Q_LOAD_B_MVAR", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_P_LOAD_C_MW", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_Q_LOAD_C_MVAR", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_P_LOAD_N_MW", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_Q_LOAD_N_MVAR", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_P_GEN_A_MW", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_Q_GEN_A_MVAR", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_P_GEN_B_MW", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_Q_GEN_B_MVAR", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_P_GEN_C_MW", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_Q_GEN_C_MVAR", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_P_GEN_N_MW", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_Q_GEN_N_MVAR", DecimalType(18, 6), nullable=False),
    StructField("UNBALANCEVOLTAGEPERCENTAGE", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_P_FROMBUS_A_MW", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_Q_FROMBUS_A_MVAR", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_P_FROMBUS_B_MW", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_Q_FROMBUS_B_MVAR", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_P_FROMBUS_C_MW", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_Q_FROMBUS_C_MVAR", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_P_FROMBUS_N_MW", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_Q_FROMBUS_N_MVAR", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_P_TOBUS_A_MW", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_Q_TOBUS_A_MVAR", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_P_TOBUS_B_MW", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_Q_TOBUS_B_MVAR", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_P_TOBUS_C_MW", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_Q_TOBUS_C_MVAR", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_P_TOBUS_N_MW", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_Q_TOBUS_N_MVAR", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_P_LOSS_A_MW", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_Q_LOSS_A_MVAR", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_P_LOSS_B_MW", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_Q_LOSS_B_MVAR", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_P_LOSS_C_MW", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_Q_LOSS_C_MVAR", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_P_LOSS_N_MW", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_Q_LOSS_N_MVAR", DecimalType(18, 6), nullable=False),
    StructField("SVCURRENT_CURRENT_FROMBUS_A_KA", DecimalType(18, 6), nullable=False),
    StructField("SVCURRENT_CURRENT_FROMBUS_B_KA", DecimalType(18, 6), nullable=False),
    StructField("SVCURRENT_CURRENT_FROMBUS_C_KA", DecimalType(18, 6), nullable=False),
    StructField("SVCURRENT_CURRENT_FROMBUS_N_KA", DecimalType(18, 6), nullable=False),
    StructField("SVCURRENT_CURRENT_TOBUS_A_KA", DecimalType(18, 6), nullable=False),
    StructField("SVCURRENT_CURRENT_TOBUS_B_KA", DecimalType(18, 6), nullable=False),
    StructField("SVCURRENT_CURRENT_TOBUS_C_KA", DecimalType(18, 6), nullable=False),
    StructField("SVCURRENT_CURRENT_TOBUS_N_KA", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_PERCENT_LOAD_A", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_PERCENT_LOAD_B", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_PERCENT_LOAD_C", DecimalType(18, 6), nullable=False),
    StructField("SVPOWERFLOW_PERCENT_LOAD_N", DecimalType(18, 6), nullable=False),
    StructField("METRICS", StringType(), nullable=False),
    StructField("YEAR_ID", IntegerType(), nullable=False),
    StructField("MONTH_ID", IntegerType(), nullable=False),
    StructField("STUDY_ID", StringType(), nullable=True),
    StructField("EQUIPMENT_STATE", StringType(), nullable=True),
    StructField("CHANGED_DATE", DateType(), nullable=True),
    StructField("EDW_CREATED_DATE", TimestampType(), nullable=False),
    StructField("EDW_CREATED_BY", StringType(), nullable=False),
    StructField("EDW_MODIFIED_DATE", TimestampType(), nullable=False),
    StructField("EDW_MODIFIED_BY", StringType(), nullable=False),
    StructField("EDW_BATCH_ID", StringType(), nullable=False),
    StructField("EDW_BATCH_DETAIL_ID", StringType(), nullable=False),
    StructField("EDW_LAST_DML_CD", StringType(), nullable=False),

]

LOAD_COLS = [
    "CIRCUIT_KEY",
    "POWER_SYSTEM_RESOURCE_MRID",
    "TAG_NAME",
    "REPORTED_DTTM",
    "YEAR_ID",
    "MONTH_ID",
    "BATCH_PERIOD",
    "READINGS",
    "CA_BLOB",
    "EXTRA_METADATA",
]


class LoadQuery:
    def __init__(self, pf_config, circuit_batch: list, start_time_dt: str, end_time_dt: str, run_tag: str):
        self.circuit_batch = circuit_batch
        self.run_tag = run_tag
        self.pf_config: PowerflowConfig = pf_config
        self.start_time_dt = start_time_dt
        self.end_time_dt = end_time_dt

    def parameters(self):
        days_per_batch = self.pf_config.get_days_per_batch()
        hours_per_batch = max(1, round(float(days_per_batch) * 24))        
        params = []
        params.extend(self.circuit_batch)
        params.append(hours_per_batch)
        params.append(self.start_time_dt)
        params.append(self.end_time_dt)
        params.append(self.start_time_dt)
        params.append(self.start_time_dt)
        params.append(self.start_time_dt)
        params.append(self.run_tag)

        return params

    def sql(self):
        placeholders = ', '.join(['?'] * len(self.circuit_batch))

        return f"""
WITH Circuit_Sizes AS (
    SELECT
        circuit_key,
        COUNT(DISTINCT replace(CONNECTIVITY_NODEID, 's', '')) as bus_count
    FROM
        {self.pf_config.get_connectivity_table_database()}.{self.pf_config.get_connectivity_table_schema()}.{self.pf_config.get_connectivity_table()}
    WHERE
        circuit_key IN ({placeholders})
    GROUP BY
        circuit_key
),
Circuit_Batches AS (
    SELECT
        circuit_key,
        bus_count,
        ? as hours_per_batch
    FROM
        Circuit_Sizes
),
load AS (
    SELECT
        *,
        CASE
            WHEN POWER_SYSTEM_RESOURCE_TYPE = 'STRUCTURE' THEN STRUCT_NUM
            WHEN POWER_SYSTEM_RESOURCE_TYPE = 'DER_PROJECT' THEN PROJECT_ID || '_' || TECH_TYPE
        END AS NAME,
        cb.hours_per_batch
    FROM
        {self.pf_config.get_load_table_database()}.{self.pf_config.get_load_table_schema()}.{self.pf_config.get_load_table()} l
        join Circuit_Batches cb on l.POWER_SYSTEM_RESOURCE_KEY = cb.circuit_key
        WHERE 1=1
        AND l.REPORTED_DTTM >= ? AND l.REPORTED_DTTM <= ?
),
Load_Profile AS (
    SELECT
        POWER_SYSTEM_RESOURCE_KEY CIRCUIT_KEY,
        POWER_SYSTEM_RESOURCE_MRID,
        REPORTED_DTTM ::TIMESTAMP_NTZ(9) AS REPORTED_DTTM,
        year_id,
        month_id,
        date_trunc(
            'hour',
            dateadd(
                'hour',
                ci.hours_per_batch * FLOOR(
                    DATEDIFF(
                        'hour',
                        date_trunc('hour', ? ::timestamp),
                        l.REPORTED_DTTM
                    ) / ci.hours_per_batch
                ),
                ? ::timestamp
            )
        ) as batch_period,
        ROW_NUMBER() OVER (
            PARTITION BY l.POWER_SYSTEM_RESOURCE_KEY,
            year_id,
            month_id,
            FLOOR(
                DATEDIFF(
                    'hour',
                    date_trunc('hour', ? ::timestamp),
                    l.REPORTED_DTTM
                ) / ci.hours_per_batch
            )
            ORDER BY
                year_id
        ) Rnk,
        ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'NAME',
                NAME,
                'RESOURCE_TYPE',
                POWER_SYSTEM_RESOURCE_TYPE,
                'MEASURE_VALUE',
                MEASURE_VALUE,
                'MEASURE_VALUE_A',
                MEASURE_VALUE_A,
                'MEASURE_VALUE_B',
                MEASURE_VALUE_B,
                'MEASURE_VALUE_C',
                MEASURE_VALUE_C,
                'RDNG_MEAS_MVAR',
                RDNG_MEAS_MVAR,
                'RDNG_MEAS_MVAR_A',
                RDNG_MEAS_MVAR_A,
                'RDNG_MEAS_MVAR_B',
                RDNG_MEAS_MVAR_B,
                'RDNG_MEAS_MVAR_C',
                RDNG_MEAS_MVAR_C
            )
        ) AS READINGS
    FROM
        load l
        JOIN Circuit_Batches ci ON l.circuit_key = ci.circuit_key
    GROUP BY
        POWER_SYSTEM_RESOURCE_KEY,
        POWER_SYSTEM_RESOURCE_MRID,
        REPORTED_DTTM,
        year_id,
        month_id,
        ci.hours_per_batch
) 
SELECT
    ? TAG_NAME,
    OBJECT_CONSTRUCT(
        'EDW_CREATED_DATE', TO_CHAR(SYSDATE(), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'),
        'EDW_CREATED_BY', TO_CHAR(CURRENT_USER()),
        'EDW_MODIFIED_DATE', TO_CHAR(SYSDATE(), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'),
        'EDW_MODIFIED_BY', TO_CHAR(CURRENT_USER()),
        'EWD_BATCH_ID', TO_CHAR(DATE_PART(epoch_microsecond, CURRENT_TIMESTAMP())),
        'EWD_BATCH_DETAIL_ID', TO_CHAR(DATE_PART(epoch_microsecond, CURRENT_TIMESTAMP())),
        'EDW_LAST_DML_CD', 'I'
    )::VARCHAR AS EXTRA_METADATA,
    --cast(CASE WHEN rnk=1 THEN BASE64_ENCODE(ca.preload_encoded_ca) ELSE NULL END as varchar) ca_blob,
    CASE WHEN rnk=1 THEN ca.preload_encoded_ca ELSE NULL END CA_BLOB,
    lf.*
FROM
    Load_Profile lf
    JOIN {self.pf_config.get_database()}.{self.pf_config.get_database_schema()}.{self.pf_config.get_encoded_circuits_table()} ca ON lf.CIRCUIT_KEY = ca.circuit_key
WHERE
    ca.preload_encoded_ca IS NOT NULL
    """

pf_load_values_type = StructType(
    [
        StructField("NAME", StringType()),
        StructField("RESOURCE_TYPE", StringType()),
        StructField("MEASURE_VALUE", DecimalType()),
        StructField("MEASURE_VALUE_A", DecimalType()),
        StructField("MEASURE_VALUE_B", DecimalType()),
        StructField("MEASURE_VALUE_C", DecimalType()),
        StructField("RDNG_MEAS_MVAR", DecimalType()),
        StructField("RDNG_MEAS_MVAR_A", DecimalType()),
        StructField("RDNG_MEAS_MVAR_B", DecimalType()),
        StructField("RDNG_MEAS_MVAR_C", DecimalType()),
    ]
)

output_metadata = StructType(
    [
        StructField("CREATED_BY_USER", StringType()),
    ]
)

class SnowflakePowerflowRunner:
    def __init__(
        self,
        session: snowflake.snowpark.Session,
        pf_config_json: str,
        start_time: str,
        end_time: str,
        circuit_keys: str,
        circuit_size: str,
    ):
        self.logger = logging.getLogger("SnowflakePowerflowRunner_Logger")
        self.session = session
        self.pf_config_json = pf_config_json
        self.start_time = start_time
        self.end_time = end_time
        self.circuit_keys = circuit_keys
        self.pf_config = PowerflowConfig()
        self.pf_config.load_config_from_json(self.pf_config_json)
        # self.pf_config = self.load_config_from_stage(self.pf_config_json, "env.json")

    def load_config_from_stage(self, stage_name, file_path):
        file_format = 'json_format'
        with self.session.connection.cursor() as cursor:
            cursor.execute(
                f"CREATE OR REPLACE FILE FORMAT {file_format} TYPE = 'JSON'")
            sql_query = f"""
            --SELECT PARSE_JSON($1) as parsed_json
            SELECT $1 as json
            FROM @{stage_name}/{file_path}
            (FILE_FORMAT => '{file_format}')
            """
            cursor.execute(sql_query)
            result = cursor.fetchall()
            
            for row in result:
                parsed_json = row[0]
                pf_config = PowerflowConfig()
                pf_config.load_config_from_json(parsed_json)
                return pf_config            
                    
    def get_circuit_segment(self, segment_name):
        curcuit_seg = self.pf_config.get_circuit_segmenation()
        updated_tuples = [(k, float('inf') if v == 0 else v) for k, v in curcuit_seg.items()]
        sorted_result = sorted(updated_tuples, key=lambda item: item[1])
        sorted_result.insert(0, ('', 0))
        bus_count_min = 0
        bus_count_max = 100_000_000
        for item1, item2 in zip(sorted_result, sorted_result[1:]):
            if item2[0] == segment_name:
                bus_count_min = item1[1]
                bus_count_max = item2[1]
        return (segment_name, bus_count_min, bus_count_max)
                       
    
    def run_pf_batches(self):
        e2e_start_time = time.perf_counter()
        run_tag = str(uuid.uuid4())
        async_queries = []
        logger.info(f"run_pf_batches::multiconductor version: {mc.__version__}")

        segments = [x for x in self.pf_config.get_circuit_segmenation().keys()]
        logger.info(f"segments: {segments}")
        for seg_name in segments:
            self.circuit_seg = self.get_circuit_segment(seg_name)
            circuit_batch_iterator = (
                StaticCircuitBatchIterator(self.session, self.pf_config, self.circuit_keys, self.circuit_seg)
                if self.circuit_keys and self.circuit_keys != ""
                else DbCircuitBatchIterator(self.session, self.pf_config, self.circuit_seg)
            )
            batch = circuit_batch_iterator.get_circuit_batches() 
            logger.info(f"Batch : {seg_name} {batch}")

            
            stage_location=f"{self.pf_config.get_database()}.{self.pf_config.get_database_schema()}.{self.pf_config.get_stage_name()}"
            logger.info(f"Using stage location: {stage_location}")
            
            udtf_name = "PowerFlowAnalysis" if seg_name == "" else "PowerFlowAnalysis_" + seg_name.replace(" ", "_").replace("-", "_").upper()
            logger.info(f"UDTF name: {udtf_name}")

            @pandas_udtf(
                packages=(
                    "snowflake-snowpark-python",
                    "geojson",
                    "deepdiff",
                    "lxml",
                    "numba",
                    "typing_extensions==4.15.0",
                    "numpy==1.26.4",
                    "packaging==25.0",
                    "tqdm==4.67.1",
                    "pandas==2.3.2",
                    "networkx==3.4.2",
                    "scipy==1.15.3",
                    "Jinja2",
                ),
                name = f"{self.pf_config.get_database()}.{self.pf_config.get_database_schema()}.{udtf_name}",
                replace = True,
                stage_location=stage_location,
                output_schema=PandasDataFrameType(
                    [f.datatype for f in OUTPUT_FIELDS], [f.name for f in OUTPUT_FIELDS]
                ),
                input_types=[
                    PandasDataFrameType(
                        [
                            StringType(),
                            StringType(),
                            StringType(),
                            TimestampType(9),
                            IntegerType(),
                            IntegerType(),
                            TimestampType(9),
                            ArrayType(pf_load_values_type),
                            BinaryType(),
                            StringType(),
                        ]
                    )
                ],
                input_names=[lcol.upper() for lcol in LOAD_COLS],
                statement_params={"STATEMENT_TIMEOUT_IN_SECONDS": 7200},
                is_permanent=True,
            )
            class PowerflowUDTF:
                def __init__(self):
                    self.logger = logging.getLogger("PowerflowUDTF_Logger")
                    
                def create_empty_df(self):
                    dtypes = {}
                    for f in OUTPUT_FIELDS:
                        match f.datatype:
                            case StringType():
                                dtypes[f.name] = pd.Series(dtype='str')
                            case IntegerType():
                                dtypes[f.name] = pd.Series(dtype='int64')
                            case DecimalType():
                                dtypes[f.name] = pd.Series(dtype='float64')
                            case TimestampType():
                                dtypes[f.name] = pd.Series(dtype='datetime64[ns]')                                
                            case DateType():
                                dtypes[f.name] = pd.Series(dtype='datetime64[ns]')                                
                    return pd.DataFrame(dtypes)
                
                def end_partition(self, df: PandasDataFrame):
                    start_time = time.perf_counter()
                    pf_pipeline_pkl = df["CA_BLOB"].dropna().iloc[0]
                    tag_name = df["TAG_NAME"].iloc[0]
                    circuit_key = df["CIRCUIT_KEY"].iloc[0]

                    pf_pipeline: Pipeline = pickle.loads(pf_pipeline_pkl)

                    dfs = []
                    run_count = 0
                    error_count = 0
                    
                    # for index, row in df.sample(frac=0.01, random_state=42).iterrows():
                    for index, row in df.iterrows():
                        
                        reported_dttm = row['REPORTED_DTTM']
                        exception_log = None
                        try:
                            results = pf_pipeline.run_analysis(row)
                            results.loc[0, 'METRICS'] = pf_pipeline.get_metrics()
                            results.loc[1:, 'METRICS'] = ""
                            dfs.append(results)
                        # except (ControllerNotConverged, LoadflowNotConverged) as e:
                        except (ControllerNotConverged) as e:
                            error_count = error_count + 1
                            exception_log = {
                                "pf_evt_type": "not_converged",
                                "ckey": circuit_key,
                                "ts": str(reported_dttm),
                                "err_msg":  str(e),
                                "study_id":  tag_name,
                            }
                        except Exception as e:
                            error_count = error_count + 1
                            exception_log = {
                                "pf_evt_type": "pf_error",
                                "ckey": circuit_key,
                                "ts": str(reported_dttm),
                                "err_msg":  str(e),
                                "study_id":  tag_name,
                            }
                        finally:
                            if exception_log:
                                self.logger.info(json.dumps(exception_log))
                                
                        run_count = run_count + 1
                                                
                    end_time = time.perf_counter()
                    elapsed_time = end_time - start_time
                    df_all =  pd.concat(dfs, ignore_index=True) if len(dfs) > 0 else self.create_empty_df()
                    summary_log = {
                        "pf_evt_type": "ep_summary",
                        "ckey": circuit_key,
                        "part_size": len(df),
                        "runs": run_count,
                        "errors": error_count,
                        "res_rows": len(df_all),
                        "study_id":  tag_name,
                        
                    }
                    self.logger.info(json.dumps(summary_log))                    
                    return df_all

            
            if len(batch) > 0:
                load_query = LoadQuery(self.pf_config, [c[0] for c in batch], self.start_time, self.end_time, run_tag)
                self.logger.info(f"1.........{load_query.parameters()}")
                main_query = self.session.sql(load_query.sql(), load_query.parameters())
                # main_query = self.session.sql(load_query.sql())
                q = main_query.select(
                    PowerflowUDTF(*LOAD_COLS).over(
                        partition_by=["CIRCUIT_KEY", "batch_period"]
                    )
                )
            
                profile_count = main_query.count()
                logger.info(f"Profile count: {profile_count}")
                
                batch_summary = {
                    "pf_evt_type": "batch_summary",
                    "profile_count": profile_count,
                    "circuit_count": len(batch),
                    "mc_version": mc.__version__,
                    "study_id":  run_tag,
                }
                self.logger.info(json.dumps(batch_summary))    
                
                async_query = q.write.mode("append").save_as_table(
                    table_name=f"{self.pf_config.get_database()}.{self.pf_config.get_database_schema()}.{self.pf_config.get_output_table()}", block=False,
                    column_order="name"
                )
                
                async_queries.append(async_query)
            
        # Monitor progress of all batches
        pf_completion_time = time.perf_counter()
        total_queries = len(async_queries)
        self.logger.info("in progress check loop starting...")
        while True:
            completed = sum(1 for q in async_queries if q.is_done())
            if completed == total_queries:
                self.logger.info(f"COMPLETED {completed}/{total_queries}")
                break
                
            running = total_queries - completed
            time.sleep(15)
        e2e_end_time = time.perf_counter()
        elapsed_time = e2e_end_time - e2e_start_time
        output_write_time = e2e_end_time - pf_completion_time
        self.logger.info(f"Output write time: {output_write_time}")
        self.logger.info(f"E2E completion time: {elapsed_time}")
        
        # query_id = self.session.sql("select LAST_QUERY_ID() query_id").collect()[0]['QUERY_ID']
        # with self.session.connection.cursor() as cur:
        #     cur.execute("insert into GRIDMOD_DEV_TD.UC_POC.POWERFLOW_RUN(QUERY_ID, CIRCUIT_SIZE, CIRCUIT_BATCH) values (?, ?, ?)", 
        #                 (query_id, seg_name, str(batch)))        
        
        return run_tag