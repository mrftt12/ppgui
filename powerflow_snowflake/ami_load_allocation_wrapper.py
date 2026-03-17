import json
import pickle
import time
import snowflake.snowpark
import logging

from powerflow_pipeline.powerflow import Pipeline
from powerflow_pipeline.util import PowerflowConfig
from powerflow_snowflake.load_allocation_query import LoadAllocationQuery

import pandas as pd

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
from powerflow_snowflake.load_allocation_wrapper import (
    OUTPUT_FIELDS,
    create_measurements_df,
)
from powerflow_snowflake.batch_runner import LoadQuery, pf_load_values_type, LOAD_COLS
from sce.load_allocation_utils import PACKAGE_DEPENDENCIES

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_load_alloc_output_dataframe(data: list[dict]) -> pd.DataFrame:
    """
    Creates a pandas DataFrame from a list of dictionaries conforming to the
    Snowflake UDTF schema.
    """
    df = pd.DataFrame(data)

    type_mapping = {
        "POWER_SYSTEM_RESOURCE_MRID": "string",
        "POWER_SYSTEM_RESOURCE_KEY": "string",
        "POWER_SYSTEM_RESOURCE_NUM": "string",
        "MEASURE_VALUE": "float64",
        "MEASURE_VALUE_A": "float64",
        "MEASURE_VALUE_B": "float64",
        "MEASURE_VALUE_C": "float64",
        "RDNG_MEAS_MVAR": "float64",
        "RDNG_MEAS_MVAR_A": "float64",
        "RDNG_MEAS_MVAR_B": "float64",
        "RDNG_MEAS_MVAR_C": "float64",
        "UNIT_SYMBOL": "string",
        "MEASUREMENT_TYPE": "string",
        "POWER_SYSTEM_RESOURCE_TYPE": "string",
        "YEAR_ID": "Int64",  
        "MONTH_ID": "Int64",
        "PROFILE_TYPE": "string",
        "PROFILE_STATE": "string",
        "EDW_CREATED_BY": "string",
        "EDW_MODIFIED_BY": "string",
        "EDW_BATCH_ID": "Int64",
        "EDW_BATCH_DETAIL_ID": "Int64",
        "EDW_LAST_DML_CD": "string",
    }

    for col, dtype in type_mapping.items():
        if col in df.columns:
            df[col] = df[col].astype(dtype)

    datetime_cols = [
        "REPORTED_DTTM",
        "REPORTED_DT",
        "EDW_CREATED_DATE",
        "EDW_MODIFIED_DATE",
    ]

    for col in datetime_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])

    # Ensure all columns exist in the output, even if missing in input data
    all_fields = [
        "POWER_SYSTEM_RESOURCE_MRID",
        "POWER_SYSTEM_RESOURCE_KEY",
        "POWER_SYSTEM_RESOURCE_NUM",
        "REPORTED_DTTM",
        "REPORTED_DT",
        "MEASURE_VALUE",
        "MEASURE_VALUE_A",
        "MEASURE_VALUE_B",
        "MEASURE_VALUE_C",
        "RDNG_MEAS_MVAR",
        "RDNG_MEAS_MVAR_A",
        "RDNG_MEAS_MVAR_B",
        "RDNG_MEAS_MVAR_C",
        "UNIT_SYMBOL",
        "MEASUREMENT_TYPE",
        "POWER_SYSTEM_RESOURCE_TYPE",
        "YEAR_ID",
        "MONTH_ID",
        "PROFILE_TYPE",
        "PROFILE_STATE",
        "EDW_CREATED_DATE",
        "EDW_CREATED_BY",
        "EDW_MODIFIED_DATE",
        "EDW_MODIFIED_BY",
        "EDW_BATCH_ID",
        "EDW_BATCH_DETAIL_ID",
        "EDW_LAST_DML_CD",
    ]

    return df.reindex(columns=all_fields)


class AMILoadAllocationQuery:
    def __init__(self, pf_config: PowerflowConfig, circuit_key, start_dt, end_dt):
        self.circuit_key = circuit_key
        self.start_dt = start_dt
        self.end_dt = end_dt
        self.pf_config: PowerflowConfig = pf_config

    def sql(self):
        return f"""
WITH 
load AS (
    SELECT
        *,
        CASE
            WHEN POWER_SYSTEM_RESOURCE_TYPE = 'STRUCTURE' THEN STRUCT_NUM
            WHEN POWER_SYSTEM_RESOURCE_TYPE = 'DER_PROJECT' THEN PROJECT_ID || '_' || TECH_TYPE
        END AS NAME
    FROM
        {self.pf_config.get_load_table_database()}.{self.pf_config.get_load_table_schema()}.{self.pf_config.get_load_table()} l
        WHERE 1=1
        AND l.power_system_resource_key = '{self.circuit_key}'
        AND l.REPORTED_DTTM >= '{self.start_dt}' and l.REPORTED_DTTM <= '{self.end_dt}'
),
LoadConnectivity as (
    select feeder_mrid, conducting_equipmentid, structure_mrid, structure_num 
    from {self.pf_config.get_connectivity_table_database()}.{self.pf_config.get_connectivity_table_schema()}.{self.pf_config.get_connectivity_table()} 
    where circuit_key = '{self.circuit_key}' and linked_equiptype = 'LOAD'
),
Load_Profile AS (
    SELECT
        POWER_SYSTEM_RESOURCE_KEY CIRCUIT_KEY,
        POWER_SYSTEM_RESOURCE_MRID,
        REPORTED_DTTM ::TIMESTAMP_NTZ(9) AS REPORTED_DTTM,
        YEAR_ID,
        MONTH_ID,
        ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'NAME',
                NAME,
                'CONDUCTING_EQUIPMENTID',
                lc.CONDUCTING_EQUIPMENTID,
                'STRUCTURE_MRID',
                lc.STRUCTURE_MRID,
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
        load l join LoadConnectivity lc on l.NAME=lc.STRUCTURE_NUM
    GROUP BY
        POWER_SYSTEM_RESOURCE_KEY,
        POWER_SYSTEM_RESOURCE_MRID,
        REPORTED_DTTM,
        year_id,
        month_id
) 
SELECT
    'tag1' TAG_NAME,
    OBJECT_CONSTRUCT(
        'EDW_CREATED_DATE', TO_CHAR(SYSDATE(), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'),
        'EDW_CREATED_BY', TO_CHAR(CURRENT_USER()),
        'EDW_MODIFIED_DATE', TO_CHAR(SYSDATE(), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'),
        'EDW_MODIFIED_BY', TO_CHAR(CURRENT_USER()),
        'EDW_BATCH_ID', TO_CHAR(DATE_PART(epoch_microsecond, CURRENT_TIMESTAMP())),
        'EDW_BATCH_DETAIL_ID', TO_CHAR(DATE_PART(epoch_microsecond, CURRENT_TIMESTAMP())),
        'EDW_LAST_DML_CD', 'I'
    )::VARCHAR AS EXTRA_METADATA,
    ca.preload_encoded_ca CA_BLOB,
    lf.*
FROM
    Load_Profile lf
    JOIN GRIDMOD_DEV_TD.UC_POC.NMM_F_TOPOLOGICALNODE_ENCODED_CIRCUITS_C_MC11 ca ON lf.CIRCUIT_KEY = ca.circuit_key
WHERE
    ca.preload_encoded_ca IS NOT NULL
"""


ami_load_alloc_values_type = StructType(
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

AMI_LOAD_ALLOC_IN_COLS = [
    "CIRCUIT_KEY",
    "POWER_SYSTEM_RESOURCE_MRID",
    "TAG_NAME",
    "REPORTED_DTTM",
    "YEAR_ID",
    "MONTH_ID",
    "READINGS",
    "CA_BLOB",
    "EXTRA_METADATA",
]


class AMILoadAllocation:
    def __init__(
        self,
        session: snowflake.snowpark.Session,
        pf_config_json: str,
        start_time: str,
        end_time: str,
        circuit_key: str,
    ):
        self.logger = logging.getLogger("AMILoadAllocation_Logger")
        self.session = session
        self.pf_config_json = pf_config_json
        self.start_time = start_time
        self.end_time = end_time
        self.circuit_key = circuit_key
        self.pf_config = PowerflowConfig()
        self.pf_config.load_config_from_json(self.pf_config_json)

    def run_load_allocation(self):
        stage_location = f"{self.pf_config.get_database()}.{self.pf_config.get_database_schema()}.{self.pf_config.get_stage_name()}"

        @pandas_udtf(
            packages=PACKAGE_DEPENDENCIES,
            name=f"{self.pf_config.get_database()}.{self.pf_config.get_database_schema()}.AMILoadAllocation",
            replace=True,
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
                        ArrayType(ami_load_alloc_values_type),
                        BinaryType(),
                        StringType(),
                    ]
                )
            ],
            input_names=[lcol.upper() for lcol in AMI_LOAD_ALLOC_IN_COLS],
            statement_params={"STATEMENT_TIMEOUT_IN_SECONDS": 7200},
            is_permanent=True,
        )
        class AMILoadAllocationUDTF:
            def __init__(self):
                self.logger = logging.getLogger("AMILoadAllocationUDTF_Logger")

            def end_partition(self, df: PandasDataFrame):
                start_time = time.perf_counter()
                pf_pipeline_pkl = df["CA_BLOB"].dropna().iloc[0]
                pf_pipeline: Pipeline = pickle.loads(pf_pipeline_pkl)
                dfs = []
                for index, row in df.iterrows():
                    data = pf_pipeline.run_load_allocation("AMI_ALLOCATION", row)
                    dfs.append(create_load_alloc_output_dataframe(data))
                df_all = (
                    pd.concat(dfs, ignore_index=True)
                    if len(dfs) > 0
                    else create_measurements_df()
                )
                return df_all

        load_query = AMILoadAllocationQuery(
            self.pf_config,
            self.circuit_key,
            self.start_time,
            self.end_time,
        ).sql()
        main_query = self.session.sql(load_query)
        q = main_query.select(
            AMILoadAllocationUDTF(*AMI_LOAD_ALLOC_IN_COLS).over(
                partition_by=["CIRCUIT_KEY"]
            )
        )
        # q.count()
        async_query = q.write.mode("append").save_as_table(
            table_name=f"{self.pf_config.get_load_allocation_table_database()}.{self.pf_config.get_load_allocation_table_schema()}.{self.pf_config.get_load_allocation_table()}",
            block=True,
            column_order="name",
        )
