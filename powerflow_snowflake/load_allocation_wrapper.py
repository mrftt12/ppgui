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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_measurements_df(data=None) -> pd.DataFrame:
    columns = [
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

    dtypes = {
        "POWER_SYSTEM_RESOURCE_MRID": "object",
        "POWER_SYSTEM_RESOURCE_KEY": "object",
        "POWER_SYSTEM_RESOURCE_NUM": "object",
        "REPORTED_DTTM": "datetime64[ns]",
        "REPORTED_DT": "datetime64[ns]",
        "MEASURE_VALUE": "float64",
        "MEASURE_VALUE_A": "float64",
        "MEASURE_VALUE_B": "float64",
        "MEASURE_VALUE_C": "float64",
        "RDNG_MEAS_MVAR": "float64",
        "RDNG_MEAS_MVAR_A": "float64",
        "RDNG_MEAS_MVAR_B": "float64",
        "RDNG_MEAS_MVAR_C": "float64",
        "UNIT_SYMBOL": "object",
        "MEASUREMENT_TYPE": "object",
        "POWER_SYSTEM_RESOURCE_TYPE": "object",
        "YEAR_ID": "Int64",
        "MONTH_ID": "Int64",
        "PROFILE_TYPE": "object",
        "PROFILE_STATE": "object",
        "EDW_CREATED_DATE": "datetime64[ns]",
        "EDW_CREATED_BY": "object",
        "EDW_MODIFIED_DATE": "datetime64[ns]",
        "EDW_MODIFIED_BY": "object",
        "EDW_BATCH_ID": "Int64",
        "EDW_BATCH_DETAIL_ID": "Int64",
        "EDW_LAST_DML_CD": "object",
    }

    if data is None:
        return pd.DataFrame({col: pd.Series(dtype=dt) for col, dt in dtypes.items()})

    df = pd.DataFrame(data)

    for col in columns:
        if col not in df.columns:
            df[col] = None

    df = df[columns]

    for col, dtype in dtypes.items():
        if dtype == "datetime64[ns]":
            df[col] = pd.to_datetime(df[col])
        else:
            df[col] = df[col].astype(dtype)

    return df

measurements_type = StructType(
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
        StructField("UNIT_SYMBOL", StringType()),
        StructField("MEASUREMENT_TYPE", StringType()),
        StructField("POWER_SYSTEM_RESOURCE_TYPE", StringType()),

        StructField("PROFILE_TYPE", StringType()),
        StructField("REPORTED_DT", DateType()),
        StructField("POWER_SYSTEM_RESOURCE_NUM", StringType()),
    ]
)

OUTPUT_FIELDS = [
    StructField("POWER_SYSTEM_RESOURCE_MRID", StringType(), nullable=False),
    StructField("POWER_SYSTEM_RESOURCE_KEY", StringType(), nullable=True),
    StructField("POWER_SYSTEM_RESOURCE_NUM", StringType(), nullable=True),
    StructField("REPORTED_DTTM", TimestampType(), nullable=False),
    StructField("REPORTED_DT", DateType(), nullable=True),
    StructField("MEASURE_VALUE", DoubleType(), nullable=True),
    StructField("MEASURE_VALUE_A", DoubleType(), nullable=True),
    StructField("MEASURE_VALUE_B", DoubleType(), nullable=True),
    StructField("MEASURE_VALUE_C", DoubleType(), nullable=True),
    StructField("RDNG_MEAS_MVAR", DoubleType(), nullable=True),
    StructField("RDNG_MEAS_MVAR_A", DoubleType(), nullable=True),
    StructField("RDNG_MEAS_MVAR_B", DoubleType(), nullable=True),
    StructField("RDNG_MEAS_MVAR_C", DoubleType(), nullable=True),
    StructField("UNIT_SYMBOL", StringType(), nullable=True),
    StructField("MEASUREMENT_TYPE", StringType(), nullable=True),
    StructField("POWER_SYSTEM_RESOURCE_TYPE", StringType(), nullable=True),
    StructField("YEAR_ID", LongType(), nullable=True),
    StructField("MONTH_ID", LongType(), nullable=True),
    StructField("PROFILE_TYPE", StringType(), nullable=True),
    StructField("PROFILE_STATE", StringType(), nullable=True),
    StructField("EDW_CREATED_DATE", TimestampType(), nullable=False),
    StructField("EDW_CREATED_BY", StringType(), nullable=True),
    StructField("EDW_MODIFIED_DATE", TimestampType(), nullable=False),
    StructField("EDW_MODIFIED_BY", StringType(), nullable=True),
    StructField("EDW_BATCH_ID", LongType(), nullable=True),
    StructField("EDW_BATCH_DETAIL_ID", LongType(), nullable=True),
    StructField("EDW_LAST_DML_CD", StringType(), nullable=True),
]

MEAS_COLUMNS = [
    "POWER_SYSTEM_RESOURCE_MRID",
    "POWER_SYSTEM_RESOURCE_KEY",
    "REPORTED_DTTM",
    "YEAR_ID",
    "MONTH_ID",
    "READINGS",
    "CA_BLOB",
    "EXTRA_METADATA",
]

class LoadAllocation:
    def __init__(
        self,
        session: snowflake.snowpark.Session,
        pf_config_json: str,
        start_time: str,
        end_time: str,
        circuit_key: str,
    ):
        self.logger = logging.getLogger("LoadAllocation_Logger")
        self.session = session
        self.pf_config_json = pf_config_json
        self.start_time = start_time
        self.end_time = end_time
        self.circuit_key = circuit_key
        self.pf_config = PowerflowConfig()
        self.pf_config.load_config_from_json(self.pf_config_json)

    def run_load_allocation(self):
        load_alloc_query = LoadAllocationQuery(
            self.pf_config,
            self.circuit_key,
            self.start_time,
            self.end_time,
            "DummyValue",
        )

        stage_location = f"{self.pf_config.get_database()}.{self.pf_config.get_database_schema()}.{self.pf_config.get_stage_name()}"
        logger.info(f"Using stage location: {stage_location}")

        main_query = self.session.sql(load_alloc_query.sql(), load_alloc_query.parameters())

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
            name=f"{self.pf_config.get_database()}.{self.pf_config.get_database_schema()}.LoadAllocation",
            replace=True,
            stage_location=stage_location,
            output_schema=PandasDataFrameType(
                [f.datatype for f in OUTPUT_FIELDS], [f.name for f in OUTPUT_FIELDS]
            ),
            input_types=[
                PandasDataFrameType(
                    [
                        StringType(), #POWER_SYSTEM_RESOURCE_MRID
                        StringType(), #POWER_SYSTEM_RESOURCE_KEY
                        TimestampType(9), #REPORTED_DTTM
                        IntegerType(),
                        IntegerType(),
                        ArrayType(measurements_type), #READINGS
                        BinaryType(), #CA_BLOB
                        StringType(), #EXTRA_METADATA
                    ]
                )
            ],
            input_names=[lcol.upper() for lcol in MEAS_COLUMNS],
            statement_params={"STATEMENT_TIMEOUT_IN_SECONDS": 7200},
            is_permanent=True,
        )
        class LoadAllocationUDTF:
            def __init__(self):
                self.logger = logging.getLogger("LoadAllocationUDTF_Logger")

            def end_partition(self, df: PandasDataFrame):
                start_time = time.perf_counter()
                pf_pipeline_pkl = df["CA_BLOB"].dropna().iloc[0]
                dfs = []
                for index, row in df.iterrows():
                    pf_pipeline: Pipeline = pickle.loads(pf_pipeline_pkl)
                    data = pf_pipeline.run_load_allocation("CC_ALLOCATION", row)

                    df_with_data = create_measurements_df(data)

                    dfs.append(df_with_data)
                df_all =  pd.concat(dfs, ignore_index=True) if len(dfs) > 0 else create_measurements_df()
                return df_all

        q = main_query.select(
            LoadAllocationUDTF(*MEAS_COLUMNS).over(
                partition_by=["REPORTED_DTTM"]
            )
        )
        async_query = q.write.mode("append").save_as_table(
            table_name=f"{self.pf_config.get_database()}.{self.pf_config.get_database_schema()}.{self.pf_config.get_load_allocation_table()}", block=True,
            column_order="name"
        )               
