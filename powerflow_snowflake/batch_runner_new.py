from abc import ABC, abstractmethod
from dataclasses import asdict
from datetime import datetime
import json
import uuid
from snowflake.snowpark.functions import col, explode, udtf

import time
import os
import pickle
from pathlib import Path

import pandas as pd
import snowflake.snowpark

import logging

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


class StaticCircuitBatchIterator(CircuitBatchIterator):
    def __init__(self, circuit_ids, batch_size):
        self.circuit_ids = [item.strip() for item in circuit_ids.split(',')] 
        self.batch_size = batch_size

    def get_circuit_batches(self):
        """Yield batches of circuit IDs from the static list."""
        for i in range(0, len(self.circuit_ids), self.batch_size):
            yield self.circuit_ids[i : i + self.batch_size]


class DbCircuitBatchIterator(CircuitBatchIterator):
    def __init__(self, session, pf_config, batch_size):
        self.session: snowflake.snowpark.Session = session
        self.batch_size = batch_size
        self.pf_config: PowerflowConfig = pf_config
        
    def select_circuits_sql(self):
        return f"""
        SELECT
            circuit_key,
            COUNT(DISTINCT replace(CONNECTIVITY_NODEID, 's', '')) as bus_count
        FROM
            {self.pf_config.get_connectivity_table_database()}.{self.pf_config.get_connectivity_table_schema()}.{self.pf_config.get_connectivity_table()} where circuit_key is not null
        GROUP BY
            circuit_key
        ORDER BY
            bus_count
        """
    def get_circuit_batches(self):
        all_circuits_query = self.session.sql(self.select_circuits_sql())
        all_circuits = [c[0] for c in all_circuits_query.collect()]
        for i in range(0, len(all_circuits), self.batch_size):
            yield all_circuits[i : i + self.batch_size]


BATCH_SIZE = 150


metric_fields  = [
    StructField("FUNCTION_NAME", StringType(), nullable=False),
    StructField("EXECUTION_TIME", FloatType(), nullable=False),
    StructField("RUNID", StringType(), nullable=False),
]

OUTPUT_FIELDS = [
    StructField("PARAMETER", StringType(), nullable=False),
    StructField("LINKED_EQUIPTYPE", StringType(), nullable=False),
    StructField("POWER_SYSTEM_RESOURCE_MRID", StringType(), nullable=False),
    StructField("POWER_SYSTEM_RESOURCE_TYPE", StringType(), nullable=False),
    StructField("CONDUCTING_EQUIPMENTID", StringType(), nullable=False),
    StructField("REPORTED_DTTM", StringType(), nullable=False),
    StructField("REPORTED_DT", StringType(), nullable=False),
    StructField("HOUR_ID", StringType(), nullable=False),
    StructField("YEAR_ID", StringType(), nullable=False),
    StructField("MONTH_ID", StringType(), nullable=False),
    StructField("TAG_NAME", StringType(), nullable=False),
    StructField("VM_A_PU", DoubleType(), nullable=False),
    StructField("VA_A_DEGREE", DoubleType(), nullable=False),
    StructField("VM_B_PU", DoubleType(), nullable=False),
    StructField("VA_B_DEGREE", DoubleType(), nullable=False),
    StructField("VM_C_PU", DoubleType(), nullable=False),
    StructField("VA_C_DEGREE", DoubleType(), nullable=False),
    StructField("VM_N_PU", DoubleType(), nullable=False),
    StructField("VA_N_DEGREE", DoubleType(), nullable=False),
    StructField("P_A_MW", DoubleType(), nullable=False),
    StructField("Q_A_MVAR", DoubleType(), nullable=False),
    StructField("P_B_MW", DoubleType(), nullable=False),
    StructField("Q_B_MVAR", DoubleType(), nullable=False),
    StructField("P_C_MW", DoubleType(), nullable=False),
    StructField("Q_C_MVAR", DoubleType(), nullable=False),
    StructField("P_N_MW", DoubleType(), nullable=False),
    StructField("Q_N_MVAR", DoubleType(), nullable=False),
    StructField("IM_A_KA", DoubleType(), nullable=False),
    StructField("IA_A_DEGREE", DoubleType(), nullable=False),
    StructField("IM_B_KA", DoubleType(), nullable=False),
    StructField("IA_B_DEGREE", DoubleType(), nullable=False),
    StructField("IM_C_KA", DoubleType(), nullable=False),
    StructField("IA_C_DEGREE", DoubleType(), nullable=False),
    StructField("IM_N_KA", DoubleType(), nullable=False),
    StructField("IA_N_DEGREE", DoubleType(), nullable=False),
    StructField("P_LOAD_A_MW", DoubleType(), nullable=False),
    StructField("Q_LOAD_A_MVAR", DoubleType(), nullable=False),
    StructField("P_LOAD_B_MW", DoubleType(), nullable=False),
    StructField("Q_LOAD_B_MVAR", DoubleType(), nullable=False),
    StructField("P_LOAD_C_MW", DoubleType(), nullable=False),
    StructField("Q_LOAD_C_MVAR", DoubleType(), nullable=False),
    StructField("P_LOAD_N_MW", DoubleType(), nullable=False),
    StructField("Q_LOAD_N_MVAR", DoubleType(), nullable=False),
    StructField("P_GEN_A_MW", DoubleType(), nullable=False),
    StructField("Q_GEN_A_MVAR", DoubleType(), nullable=False),
    StructField("P_GEN_B_MW", DoubleType(), nullable=False),
    StructField("Q_GEN_B_MVAR", DoubleType(), nullable=False),
    StructField("P_GEN_C_MW", DoubleType(), nullable=False),
    StructField("Q_GEN_C_MVAR", DoubleType(), nullable=False),
    StructField("P_GEN_N_MW", DoubleType(), nullable=False),
    StructField("Q_GEN_N_MVAR", DoubleType(), nullable=False),
    StructField("UNBALANCE_PERCENT", DoubleType(), nullable=False),
    StructField("P_A_FROM_MW", DoubleType(), nullable=False),
    StructField("Q_A_FROM_MVAR", DoubleType(), nullable=False),
    StructField("P_B_FROM_MW", DoubleType(), nullable=False),
    StructField("Q_B_FROM_MVAR", DoubleType(), nullable=False),
    StructField("P_C_FROM_MW", DoubleType(), nullable=False),
    StructField("Q_C_FROM_MVAR", DoubleType(), nullable=False),
    StructField("P_N_FROM_MW", DoubleType(), nullable=False),
    StructField("Q_N_FROM_MVAR", DoubleType(), nullable=False),
    StructField("P_A_TO_MW", DoubleType(), nullable=False),
    StructField("Q_A_TO_MVAR", DoubleType(), nullable=False),
    StructField("P_B_TO_MW", DoubleType(), nullable=False),
    StructField("Q_B_TO_MVAR", DoubleType(), nullable=False),
    StructField("P_C_TO_MW", DoubleType(), nullable=False),
    StructField("Q_C_TO_MVAR", DoubleType(), nullable=False),
    StructField("P_N_TO_MW", DoubleType(), nullable=False),
    StructField("Q_N_TO_MVAR", DoubleType(), nullable=False),
    StructField("PL_A_MW", DoubleType(), nullable=False),
    StructField("QL_A_MVAR", DoubleType(), nullable=False),
    StructField("PL_B_MW", DoubleType(), nullable=False),
    StructField("QL_B_MVAR", DoubleType(), nullable=False),
    StructField("PL_C_MW", DoubleType(), nullable=False),
    StructField("QL_C_MVAR", DoubleType(), nullable=False),
    StructField("PL_N_MW", DoubleType(), nullable=False),
    StructField("QL_N_MVAR", DoubleType(), nullable=False),
    StructField("I_A_FROM_KA", DoubleType(), nullable=False),
    StructField("I_B_FROM_KA", DoubleType(), nullable=False),
    StructField("I_C_FROM_KA", DoubleType(), nullable=False),
    StructField("I_N_FROM_KA", DoubleType(), nullable=False),
    StructField("I_A_TO_KA", DoubleType(), nullable=False),
    StructField("I_B_TO_KA", DoubleType(), nullable=False),
    StructField("I_C_TO_KA", DoubleType(), nullable=False),
    StructField("I_N_TO_KA", DoubleType(), nullable=False),
    StructField("LOADING_A_PERCENT", DoubleType(), nullable=False),
    StructField("LOADING_B_PERCENT", DoubleType(), nullable=False),
    StructField("LOADING_C_PERCENT", DoubleType(), nullable=False),
    StructField("LOADING_N_PERCENT", DoubleType(), nullable=False),
    StructField("METRICS", StringType(), nullable=False),
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
]

LOAD_COLS_DICT = {item: f"ARG{i+1}" for i, item in enumerate(LOAD_COLS)}


class LoadQuery:
    def __init__(self, pf_config, circuit_batch: list, start_time_dt: str, run_tag: str):
        self.circuit_batch = circuit_batch
        self.run_tag = run_tag
        self.pf_config: PowerflowConfig = pf_config
        self.start_time_dt = start_time_dt

    def sql(self):
        circuit_keys_encoded = "('" + "','".join(self.circuit_batch) + "')"
        days_per_batch = self.pf_config.get_days_per_batch() if self.pf_config.get_days_per_batch() is not None else "least(365, GREATEST(3, FLOOR(500 / sqrt(bus_count))))"
        return f"""
WITH Circuit_Sizes AS (
    SELECT
        circuit_key,
        COUNT(DISTINCT replace(CONNECTIVITY_NODEID, 's', '')) as bus_count
    FROM
        {self.pf_config.get_connectivity_table_database()}.{self.pf_config.get_connectivity_table_schema()}.{self.pf_config.get_connectivity_table()}
    WHERE
        circuit_key IN {circuit_keys_encoded}
    GROUP BY
        circuit_key
),
Circuit_Batches AS (
    SELECT
        circuit_key,
        bus_count,
        --least(365, GREATEST(3, FLOOR(500 / sqrt(bus_count)))) as days_per_batch
        {days_per_batch} as days_per_batch
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
        cb.days_per_batch
    FROM
        {self.pf_config.get_load_table_database()}.{self.pf_config.get_load_table_schema()}.{self.pf_config.get_load_table()} l
        join Circuit_Batches cb on l.POWER_SYSTEM_RESOURCE_KEY = cb.circuit_key
),
Load_Profile AS (
    SELECT
        POWER_SYSTEM_RESOURCE_KEY CIRCUIT_KEY,
        POWER_SYSTEM_RESOURCE_MRID,
        REPORTED_DTTM ::TIMESTAMP_NTZ(9) AS REPORTED_DTTM,
        year_id,
        month_id,
        date_trunc(
            'day',
            dateadd(
                'day',
                ci.days_per_batch * FLOOR(
                    DATEDIFF(
                        'day',
                        date_trunc('day', '{self.start_time_dt}' ::timestamp),
                        l.REPORTED_DTTM
                    ) / ci.days_per_batch
                ),
                '{self.start_time_dt}' ::timestamp
            )
        ) as batch_period,
        ROW_NUMBER() OVER (
            PARTITION BY l.POWER_SYSTEM_RESOURCE_KEY,
            year_id,
            month_id,
            FLOOR(
                DATEDIFF(
                    'day',
                    date_trunc('day', '{self.start_time_dt}' ::timestamp),
                    l.REPORTED_DTTM
                ) / ci.days_per_batch
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
        ci.days_per_batch
) 
SELECT
    '{self.run_tag}' TAG_NAME,
    --cast(CASE WHEN rnk=1 THEN BASE64_ENCODE(ca.preload_encoded_ca) ELSE NULL END as varchar) ca_blob,
    CASE WHEN rnk=1 THEN ca.preload_encoded_ca ELSE NULL END CA_BLOB,
    lf.*
FROM
    Load_Profile lf
    JOIN {self.pf_config.get_database()}.{self.pf_config.get_database_schema()}.{self.pf_config.get_encoded_circuits_table()} ca ON lf.CIRCUIT_KEY = ca.circuit_key
WHERE
    ca.preload_encoded_ca IS NOT NULL
    """

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
    ]
)


class SnowflakePowerflowRunner:
    def __init__(
        self,
        session: snowflake.snowpark.Session,
        in_last_process_batch: int,
        in_batch_id: int,
        in_batch_detail_id: int,
        pf_config: str,
        capacitor_enabled: bool,
        generator_enabled: bool,
        transformer_enabled: bool,
        regulator_enabled: bool,
        balanced_load: bool,
        start_time: str,
        end_time: str,
        circuit_keys: str,
        plng_year_id: int,
        fcst_abbrv_code: str,
        use_timeseries: bool,
    ):
        self.logger = logging.getLogger("SnowflakePowerflowRunner_Logger")
        self.session = session
        self.in_last_process_batch = in_last_process_batch
        self.in_batch_id = in_batch_id
        self.in_batch_detail_id = in_batch_detail_id
        self.pf_config = pf_config
        self.capacitor_enabled = capacitor_enabled
        self.generator_enabled = generator_enabled
        self.transformer_enabled = transformer_enabled
        self.regulator_enabled = regulator_enabled
        self.balanced_load = balanced_load
        self.start_time = start_time
        self.end_time = end_time
        self.circuit_keys = circuit_keys
        self.pf_config = PowerflowConfig()
        self.pf_config.load_config_from_json(self.pf_config)
        self.circuit_batch_iterator = (
            StaticCircuitBatchIterator(circuit_keys, BATCH_SIZE)
            if circuit_keys and circuit_keys != ""
            else DbCircuitBatchIterator(session, self.pf_config, BATCH_SIZE)
        )
        self.plng_year_id = plng_year_id
        self.fcst_abbrv_code = fcst_abbrv_code
        self.use_timeseries = use_timeseries

    def run_pf_batches(self):
        async_queries = []
        for batch_index, batch in enumerate(
            self.circuit_batch_iterator.get_circuit_batches()
        ):
            logger.info(f"Batch {batch_index}: {batch}")
            e2e_start_time = time.perf_counter()

            # @pandas_udtf(
                # packages=(
                #     "snowflake-snowpark-python",
                #     "geojson",
                #     "deepdiff",
                #     "lxml",
                #     "numba",
                #     "typing_extensions==4.15.0",
                #     "numpy==1.26.4",
                #     "packaging==25.0",
                #     "tqdm==4.67.1",
                #     "pandas==2.3.2",
                #     "networkx==3.4.2",
                #     "scipy==1.15.3",
                # ),
            #     output_schema=PandasDataFrameType(
            #         [f.datatype for f in OUTPUT_FIELDS], [f.name for f in OUTPUT_FIELDS]
            #     ),
            #     input_types=[
            #         PandasDataFrameType(
            #             [
            #                 StringType(),
            #                 StringType(),
            #                 StringType(),
            #                 TimestampType(9),
            #                 IntegerType(),
            #                 IntegerType(),
            #                 TimestampType(9),
            #                 ArrayType(measurements_type),
            #                 BinaryType(),
            #             ]
            #         )
            #     ],
            #     input_names=[lcol.upper() for lcol in LOAD_COLS],
            #     statement_params={"STATEMENT_TIMEOUT_IN_SECONDS": 7200},
            #     is_permanent=False,
            # )
            @udtf(name="PowerFlow",replace=True, output_schema=PandasDataFrameType(
                    [f.datatype for f in OUTPUT_FIELDS], [f.name for f in OUTPUT_FIELDS]
                ), input_types=[
                    PandasDataFrameType(
                        [
                            StringType(),
                            StringType(),
                            StringType(),
                            TimestampType(9),
                            IntegerType(),
                            IntegerType(),
                            TimestampType(9),
                            ArrayType(measurements_type),
                            BinaryType(),
                        ]
                    )
                ], packages=(
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
                ), statement_params={"STATEMENT_TIMEOUT_IN_SECONDS": 7200}, stage_location='@GRIDMOD_DEV_TD.UC_POC.python_modules',
                is_permanent=True)
            class PowerflowUDTF:
                def __init__(self):
                    self.logger = logging.getLogger("PowerflowUDTF_Logger")
                # def end_partition(self, df: PandasDataFrame):
                # def end_partition(self, df: pd.core.frame.DataFrame):
                def end_partition(self, df):
                    dtype = type(df)
                    self.logger.info(f"1...type: {dtype}")
                    self.logger.info(f"1.1....{df.iloc[0]}")
                    self.logger.info(f"2...columns: {df.columns}")
                    start_time = time.perf_counter()
                    pf_pipeline_pkl = df[LOAD_COLS_DICT["CA_BLOB"]].dropna().iloc[0]

                    pf_pipeline: Pipeline = pickle.loads(pf_pipeline_pkl)

                    dfs = []
                    run_count = 0
                    error_count = 0
                    
                    for index, row in df.sample(frac=0.5, random_state=42).iterrows():
                    # for index, row in df.iterrows():
                        # circuit_key = row['CIRCUIT_KEY']
                        circuit_key = row[LOAD_COLS_DICT["CIRCUIT_KEY"]]
                        # reported_dttm = row['REPORTED_DTTM']
                        reported_dttm = row[LOAD_COLS_DICT["REPORTED_DTTM"]]
                        try:
                            results = pf_pipeline.run_analysis(row)
                            results.loc[0, 'METRICS'] = pf_pipeline.get_metrics()
                            results.loc[1:, 'METRICS'] = ""                                
                            
                            dfs.append(results)
                        except Exception as e:
                            error_count = error_count + 1
                            self.logger.info(f"run_flow exception({circuit_key}/{reported_dttm}): {e}")
                                
                        run_count = run_count + 1
                    end_time = time.perf_counter()
                    elapsed_time = end_time - start_time
                    df_all = pd.concat(dfs, ignore_index=True)
                    self.logger.info(f"Run time: {elapsed_time:.4f} seconds, {run_count}/{len(df)} runs, errors: {error_count}, results: {len(df_all)}")
                    return df_all

            run_tag = str(uuid.uuid4())
            load_query = LoadQuery(self.pf_config, batch, self.start_time, run_tag).sql()
            main_query = self.session.sql(load_query)
            q = main_query.select(
                PowerflowUDTF(*LOAD_COLS).over(
                    partition_by=["CIRCUIT_KEY", "batch_period"]
                )
            )
          
            logger.info(f"Profile count: {main_query.count()}")
            
            async_query = q.write.mode("append").save_as_table(
                f"{self.pf_config.get_database()}.{self.pf_config.get_database_schema()}.{self.pf_config.get_output_table()}", block=False
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
        return run_tag