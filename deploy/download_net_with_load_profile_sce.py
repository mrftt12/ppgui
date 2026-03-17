from snowflake.snowpark import Session
import json
import os
import pathlib
import pickle
import traceback
import pandas as pd
import snowflake.connector

from powerflow_pipeline.util import PowerflowConfig
from powerflow_snowflake.batch_runner import LoadQuery
from sce.sce_mc_mapping import SCEPowerflowPipelineTemp

import types

user = os.getenv("SNOWFLAKE_USER")
password = os.getenv("SNOWFLAKE_PASSWORD")
account = os.getenv("SNOWFLAKE_ACCOUNT")
warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
database = "GRIDMOD_DEV_TD"
schema = "UC_POC"


def query(circuit_key, reported_dttm):
    pf_config = PowerflowConfig()
    current_dir = pathlib.Path(__file__).parent.resolve()
    pf_config.load_config(f"{current_dir}/env.json")

    start_time = reported_dttm
    end_time = reported_dttm
    return LoadQuery(pf_config, [circuit_key], start_time, end_time, "tag1").sql()


def before_run(self, net):
    script_directory = pathlib.Path(__file__).resolve().parent
    with open(f'{script_directory}/pp_net.pkl', "wb") as file:
        pickle.dump(self.net, file)


def run_pf(self):
    pass


def handle_result(self, res):
    pass


def download_circuit(circuit_key, reported_dttm, run_pf=False):
    conn = None
    try:
        # conn = snowflake.connector.connect(
        #     user=user,
        #     password=password,
        #     account=account,
        #     warehouse=warehouse,
        #     database=database,
        #     schema=schema,
        # )
        connection_parameters = {
            "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
            "user": os.environ.get("SNOWFLAKE_USER"),
            'role': os.environ.get("SNOWFLAKE_ROLE"),
            "PYTHON_UDTF_END_PARTITION_TIMEOUT_SECONDS": 3600,
            "authenticator": "externalbrowser",
        }
        pass
        with Session.builder.configs(connection_parameters).create() as session:
            conn = session.connection
            print("connected")
            cur = conn.cursor()
            cur.execute(
                f"SELECT PRELOAD_ENCODED_CA FROM GRIDMOD_DEV_TD_FERC.SPEED_UI.NMM_F_TOPOLOGICALNODE_ENCODED_CIRCUITS_C where CIRCUIT_KEY = '{circuit_key}'"
            )
            print("executed")
            results = cur.fetchall()
            for row in results:
                encoded = row[0]
                pipeline: SCEPowerflowPipelineTemp = pickle.loads(encoded)
                pipeline.before_run = types.MethodType(before_run, pipeline)
                if not run_pf:
                    pipeline._run_pf = types.MethodType(run_pf, pipeline)
                    pipeline._handle_result = types.MethodType(
                        handle_result, pipeline)
                #
                session = Session.builder.configs(
                    {"connection": conn}).create()
                df = session.sql(query(circuit_key, reported_dttm))
                row_iterator = df.to_local_iterator()
                for row in row_iterator:
                    row_dict = row.as_dict()
                    row_dict['READINGS'] = json.loads(row['READINGS'])
                    pandas_series = pd.Series(row_dict)
                    pipeline.run_analysis(pandas_series)

    except Exception as e:
        print(f"An error occurred: {e}")
        lines = traceback.format_exception(e)
        stack_trace_str = ''.join(lines)
        print(stack_trace_str)

    finally:
        if "cur" in locals() and cur:
            cur.close()
        if "conn" in locals() and conn:
            conn.close()


download_circuit(circuit_key="CKT_3454_11625",
                 reported_dttm="2025-09-09 08:00:00", run_pf=True)
