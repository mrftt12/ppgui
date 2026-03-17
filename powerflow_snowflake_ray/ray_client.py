

import json
import os
import pathlib
from powerflow_snowflake_ray.ray_sproc import RaySProcClient
from powerflow_pipeline.util import PowerflowConfig
from powerflow_snowflake_ray.ray_task import launch_powerflow_dpf_job
from snowflake.snowpark import Session

from powerflow_snowflake_ray.util import stage, job_stage

# dpf_input_table_name = "ML_DB.DPF_EXPERIMENTS.IEEE_DPF_SYNTHETIC"
# stage = "ML_DB.DPF_EXPERIMENTS.DPF_RUNS"
# job_stage = "ML_DB.DPF_EXPERIMENTS.JOB_STAGE"
# compute_pool = "ML_CPU_S"

# def create_stage(session):
#     session.sql(f"CREATE STAGE IF NOT EXISTS {stage}").collect()

# def create_session():
#     connection_parameters = {
#         "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
#         "user": os.environ.get("SNOWFLAKE_USER"),
#         'role': os.environ.get("SNOWFLAKE_ROLE"),
#         "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE"),
#         "PYTHON_UDTF_END_PARTITION_TIMEOUT_SECONDS": 3600,
#         "authenticator": "externalbrowser",
#     }

#     with Session.builder.configs(connection_parameters).create() as session:
#         sproc = session.sproc.register(
#             func=run_powerflow_sproc,
#             stage_location=job_stage,
#             name="RUN_POWERFLOW_SPROC",
#             is_permanent=True,
#             return_type=T.StringType(),
#             replace=True,
#             packages=["snowflake-ml-python"],
#         )

#         async_sproc = session.sql("CALL RUN_POWERFLOW_SPROC()").collect_nowait()


def run_powerflow_sproc(session: Session,):
    """Wrapper function for stored procedure"""
    job = launch_powerflow_dpf_job(session)
    job.wait()
    run_id = job.result()
    return run_id


def main():
    pf_config = PowerflowConfig()
    current_dir = pathlib.Path(__file__).parent.resolve()
    pf_config.load_config(f"{current_dir}/env.json")    
    cli = RaySProcClient(
        stage_name= job_stage, #f"{pf_config.get_database()}.{pf_config.get_database_schema()}.{pf_config.get_stage_name()}",
        pkg_name="powerflow",
        pkg_src="src",
        recreate_stage=True,
    )
    cli.register_proc_and_run(
        f"{pf_config.get_database()}.{pf_config.get_database_schema()}.RUN_POWERFLOW_RAY_SPROC",
        run_powerflow_sproc,
    )

if __name__ == "__main__":
    main()        