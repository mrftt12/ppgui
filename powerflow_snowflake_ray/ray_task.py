import time
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
import pandapower as pp
import pandapower.networks as pn

from snowflake.snowpark.context import get_active_session
import snowflake.snowpark.functions as F
import snowflake.snowpark.types as T

from snowflake.ml.jobs import remote
from snowflake.snowpark import Session

from .util import job_stage, dpf_input_table_name, compute_pool, stage



# session.sql(f"CREATE STAGE IF NOT EXISTS {stage}").collect()


def get_result_sdf(session, run_id, stage=stage):
    # Create file format
    session.sql(
        "CREATE FILE FORMAT IF NOT EXISTS parquet_format TYPE = 'PARQUET'"
    ).collect()

    # Query with clean column names
    results_sdf = session.sql(
        f"""
        SELECT 
            $1:CIRCUIT::STRING AS CIRCUIT,
            $1:BUS::INTEGER AS BUS,
            $1:TIMESTAMP::STRING AS TIMESTAMP,
            $1:V_PU::FLOAT AS V_PU
        FROM @{stage}/{run_id}/ (FILE_FORMAT => parquet_format, PATTERN => '.*\\.parquet')
    """
    )

    return results_sdf


def powerflow_partition(data_connector, context):
    """Run power flow on a partition. Network loaded from JSON column."""
    import pandapower as pp
    from io import StringIO
    import pyarrow as pa
    import pyarrow.parquet as pq

    df = data_connector.to_pandas()
    net = pp.from_json(StringIO(df["NETWORK_JSON"].iloc[0]))

    results = []
    for ts, group in df.groupby("TIMESTAMP"):
        # Update loads
        for _, row in group.iterrows():
            mask = net.load["bus"] == row["BUS"]
            net.load.loc[mask, ["p_mw", "q_mvar"]] = row["P_MW"], row["Q_MVAR"]

        pp.runpp(net, verbose=False)

        for _, row in group.iterrows():
            results.append(
                {
                    "CIRCUIT": df["CIRCUIT_KEY"].iloc[0],
                    "BUS": row["BUS"],
                    "TIMESTAMP": str(ts),
                    "V_PU": float(net.res_bus.loc[row["BUS"], "vm_pu"]),
                }
            )

    # Save results directly as parquet using PyArrow (faster than pandas)
    context.upload_to_stage(
        results,
        "results.parquet",
        write_function=lambda data, path: pq.write_table(
            pa.Table.from_pylist(data), path
        ),
    )


@remote(
    compute_pool=compute_pool,
    pip_requirements=["pandapower"],
    external_access_integrations=["PYPI_ACCESS"],
    stage_name=job_stage,
    target_instances=3,
)
def launch_powerflow_dpf_job(session: Session):
    """
    Launch a DPF distributed run remotely inside a Snowflake ML Job.
    """
    from snowflake.ml.modeling.distributors.distributed_partition_function.dpf import DPF
    from snowflake.ml.modeling.distributors.distributed_partition_function.entities import (
        ExecutionOptions,
    )
    # Run DPF
    dpf_input = session.table(dpf_input_table_name)

    dpf = DPF(func=powerflow_partition, stage_name=stage)
    run = dpf.run(
        partition_by="PARTITION_KEY",
        snowpark_dataframe=dpf_input,
        run_id=f"pf_{datetime.now():%Y%m%d_%H%M%S}",
        execution_options=ExecutionOptions(use_head_node=False),
    )
    run.wait()

    print(f"Launched: {run.run_id}")
    print("DPF run complete")
    return run.run_id
