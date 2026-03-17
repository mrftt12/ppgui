import os
import time
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
import pandapower as pp
import pandapower.networks as pn
from snowflake.snowpark import Session
import snowflake.snowpark.functions as F
from powerflow_snowflake_ray.util import dpf_input_table_name


def create_synthetic_data():
    connection_parameters = {
        "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
        "user": os.environ.get("SNOWFLAKE_USER"),
        'role': os.environ.get("SNOWFLAKE_ROLE"),
        "database": os.environ.get("SNOWFLAKE_DATABASE"),
        "schema": os.environ.get("SNOWFLAKE_SCHEMA"),        
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE"),
        "PYTHON_UDTF_END_PARTITION_TIMEOUT_SECONDS": 3600,
        "authenticator": "externalbrowser",
    }

    print("Creating data")

    with Session.builder.configs(connection_parameters).create() as session:
        # Create test networks and serialize to JSON
        networks = {"IEEE_14": pn.case14(), "IEEE_33": pn.case33bw()}
        network_json = {k: pp.to_json(v) for k, v in networks.items()}

        # Generate 30 days of load data per network
        data = []
        for name, net in networks.items():
            for hour in range(24 * 30):
                load_factor = 0.7 + 0.3 * np.sin(np.pi * hour / 12)
                for _, load in net.load.iterrows():
                    data.append(
                        {
                            "CIRCUIT_KEY": name,
                            "BUS": int(load["bus"]),
                            "TIMESTAMP": datetime(2024, 1, 1) + timedelta(hours=hour),
                            "P_MW": load["p_mw"] * load_factor,
                            "Q_MVAR": load["q_mvar"] * load_factor,
                            "NETWORK_JSON": network_json[name],
                        }
                    )


        # Add a partition key and write to Snowflake
        df = pd.DataFrame(data)
        sdf = session.create_dataframe(df)

        # Add partition key: Date (24 timestamps) + Circuit Key
        sdf = sdf.with_column(
            "PARTITION_KEY",
            F.concat(
                F.to_date(F.col("TIMESTAMP")).cast("STRING"), F.lit("_"), F.col("CIRCUIT_KEY")
            ),
        )

        sdf.write.mode("overwrite").save_as_table(dpf_input_table_name)
        print(f"Created {len(df)} rows")

create_synthetic_data()


