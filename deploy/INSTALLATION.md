# Powerflow Packagae Installation

## Prerequisites

- Python 3.10

## Extract powerflow_r8.zip

```bash
unzip powerflow_r8.zip
```

```bash
$ tree
.
├── Dockerfile
├── _staging
│   ├── dotted_dict.zip
│   ├── multiconductor.zip
│   ├── pandapower.zip
│   └── powerflow.zip
├── define_circuits.py
├── deploy
│   └── snowflake_procedure_runner.py
├── env.json
├── ingest_circuit_data.py
├── ingest_profile_data.py
├── install.py
├── powerflow_pipeline
│   └── util.py
├── powerflow_snowflake
│   └── procedure.py
├── requirements.txt
└── run_powerflow.py
```
## create virtual environment

```bash
python -m venv ~/venv1
source ~/venv1/bin/activate
```
## Install dependencies

```bash
pip install -r requirements.tx
```

## Set environment variables

Create a .env file in the root directory with the following content

```json
SNOWFLAKE_WAREHOUSE=<warehouse name>
SNOWFLAKE_PASSWORD=<password>
SNOWFLAKE_ACCOUNT=<account>
SNOWFLAKE_DATABASE=<database>
SNOWFLAKE_SCHEMA=<schema>
SNOWFLAKE_USER=<user name>
```

## Update Configuration File

Edit env.json and set database/schema/table names matching the environment. 
Clone/update `dev` and set active-env.

```json
{
    "active-env": "dev",
    "stage-name": "python_modules",
    "defaults": {
        "DATABASE": "GRIDMOD_DEV_TD",
        "DATABASE_SCHEMA": "UC_POC",
        "CONNECTIVITY_TABLE": "NMM_D_TRACED_CIRCUIT_CONNECTIVITY_C_PP_VW_MC3",
        "BUS_TABLE": "NMM_D_BUS_C_PP_VW_MC3",
        "LOAD_TABLE": "NMM_F_HIST_HR_GROSS_DERGEN_DLY_C_PP_VW_MC3",
        "ENCODED_CIRCUITS_TABLE": "NMM_F_TOPOLOGICALNODE_ENCODED_CIRCUITS_C_MC3",
        "OUTPUT_TABLE": "NMM_F_TOPOLOGICALNODE_C",
        "DEFINE_CIRCUITS_PROC": "SP_EDW_PF_F_TOPOLOGICALNODE_DEFINE_CIRCUIT_C_PY_LOADCORETOUC_MC3",
        "RUN_POWERFLOW_PROC": "SP_EDW_PF_F_TOPOLOGICALNODE_C_PY_LOADCORETOUC_MC3"
    },
    "dev": {
        "DATABASE": "GRIDMOD_DEV_TD",
        "DATABASE_SCHEMA": "UC_POC",
        "CONNECTIVITY_TABLE": "NMM_D_TRACED_CIRCUIT_CONNECTIVITY_C_PP_VW_MC3",
        "BUS_TABLE": "NMM_D_BUS_C_PP_VW_MC3",
        "LOAD_TABLE": "NMM_F_HIST_HR_GROSS_DERGEN_DLY_C_PP_VW_MC3",
        "ENCODED_CIRCUITS_TABLE": "NMM_F_TOPOLOGICALNODE_ENCODED_CIRCUITS_C_MC3",
        "OUTPUT_TABLE": "NMM_F_TOPOLOGICALNODE_C",
        "DEFINE_CIRCUITS_PROC": "SP_EDW_PF_F_TOPOLOGICALNODE_DEFINE_CIRCUIT_C_PY_LOADCORETOUC_MC3",
        "RUN_POWERFLOW_PROC": "SP_EDW_PF_F_TOPOLOGICALNODE_C_PY_LOADCORETOUC_MC3"
    }
}
```

## Install Stored Procedures

There two stored procedures

- SP_EDW_PF_F_TOPOLOGICALNODE_DEFINE_CIRCUIT_C_PY_LOADCORETOUC
- SP_EDW_PF_F_TOPOLOGICALNODE_C_PY_LOADCORETOUC

Run intall.py

```bash
python install.py
```

## Create Circuit Cache

Run define_circuits.py

```bash
python define_circuits.py
```

## Run Powerflow

```bash
python run_powerflow.py
```