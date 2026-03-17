# Run Powerflow in Snowflake

## Tables

```
SNOWFLAKE_DB ---> GRIDMOD_DEV_TD
SNOWFLAKE_DB_FERC ---> GRIDMOD_DEV_TD
SNOWFLAKE_SCHEMA ---> UC_POC
SNOWFLAKE_SCHEMA_V ---> UC_POC
HIERARCHY_TABLE ---> GRIDMOD_DEV_TD.UC_POC.NMM_D_TRACED_CIRCUIT_CONNECTIVITY_C_PP_VW_11_27
BUS_TABLE ---> GRIDMOD_DEV_TD.UC_POC.NMM_D_BUS_C_PP_VW_11_27
LOAD_TABLE ---> GRIDMOD_DEV_TD.UC_POC.NMM_F_HIST_HR_GROSS_DERGEN_DLY_C_PP_VW_10_11
OUTPUT_TABLE ---> GRIDMOD_DEV_TD.UC_POC.NMM_F_TOPOLOGICALNODE_C_10_1
CONTROL_TABLE ---> GRIDMOD_DEV_TD.UC_POC.NMM_F_LOADFLOW_CONFIG_CONTROLLER_I_10_1
```

1. Download multiconductor

```bash
pip download --only-binary :all: --dest ./wheels_folder multiconductor==0.0.1.6
```

1. Download tar file

```bash
pip download \
--index-url https://<Azure PAT token>@pkgs.dev.azure.com/itron/_packaging/Itron.Cloud.Platform/pypi/simple/ \
--extra-index-url https://pypi.org/simple/ \
--no-binary :all: --dest ./ --no-deps multiconductor==0.0.1.6

pip download --no-binary :all: --dest ./ --no-deps pandapower==3.1.2
pip download --no-binary :all: --dest ./ --no-deps dotted_dict
```
