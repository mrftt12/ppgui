-- =============================================================================
-- VALIDATE_IO: Field-level comparison of input data (from GET_* table functions)
-- with the output multiconductor network stored in the encoded circuit pickle.
--
-- For each element type, compares fields that are directly set from input
-- (as defined in sce_mc_mapping.py / sce_field_mapping). Returns one row per
-- element per field, plus an ELEMENT_COUNT summary row per type.
-- =============================================================================
CREATE OR REPLACE PROCEDURE TESTDB.PUBLIC.VALIDATE_IO("P_CIRCUIT_KEY" VARCHAR)
RETURNS TABLE (
    "CIRCUIT_KEY" VARCHAR,
    "ELEMENT_TYPE" VARCHAR,
    "ELEMENT_NAME" VARCHAR,
    "FIELD_NAME" VARCHAR,
    "INPUT_VALUE" VARCHAR,
    "OUTPUT_VALUE" VARCHAR,
    "MATCH" BOOLEAN
)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python','scipy','numpy','pandas','geojson','networkx','deepdiff','packaging','typing_extensions','tqdm','lxml','numba')
HANDLER = 'validate_io'
IMPORTS = ('@TESTDB.PUBLIC.SERVICE_STAGE/pandapower.zip','@TESTDB.PUBLIC.SERVICE_STAGE/powerflow_snowflake.zip','@TESTDB.PUBLIC.SERVICE_STAGE/powerflow.zip','@TESTDB.PUBLIC.SERVICE_STAGE/snowflake_udf_pkg.zip','@TESTDB.PUBLIC.SERVICE_STAGE/dotted_dict.zip','@TESTDB.PUBLIC.SERVICE_STAGE/multiconductor.zip')
EXECUTE AS OWNER
AS '
import sys
import os
import pickle

IMPORT_DIR = sys._xoptions.get("snowflake_import_directory", "/tmp")
for pkg in ("pandapower.zip", "powerflow_snowflake.zip", "powerflow.zip", "snowflake_udf_pkg.zip", "dotted_dict.zip", "multiconductor.zip"):
    pkg_path = os.path.join(IMPORT_DIR, pkg)
    if pkg_path not in sys.path:
        sys.path.insert(0, pkg_path)

import pandas as pd
from snowflake.snowpark.types import StructType, StructField, StringType, BooleanType

RESULT_SCHEMA = StructType([
    StructField("CIRCUIT_KEY", StringType()),
    StructField("ELEMENT_TYPE", StringType()),
    StructField("ELEMENT_NAME", StringType()),
    StructField("FIELD_NAME", StringType()),
    StructField("INPUT_VALUE", StringType()),
    StructField("OUTPUT_VALUE", StringType()),
    StructField("MATCH", BooleanType()),
])

_TOLERANCE = 1e-4

# ── Field mapping derived from sce_mc_mapping.py / sce_field_mapping ──
# Only fields that are directly set from input (not CALCULATED, not bus-index
# lookups, not phase-parsed, not power-redistributed).
# Each entry: (label, GET_* func, net table, input name column, [(net_field, input_col)])
_FIELD_MAP = [
    {
        "label": "BUS",
        "get_func": "TESTDB.PUBLIC.GET_BUS",
        "net_table": "bus",
        "input_name_col": "CONNECTIVITY_NODEID",
        "fields": [
            ("vn_kv", "BUS_VN_KV"),
            ("in_service", "BUS_IN_SERVICE"),
            ("type", "BUS_TYPE"),
            ("zone", "BUS_ZONE"),
            ("grounding_r_ohm", "BUS_GROUNDING_R_OHM"),
            ("grounding_x_ohm", "BUS_GROUNDING_X_OHM"),
        ],
    },
    {
        "label": "EXT_GRID",
        "get_func": "TESTDB.PUBLIC.GET_EXT_GRID",
        "net_table": "ext_grid",
        "input_name_col": "CONDUCTING_EQUIPMENTID",
        "fields": [
            ("vm_pu", "EXTERNALGRID_VM_PU"),
            ("va_degree", "EXTERNALGRID_VA_DEGREE"),
            ("sn_mva", "EXTERNALGRID_SC_MVA"),
            ("rx", "EXTERNALGRID_RX"),
            ("x0x", "EXTERNALGRID_X0X"),
            ("r0x0", "EXTERNALGRID_R0X0"),
            ("z2z1", "EXTERNALGRID_Z2Z1"),
            ("c", "EXTERNALGRID_C"),
            ("z1_weight", "EXTERNALGRID_Z1_WEIGHT"),
            ("in_service", "EXTERNALGRID_IN_SERVICE"),
        ],
    },
    {
        "label": "LINE",
        "get_func": "TESTDB.PUBLIC.GET_LINE",
        "net_table": "line",
        "input_name_col": "CONDUCTING_EQUIPMENTID",
        "fields": [
            ("model_type", "LINE_MODEL_TYPE"),
            ("in_service", "IN_SERVICE"),
        ],
    },
    {
        "label": "SWITCH",
        "get_func": "TESTDB.PUBLIC.GET_SWITCH",
        "net_table": "switch",
        "input_name_col": "CONDUCTING_EQUIPMENTID",
        "fields": [
            ("closed", "SWITCH_CLOSED"),
            ("type", "SWITCH_TYPE"),
            ("et", "SWITCH_ET"),
        ],
    },
    {
        "label": "SHUNT",
        "get_func": "TESTDB.PUBLIC.GET_SHUNT",
        "net_table": "asymmetric_shunt",
        "input_name_col": "CONDUCTING_EQUIPMENTID",
        "fields": [
            ("control_mode", "SHUNT_CONTROL_MODE"),
            ("closed", "SHUNT_CLOSED"),
            ("v_threshold_on", "SHUNT_V_THR_ON"),
            ("v_threshold_off", "SHUNT_V_THR_OFF"),
        ],
    },
    {
        "label": "XFMR",
        "get_func": "TESTDB.PUBLIC.GET_XFMR",
        "net_table": "trafo1ph",
        "input_name_col": "CONDUCTING_EQUIPMENTID",
        "fields": [
            ("sn_mva", "XFRMR_SN_MVA"),
            ("vk_percent", "XFRMR_VK_PERCENT"),
            ("vkr_percent", "XFRMR_VKR_PERCENT"),
            ("pfe_kw", "XFRMR_PFE_KW"),
            ("i0_percent", "XFRMR_I0_PERCENT"),
        ],
    },
    {
        "label": "LOAD",
        "get_func": "TESTDB.PUBLIC.GET_LOAD",
        "net_table": "asymmetric_load",
        "input_name_col": "STRUCTURE_NUM",
        "fields": [
            ("scaling", "LOAD_SCALING"),
            ("in_service", "LOAD_IN_SERVICE"),
            ("type", "LOAD_TYPE"),
            ("sn_mva", "LOAD_SN_MVA"),
            ("const_z_percent_p", "LOAD_CONST_Z_PERCENT_P"),
            ("const_i_percent_p", "LOAD_CONST_I_PERCENT_P"),
            ("const_z_percent_q", "LOAD_CONST_Z_PERCENT_Q"),
            ("const_i_percent_q", "LOAD_CONST_I_PERCENT_Q"),
        ],
    },
    {
        "label": "GEN",
        "get_func": "TESTDB.PUBLIC.GET_GEN",
        "net_table": "asymmetric_gen",
        "input_name_col": "CONDUCTING_EQUIPMENTID",
        "fields": [
            ("vm_pu", "GEN_VM_PU"),
            ("sn_mva", "GEN_SN_MVA"),
            ("scaling", "GEN_SCALING"),
            ("in_service", "GEN_IN_SERVICE"),
            ("type", "GEN_TYPE"),
        ],
    },
    {
        "label": "SGEN",
        "get_func": "TESTDB.PUBLIC.GET_SGEN",
        "net_table": "asymmetric_sgen",
        "input_name_col": "CONDUCTING_EQUIPMENTID",
        "fields": [
            ("sn_mva", "SGEN_SN_MVA"),
            ("scaling", "SGEN_SCALING"),
            ("in_service", "SGEN_IN_SERVICE"),
            ("type", "SGEN_TYPE"),
        ],
    },
]


def _element_count(tbl):
    """Count unique elements (top-level index) in a possibly multi-indexed DataFrame."""
    if hasattr(tbl.index, "nlevels") and tbl.index.nlevels > 1:
        return int(tbl.index.get_level_values(0).nunique())
    return len(tbl)


def _get_unique_elements(tbl):
    """Collapse multi-index (index, phase) to one row per element, keeping first phase row."""
    if hasattr(tbl.index, "nlevels") and tbl.index.nlevels > 1:
        return tbl.groupby(level=0).first()
    return tbl


def _to_str(val):
    """Convert a value to string for display, returning None for missing values."""
    if val is None:
        return None
    if isinstance(val, float) and pd.isna(val):
        return None
    return str(val)


def _vals_match(in_val, out_val):
    """Compare an input value to an output value with tolerance for numerics."""
    in_s = _to_str(in_val)
    out_s = _to_str(out_val)
    if in_s is None and out_s is None:
        return True
    if in_s is None or out_s is None:
        return False
    # Try numeric comparison first
    try:
        f_in = float(in_s)
        f_out = float(out_s)
        return abs(f_in - f_out) <= _TOLERANCE
    except (ValueError, TypeError):
        pass
    # Boolean comparison (handle Snowflake True/False vs Python bool)
    bool_map = {"TRUE": True, "FALSE": False, "1": True, "0": False}
    if in_s.upper() in bool_map and out_s.upper() in bool_map:
        return bool_map[in_s.upper()] == bool_map[out_s.upper()]
    # String comparison (case-insensitive, trimmed)
    return in_s.strip().upper() == out_s.strip().upper()


def validate_io(session, p_circuit_key: str):
    safe_key = p_circuit_key.replace("''", "''''")

    # ── Load the multiconductor network (OUTPUT) from encoded pickle ──
    rows = session.sql(
        f"""
        SELECT CIRCUIT_KEY, PRELOAD_ENCODED_CA
        FROM SCE_SHARE.UC_SPEED.NMM_F_TOPOLOGICALNODE_ENCODED_CIRCUITS_C
        WHERE CIRCUIT_KEY = ''{safe_key}''
        """
    ).collect()

    if not rows:
        err = {
            "CIRCUIT_KEY": p_circuit_key, "ELEMENT_TYPE": "ERROR",
            "ELEMENT_NAME": "", "FIELD_NAME": "NETWORK_NOT_FOUND",
            "INPUT_VALUE": None, "OUTPUT_VALUE": None, "MATCH": False,
        }
        return session.create_dataframe(pd.DataFrame([err]), schema=RESULT_SCHEMA)

    encoded_bytes = bytes(rows[0]["PRELOAD_ENCODED_CA"])
    obj = pickle.loads(encoded_bytes)
    net = obj.net

    result_rows = []

    for elem_cfg in _FIELD_MAP:
        label = elem_cfg["label"]
        get_func = elem_cfg["get_func"]
        net_table = elem_cfg["net_table"]
        input_name_col = elem_cfg["input_name_col"]
        fields = elem_cfg["fields"]

        # ── OUTPUT: extract from multiconductor network ──
        tbl = net.get(net_table, None) if isinstance(net, dict) else getattr(net, net_table, None)
        if tbl is not None and isinstance(tbl, pd.DataFrame) and len(tbl) > 0:
            out_unique = _get_unique_elements(tbl)
            out_count = len(out_unique)
        else:
            out_unique = pd.DataFrame()
            out_count = 0

        # ── INPUT: call the GET_* table function ──
        try:
            input_df = session.sql(
                f"SELECT * FROM TABLE({get_func}(''{safe_key}''))"
            ).to_pandas()
        except Exception:
            input_df = pd.DataFrame()

        in_count = 0
        if len(input_df) > 0:
            if input_name_col and input_name_col in input_df.columns:
                in_count = int(input_df[input_name_col].nunique())
            else:
                in_count = len(input_df)

        # ── Element count comparison row ──
        result_rows.append({
            "CIRCUIT_KEY": p_circuit_key, "ELEMENT_TYPE": label,
            "ELEMENT_NAME": "*", "FIELD_NAME": "ELEMENT_COUNT",
            "INPUT_VALUE": str(in_count), "OUTPUT_VALUE": str(out_count),
            "MATCH": in_count == out_count,
        })

        # Skip field-level comparison if either side is empty
        if len(input_df) == 0 or len(out_unique) == 0:
            continue

        # ── Build output lookup by name ──
        if "name" not in out_unique.columns:
            continue
        out_by_name = {}
        for idx, row in out_unique.iterrows():
            n = _to_str(row.get("name", ""))
            if n is not None:
                out_by_name[n] = row

        # ── Compare each input element field by field ──
        seen_names = set()
        for _, in_row in input_df.iterrows():
            elem_name = _to_str(in_row.get(input_name_col, ""))
            if elem_name is None:
                continue
            # Skip duplicate input rows with same name (already compared)
            if elem_name in seen_names:
                continue
            seen_names.add(elem_name)

            if elem_name not in out_by_name:
                # Input element not found in output network
                for net_field, in_col in fields:
                    in_val = in_row.get(in_col, None)
                    result_rows.append({
                        "CIRCUIT_KEY": p_circuit_key, "ELEMENT_TYPE": label,
                        "ELEMENT_NAME": elem_name, "FIELD_NAME": net_field,
                        "INPUT_VALUE": _to_str(in_val), "OUTPUT_VALUE": None,
                        "MATCH": False,
                    })
                continue

            out_row = out_by_name[elem_name]
            for net_field, in_col in fields:
                in_val = in_row.get(in_col, None)
                out_val = out_row.get(net_field, None)
                match = _vals_match(in_val, out_val)
                result_rows.append({
                    "CIRCUIT_KEY": p_circuit_key, "ELEMENT_TYPE": label,
                    "ELEMENT_NAME": elem_name, "FIELD_NAME": net_field,
                    "INPUT_VALUE": _to_str(in_val), "OUTPUT_VALUE": _to_str(out_val),
                    "MATCH": match,
                })

    if not result_rows:
        result_rows.append({
            "CIRCUIT_KEY": p_circuit_key, "ELEMENT_TYPE": "EMPTY",
            "ELEMENT_NAME": "", "FIELD_NAME": "",
            "INPUT_VALUE": None, "OUTPUT_VALUE": None, "MATCH": False,
        })

    return session.create_dataframe(pd.DataFrame(result_rows), schema=RESULT_SCHEMA)
';

-- Usage:
--   CALL TESTDB.PUBLIC.VALIDATE_IO('CKT_114_16955');

-- =============================================================================
-- Reference: Input table functions used by VALIDATE_IO
-- =============================================================================

CREATE OR REPLACE FUNCTION TESTDB.PUBLIC.GET_BUS("CKT_KEY" VARCHAR)
RETURNS TABLE ("BUS_VN_KV" NUMBER(38,6), "CONNECTIVITY_NODEID" VARCHAR, "BUS_PHASES" VARCHAR, "BUS_GROUNDED_PHASES" NUMBER(38,0), "BUS_GROUNDING_R_OHM" NUMBER(38,6), "BUS_GROUNDING_X_OHM" NUMBER(38,6), "BUS_IN_SERVICE" BOOLEAN, "BUS_TYPE" VARCHAR, "BUS_ZONE" VARCHAR, "FEEDER_MRID" VARCHAR, "CIRCUIT_KEY" VARCHAR)
LANGUAGE SQL
AS '
  select
	BUS_OPERATINGVOLTAGE_VALUE_KV::number(38,6) AS BUS_VN_KV,
    CONNECTIVITY_NODEID, 
    BUS_PHASES,
    BUS_GROUNDED_PHASES,
    BUS_GROUNDING_R_OHM::number(38,6) BUS_GROUNDING_R_OHM,
    BUS_GROUNDING_X_OHM::number(38,6) BUS_GROUNDING_X_OHM,
    BUS_IN_SERVICE::BOOLEAN AS BUS_IN_SERVICE,
    BUS_TYPE,
    BUS_ZONE,
    FEEDER_MRID,
    CIRCUIT_KEY
from
      SCE_SHARE.UC_SPEED.NMM_D_BUS_C_PP_VW
where
	circuit_key = CKT_KEY
    ';
CREATE OR REPLACE FUNCTION TESTDB.PUBLIC.GET_EXT_GRID("CKT_KEY" VARCHAR)
RETURNS TABLE ("DOWNSTREAM_CONNECTIVITYNODEID" VARCHAR, "CONDUCTING_EQUIPMENTID" VARCHAR, "EXTERNALGRID_CONNECTION_TYPE" VARCHAR, "EXTERNALGRID_CONNECTED_PHASE" VARCHAR, "EXTERNALGRID_FROM_PHASE" VARCHAR, "EXTERNALGRID_TO_PHASE" VARCHAR, "EXTERNALGRID_NOMINAL_VOLTAGE" NUMBER(38,6), "EXTERNALGRID_OPERATING_VOLTAGE" NUMBER(38,6), "EXTERNALGRID_VM_PU" NUMBER(38,6), "EXTERNALGRID_VA_DEGREE" NUMBER(38,6), "EXTERNALGRID_SC_MVA" NUMBER(38,6), "EXTERNALGRID_RX" NUMBER(38,6), "EXTERNALGRID_X0X" NUMBER(38,6), "EXTERNALGRID_R0X0" NUMBER(38,6), "EXTERNALGRID_Z2Z1" NUMBER(38,6), "EXTERNALGRID_C" NUMBER(38,6), "EXTERNALGRID_Z1_WEIGHT" NUMBER(38,6), "EXTERNALGRID_IN_SERVICE" BOOLEAN, "FEEDER_MRID" VARCHAR, "CIRCUIT_KEY" VARCHAR, "CONNECTIVITY_NODEID" VARCHAR, "UPSTREAM_CONNECTIVITYNODEID" VARCHAR, "STRUCTURE_MRID" VARCHAR, "STRUCTURE_NUM" VARCHAR)
LANGUAGE SQL
AS '
   select
	DOWNSTREAM_CONNECTIVITYNODEID,
	CONDUCTING_EQUIPMENTID,
	EXTERNALGRID_CONNECTION_TYPE,
	EXTERNALGRID_CONNECTED_PHASE,
	EXTERNALGRID_FROM_PHASE,
	EXTERNALGRID_TO_PHASE,
	EXTERNALGRID_NOMINAL_VOLTAGE::number(38,6) AS EXTERNALGRID_NOMINAL_VOLTAGE,
	EXTERNALGRID_OPERATING_VOLTAGE::number(38,6) AS EXTERNALGRID_OPERATING_VOLTAGE,
	EXTERNALGRID_VM_PU::number(38,6) AS EXTERNALGRID_VM_PU,
	EXTERNALGRID_VA_DEGREE::number(38,6) AS EXTERNALGRID_VA_DEGREE,
	EXTERNALGRID_SC_MVA::number(38,6) AS EXTERNALGRID_SC_MVA,
	EXTERNALGRID_RX::number(38,6) AS EXTERNALGRID_RX,
	EXTERNALGRID_X0X::number(38,6) AS EXTERNALGRID_X0X,
	EXTERNALGRID_R0X0::number(38,6) AS EXTERNALGRID_R0X0,
	EXTERNALGRID_Z2Z1::number(38,6) AS EXTERNALGRID_Z2Z1,
	EXTERNALGRID_C::number(38,6) AS EXTERNALGRID_C,
	EXTERNALGRID_Z1_WEIGHT::number(38,6) AS EXTERNALGRID_Z1_WEIGHT,
	EXTERNALGRID_IN_SERVICE::BOOLEAN AS EXTERNALGRID_IN_SERVICE,
	FEEDER_MRID,
	CIRCUIT_KEY,
	CONNECTIVITY_NODEID,
	UPSTREAM_CONNECTIVITYNODEID,
	STRUCTURE_MRID,
	STRUCTURE_NUM
from
     SCE_SHARE.UC_SPEED.NMM_D_TRACED_CIRCUIT_CONNECTIVITY_C
where
	linked_equiptype = ''CIRCUIT_HEAD''
	AND circuit_key =  CKT_KEY
    ';
CREATE OR REPLACE FUNCTION TESTDB.PUBLIC.GET_GEN("CKT_KEY" VARCHAR)
RETURNS TABLE ("UPSTREAM_CONNECTIVITYNODEID" VARCHAR, "CONDUCTING_EQUIPMENTID" VARCHAR, "GEN_CONNECTED_PHASE" VARCHAR, "GEN_FROM_PHASE" VARCHAR, "GEN_TO_PHASE" VARCHAR, "GEN_P_MW" VARCHAR, "GEN_VM_PU" NUMBER(38,6), "GEN_K" NUMBER(38,6), "GEN_K_REF" VARCHAR, "GEN_RDSS_OHM" NUMBER(38,6), "GEN_XDSS_OHM" NUMBER(38,6), "GEN_SN_MVA" NUMBER(38,6), "GEN_SCALING" NUMBER(38,6), "GEN_TYPE" VARCHAR, "GEN_CONNECTION_TYPE" VARCHAR, "GEN_IN_SERVICE" BOOLEAN, "FEEDER_MRID" VARCHAR, "CIRCUIT_KEY" VARCHAR, "CONNECTIVITY_NODEID" VARCHAR, "STRUCTURE_MRID" VARCHAR, "STRUCTURE_NUM" VARCHAR)
LANGUAGE SQL
AS '
        select
            UPSTREAM_CONNECTIVITYNODEID,
            CONDUCTING_EQUIPMENTID,
            GEN_CONNECTED_PHASE,
            GEN_FROM_PHASE,
            GEN_TO_PHASE,
            GEN_P_MW,
            GEN_VM_PU::number(38,6) AS GEN_VM_PU,
            GEN_K::number(38,6) AS GEN_K,
            GEN_K_REF,
            GEN_RDSS_OHM::number(38,6) AS GEN_RDSS_OHM,
            GEN_XDSS_OHM::number(38,6) AS GEN_XDSS_OHM,
            GEN_SN_MVA::number(38,6) AS GEN_SN_MVA,
            GEN_SCALING::number(38,6) AS GEN_SCALING,
            GEN_TYPE,
            GEN_CONNECTION_TYPE,
            GEN_IN_SERVICE,
            FEEDER_MRID,
            CIRCUIT_KEY,
            CONNECTIVITY_NODEID,
            STRUCTURE_MRID,
            STRUCTURE_NUM

        from
              SCE_SHARE.UC_SPEED.NMM_D_TRACED_CIRCUIT_CONNECTIVITY_C 
        where
            linked_equiptype = ''GEN''
            and circuit_key =  CKT_KEY
    ';
CREATE OR REPLACE FUNCTION TESTDB.PUBLIC.GET_LINE("CKT_KEY" VARCHAR)
RETURNS TABLE ("CONDUCTING_EQUIPMENTID" VARCHAR, "UPSTREAM_CONNECTIVITYNODEID" VARCHAR, "DOWNSTREAM_CONNECTIVITYNODEID" VARCHAR, "MATERIAL_CODE" VARCHAR, "LINE_MODEL_TYPE" VARCHAR, "LINE_CONNECTED_PHASE" VARCHAR, "LINE_FROM_PHASE" VARCHAR, "LINE_TO_PHASE" VARCHAR, "IN_SERVICE" BOOLEAN, "FEEDER_MRID" VARCHAR, "CIRCUIT_KEY" VARCHAR, "CONNECTIVITY_NODEID" VARCHAR, "STRUCTURE_MRID" VARCHAR, "STRUCTURE_NUM" VARCHAR)
LANGUAGE SQL
AS '
     select
    CONDUCTING_EQUIPMENTID,
    UPSTREAM_CONNECTIVITYNODEID,
    DOWNSTREAM_CONNECTIVITYNODEID,
    MATERIAL_CODE,
    LINE_MODEL_TYPE,
    LINE_CONNECTED_PHASE,
    LINE_FROM_PHASE,
    LINE_TO_PHASE,
    IN_SERVICE,
    FEEDER_MRID,
    CIRCUIT_KEY,
    CONNECTIVITY_NODEID,
    STRUCTURE_MRID,
    STRUCTURE_NUM
from
 SCE_SHARE.UC_SPEED.NMM_D_TRACED_CIRCUIT_CONNECTIVITY_C
where
    linked_equiptype in (
        ''OH_PRIMARY_CONDUCTOR'',
        ''UG_PRIMARY_CONDUCTOR''
    )
    and circuit_key =  CKT_KEY
    ';
CREATE OR REPLACE FUNCTION TESTDB.PUBLIC.GET_LOAD("CKT_KEY" VARCHAR)
RETURNS TABLE ("UPSTREAM_CONNECTIVITYNODEID" VARCHAR, "CONDUCTING_EQUIPMENTID" VARCHAR, "STRUCTURE_NUM" VARCHAR, "LOAD_FROM_PHASE" VARCHAR, "LOAD_TO_PHASE" VARCHAR, "LOAD_P_MW" VARCHAR, "LOAD_Q_MVAR" VARCHAR, "LOAD_CONNECTED_PHASE" VARCHAR, "LOAD_CONST_Z_PERCENT_P" NUMBER(38,6), "LOAD_CONST_I_PERCENT_P" NUMBER(38,6), "LOAD_CONST_Z_PERCENT_Q" NUMBER(38,6), "LOAD_CONST_I_PERCENT_Q" NUMBER(38,6), "LOAD_SN_MVA" NUMBER(38,6), "LOAD_SCALING" NUMBER(38,6), "LOAD_TYPE" VARCHAR, "LOAD_CONNECTION_TYPE" VARCHAR, "LOAD_IN_SERVICE" BOOLEAN, "FEEDER_MRID" VARCHAR, "CIRCUIT_KEY" VARCHAR, "CONNECTIVITY_NODEID" VARCHAR, "STRUCTURE_MRID" VARCHAR)
LANGUAGE SQL
AS '
        select
            UPSTREAM_CONNECTIVITYNODEID,
            CONDUCTING_EQUIPMENTID,
            STRUCTURE_NUM,
            LOAD_FROM_PHASE,
            LOAD_TO_PHASE,
            LOAD_P_MW,
            LOAD_Q_MVAR,
            LOAD_CONNECTED_PHASE,
            LOAD_CONST_Z_PERCENT_P::number(38,6) AS LOAD_CONST_Z_PERCENT_P,
            LOAD_CONST_I_PERCENT_P::number(38,6) AS LOAD_CONST_I_PERCENT_P,
            LOAD_CONST_Z_PERCENT_Q::number(38,6) AS LOAD_CONST_Z_PERCENT_Q,
            LOAD_CONST_I_PERCENT_Q::number(38,6) AS LOAD_CONST_I_PERCENT_Q,
            LOAD_SN_MVA::number(38,6) AS LOAD_SN_MVA,
            LOAD_SCALING::number(38,6) AS LOAD_SCALING,
            LOAD_TYPE,
            LOAD_CONNECTION_TYPE,
            LOAD_IN_SERVICE,
            FEEDER_MRID,
            CIRCUIT_KEY,
            CONNECTIVITY_NODEID,
            STRUCTURE_MRID

        from
              SCE_SHARE.UC_SPEED.NMM_D_TRACED_CIRCUIT_CONNECTIVITY_C
        where
            linked_equiptype = ''LOAD''
            and circuit_key =  CKT_KEY
    ';
CREATE OR REPLACE FUNCTION TESTDB.PUBLIC.GET_SGEN("CKT_KEY" VARCHAR)
RETURNS TABLE ("UPSTREAM_CONNECTIVITYNODEID" VARCHAR, "CONDUCTING_EQUIPMENTID" VARCHAR, "SGEN_CONNECTED_PHASE" VARCHAR, "SGEN_FROM_PHASE" VARCHAR, "SGEN_TO_PHASE" VARCHAR, "SGEN_P_MW" VARCHAR, "SGEN_Q_MVAR" VARCHAR, "SGEN_K" NUMBER(38,0), "SGEN_K_REF" VARCHAR, "SGEN_SN_MVA" NUMBER(38,6), "SGEN_SCALING" NUMBER(38,6), "SGEN_TYPE" VARCHAR, "SGEN_CONNECTION_TYPE" VARCHAR, "SGEN_IN_SERVICE" BOOLEAN, "SGEN_CONTROL_MODE" VARCHAR, "SGEN_CONTROL_CURVE" VARCHAR, "FEEDER_MRID" VARCHAR, "CIRCUIT_KEY" VARCHAR, "CONNECTIVITY_NODEID" VARCHAR, "STRUCTURE_MRID" VARCHAR, "STRUCTURE_NUM" VARCHAR)
LANGUAGE SQL
AS '
        select
            UPSTREAM_CONNECTIVITYNODEID,
            CONDUCTING_EQUIPMENTID,
            SGEN_CONNECTED_PHASE,
            SGEN_FROM_PHASE,
            SGEN_TO_PHASE,
            SGEN_P_MW,
            SGEN_Q_MVAR,
            SGEN_K,
            SGEN_K_REF,
            SGEN_SN_MVA::number(38,6) AS SGEN_SN_MVA,
            SGEN_SCALING::number(38,6) AS SGEN_SCALING,
            SGEN_TYPE,
            SGEN_CONNECTION_TYPE,
            SGEN_IN_SERVICE,
            SGEN_CONTROL_MODE,
            SGEN_CONTROL_CURVE,
            FEEDER_MRID,
            CIRCUIT_KEY,
            CONNECTIVITY_NODEID,
            STRUCTURE_MRID,
            STRUCTURE_NUM
        from
              SCE_SHARE.UC_SPEED.NMM_D_TRACED_CIRCUIT_CONNECTIVITY_C
        where
            linked_equiptype = ''SGEN''
            and circuit_key =  CKT_KEY
    ';
CREATE OR REPLACE FUNCTION TESTDB.PUBLIC.GET_SHUNT("CKT_KEY" VARCHAR)
RETURNS TABLE ("CONDUCTING_EQUIPMENTID" VARCHAR, "MATERIAL_CODE" VARCHAR, "UPSTREAM_CONNECTIVITYNODEID" VARCHAR, "SHUNT_CONNECTION_TYPE" VARCHAR, "SHUNT_CONNECTED_PHASE" VARCHAR, "SHUNT_FROM_PHASE" VARCHAR, "SHUNT_TO_PHASE" VARCHAR, "SHUNT_VN_KV" NUMBER(38,6), "SHUNT_CLOSED" BOOLEAN, "SHUNT_CONTROL_MODE" VARCHAR, "SHUNT_V_THR_ON" NUMBER(18,6), "SHUNT_V_THR_OFF" NUMBER(18,6), "SHUNT_IN_SERVICE" BOOLEAN, "FEEDER_MRID" VARCHAR, "CIRCUIT_KEY" VARCHAR, "CONNECTIVITY_NODEID" VARCHAR, "STRUCTURE_MRID" VARCHAR, "STRUCTURE_NUM" VARCHAR)
LANGUAGE SQL
AS '
       select
    CONDUCTING_EQUIPMENTID,
    MATERIAL_CODE,
    UPSTREAM_CONNECTIVITYNODEID,
    SHUNT_CONNECTION_TYPE,
    SHUNT_CONNECTED_PHASE,
    SHUNT_FROM_PHASE,
    SHUNT_TO_PHASE,
    SHUNT_VN_KV::number(38,6) AS SHUNT_VN_KV,
    SHUNT_CLOSED,
    SHUNT_CONTROL_MODE,
    SHUNT_V_THR_ON,
    SHUNT_V_THR_OFF,
    SHUNT_IN_SERVICE,
    FEEDER_MRID,
    CIRCUIT_KEY,
    CONNECTIVITY_NODEID,
    STRUCTURE_MRID,
    STRUCTURE_NUM
from
  SCE_SHARE.UC_SPEED.NMM_D_TRACED_CIRCUIT_CONNECTIVITY_C
where
    linked_equiptype = ''CAPACITOR_BANK''
    and circuit_key =   CKT_KEY
    ';
CREATE OR REPLACE FUNCTION TESTDB.PUBLIC.GET_XFMR("CKT_KEY" VARCHAR)
RETURNS TABLE ("CIRCUIT_KEY" VARCHAR, "CONDUCTING_EQUIPMENTID" VARCHAR, "UPSTREAM_CONNECTIVITYNODEID" VARCHAR, "DOWNSTREAM_CONNECTIVITYNODEID" VARCHAR, "MATERIAL_CODE" VARCHAR, "XFRMR_SN_MVA" NUMBER(38,6), "XFRMR_VN_HV_KV" NUMBER(38,6), "XFRMR_VN_LV_KV" NUMBER(38,6), "XFRMR_VK_PERCENT" NUMBER(38,6), "XFRMR_VKR_PERCENT" NUMBER(38,6), "XFRMR_PFE_KW" NUMBER(38,6), "XFRMR_I0_PERCENT" NUMBER(38,6), "XFRMR_VK0_PERCENT" NUMBER(38,6), "XFRMR_VKR0_PERCENT" NUMBER(38,6), "XFRMR_MAG0_PERCENT" NUMBER(38,6), "XFRMR_MAG0_RX" NUMBER(38,6), "XFRMR_SI0_HV_PARTIAL" NUMBER(38,6), "XFRMR_VECTOR_GROUP" VARCHAR, "XFRMR_SHIFT_DEGREE" NUMBER(38,6), "XFRMR_TAP_SIDE" VARCHAR, "XFRMR_TAP_NEUTRAL" NUMBER(38,6), "XFRMR_TAP_MIN" NUMBER(38,6), "XFRMR_TAP_MAX" NUMBER(38,6), "XFRMR_TAP_STEP_PERCENT" NUMBER(38,6), "XFRMR_TAP_STEP_DEGREE" NUMBER(38,6), "XFRMR_Z0_PERCENT" NUMBER(38,6), "XFRMR_Z1_PERCENT" NUMBER(38,6), "XFRMR_X1R1_RATIO" NUMBER(38,6), "XFRMR_X0R0_RATIO" NUMBER(38,6), "XFRMR_HV_CONNECTION_TYPE" VARCHAR, "XFRMR_LV_CONNECTION_TYPE" VARCHAR, "XFRMR_CONNECTED_PHASE" VARCHAR, "XFRMR_HV_FROM_PHASE" VARCHAR, "XFRMR_HV_TO_PHASE" VARCHAR, "XFRMR_LV_FROM_PHASE" VARCHAR, "XFRMR_LV_TO_PHASE" VARCHAR, "XFRMR_TAP_POS" NUMBER(38,6), "XFRMR_IN_SERVICE" BOOLEAN, "XFRMR_CONTROL_MODE" VARCHAR, "XFRMR_CONTROL_PARAMETERS" VARCHAR, "FEEDER_MRID" VARCHAR, "CONNECTIVITY_NODEID" VARCHAR, "STRUCTURE_MRID" VARCHAR, "STRUCTURE_NUM" VARCHAR)
LANGUAGE SQL
AS '
       select
        CIRCUIT_KEY,
        CONDUCTING_EQUIPMENTID,
        UPSTREAM_CONNECTIVITYNODEID,
        DOWNSTREAM_CONNECTIVITYNODEID,
        MATERIAL_CODE,
        XFRMRASSETINFO_NORMALMVA::number(38,6) XFRMR_SN_MVA,
        XFRMRASSETINFO_HIGHSIDEU_VALUE::number(38,6) XFRMR_VN_HV_KV,
        XFRMRASSETINFO_LOWSIDEU_VALUE::number(38,6) XFRMR_VN_LV_KV,
        XFRMR_VK0_PERCENT::number(38,6) XFRMR_VK_PERCENT,
        XFRMR_VK0_PERCENT::number(38,6) XFRMR_VKR_PERCENT,
        XFRMR_PFE_KW::number(38,6) XFRMR_PFE_KW,
        XFRMR_I0_PERCENT::number(38,6) XFRMR_I0_PERCENT,
        XFRMR_VK0_PERCENT::number(38,6) XFRMR_VK0_PERCENT,
        XFRMR_VKR0_PERCENT::number(38,6) XFRMR_VKR0_PERCENT,
        XFRMR_MAG0_PERCENT::number(38,6) XFRMR_MAG0_PERCENT,
        XFRMR_MAG0_RX::number(38,6) XFRMR_MAG0_RX,
        XFRMR_SI0_HV_PARTIAL::number(38,6) XFRMR_SI0_HV_PARTIAL,
        XFRMR_VECTOR_GROUP,
        XFRMR_SHIFT_DEGREE::number(38,6) XFRMR_SHIFT_DEGREE,
        XFRMR_TAP_SIDE,
        XFRMR_TAP_NEUTRAL::number(38,6) XFRMR_TAP_NEUTRAL,
        XFRMR_TAP_MIN::number(38,6) XFRMR_TAP_MIN,
        XFRMR_TAP_MAX::number(38,6) XFRMR_TAP_MAX,
        XFRMR_TAP_STEP_PERCENT::number(38,6) XFRMR_TAP_STEP_PERCENT,
        XFRMR_TAP_STEP_DEGREE::number(38,6) XFRMR_TAP_STEP_DEGREE,
        XFRMREND_Z0_VALUE::number(38,6) XFRMR_Z0_PERCENT,
        XFRMREND_Z1_VALUE::number(38,6) XFRMR_Z1_PERCENT,
        XFRMREND_X1R1_RATIO::number(38,6) XFRMR_X1R1_RATIO,
        XFRMREND_X0R0_RATIO::number(38,6) XFRMR_X0R0_RATIO,
        XFRMR_HV_CONNECTION_TYPE,
        XFRMR_LV_CONNECTION_TYPE,
        XFRMR_CONNECTED_PHASE,
        XFRMR_HV_FROM_PHASE,
        XFRMR_HV_TO_PHASE,
        XFRMR_LV_FROM_PHASE,
        XFRMR_LV_TO_PHASE,
        XFRMR_TAP_POS::NUMBER as XFRMR_TAP_POS,
        XFRMR_IN_SERVICE,
        XFRMR_CONTROL_MODE,
        XFRMR_CONTROL_PARAMETERS,
        FEEDER_MRID,
        CONNECTIVITY_NODEID,
        STRUCTURE_MRID,
        STRUCTURE_NUM
from
   SCE_SHARE.UC_SPEED.NMM_D_TRACED_CIRCUIT_CONNECTIVITY_C 
where
    linked_equiptype in (
        ''OH_TRANSFORMER'',
        ''UG_TRANSFORMER'',
        ''VOLTAGE_REGULATOR'',
        ''LINE_REGULATOR''
    )
    and circuit_key =  CKT_KEY
    ';
CREATE OR REPLACE FUNCTION TESTDB.PUBLIC.GET_SWITCH("CKT_KEY" VARCHAR)
RETURNS TABLE ("CONDUCTING_EQUIPMENTID" VARCHAR, "UPSTREAM_CONNECTIVITYNODEID" VARCHAR, "DOWNSTREAM_CONNECTIVITYNODEID" VARCHAR, "SWITCH_CONNECTED_PHASE" VARCHAR, "SWITCH_ELEMENT" VARCHAR, "SWITCH_FROM_PHASE" VARCHAR, "SWITCH_ET" VARCHAR, "SWITCH_CLOSED" BOOLEAN, "SWITCH_Z_OHM" NUMBER(18,6), "SWITCH_TYPE" VARCHAR, "FEEDER_MRID" VARCHAR, "CIRCUIT_KEY" VARCHAR, "CONNECTIVITY_NODEID" VARCHAR, "STRUCTURE_MRID" VARCHAR, "STRUCTURE_NUM" VARCHAR)
LANGUAGE SQL
AS '
     select
    CONDUCTING_EQUIPMENTID,
    UPSTREAM_CONNECTIVITYNODEID,
    DOWNSTREAM_CONNECTIVITYNODEID,

    SWITCH_CONNECTED_PHASE,
    SWITCH_CONNECTINGELEMENT_TYPE SWITCH_ELEMENT,
    SWITCH_FROM_PHASE,
    SWITCH_CONNECTINGELEMENT_TYPE SWITCH_ET,
    SWITCH_STATUS_VALUE SWITCH_CLOSED,
    SWITCH_Z_OHM,
    SWITCH_TYPE,
    FEEDER_MRID,
    CIRCUIT_KEY,
    CONNECTIVITY_NODEID,
    STRUCTURE_MRID,
    STRUCTURE_NUM
from
   SCE_SHARE.UC_SPEED.NMM_D_TRACED_CIRCUIT_CONNECTIVITY_C
where
    linked_equiptype in (
        ''UG_SWITCH'',
       ''OH_SWITCH'',
        ''ELBOW'',
        ''UG_BUS'',
        ''JUNCTION_BAR'',
        ''PE_GEAR_SWITCH'',
        ''FUSED_CUTOUT'',
        ''BRANCH_LINE_FUSE'',
        ''AUTOMATIC_RECLOSER'',
        ''FAULT_INTERRUPTER'',
        ''VAC_FAULT_INTERRUPTER'',
        ''OH_TRANSFORMER_LOCATION'',
        ''GROUND_BANK'',
        ''IBANK'',
        ''TAP'',
        ''POTHEAD'',
        ''TIE_CABLE'',
        ''CUTOUT'',
        ''DISTRIBUTION_TAP''        
    )
    and circuit_key = CKT_KEY
    ';