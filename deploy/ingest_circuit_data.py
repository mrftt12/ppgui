import os
from pathlib import Path
import snowflake.connector

user = os.getenv("SNOWFLAKE_USER")
password = os.getenv("SNOWFLAKE_PASSWORD")
account = os.getenv("SNOWFLAKE_ACCOUNT")
warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
database = "GRIDMOD_DEV_TD"
schema = "UC_POC"


tc_table = "NMM_D_TRACED_CIRCUIT_CONNECTIVITY_C_PP_VW_MC4"
bus_table = "NMM_D_BUS_C_PP_VW_MC4"


def get_circuit_files(start_path):
    files = []
    for dirpath, dirnames, filenames in os.walk(start_path):
        if filenames:
            for filename in filenames:
                file_path = os.path.join(dirpath, filename)
                parent_folder = os.path.basename(os.path.normpath(dirpath))
                if parent_folder != 'profile' and (filename.endswith(".parquet") or filename.endswith(".parquet.gzip")):
                    files.append(file_path)
    return files


stage_name = "AJITHS_TEMP_STAGE"
file_format_name = "my_parquet_format"

def ingest_circuits():
    conn = None
    try:
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            # warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            warehouse="TEST_SMALL",
            database=database,
            schema=schema
        )
        cur = conn.cursor()

        cur.execute(f"CREATE OR REPLACE STAGE {stage_name}")
        print(f"Stage '{stage_name}' created or already exists.")

        cur.execute(
            f"CREATE OR REPLACE FILE FORMAT {file_format_name} TYPE = 'PARQUET'")
        print(f"File format '{file_format_name}' created or already exists.")

        for file_name in get_circuit_files('tmp/circuit_and_profile_data'):
            file_path = os.path.abspath(file_name)
            if not os.path.exists(file_path):
                print(f"File not found: {file_path}. Skipping.")
                continue

            table_name = bus_table if 'bus' in file_path else tc_table

            print(f"Processing file: {file_name}")
            print(f"Target table: {database}.{schema}.{table_name}")

            put_command = f"PUT file://{file_path} @{stage_name}"
            cur.execute(put_command)
            print(f"File '{file_name}' uploaded to stage '{stage_name}'.")

            if table_name == bus_table or False:
                copy_into_query = f"""
                COPY INTO {table_name}
                FROM '@{stage_name}/{os.path.basename(file_path)}'
                FILE_FORMAT = '{file_format_name}'
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                """
            else:
                # $1:CONDUCTING_EQUIPMENTID
                copy_into_query = f"""
                        COPY INTO {table_name} (FEEDER_MRID, CIRCUIT_KEY, CONDUCTING_EQUIPMENTID, CONNECTIVITY_NODEID, FLOW_DIRECTION, LINKED_EQUIPTYPE, EQUIPMENT_TYPE, UPSTREAM_CONNECTIVITYNODEID, DOWNSTREAM_CONNECTIVITYNODEID, IS_ORIENTED, END_TERMINAL, LEVEL_ID, SUB_LEVEL_ID, HIERARCHY_LEVEL_ID, HIERARCHY_SUB_LEVEL_ID, STRUCTURE_MRID, STRUCTURE_NUM, MATERIAL_CODE, ELL, PLL, AGE, RISK_SCORE, LINE_LENGTH_KM, LINE_R_OHM_PER_KM, LINE_X_OHM_PER_KM, LINE_R0_OHM_PER_KM, LINE_X0_OHM_PER_KM, LINE_C_NF_PER_KM, LINE_C0_NF_PER_KM, LINE_MAX_I_KA, LINE_G_US_PER_KM, LINE_B_US_PER_KM, LINE_GN_US_PER_KM, LINE_BN_US_PER_KM, LINE_RN_OHM_PER_KM, LINE_XN_OHM_PER_KM, LINE_MAX_IN_KA, LINE_TYPE, LINE_MODEL_TYPE, LINE_CONNECTED_PHASE, LINE_FROM_PHASE, LINE_TO_PHASE, IN_SERVICE, SHUNT_P_MW, SHUNT_Q_MVAR, SHUNT_CONNECTION_TYPE, SHUNT_CONNECTED_PHASE, SHUNT_FROM_PHASE, SHUNT_TO_PHASE, SHUNT_VN_KV, SHUNT_CLOSED, SHUNT_CONTROL_MODE, SHUNT_V_THR_ON, SHUNT_V_THR_OFF, SHUNT_IN_SERVICE, SWITCH_ELEMENT, SWITCH_ET, SWITCH_TYPE, SWITCH_CLOSED, SWITCH_IN_KA, SWITCH_CONNECTED_PHASE, SWITCH_FROM_PHASE, SWITCH_TO_PHASE, SWITCH_Z_OHM, XFRMR_BY_PHASE, XFRMR_SN_MVA, XFRMR_VN_HV_KV, XFRMR_VN_LV_KV, XFRMR_VK_PERCENT, XFRMR_VKR_PERCENT, XFRMR_SHIFT_DEGREE, XFRMR_PFE_KW, XFRMR_I0_PERCENT, XFRMR_VK0_PERCENT, XFRMR_VKR0_PERCENT, XFRMR_MAG0_PERCENT, XFRMR_MAG0_RX, XFRMR_SI0_HV_PARTIAL, XFRMR_VECTOR_GROUP, XFRMR_TAP_SIDE, XFRMR_TAP_NEUTRAL, XFRMR_TAP_MIN, XFRMR_TAP_MAX, XFRMR_TAP_STEP_PERCENT, XFRMR_TAP_STEP_DEGREE, XFRMR_Z0_PERCENT, XFRMR_Z1_PERCENT, XFRMR_X1R1_RATIO, XFRMR_X0R0_RATIO, XFRMR_HV_CONNECTION_TYPE, XFRMR_LV_CONNECTION_TYPE, XFRMR_CONNECTED_PHASE, XFRMR_HV_FROM_PHASE, XFRMR_HV_TO_PHASE, XFRMR_LV_FROM_PHASE, XFRMR_LV_TO_PHASE, XFRMR_TAP_POS, XFRMR_IN_SERVICE, XFRMR_CONTROL_MODE, XFRMR_CONTROL_PARAMETERS, XFRMR_VM_LOWER_PU, XFRMR_VM_UPPER_PU, XFRMR_CONTROL_SIDE, XFRMR_TOL, XFRMR_PT_RATIO, XFRMR_CT_RATING_A, XFRMR_R_LDC_FORWARD_V, XFRMR_X_LDC_FORWARD_V, XFRMR_R_LDC_REVERSE_V, XFRMR_X_LDC_REVERSE_V, XFRMR_BANDWIDTH_FORWARD_V, XFRMR_BANDWIDTH_REVERSE_V, XFRMR_V_SET_SECONDARY_V, XFRMR_MODE, SGEN_CONNECTED_PHASE, SGEN_FROM_PHASE, SGEN_TO_PHASE, SGEN_P_MW, SGEN_Q_MVAR, SGEN_K, SGEN_K_REF, SGEN_SN_MVA, SGEN_SCALING, SGEN_TYPE, SGEN_CONNECTION_TYPE, SGEN_IN_SERVICE, SGEN_CONTROL_MODE, SGEN_CONTROL_CURVE, LOAD_CONNECTED_PHASE, LOAD_FROM_PHASE, LOAD_TO_PHASE, LOAD_P_MW, LOAD_Q_MVAR, LOAD_CONST_Z_PERCENT_P, LOAD_CONST_I_PERCENT_P, LOAD_CONST_Z_PERCENT_Q, LOAD_CONST_I_PERCENT_Q, LOAD_SN_MVA, LOAD_SCALING, LOAD_TYPE, LOAD_CONNECTION_TYPE, LOAD_IN_SERVICE, EXTERNALGRID_CONNECTION_TYPE, EXTERNALGRID_CONNECTED_PHASE, EXTERNALGRID_FROM_PHASE, EXTERNALGRID_TO_PHASE, EXTERNALGRID_NOMINAL_VOLTAGE, EXTERNALGRID_OPERATING_VOLTAGE, EXTERNALGRID_VM_PU, EXTERNALGRID_VA_DEGREE, EXTERNALGRID_SC_MVA, EXTERNALGRID_RX, EXTERNALGRID_X0X, EXTERNALGRID_R0X0, EXTERNALGRID_Z2Z1, EXTERNALGRID_C, EXTERNALGRID_Z1_WEIGHT, EXTERNALGRID_IN_SERVICE, GEN_CONNECTED_PHASE, GEN_FROM_PHASE, GEN_TO_PHASE, GEN_P_MW, GEN_VM_PU, GEN_K, GEN_K_REF, GEN_RDSS_OHM, GEN_XDSS_OHM, GEN_SN_MVA, GEN_SCALING, GEN_TYPE, GEN_CONNECTION_TYPE, GEN_IN_SERVICE, EQUIPMENT_STATE, CHANGED_DATE, EDW_CREATED_DATE, EDW_CREATED_BY, EDW_MODIFIED_DATE, EDW_MODIFIED_BY, EDW_BATCH_ID, EDW_BATCH_DETAIL_ID, EDW_LAST_DML_CD)
                        FROM (
                        SELECT
                            $1:FEEDER_MRID::VARCHAR(255),
                            $1:CIRCUIT_KEY::VARCHAR(255),
                            $1:CONDUCTING_EQUIPMENTID::VARCHAR(255),
                            $1:CONNECTIVITY_NODEID::VARCHAR(255),
                            $1:FLOW_DIRECTION::VARCHAR(255),
                            $1:LINKED_EQUIPTYPE::VARCHAR(255),
                            $1:EQUIPMENT_TYPE::VARCHAR(33),
                            $1:UPSTREAM_CONNECTIVITYNODEID::VARCHAR(255),
                            $1:DOWNSTREAM_CONNECTIVITYNODEID::VARCHAR(255),
                            $1:IS_ORIENTED::BOOLEAN,
                            $1:END_TERMINAL::BOOLEAN,
                            $1:LEVEL_ID::NUMBER(38,0),
                            $1:SUB_LEVEL_ID::NUMBER(38,0),
                            $1:HIERARCHY_LEVEL_ID::NUMBER(38,0),
                            $1:HIERARCHY_SUB_LEVEL_ID::NUMBER(38,0),
                            $1:STRUCTURE_MRID::VARCHAR(255),
                            $1:STRUCTURE_NUM::VARCHAR(255),
                            $1:MATERIAL_CODE::VARCHAR(255),
                            $1:ELL::NUMBER(18,6),
                            $1:PLL::NUMBER(18,6),
                            $1:AGE::NUMBER(18,6),
                            $1:RISK_SCORE::NUMBER(18,6),
                            $1:LINE_LENGTH_KM::NUMBER(18,6),
                            $1:LINE_R_OHM_PER_KM::NUMBER(18,6),
                            $1:LINE_X_OHM_PER_KM::NUMBER(18,6),
                            $1:LINE_R0_OHM_PER_KM::NUMBER(18,6),
                            $1:LINE_X0_OHM_PER_KM::NUMBER(18,6),
                            $1:LINE_C_NF_PER_KM::NUMBER(18,6),
                            $1:LINE_C0_NF_PER_KM::NUMBER(18,6),
                            $1:LINE_MAX_I_KA::NUMBER(18,6),
                            $1:LINE_G_US_PER_KM::NUMBER(18,6),
                            $1:LINE_B_US_PER_KM::NUMBER(18,6),
                            $1:LINE_GN_US_PER_KM::NUMBER(18,6),
                            $1:LINE_BN_US_PER_KM::NUMBER(18,6),
                            $1:LINE_RN_OHM_PER_KM::NUMBER(18,6),
                            $1:LINE_XN_OHM_PER_KM::NUMBER(18,6),
                            $1:LINE_MAX_IN_KA::NUMBER(18,6),
                            $1:LINE_TYPE::VARCHAR(255),
                            $1:LINE_MODEL_TYPE::VARCHAR(255),
                            $1:LINE_CONNECTED_PHASE::VARCHAR(255),
                            $1:LINE_FROM_PHASE::VARCHAR(255),
                            $1:LINE_TO_PHASE::VARCHAR(255),
                            $1:IN_SERVICE::BOOLEAN,
                            $1:SHUNT_P_MW::NUMBER(18,6),
                            $1:SHUNT_Q_MVAR::NUMBER(18,6),
                            $1:SHUNT_CONNECTION_TYPE::VARCHAR(255),
                            $1:SHUNT_CONNECTED_PHASE::VARCHAR(255),
                            $1:SHUNT_FROM_PHASE::VARCHAR(255),
                            $1:SHUNT_TO_PHASE::VARCHAR(255),
                            $1:SHUNT_VN_KV::NUMBER(18,6),
                            $1:SHUNT_CLOSED::BOOLEAN,
                            $1:SHUNT_CONTROL_MODE::VARCHAR(255),
                            $1:SHUNT_V_THR_ON::NUMBER(18,6),
                            $1:SHUNT_V_THR_OFF::NUMBER(18,6),
                            $1:SHUNT_IN_SERVICE::BOOLEAN,
                            $1:SWITCH_ELEMENT::VARCHAR(255),
                            $1:SWITCH_ET::VARCHAR(255),
                            $1:SWITCH_TYPE::VARCHAR(255),
                            $1:SWITCH_CLOSED::VARCHAR(255),
                            $1:SWITCH_IN_KA::NUMBER(18,6),
                            $1:SWITCH_CONNECTED_PHASE::VARCHAR(255),
                            $1:SWITCH_FROM_PHASE::VARCHAR(255),
                            $1:SWITCH_TO_PHASE::VARCHAR(255),
                            $1:SWITCH_Z_OHM::NUMBER(18,6),
                            $1:XFRMR_BY_PHASE::VARCHAR(16777216),
                            $1:XFRMR_SN_MVA::NUMBER(18,6),
                            $1:XFRMR_VN_HV_KV::VARCHAR(255),
                            $1:XFRMR_VN_LV_KV::VARCHAR(255),
                            $1:XFRMR_VK_PERCENT::NUMBER(18,6),
                            $1:XFRMR_VKR_PERCENT::NUMBER(18,6),
                            $1:XFRMR_SHIFT_DEGREE::NUMBER(18,6),
                            $1:XFRMR_PFE_KW::NUMBER(18,6),
                            $1:XFRMR_I0_PERCENT::NUMBER(18,6),
                            $1:XFRMR_VK0_PERCENT::NUMBER(18,6),
                            $1:XFRMR_VKR0_PERCENT::NUMBER(18,6),
                            $1:XFRMR_MAG0_PERCENT::NUMBER(18,6),
                            $1:XFRMR_MAG0_RX::NUMBER(18,6),
                            $1:XFRMR_SI0_HV_PARTIAL::NUMBER(18,6),
                            $1:XFRMR_VECTOR_GROUP::VARCHAR(255),
                            $1:XFRMR_TAP_SIDE::VARCHAR(255),
                            $1:XFRMR_TAP_NEUTRAL::NUMBER(36,0),
                            $1:XFRMR_TAP_MIN::NUMBER(36,0),
                            $1:XFRMR_TAP_MAX::NUMBER(36,0),
                            $1:XFRMR_TAP_STEP_PERCENT::NUMBER(18,6),
                            $1:XFRMR_TAP_STEP_DEGREE::NUMBER(18,6),
                            $1:XFRMR_Z0_PERCENT::NUMBER(18,6),
                            $1:XFRMR_Z1_PERCENT::NUMBER(18,6),
                            $1:XFRMR_X1R1_RATIO::NUMBER(18,6),
                            $1:XFRMR_X0R0_RATIO::NUMBER(18,6),
                            $1:XFRMR_HV_CONNECTION_TYPE::VARCHAR(255),
                            $1:XFRMR_LV_CONNECTION_TYPE::VARCHAR(255),
                            $1:XFRMR_CONNECTED_PHASE::VARCHAR(255),
                            $1:XFRMR_HV_FROM_PHASE::VARCHAR(255),
                            $1:XFRMR_HV_TO_PHASE::VARCHAR(255),
                            $1:XFRMR_LV_FROM_PHASE::VARCHAR(255),
                            $1:XFRMR_LV_TO_PHASE::VARCHAR(255),
                            $1:XFRMR_TAP_POS::NUMBER(36,0),
                            $1:XFRMR_IN_SERVICE::BOOLEAN,
                            $1:XFRMR_CONTROL_MODE::VARCHAR(255),
                            $1:XFRMR_CONTROL_PARAMETERS::VARCHAR(255),
                            $1:XFRMR_VM_LOWER_PU::NUMBER(18,6),
                            $1:XFRMR_VM_UPPER_PU::NUMBER(18,6),
                            $1:XFRMR_CONTROL_SIDE::VARCHAR(255),
                            $1:XFRMR_TOL::NUMBER(18,6),
                            $1:XFRMR_PT_RATIO::NUMBER(18,6),
                            $1:XFRMR_CT_RATING_A::NUMBER(18,6),
                            $1:XFRMR_R_LDC_FORWARD_V::NUMBER(18,6),
                            $1:XFRMR_X_LDC_FORWARD_V::NUMBER(18,6),
                            $1:XFRMR_R_LDC_REVERSE_V::NUMBER(18,6),
                            $1:XFRMR_X_LDC_REVERSE_V::NUMBER(18,6),
                            $1:XFRMR_BANDWIDTH_FORWARD_V::NUMBER(18,6),
                            $1:XFRMR_BANDWIDTH_REVERSE_V::NUMBER(18,6),
                            $1:XFRMR_V_SET_SECONDARY_V::NUMBER(18,6),
                            $1:XFRMR_MODE::VARCHAR(255),
                            $1:SGEN_CONNECTED_PHASE::VARCHAR(255),
                            $1:SGEN_FROM_PHASE::VARCHAR(255),
                            $1:SGEN_TO_PHASE::VARCHAR(255),
                            $1:SGEN_P_MW::VARCHAR(255),
                            $1:SGEN_Q_MVAR::VARCHAR(255),
                            $1:SGEN_K::NUMBER(18,6),
                            $1:SGEN_K_REF::VARCHAR(255),
                            $1:SGEN_SN_MVA::NUMBER(18,6),
                            $1:SGEN_SCALING::NUMBER(18,6),
                            $1:SGEN_TYPE::VARCHAR(255),
                            $1:SGEN_CONNECTION_TYPE::VARCHAR(255),
                            $1:SGEN_IN_SERVICE::BOOLEAN,
                            $1:SGEN_CONTROL_MODE::VARCHAR(255),
                            $1:SGEN_CONTROL_CURVE::VARCHAR(255),
                            $1:LOAD_CONNECTED_PHASE::VARCHAR(255),
                            $1:LOAD_FROM_PHASE::VARCHAR(255),
                            $1:LOAD_TO_PHASE::VARCHAR(255),
                            $1:LOAD_P_MW::VARCHAR(255),
                            $1:LOAD_Q_MVAR::VARCHAR(255),
                            $1:LOAD_CONST_Z_PERCENT_P::NUMBER(18,6),
                            $1:LOAD_CONST_I_PERCENT_P::NUMBER(18,6),
                            $1:LOAD_CONST_Z_PERCENT_Q::NUMBER(18,6),
                            $1:LOAD_CONST_I_PERCENT_Q::NUMBER(18,6),
                            $1:LOAD_SN_MVA::NUMBER(18,6),
                            $1:LOAD_SCALING::NUMBER(18,6),
                            $1:LOAD_TYPE::VARCHAR(255),
                            $1:LOAD_CONNECTION_TYPE::VARCHAR(255),
                            $1:LOAD_IN_SERVICE::BOOLEAN,
                            $1:EXTERNALGRID_CONNECTION_TYPE::VARCHAR(255),
                            $1:EXTERNALGRID_CONNECTED_PHASE::VARCHAR(255),
                            $1:EXTERNALGRID_FROM_PHASE::VARCHAR(255),
                            $1:EXTERNALGRID_TO_PHASE::VARCHAR(255),
                            $1:EXTERNALGRID_NOMINAL_VOLTAGE::NUMBER(18,6),
                            $1:EXTERNALGRID_OPERATING_VOLTAGE::NUMBER(18,6),
                            $1:EXTERNALGRID_VM_PU::NUMBER(18,6),
                            $1:EXTERNALGRID_VA_DEGREE::NUMBER(18,6),
                            $1:EXTERNALGRID_SC_MVA::NUMBER(18,6),
                            $1:EXTERNALGRID_RX::NUMBER(18,6),
                            $1:EXTERNALGRID_X0X::NUMBER(18,6),
                            $1:EXTERNALGRID_R0X0::NUMBER(18,6),
                            $1:EXTERNALGRID_Z2Z1::NUMBER(18,6),
                            $1:EXTERNALGRID_C::NUMBER(18,6),
                            $1:EXTERNALGRID_Z1_WEIGHT::NUMBER(18,6),
                            $1:EXTERNALGRID_IN_SERVICE::BOOLEAN,
                            $1:GEN_CONNECTED_PHASE::VARCHAR(255),
                            $1:GEN_FROM_PHASE::VARCHAR(255),
                            $1:GEN_TO_PHASE::VARCHAR(255),
                            $1:GEN_P_MW::VARCHAR(255),
                            $1:GEN_VM_PU::NUMBER(18,6),
                            $1:GEN_K::NUMBER(18,6),
                            $1:GEN_K_REF::VARCHAR(255),
                            $1:GEN_RDSS_OHM::NUMBER(18,6),
                            $1:GEN_XDSS_OHM::NUMBER(18,6),
                            $1:GEN_SN_MVA::NUMBER(18,6),
                            $1:GEN_SCALING::NUMBER(18,6),
                            $1:GEN_TYPE::VARCHAR(255),
                            $1:GEN_CONNECTION_TYPE::VARCHAR(255),
                            $1:GEN_IN_SERVICE::BOOLEAN,
                            $1:EQUIPMENT_STATE::VARCHAR(255),
                            $1:CHANGED_DATE::DATE,
                            TO_TIMESTAMP_NTZ(($1:EDW_CREATED_DATE / 1000)::BIGINT, 6),
                            $1:EDW_CREATED_BY::VARCHAR(20),
                            TO_TIMESTAMP_NTZ(($1:EDW_MODIFIED_DATE / 1000)::BIGINT, 6),
                            $1:EDW_MODIFIED_BY::VARCHAR(20),
                            $1:EDW_BATCH_ID::NUMBER(38,0),
                            $1:EDW_BATCH_DETAIL_ID::NUMBER(38,0),
                            $1:EDW_LAST_DML_CD::VARCHAR(1)
                        FROM '@{stage_name}/{os.path.basename(file_path)}'
                        )
                        FILE_FORMAT = (TYPE = PARQUET);
                    """            
            cur.execute(copy_into_query)
            print(f"Data loaded into table '{table_name}'.")

            cur.execute(f"REMOVE @{stage_name}/{os.path.basename(file_path)}")
            print(f"File '{file_name}' removed from stage.")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        if 'cur' in locals() and cur:
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()

ingest_circuits()