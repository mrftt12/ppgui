import logging


class SQLQueries:
    def __init__(
        self,
        database_name,
        schema_name,
        bus_table_database,
        bus_table_schema,
        traced_connectivity_table_database,
        traced_connectivity_table_schema,
        bus_table,
        traced_connectivity_table,
        circuit_key,
    ):
        self.logger = logging.getLogger("SQLQueries_Logger")
        self.database_name = database_name
        self.schema_name = schema_name
        self.bus_table = f"{bus_table_database}.{bus_table_schema}.{bus_table}"
        self.traced_connectivity_table = (
            f"{traced_connectivity_table_database}.{traced_connectivity_table_schema}.{traced_connectivity_table}"
        )
        self.circuit_key = circuit_key
        self.queries = {}

        self.initialize_queries()

    def get_queries(self):
        return self.queries

    def get_query(self, query_name):
        return self.queries[query_name] if query_name in self.queries else None

    def initialize_queries(self):
        self.queries[
            "bus"
        ] = f"""
select
	BUS_VN_KV::number(38,6) AS BUS_VN_KV,
    CONNECTIVITY_NODEID, 
    BUS_PHASES,
    BUS_GROUNDED_PHASES,
    BUS_GROUNDING_R_OHM::number(38,6) BUS_GROUNDING_R_OHM,
    BUS_GROUNDING_X_OHM::number(38,6) BUS_GROUNDING_X_OHM,
    BUS_IN_SERVICE::BOOLEAN AS BUS_IN_SERVICE,
    BUS_TYPE,
    BUS_ZONE
from
	{ self.bus_table }
where
	circuit_key = '{self.circuit_key}'
"""
        self.queries[
            "ext_grid"
        ] = f"""
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
	EXTERNALGRID_IN_SERVICE::BOOLEAN AS EXTERNALGRID_IN_SERVICE
from
	{self.traced_connectivity_table}
where
	linked_equiptype = 'CIRCUIT HEAD BREAKER'
	AND circuit_key = '{self.circuit_key}';        
"""

        self.queries[
            "line_std_type"
        ] = f"""
select
    distinct MATERIAL_CODE,
    LINE_R_OHM_PER_KM::number(38,6) AS LINE_R_OHM_PER_KM,
    LINE_X_OHM_PER_KM::number(38,6) AS LINE_X_OHM_PER_KM,
    LINE_C_NF_PER_KM::number(38,6) AS LINE_C_NF_PER_KM,
    LINE_R0_OHM_PER_KM::number(38,6) AS LINE_R0_OHM_PER_KM,
    LINE_X0_OHM_PER_KM::number(38,6) AS LINE_X0_OHM_PER_KM,
    LINE_C0_NF_PER_KM::number(38,6) AS LINE_C0_NF_PER_KM,
    LINE_MAX_I_KA::number(38,6) AS LINE_MAX_I_KA,
    LINE_G_US_PER_KM::number(38,6) AS LINE_G_US_PER_KM,
    LINE_B_US_PER_KM::number(38,6) AS LINE_B_US_PER_KM,
    LINE_RN_OHM_PER_KM::number(38,6) AS LINE_RN_OHM_PER_KM,
    LINE_XN_OHM_PER_KM::number(38,6) AS LINE_XN_OHM_PER_KM,
    LINE_MAX_IN_KA::number(38,6) AS LINE_MAX_IN_KA,
    LINE_TYPE,
    LINE_GN_US_PER_KM::number(38,6) AS LINE_GN_US_PER_KM,
	LINE_BN_US_PER_KM::number(38,6) AS LINE_BN_US_PER_KM
from
    {self.traced_connectivity_table}
where
    linked_equiptype in (
        'OH_PRIMARY_CONDUCTOR',
        'UG_PRIMARY_CONDUCTOR',
        'POTHEAD',
        'TAP'
    )
    and circuit_key = '{self.circuit_key}'        
"""

        self.queries[
            "line"
        ] = f"""
select
    CONDUCTING_EQUIPMENTID,
    UPSTREAM_CONNECTIVITYNODEID,
    DOWNSTREAM_CONNECTIVITYNODEID,
    MATERIAL_CODE,
    LINE_MODEL_TYPE,
    LINE_CONNECTED_PHASE,
    LINE_FROM_PHASE,
    LINE_TO_PHASE,
    LINE_LENGTH_KM::number(38,6) AS LINE_LENGTH_KM,
    IN_SERVICE
from
    {self.traced_connectivity_table}
where
    linked_equiptype in (
        'OH_PRIMARY_CONDUCTOR',
        'UG_PRIMARY_CONDUCTOR'
        -- 'POTHEAD', removed
        -- 'TAP' removed
    )
    and circuit_key = '{self.circuit_key}'        
"""

        self.queries[
            "switch"
        ] = f"""
select
    CONDUCTING_EQUIPMENTID,
    UPSTREAM_CONNECTIVITYNODEID,
    DOWNSTREAM_CONNECTIVITYNODEID,
    MATERIAL_CODE SWITCH_IN_KA,
    SWITCH_CONNECTED_PHASE,
    SWITCH_ELEMENT,
    SWITCH_FROM_PHASE,
    SWITCH_ET,
    SWITCH_CLOSED,
    SWITCH_Z_OHM,
    SWITCH_TYPE
from
    { self.traced_connectivity_table }
where
    linked_equiptype in (
        'UG_SWITCH',
        'OH_SWITCH',
        'ELBOW',
        'UG_BUS',
        'JUNCTION_BAR',
        'PE_GEAR_SWITCH',
        'FUSED_CUTOUT',
        'BRANCH_LINE_FUSE',
        'AUTOMATIC_RECLOSER',
        'FAULT_INTERRUPTER',
        'VAC_FAULT_INTERRUPTER',
        'OH_TRANSFORMER_LOCATION',
        'GROUND_BANK',
        'IBANK',
        'TAP',
        'POTHEAD',
        'TIE_CABLE',
        'CUTOUT',
        'DISTRIBUTION_TAP'        
    )
    and circuit_key = '{self.circuit_key}'        
"""

        self.queries[
            "shunt"
        ] = f"""
select
    CONDUCTING_EQUIPMENTID,
    MATERIAL_CODE,
    UPSTREAM_CONNECTIVITYNODEID,
    SHUNT_P_MW::number(38,6) AS SHUNT_P_MW,
    SHUNT_Q_MVAR::number(38,6) AS SHUNT_Q_MVAR,
    SHUNT_CONNECTION_TYPE,
    SHUNT_CONNECTED_PHASE,
    SHUNT_FROM_PHASE,
    SHUNT_TO_PHASE,
    SHUNT_VN_KV::number(38,6) AS SHUNT_VN_KV,
    SHUNT_CLOSED,
    SHUNT_CONTROL_MODE,
    SHUNT_V_THR_ON,
    SHUNT_V_THR_OFF,
    SHUNT_IN_SERVICE
from
    {self.traced_connectivity_table}
where
    linked_equiptype = 'CAPACITOR_BANK'
    and circuit_key = '{self.circuit_key}'        
"""

#cris 12/10 update start
        self.queries[
            "transformer"
        ] = f"""
select
    CIRCUIT_KEY,
    CONDUCTING_EQUIPMENTID,
    UPSTREAM_CONNECTIVITYNODEID,
    DOWNSTREAM_CONNECTIVITYNODEID,
    MATERIAL_CODE,
    XFRMR_SN_MVA::number(38,6) XFRMR_SN_MVA,
    XFRMR_VN_HV_KV::number(38,6) XFRMR_VN_HV_KV,
    XFRMR_VN_LV_KV::number(38,6) XFRMR_VN_LV_KV,
    XFRMR_VK_PERCENT::number(38,6) XFRMR_VK_PERCENT,
    XFRMR_VKR_PERCENT::number(38,6) XFRMR_VKR_PERCENT,
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
    XFRMR_Z0_PERCENT::number(38,6) XFRMR_Z0_PERCENT,
    XFRMR_Z1_PERCENT::number(38,6) XFRMR_Z1_PERCENT,
    XFRMR_X1R1_RATIO::number(38,6) XFRMR_X1R1_RATIO,
    XFRMR_X0R0_RATIO::number(38,6) XFRMR_X0R0_RATIO,
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
    XFRMR_BY_PHASE::BOOLEAN AS XFRMR_BY_PHASE,
    XFRMR_VM_LOWER_PU::number(38,6) as XFRMR_VM_LOWER_PU,
    XFRMR_VM_UPPER_PU::number(38,6) as XFRMR_VM_UPPER_PU,
    XFRMR_CONTROL_SIDE,
    XFRMR_TOL::number(38,6) as XFRMR_TOL,
    XFRMR_PT_RATIO::number(38,6) as XFRMR_PT_RATIO,
    XFRMR_CT_RATING_A::number(38,6) as XFRMR_CT_RATING_A,
    XFRMR_R_LDC_FORWARD_V::number(38,6) as XFRMR_R_LDC_FORWARD_V,
    XFRMR_X_LDC_FORWARD_V::number(38,6) as XFRMR_X_LDC_FORWARD_V,
    XFRMR_R_LDC_REVERSE_V::number(38,6) as XFRMR_R_LDC_REVERSE_V,
    XFRMR_X_LDC_REVERSE_V::number(38,6) as XFRMR_X_LDC_REVERSE_V,
    XFRMR_BANDWIDTH_FORWARD_V::number(38,6) as XFRMR_BANDWIDTH_FORWARD_V,
    XFRMR_BANDWIDTH_REVERSE_V::number(38,6) as XFRMR_BANDWIDTH_REVERSE_V,
    XFRMR_V_SET_SECONDARY_V::number(38,6) as XFRMR_V_SET_SECONDARY_V,
    XFRMR_MODE
from
    {self.traced_connectivity_table}
where
    linked_equiptype in (
        'OH_TRANSFORMER',
        'UG_TRANSFORMER',
        'VOLTAGE_REGULATOR',
        'LINE_REGULATOR'
    )
    and circuit_key = '{self.circuit_key}'        
"""
#cris 12/10 update end
######################## cris update START ########################
        self.queries[
            "load"
        ] = f"""
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
    LOAD_IN_SERVICE
from
    {self.traced_connectivity_table}
where
    linked_equiptype = 'LOAD'
    and circuit_key = '{self.circuit_key}'      
"""
######################## cris update END ########################

        self.queries[
            "gen"
        ] = f"""
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
    GEN_IN_SERVICE
from
    {self.traced_connectivity_table}
where
    linked_equiptype = 'GEN'
    and circuit_key = '{self.circuit_key}'        
"""

        self.queries[
            "sgen"
        ] = f"""
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
    SGEN_CONTROL_CURVE
from
    {self.traced_connectivity_table}
where
    linked_equiptype = 'SGEN'
    and circuit_key = '{self.circuit_key}' 
"""

        self.queries[
            "udf_mklist"
        ] = f"""
CREATE OR REPLACE FUNCTION {self.database_name}.{self.schema_name}.mklist(input_string VARCHAR)
RETURNS ARRAY(NUMBER)
LANGUAGE JAVASCRIPT
AS $$
  if (INPUT_STRING === null || INPUT_STRING === '') {{
    return [];
  }}
  return INPUT_STRING.split('').map(digit => digit);
$$;        
"""
