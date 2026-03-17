
from io import StringIO
import base64
import pickle
from abc import ABC, abstractmethod
import pandas as pd
from snowflake.snowpark import Session
from snowflake.snowpark import functions as F
from snowflake.snowpark.types import (IntegerType, StringType, StructType, StructField, DoubleType, FloatType,
                                      PandasSeriesType, PandasDataFrame,
                                      PandasDataFrameType, DecimalType, TimestampType, DateType)
import pandapower as pp

OUTPUT_FIELDS = [
    StructField("FEEDER_MRID", StringType(), nullable=False),
    StructField("CONNECTIVITY_NODEID", StringType(), nullable=False),
    StructField("LINKED_EQUIPTYPE", StringType()),
    StructField("REPORTED_DTTM", TimestampType(), nullable=False),
    StructField("REPORTED_DT", DateType()),
    StructField("HOUR_ID", IntegerType()),
    StructField("BASEVOLTAGE_VALUE_KVLL", DecimalType(18, 6)),
    StructField("SV_CURRENT_ANGLE", DecimalType(18, 6)),
    StructField("SV_CURRENT_CURRENT_VALUE", DecimalType(18, 6)),
    StructField("SV_CURRENT_A_ANGLE", DecimalType(18, 6)),
    StructField("SV_CURRENT_A_CURRENT_VALUE", DecimalType(18, 6)),
    StructField("SV_CURRENT_B_ANGLE", DecimalType(18, 6)),
    StructField("SV_CURRENT_B_CURRENT_VALUE", DecimalType(18, 6)),
    StructField("SV_CURRENT_C_ANGLE", DecimalType(18, 6)),
    StructField("SV_CURRENT_C_CURRENT_VALUE", DecimalType(18, 6)),
    StructField("SV_POWERFLOW_P_MW", DecimalType(18, 6)),
    StructField("SV_POWERFLOW_P_A_MW", DecimalType(18, 6)),
    StructField("SV_POWERFLOW_P_B_MW", DecimalType(18, 6)),
    StructField("SV_POWERFLOW_P_C_MW", DecimalType(18, 6)),
    StructField("SV_POWERFLOW_PCAP_MW", DecimalType(18, 6)),
    StructField("SV_POWERFLOW_PMOTOR_MW", DecimalType(18, 6)),
    StructField("SV_POWERFLOW_Q_MVAR", DecimalType(18, 6)),
    StructField("SV_POWERFLOW_Q_A_MVAR", DecimalType(18, 6)),
    StructField("SV_POWERFLOW_Q_B_MVAR", DecimalType(18, 6)),
    StructField("SV_POWERFLOW_Q_C_MVAR", DecimalType(18, 6)),
    StructField("SV_POWERFLOW_QCAP_MVAR", DecimalType(18, 6)),
    StructField("SV_POWERFLOW_QMOTOR_MVAR", DecimalType(18, 6)),
    StructField("SV_VOLTAGE_ANGLE", DecimalType(18, 6)),
    StructField("SV_VOLTAGE_V_VALUE_KVLL", DecimalType(18, 6)),
    StructField("SV_VOLTAGE_V_PU_VALUE", DecimalType(18, 6)),
    StructField("SV_VOLTAGE_A_ANGLE", DecimalType(18, 6)),
    StructField("SV_VOLTAGE_V_A_VALUE_KVLL", DecimalType(18, 6)),
    StructField("SV_VOLTAGE_V_A_PU_VALUE", DecimalType(18, 6)),
    StructField("SV_VOLTAGE_B_ANGLE", DecimalType(18, 6)),
    StructField("SV_VOLTAGE_V_B_VALUE_KVLL", DecimalType(18, 6)),
    StructField("SV_VOLTAGE_V_B_PU_VALUE", DecimalType(18, 6)),
    StructField("SV_VOLTAGE_C_ANGLE", DecimalType(18, 6)),
    StructField("SV_VOLTAGE_V_C_VALUE_KVLL", DecimalType(18, 6)),
    StructField("SV_VOLTAGE_V_C_PU_VALUE", DecimalType(18, 6)),
    StructField("YEAR_ID", IntegerType()),
    StructField("MONTH_ID", IntegerType())
]
OUTPUT_COLUMNS = [f.name for f in OUTPUT_FIELDS]

LOAD_COLS = ['name', 'linked_equiptype', 'hv_bus', 'lv_bus', 'bus', 'p_mw', 'q_mvar', 'p_a_mw', 'q_a_mvar', 'p_b_mw',
             'q_b_mvar', 'p_c_mw', 'q_c_mvar', 'measurement_type', 'reported_dttm']

circuit_analysis_cols = """analysis_id
    circuit_key
    status
    config_options
    code_version
    net_blob
    diagnostic_output
    flag_array 
    input_data 
    created_at 
    updated_at""".split()

def get_table_lookup(env):
    TABLE_LOOKUP = {}

    if env == "local":
        SNOWFLAKE_DB = "GRIDMOD_DEV_TD"
        SNOWFLAKE_DB_FERC = SNOWFLAKE_DB
        SNOWFLAKE_SCHEMA = "UC_POC"
        SNOWFLAKE_SCHEMA_V = "UC_POC"
    else:
        SNOWFLAKE_DB = f"GRIDMOD_{env.upper()}_TD"
        SNOWFLAKE_DB_FERC = f"GRIDMOD_{env.upper()}_TD_FERC"
        SNOWFLAKE_SCHEMA = "UC_ENGPLNG"
        SNOWFLAKE_SCHEMA_V = "UC_ENGPLNG_V"

    TABLE_LOOKUP["SNOWFLAKE_DB"] = SNOWFLAKE_DB
    TABLE_LOOKUP["SNOWFLAKE_DB_FERC"] = SNOWFLAKE_DB_FERC
    TABLE_LOOKUP["SNOWFLAKE_SCHEMA"] = SNOWFLAKE_SCHEMA
    TABLE_LOOKUP["SNOWFLAKE_SCHEMA_V"] = SNOWFLAKE_SCHEMA_V

    if env == "local":
        TABLE_LOOKUP[
            "HIERARCHY_TABLE"] = f"{SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA_V}.NMM_D_TRACED_CIRCUIT_CONNECTIVITY_C_PP_VW_11_27"
        TABLE_LOOKUP["BUS_TABLE"] = f"{SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA_V}.NMM_D_BUS_C_PP_VW_11_27"
        TABLE_LOOKUP[
            "LOAD_TABLE"] = f"{SNOWFLAKE_DB_FERC}.{SNOWFLAKE_SCHEMA_V}.NMM_F_HIST_HR_GROSS_DERGEN_DLY_C_PP_VW_10_11"
        TABLE_LOOKUP["OUTPUT_TABLE"] = f"{SNOWFLAKE_DB_FERC}.{SNOWFLAKE_SCHEMA}.NMM_F_TOPOLOGICALNODE_C_10_1"
        TABLE_LOOKUP[
            "CONTROL_TABLE"] = f"{SNOWFLAKE_DB_FERC}.{SNOWFLAKE_SCHEMA}.NMM_F_LOADFLOW_CONFIG_CONTROLLER_I_10_1"

    else:
        TABLE_LOOKUP[
            "HIERARCHY_TABLE"] = f"{SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA_V}.NMM_D_TRACED_CIRCUIT_CONNECTIVITY_C_PP_VW"
        TABLE_LOOKUP["BUS_TABLE"] = f"{SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA_V}.NMM_D_BUS_C_PP_VW"
        TABLE_LOOKUP["LOAD_TABLE"] = f"{SNOWFLAKE_DB_FERC}.{SNOWFLAKE_SCHEMA_V}.NMM_F_HIST_HR_GROSS_DERGEN_DLY_C_PP_VW"
        TABLE_LOOKUP["OUTPUT_TABLE"] = f"{SNOWFLAKE_DB_FERC}.{SNOWFLAKE_SCHEMA}.NMM_F_TOPOLOGICALNODE_C"
        TABLE_LOOKUP["CONTROL_TABLE"] = f"{SNOWFLAKE_DB_FERC}.{SNOWFLAKE_SCHEMA}.NMM_F_LOADFLOW_CONFIG_CONTROLLER_I"

    return TABLE_LOOKUP


class DataSource(ABC):
    def _create_pandapower_net(self, *args, **kwargs):
        raise NotImplementedError


class PandaPowerJsonDataSource(DataSource):
    def __init__(self, json_str):
        self.net = pp.from_json(StringIO(json_str))


class PandaPowerSnowflakeDataSource(DataSource):
    def __init__(self, *args, **kwargs):
        pass


class AbstractSnowflakeDataSource(DataSource):
    def __init__(self, session, table_source, *args, **kwargs):
        super().__init__(*args, **kwargs)  # Call parent DataSource constructor
        self.session = session
        self.table_source = table_source
        
class PandaPowerSnowflakeDataSource(DataSource):
    def __init__(self, session, table_lookup, circuit_key):
        super().__init__(session, table_lookup)  # Call parent constructor
        self.circuit_id = circuit_id

    #todo:
    #queries = {'pp_net': f'select encoded_ppnet from {self.table_lookup['ppnet']} where circuit_id = "{self.circuit_id}"'}

    def create_network(self):
        #todo: pull the net from the query and assign it to vriable encoded_ppnet
        return pickle.loads(base64.b64decode(encoded_ppnet))



class SCESnowflakeDataSource(AbstractSnowflakeDataSource):
    def __init__(self, session, table_lookup, circuit_key):
        super().__init__(session, table_lookup)  # Call parent constructor
        self.circuit_key = circuit_key  # Fix: was self.circuit_key = self.circuit_key

        self.queries = {
        "BUS": f"""SELECT
            cast(CONNECTIVITY_NODEID as string()) as bus, 
            cast(BUS_VN_KV as number(38,6)) as vn_kv
            FROM {self.table_lookup['BUS_TABLE']} where circuit_key = '{self.circuit_key}'
            order by CONNECTIVITY_NODEID
        """,
        "LINE": f"""
            SELECT  CONDUCTING_EQUIPMENTId as Name,
                LINKED_EQUIPTYPE, 
                cast(upstream_connectivitynodeid as string()) as from_bus, 
                cast(downstream_connectivitynodeid as string()) as to_bus,
                cast(LINE_LENGTH_KM as number(38,6)) as length_km,
               '' as material_code, 
               cast(LINE_R_OHM_PER_KM as number(38,6)) as r_ohm_per_km, 
               cast(LINE_X_OHM_PER_KM as number(38,6)) as x_ohm_per_km, 
               cast(LINE_R0_OHM_PER_KM as number(38,6)) as r0_ohm_per_km,
               cast(LINE_X0_OHM_PER_KM as number(38,6)) as x0_ohm_per_km, 
               cast(LINE_C_NF_PER_KM as number(38,6)) as c_nf_per_km,
               cast(LINE_C0_NF_PER_KM as number(38,6)) as c0_nf_per_km, 
              cast(LINE_MAX_I_KA as number(38,6)) as max_i_ka
            FROM {self.table_lookup['HIERARCHY_TABLE']}
            WHERE length_km is not null and circuit_key = '{self.circuit_key}'
            order by upstream_connectivitynodeid, downstream_connectivitynodeid
        """,
        "SWITCH": f"""select conducting_equipmentid as name,
               LINKED_EQUIPTYPE, 
                cast(upstream_connectivitynodeid as string()) as from_bus, 
                cast(downstream_connectivitynodeid as string()) as to_bus,
               cast( SWITCH_ELEMENT as number(38,0)) as element, 
               'b' as et, 
               'LBS' as type, 
               cast(switch_closed as boolean) as closed, 
               cast(SWITCH_IN_KA as number(38,6)) as in_ka
            from {self.table_lookup['HIERARCHY_TABLE']}
            where circuit_key = '{self.circuit_key}' 
            and (switch_type != '' or linked_equiptype='OH_TRANSFORMER_LOCATION')
            and linked_equiptype != 'TIE_CABLE'
            order by upstream_connectivitynodeid, downstream_connectivitynodeid
        """,
        "SHUNT": f"""
            SELECT CONDUCTING_EQUIPMENTID as name, 
               LINKED_EQUIPTYPE, 
               cast(replace(CONNECTIVITY_NODEID,'s','') as string()) as bus,
               cast(SHUNT_P_MW as number(38,6)) as p_mw, 
               cast(SHUNT_Q_MVAR as number(38,6)) as q_mvar
            FROM {self.table_lookup['HIERARCHY_TABLE']}
            WHERE EQUIPMENT_TYPE = 'NMM_D_CAPACITORBANK_LOOKUP_C_VW' and circuit_key = '{self.circuit_key}'
            order by replace(CONNECTIVITY_NODEID,'s','')
        """,
        "EXT_GRID": f"""
           SELECT DISTINCT NAME,
           linked_equiptype,
           bus,
           cast(vm_pu as number(38,6)) as vm_pu,
           cast(s_sc_max_mva as number(38,6)) as s_sc_max_mva,
           cast(rx_max as number(38,6)) as rx_max,
           cast(r0x0_max as number(38,6)) as r0x0_max, 
           cast(x0x_max as number(38,6)) as x0x_max
           from (
           SELECT CIRCUIT_KEY,
           CONDUCTING_EQUIPMENTID as NAME, 
            linked_equiptype, 
            cast(replace(connectivity_nodeid,'cbr_','-888888') as string()) as bus,
            thevenin_s_sc_max_mva as s_sc_max_mva,
            thevenin_rx_max as rx_max,
            thevenin_r0x0_max as r0x0_max,
            thevenin_x0x_max as x0x_max
            FROM  {self.table_lookup['HIERARCHY_TABLE']}
            WHERE EQUIPMENT_TYPE = 'NMM_D_BREAKER_LOOKUP_C_VW' and  circuit_key = '{self.circuit_key}'
            ) t1 join (
            SELECT CIRCUIT_KEY, BUS_OPERATINGVOLTAGE_VALUE_KV / BUS_VN_KV  as vm_pu
            FROM  {self.table_lookup['BUS_TABLE']}
            where substring(replace(connectivity_nodeid,'cbr_','-888888'), 2, 1) = 8
            and cast(replace(connectivity_nodeid,'cbr_','-888888') as number(38,0)) > -99999900000000
            and  circuit_key = '{circuit_key}'
            ) t2 on t1.CIRCUIT_KEY = t2.CIRCUIT_KEY
        """,
        "TRANSFORMER": f"""
            select conducting_equipmentid as name, 
            cast(upstream_connectivitynodeid as string()) as hv_bus, 
            cast(downstream_connectivitynodeid as string()) as lv_bus,
            cast(xfrmr_sn_mva as number(38,6)) as sn_mva,
            GREATEST(0.1,cast(xfrmr_vn_hv_kv as number(38,6))) as vn_hv_kv,  
            cast(xfrmr_vn_lv_kv as number(38,6)) as vn_lv_kv,
            GREATEST(0.001,cast(xfrmr_vk_percent as number(38,6))) as vk_percent,
            GREATEST(0.000124035,cast(xfrmr_vkr_percent as number(38,6))) as vkr_percent,
            cast(xfrmr_shift_degree as number(38,6)) as shift_degree,
            cast(0 as number(38,6)) as pfe_kw,
            cast(0 as number(38,6)) as i0_percent, 
             FROM {self.table_lookup['HIERARCHY_TABLE']}
            where circuit_key = '{self.circuit_key}' 
            and linked_equiptype in ('OH_TRANSFORMER','UG_TRANSFORMER')
            order by upstream_connectivitynodeid, downstream_connectivitynodeid
        """,

        "REGULATOR": f"""select upstream_connectivitynodeid as hv_bus, 
           downstream_connectivitynodeid as lv_bus,
           cast(REG_XFRMR_SN_MVA as number(38,6)) as sn_mva, 
           cast(REG_XFRMR_VN_HV_KV as number(38,6)) as vn_hv_kv,
           cast(REG_XFRMR_VN_LV_KV as number(38,6)) as vn_lv_kv,
           cast(REG_XFRMR_VK_PERCENT as number(38,6)) as vk_percent,
           cast(REG_XFRMR_VKR_PERCENT as number(38,6)) as vkr_percent,
           cast(REG_XFRMR_PFE_KW as number(38,6)) as pfe_kw,
           cast(REG_XFRMR_I0_PERCENT as number(38,6)) as i0_percent,
           REG_XFRMR_TAP_SIDE as tap_side,
           cast(REG_XFRMR_TAP_STEP_PERCENT as number(38,6)) as tap_step_percent,
           cast(REG_XFRMR_TAP_MAX as number(38,6)) as tap_max,
           cast(REG_XFRMR_TAP_MIN as number(38,6)) as tap_min,
           cast(REG_XFRMR_TAP_NEUTRAL as number(38,6)) as tap_neutral,
           cast(REG_XFRMR_TAP_POS as number(38,6)) as tap_pos,
           REG_XFRMR_CONNECTION as REG_XFRMR_CONNECTION,
           cast(REG_XFRMR_Z1_PCT as number(38,6)) as REG_XFRMR_Z1_PCT,
           cast(REG_XFRMR_Z0_PCT as number(38,6)) as REG_XFRMR_Z0_PCT,
           cast(REG_XFRMR_X1R1_RATIO as number(38,6)) as REG_XFRMR_X1R1_RATIO,
           cast(REG_XFRMR_X0R0_RATIO as number(38,6)) as REG_XFRMR_X0R0_RATIO,
           cast(REG_VM_LOWER_PU as number(38,6)) as vm_lower_pu,
           cast(REG_VM_UPPER_PU as number(38,6)) as vm_upper_pu,
           cast(REG_VM_SET_PU_VAL as number(38,6)) as vm_set_pu_val,
           REG_CT as ct,
           REG_PT as pt,
           cast(REG_R_COMP as number(38,6)) as r_comp,
           cast(REG_X_COMP as number(38,6)) as x_comp
        FROM {self.table_lookup['HIERARCHY_TABLE']}
        where circuit_key = '{self.circuit_key}' 
        and linked_equiptype in ('LINE_REGULATOR','VOLTAGE_REGULATOR')
        order by upstream_connectivitynodeid, downstream_connectivitynodeid""",
        "METADATA": f"""select
               FEEDER_MRID,
               cast(replace(replace(CONNECTIVITY_NODEID,'s',''),'cbr_','-888888') as string()) as bus,
               cast(downstream_connectivitynodeid as string()) as lv_bus, 
               CONDUCTING_EQUIPMENTID,
               LINKED_EQUIPTYPE,
               EQUIPMENT_STATE
               from {self.table_lookup['HIERARCHY_TABLE']}
               where circuit_key = '{self.circuit_key}' and downstream_connectivitynodeid is not null
               order by replace(replace(CONNECTIVITY_NODEID,'s',''),'cbr_','-888888')
        """}

    #TODO: this can likely be abstracted into a higher class in wihch it simply executes the queries and 
    #stores the results and we can subclass an SCE one that does more if we want.
    def _create_pandapower_net(self):
        circuit_data = self.load_circuit_data()
        # Create empty Pandapower network
        net = pp.create_empty_network()
        # min_vn_kv = data['BUS']['vn_kv'].min()
        # if not min_vn_kv:
        #    min_vn_kv = 0.1
        for index, row in self.circuit_data['BUS'].iterrows():
            # pp.create_bus(net, vn_kv=max(min_vn_kv,row['vn_kv']), name=row['bus'])
            pp.create_bus(net, vn_kv=row['vn_kv'], name=row['bus'])

        for index, row in self.circuit_data['EXT_GRID'].iterrows():
            if self.balanced_load:
                pp.create_ext_grid(net, pp.get_element_index(net, "bus", row['bus']),
                                   name=row['name'], linked_equiptype=row['linked_equiptype'],
                                   vm_pu=row['vm_pu'])
            else:
                pp.create_ext_grid(net, pp.get_element_index(net, "bus", row['bus']), name=row['name'],
                                   linked_equiptype=row['linked_equiptype'], vm_pu=row['vm_pu'],
                                   s_sc_max_mva=row['s_sc_max_mva'], rx_max=row['rx_max'], x0x_max=row['x0x_max'],
                                   r0x0_max=row['r0x0_max'])

        for index, row in self.circuit_data['REGULATOR'].iterrows():
            print(row)
            hv_bus = pp.get_element_index(net, "bus", row['hv_bus'])
            lv_bus = pp.get_element_index(net, "bus", row['lv_bus'])

            if not self.regulator_enabled:
                pp.create_switch(net, name='Tranfo1', bus=hv_bus,
                                 element=lv_bus, et='b')
            else:
                print("Creating dummy transformers")
                t1 = pp.create_transformer_from_parameters(net, hv_bus=hv_bus, lv_bus=lv_bus, name="Tranfo1",
                                                           sn_mva=float(row['sn_mva']), vn_hv_kv=float(row['vn_hv_kv']),
                                                           vn_lv_kv=float(row['vn_lv_kv']),
                                                           #vk_percent=0.002,
                                                           vk_percent=float(row['vk_percent']),
                                                           vkr_percent=float(row['vkr_percent']),
                                                           pfe_kw=float(row['pfe_kw']),
                                                           i0_percent=float(row['i0_percent']), tap_side=row['tap_side'],
                                                           tap_step_percent=float(row['tap_step_percent']),
                                                           tap_pos=float(row['tap_pos']),
                                                           tap_max=float(row['tap_max']),
                                                           tap_min=float(row['tap_min']),
                                                           tap_neutral=float(row['tap_neutral']))
                print("Creating controller")
                trafo_controller = pp.control.LineDropControl(net=net, tid=t1,  vm_lower_pu=float(float(row['vm_lower_pu'])),
                                                              vm_upper_pu=float(float(row['vm_upper_pu'])),
                                                              vm_set_pu_val=float(row['vm_set_pu_val']),
                                                              CT=row['ct'],  PT=row['pt'], R_comp=row['r_comp'],
                                                              X_comp=float(row['x_comp']), loc_sen = False)


        for index, row in self.circuit_data['LINE'].iterrows():
            from_bus = pp.get_element_index(net, "bus", row['from_bus'])
            to_bus = pp.get_element_index(net, "bus", row['to_bus'])
            pp.create_line_from_parameters(net, from_bus, to_bus, length_km=row['length_km'],
                                           r_ohm_per_km=row['r_ohm_per_km'], x_ohm_per_km=row['x_ohm_per_km'],
                                           r0_ohm_per_km=row['r0_ohm_per_km'], x0_ohm_per_km=row['x0_ohm_per_km'],
                                           c_nf_per_km=row['c_nf_per_km'], c0_nf_per_km=row['c0_nf_per_km'],
                                           max_i_ka=row['max_i_ka'])

        for index, row in self.circuit_data['SWITCH'].iterrows():
            from_bus = pp.get_element_index(net, "bus", row['from_bus'])
            to_bus = pp.get_element_index(net, "bus", row['to_bus'])

            pp.create_switch(net, name=row['name'], linked_equiptype=row['linked_equiptype'], bus=from_bus,
                             element=to_bus, et=row['et'], type=row['type'],
                             closed=row['closed'], in_ka=row['in_ka'])

        if self.transformer_enabled:
            for index, row in self.circuit_data['TRANSFORMER'].iterrows():
                hv_bus = pp.get_element_index(net, "bus", row['hv_bus'])
                lv_bus = pp.get_element_index(net, "bus", row['lv_bus'])
                # TODO : move below net[trafo] params into this call
                # TODO_DATA : the guardrails on the vk_percent and vkr_percent are only there because SCE has inappropriate zeros for those values for some transformers
                pp.create_transformer_from_parameters(net, name=row['name'], hv_bus=hv_bus, lv_bus=lv_bus,
                                                      vector_group='Dyn',
                                                      sn_mva=row['sn_mva'], vn_hv_kv=row['vn_hv_kv'],
                                                      vn_lv_kv=row['vn_lv_kv'],
                                                      vk_percent=row['vk_percent'], vkr_percent=row['vkr_percent'],
                                                      pfe_kw=row['pfe_kw'], i0_percent=row['i0_percent'],
                                                      shift_degree=row['shift_degree'])

        if self.capacitor_enabled:
            for index, row in self.circuit_data['SHUNT'].iterrows():
                bus = pp.get_element_index(net, "bus", row['bus'])
                # Note: P_MW is being set as 0 when using create_shunt_as_capacitor, unable to pass the value in
                pp.create_shunt_as_capacitor(net, bus, name=row['name'],
                                             linked_equiptype=row['linked_equiptype'],
                                             q_mvar=row['q_mvar'], loss_factor=0)
        return net
    
    def load_circuit_data(self):


        data = {}
        buses = set()
    
        # This next bit is a little weird, having to do have the processing to be able to access
        # the bus, etc.
        for file_type, query in self.queries.items():
            data[file_type] = self.session.sql(query).to_pandas()
            data[sheet] = data[sheet].rename(columns={c: c.lower() for c in data[sheet].columns})
       
        lv_bus_nodes = data['METADATA'].rename(columns={'bus': 'x'}).rename(columns={'lv_bus': 'bus'})
        lv_bus_nodes = lv_bus_nodes[lv_bus_nodes['bus'].map(lambda x: str(x).startswith('-999'))]
        data['METADATA'] = pd.concat([data['METADATA'].rename(columns={'lv_bus': 'x'}), lv_bus_nodes])

        return data

    def load_time_series_data(self, start_period, end_period):

        query = sql = f"""select c.conducting_equipmentid as name,
                   l.struct_num,
                   c.linked_equiptype,
                   cast(c.upstream_connectivitynodeid as string()) as hv_bus,
                   cast(c.downstream_connectivitynodeid as string())  as lv_bus,
                   cast(replace(c.CONNECTIVITY_NODEID,'s','') as string()) as bus,
                   cast(l.MEASURE_VALUE as number(18,6)) as p_mw,
                   cast(l.RDNG_MEAS_MVAR as number(18,6)) as q_mvar,
                   cast(l.MEASURE_VALUE_A as number(18,6)) as p_a_mw,
                   cast(l.RDNG_MEAS_MVAR_A as number(18,6)) as q_a_mvar,
                   cast(l.MEASURE_VALUE_B as number(18,6)) as p_b_mw,
                   cast(l.RDNG_MEAS_MVAR_B as number(18,6)) as q_b_mvar,
                   cast(l.MEASURE_VALUE_C as number(18,6)) as p_c_mw,
                   cast(l.RDNG_MEAS_MVAR_C as number(18,6)) as q_c_mvar,
                   l.MEASUREMENT_TYPE as measurement_type,
                   REPORTED_DTTM
            FROM ( SELECT * FROM {self.table_lookup['LOAD_TABLE']}
            ) l
            join 
            ( SELECT * FROM {self.table_lookup['HIERARCHY_TABLE']} WHERE EQUIPMENT_TYPE='NMM_D_TRANSFORMERBANK_LOOKUP_C_VW'
            )
            c on l.struct_num = c.structure_num and l.power_system_resource_key  = c.circuit_key 
            where c.circuit_key ='{circuit_key}'
            and REPORTED_DTTM >= '{start_period}' and REPORTED_DTTM < '{end_period}'
            order by c.conducting_equipmentid, l.struct_num, c.linked_equiptype, 
               l.project_id,
               l.tech_type
               """

        load_df = self.session.sql(query)

        load_df = load_df.withColumn('name', F.col('name').cast(StringType(255)))
        load_df = load_df.withColumn('linked_equiptype', F.col('linked_equiptype').cast(StringType(255)))
        # load_df = load_df.withColumn('hv_bus', F.col('hv_bus').cast(DecimalType(38, 0)))
        # load_df = load_df.withColumn('lv_bus', F.col('lv_bus').cast(DecimalType(38, 0)))
        load_df = load_df.withColumn('hv_bus', F.col('hv_bus').cast(StringType()))
        load_df = load_df.withColumn('lv_bus', F.col('lv_bus').cast(StringType()))
        # load_df = load_df.withColumn('bus', F.col('bus').cast(DecimalType(38, 0)))
        load_df = load_df.withColumn('bus', F.col('bus').cast(StringType()))

        load_df = load_df.withColumn('p_mw', F.col('p_mw').cast(DecimalType(18, 6)))
        load_df = load_df.withColumn('q_mvar', F.col('q_mvar').cast(DecimalType(18, 6)))
        load_df = load_df.withColumn('p_a_mw', F.col('p_a_mw').cast(DecimalType(18, 6)))
        load_df = load_df.withColumn('q_a_mvar', F.col('q_a_mvar').cast(DecimalType(18, 6)))
        load_df = load_df.withColumn('p_b_mw', F.col('p_b_mw').cast(DecimalType(18, 6)))
        load_df = load_df.withColumn('q_b_mvar', F.col('q_b_mvar').cast(DecimalType(18, 6)))
        load_df = load_df.withColumn('p_c_mw', F.col('p_c_mw').cast(DecimalType(18, 6)))
        load_df = load_df.withColumn('q_c_mvar', F.col('q_c_mvar').cast(DecimalType(18, 6)))
        load_df = load_df.withColumn('reported_dttm', F.col('reported_dttm').cast(TimestampType()))

        load_df = load_df.select(LOAD_COLS).to_pandas()
        load_df = load_df.rename(columns={c: c.lower() for c in load_df.columns})
        return load_df

class SCEExcelDataSource(DataSource):
    def __init__(self, file_path):
        self.file_path = file_path
        tabs = ['BUS', 'LINE', 'SWITCH', 'SHUNT', 'EXT_GRID',
                'TRANSFORMER', 'METADATA', 'LOAD', 'LOAD (GEN)', 'REGULATOR']
        self.dataframes = {}
        for t in tabs:
            try:
                self.dataframes[t] = pd.read_excel(file_path, sheet_name=t, engine='openpyxl')
            except:
                if t in ['LOAD', 'LOAD (GEN)']:
                    pass
                if t == 'REGULATOR':
                    self.dataframes[t] = pd.DataFrame()
                else:
                    raise

    def load_circuit_data(self, *args, **kwargs):
        return self.dataframes

    def load_time_series_data(self, *args, **kwargs):
        return pd.concat([self.dataframes['LOAD'], self.dataframes['LOAD (GEN)']])



def create_snowflake_session(config_path):
    """Create a Snowflake session from a config file"""
    import json
    with open(config_path, 'r') as f:
        config = json.load(f)

    session = Session.builder.configs(config).create()
    session.sql(f"USE DATABASE {config['DB']}")
    return session
