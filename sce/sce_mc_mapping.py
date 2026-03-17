import ast
from datetime import datetime
import json
import math
import numbers
import pickle
import traceback
import pandapower as pp
import pandas as pd
from powerflow_pipeline.line_matrix_std_type import MultiConductorLineMatrixStdType
from powerflow_pipeline.powerflow import (
    DataSource,
    LoadProfileController,
    NetworkDataTransformer,
    Pipeline,
    ResultTransformer,
)
import multiconductor as mc
from typing import TypedDict
from multiconductor.control import add_volt_var_control, QVCurve, add_line_drop_control, add_load_tap_changer_control

from sce.queries import SQLQueries
from sce.udtf_output_schema import output_schema

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

phase_map = {
    0: "N",
    1: "A",
    2: "B",
    3: "C",
}

class SCELoadProfileController(LoadProfileController):
    
    def __init__(self):
        self.logger = logging.getLogger("SCELoadProfileController_Logger")
        self.phase_map = {1: 'A', 2: 'B', 3: 'C'}
        
    def update_step(self, net: pp.pandapowerNet, load_data: any):
        ######################## cris update START ########################
        if net.asymmetric_load is not None and len(net.asymmetric_load) > 0:
            df = net.asymmetric_load
            df["p_mw"] = 0
            df["q_mvar"] = 0
            for data in load_data['READINGS']:
                if data['RESOURCE_TYPE'] == "STRUCTURE":
                    load_phase = df.loc[df['name']  == data['NAME']]['from_phase'].values
                    if len(load_phase) == 0:
                        continue
                    p_mw_phase_val = data['MEASURE_VALUE'] / len(load_phase)
                    q_mvar_phase_val = data['RDNG_MEAS_MVAR'] / len(load_phase)

                    for phase_num in load_phase:
                        df.loc[(df['from_phase'] == phase_num) & (df['name'] == data['NAME']), 'p_mw'] = p_mw_phase_val
                        df.loc[(df['from_phase'] == phase_num) & (df['name'] == data['NAME']), 'q_mvar'] = q_mvar_phase_val
        if net.asymmetric_sgen is not None and len(net.asymmetric_sgen) > 0:
            df = net.asymmetric_sgen
            df["p_mw"] = 0
            df["q_mvar"] = 0
            for data in load_data['READINGS']:
                if data['RESOURCE_TYPE'] == "DER_PROJECT":
                    sgen_phase = df.loc[df['name']  == data['NAME']]['from_phase'].values
                    if len(sgen_phase) == 0:
                        continue
                    p_mw_phase_val = data['MEASURE_VALUE'] / len(sgen_phase)
                    # q_mvar_phase_val = data['RDNG_MEAS_MVAR'] / len(sgen_phase)

                    for phase_num in sgen_phase:
                        df.loc[(df['from_phase'] == phase_num) & (df['name'] == data['NAME']), 'p_mw'] = p_mw_phase_val
                        # df.loc[(df['from_phase'] == phase_num) & (df['name'] == data['NAME']), 'q_mvar'] = q_mvar_phase_val

        if net.asymmetric_gen is not None and len(net.asymmetric_gen) > 0:
            df = net.asymmetric_gen
            df["p_mw"] = 0
            df["q_mvar"] = 0
            for data in load_data['READINGS']:
                if data['RESOURCE_TYPE'] == "DER_PROJECT":
                    gen_phase = df.loc[df['name']  == data['NAME']]['from_phase'].values
                    if len(gen_phase) == 0:
                        continue
                    p_mw_phase_val = data['MEASURE_VALUE'] / len(gen_phase)
                    # q_mvar_phase_val = data['RDNG_MEAS_MVAR'] / len(gen_phase)

                    for phase_num in gen_phase:
                        df.loc[(df['from_phase'] == phase_num) & (df['name'] == data['NAME']), 'p_mw'] = p_mw_phase_val
                        # df.loc[(df['from_phase'] == phase_num) & (df['name'] == data['NAME']), 'q_mvar'] = q_mvar_phase_val
        ######################## cris update END ########################


class SnowflakeDataSource(DataSource):
    def __init__(
        self,
        session,
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
        self.session = session
        self.database_name = database_name
        self.schema_name = schema_name
        self.bus_table_database = bus_table_database
        self.bus_table_schema = bus_table_schema
        self.traced_connectivity_table_database = traced_connectivity_table_database
        self.traced_connectivity_table_schema = traced_connectivity_table_schema
        self.bus_table = bus_table
        self.traced_connectivity_table = traced_connectivity_table
        self.circuit_key = circuit_key
        self.logger = logging.getLogger("SnowflakeDataSource_Logger")

    def create_udf(self, queries):
        udf_query = queries["udf_mklist"]
        self.session.sql(udf_query)

    def retrieve(self) -> dict:
        data = {}
        queries = SQLQueries(
            database_name=self.database_name,
            schema_name=self.schema_name,
            bus_table_database=self.bus_table_database,
            bus_table_schema=self.bus_table_schema,
            traced_connectivity_table_database=self.traced_connectivity_table_database,
            traced_connectivity_table_schema=self.traced_connectivity_table_schema,
            bus_table=self.bus_table,
            traced_connectivity_table=self.traced_connectivity_table,
            circuit_key=self.circuit_key,
        ).get_queries()

        self.create_udf(queries)

        circuit_data_queries = [q for q in queries if q != "udf_mklist"]
        for query_id in circuit_data_queries:
            data[query_id] = self.session.sql(queries[query_id]).to_pandas()

        return data


class SCEPklDataSource(DataSource):
    def __init__(self, file_path: str):
        self.file_path = file_path

    def retrieve(self) -> dict:
        with open(self.file_path, "rb") as f:
            self.dataframes = pickle.load(f)
            return self.dataframes


class MappedField(TypedDict):
    field: str
    source: str
    required: bool


class FieldMapping(TypedDict):
    create_method: str
    field_mapping: list[MappedField]


# fmt: off
sce_field_mapping: FieldMapping = {
    "create_bus": [
        {"field": "vn_kv", "source": "BUS_VN_KV", "required": True},
        {"field": "name", "source": "CONNECTIVITY_NODEID", "required": True},
        {"field": "num_phases", "source": "BUS_PHASES", "required": False},
        {"field": "grounded_phases", "source": "BUS_GROUNDED_PHASES", "required": False},
        {"field": "grounding_r_ohm", "source": "BUS_GROUNDING_R_OHM", "required": False},
        {"field": "grounding_x_ohm", "source": "BUS_GROUNDING_X_OHM", "required": False},
        {"field": "in_service", "source": "BUS_IN_SERVICE", "required": False},
        {"field": "type", "source": "BUS_TYPE", "required": False},
        {"field": "zone", "source": "BUS_ZONE", "required": False},
    ],
    "create_line": [
        {"field": "std_type", "source": "LINE_TYPE", "required": True},
        {"field": "model_type", "source": "LINE_MODEL_TYPE", "required": True},
        {"field": "from_bus", "source": "UPSTREAM_CONNECTIVITYNODEID_INDEX", "required": True},
        {"field": "from_phase", "source": "LINE_FROM_PHASE", "required": True},
        {"field": "to_bus", "source": "DOWNSTREAM_CONNECTIVITYNODEID_INDEX", "required": True},
        {"field": "to_phase", "source": "LINE_TO_PHASE", "required": True},
        {"field": "length_km", "source": "LINE_LENGTH_KM", "required": True},
        {"field": "name", "source": "CONDUCTING_EQUIPMENTID", "required": False},
        {"field": "in_service", "source": "LINE_IN_SERVICE", "required": False},
    ],
    "create_switch": [
        {"field": "bus", "source": "UPSTREAM_CONNECTIVITYNODEID", "required": True},
        {"field": "phase", "source": "SWITCH_PHASE", "required": True},
        {"field": "element", "source": "DOWNSTREAM_CONNECTIVITYNODEID", "required": True}, 
        {"field": "et", "source": "SWITCH_ET", "required": True},
        {"field": "closed", "source": "SWITCH_CLOSED", "required": False},
        {"field": "type", "source": "SWITCH_TYPE", "required": False},
        {"field": "name", "source": "CONDUCTING_EQUIPMENTID", "required": False},
    ],
    "create_asymmetric_shunt": [
        {"field": "name", "source": "CONDUCTING_EQUIPMENTID", "required": True}, 
        {"field": "bus", "source": "CALCULATED", "required": True}, 
        {"field": "from_phase", "source": "SHUNT_FROM_PHASE", "required": True},
        {"field": "to_phase", "source": "SHUNT_TO_PHASE", "required": True},
        {"field": "p_mw", "source": "SHUNT_P_MW", "required": True},
        {"field": "q_mvar", "source": "SHUNT_Q_MVAR", "required": True},
        {"field": "name", "source": "NAME", "required": False},
        {"field": "control_mode", "source": "SHUNT_CONTROL_MODE", "required": False},
        {"field": "closed", "source": "SHUNT_CLOSED", "required": False},
        {"field": "v_threshold_on", "source": "SHUNT_V_THR_ON", "required": False},
        {"field": "v_threshold_off", "source": "SHUNT_V_THR_OFF", "required": False},
        {"field": "vn_kv", "source": "CALCULATED", "required": False},
        {"field": "name", "source": "CONDUCTING_EQUIPMENTID", "required": False},
    ],
    "create_ext_grid_sequence": [
        {"field": "bus", "source": "DOWNSTREAM_CONNECTIVITYNODEID", "required": True},
        {"field": "from_phase", "source": "EXTERNALGRID_FROM_PHASE", "required": True},
        {"field": "to_phase", "source": "EXTERNALGRID_TO_PHASE", "required": True},
        {"field": "vm_pu", "source": "EXTERNALGRID_VM_PU", "required": True},
        {"field": "va_degree", "source": "EXTERNALGRID_VA_DEGREE", "required": True},
        {"field": "sn_mva", "source": "EXTERNALGRID_SC_MVA", "required": True},
        {"field": "rx", "source": "EXTERNALGRID_RX", "required": True},
        {"field": "x0x", "source": "EXTERNALGRID_X0X", "required": True},
        {"field": "r0x0", "source": "EXTERNALGRID_R0X0", "required": True},
        {"field": "z2z1", "source": "EXTERNALGRID_Z2Z1", "required": True},
        {"field": "c", "source": "EXTERNALGRID_C", "required": True},
        {"field": "z1_weight", "source": "EXTERNALGRID_Z1_WEIGHT", "required": True},
        {"field": "name", "source": "CONDUCTING_EQUIPMENTID", "required": True},
        {"field": "in_service", "source": "EXTERNALGRID_IN_SERVICE", "required": True},
        
    ],
    "create_transformer": [
        {"field": "hv_bus", "source": "", "required": True},
        {"field": "lv_bus", "source": "", "required": True},
        {"field": "std_type", "source": "", "required": True},
        {"field": "tap_pos", "source": "", "required": False},
        {"field": "name", "source": "", "required": False},
    ],
    "create_transformer1ph": [
        {"field": "buses", "source": "UPSTREAM_CONNECTIVITYNODEID, DOWNSTREAM_CONNECTIVITYNODEID", "required": True},
        {"field": "from_phase", "source": "XFRMR_HV_FROM_PHASE, XFRMR_LV_FROM_PHASE", "required": True},
        {"field": "to_phase", "source": "XFRMR_HV_TO_PHASE, XFRMR_LV_TO_PHASE", "required": True},
        {"field": "vn_kv", "source": "XFRMR_VN_HV_KV, XFRMR_VN_LV_KV", "required": True},
        {"field": "sn_mva", "source": "XFRMR_SN_MVA", "required": True},
        {"field": "vk_percent", "source": "XFRMR_VK_PERCENT", "required": True},
        {"field": "vkr_percent", "source": "XFRMR_VKR_PERCENT", "required": True},
        {"field": "pfe_kw", "source": "XFRMR_PFE_KW", "required": True},
        {"field": "i0_percent", "source": "XFRMR_I0_PERCENT", "required": True},
        {"field": "name", "source": "CONDUCTING_EQUIPMENTID", "required": False},
    ],

    #cris 12/08: 1.7 change start
    "create_regulator_control": [
        {"field": "trafo_top_level_index", "source": "CALCULATED", "required": True},
        {"field": "mode", "source": "CALCULATED", "required": True},
        {"field": "v_set_secondary_v", "source": "CALCULATED", "required": True},
        {"field": "bandwidth_secondary_v", "source": "CALCULATED", "required": True},
        {"field": "pt_ratio", "source": "CALCULATED", "required": True},
        {"field": "ct_primary_rating_a", "source": "CALCULATED", "required": True},
        {"field": "r_ldc_v", "source": "CALCULATED", "required": True},
        {"field": "x_ldc_v", "source": "CALCULATED", "required": True},
    ],
    #cris 12/08: 1.7 change end

    #cris 12/08: 1.8 change start
    # "create_tap_control": [
    #     {"trafo_top_level_index": "buses", "source": "CALCULATED", "required": True},
    #     {"mode": "from_phase", "source": "CALCULATED", "required": True},
    #     {"vm_lower_pu": "to_phase", "source": "CALCULATED", "required": True},
    #     {"vm_upper_pu": "vn_kv", "source": "CALCULATED", "required": True},
    #     {"detect_oscillation": "sn_mva", "source": "CALCULATED", "required": True},
    # ],
    "create_tap_control": [
        {"field": "trafo_top_level_index", "source": "CALCULATED", "required": True},
        {"field": "mode", "source": "CALCULATED", "required": True},
        {"field": "vm_lower_pu", "source": "CALCULATED", "required": True},
        {"field": "vm_upper_pu", "source": "CALCULATED", "required": True},
        {"field": "detect_oscillation", "source": "CALCULATED", "required": True},
    ],    
    # "create_tap_control": [
    #     {"field": "trafo_top_level_index", "source": "CALCULATED", "required": True},
    #     {"field": "mode": "from_phase": "CALCULATED", "required": True},
    #     {"field": "vm_lower_pu", "source": "CALCULATED", "required": True},
    #     {"field": "vm_upper_pu", "source": "CALCULATED", "required": True},
    #     {"field": "detect_oscillation", "source": "CALCULATED", "required": True},
    # ],
    #cris 12/08: 1.8 change end    
    
    "create_transformer3w": [
        {"field": "hv_bus", "source": "", "required": True},
        {"field": "mv_bus", "source": "", "required": True},
        {"field": "lv_bus", "source": "", "required": True},
        {"field": "std_type", "source": "", "required": True},
        {"field": "tap_pos", "source": "", "required": False},
        {"field": "name", "source": "", "required": False},
    ],

    "create_line_std_type": [
        {"field": "r_ohm_per_km", "source": "LINE_R_OHM_PER_KM", "required": False},
        {"field": "x_ohm_per_km", "source": "LINE_X_OHM_PER_KM", "required": False},
        {"field": "r0_ohm_per_km", "source": "LINE_R0_OHM_PER_KM", "required": False},
        {"field": "x0_ohm_per_km", "source": "LINE_X0_OHM_PER_KM", "required": False},
        {"field": "c_nf_per_km", "source": "LINE_C_NF_PER_KM", "required": False},
        {"field": "c0_nf_per_km", "source": "LINE_C0_NF_PER_KM", "required": False},
        {"field": "max_i_ka", "source": "LINE_MAX_I_KA", "required": False},
        {"field": "name", "source": "LINE_TYPE", "required": False},
    ],
    
    "create_asymmetric_load": [
        {"field": "bus", "source": "UPSTREAM_CONNECTIVITYNODEID", "required": False},
        {"field": "from_phase", "source": "LOAD_FROM_PHASE", "required": False},
        {"field": "to_phase", "source": "LOAD_TO_PHASE", "required": False},
        {"field": "p_mw", "source": "LOAD_P_MW", "required": False},
        {"field": "q_mvar", "source": "LOAD_Q_MVAR", "required": False},
        {"field": "const_z_percent_p", "source": "LOAD_CONST_Z_PERCENT_P", "required": False},
        {"field": "const_i_percent_p", "source": "LOAD_CONST_I_PERCENT_P", "required": False},
        {"field": "const_z_percent_q", "source": "LOAD_CONST_Z_PERCENT_Q", "required": False},
        {"field": "const_i_percent_q", "source": "LOAD_CONST_I_PERCENT_Q", "required": False},
        {"field": "sn_mva", "source": "LOAD_SN_MVA", "required": False},
        {"field": "scaling", "source": "LOAD_SCALING", "required": False},
        {"field": "in_service", "source": "LOAD_IN_SERVICE", "required": False},
        {"field": "name", "source": "STRUCTURE_NUM", "required": False},
        {"field": "type", "source": "LOAD_TYPE", "required": False},
        
    ],
    "create_asymmetric_gen": [
        {"field": "bus", "source": "UPSTREAM_CONNECTIVITYNODEID", "required": False},
        {"field": "from_phase", "source": "GEN_FROM_PHASE", "required": False},
        {"field": "to_phase", "source": "GEN_TO_PHASE", "required": False},
        {"field": "p_mw", "source": "gen_p_mw_list", "required": False},
        {"field": "vm_pu", "source": "GEN_VM_PU", "required": False},
        {"field": "sn_mva", "source": "GEN_SN_MVA", "required": False},
        {"field": "scaling", "source": "GEN_SCALING", "required": False},
        {"field": "in_service", "source": "GEN_IN_SERVICE", "required": False},
        {"field": "name", "source": "CONDUCTING_EQUIPMENTID", "required": False},
        {"field": "type", "source": "GEN_TYPE", "required": False}        
    ],    
    "create_asymmetric_sgen": [
        {"field": "bus", "source": "UPSTREAM_CONNECTIVITYNODEID", "required": False},
        {"field": "from_phase", "source": "SGEN_FROM_PHASE", "required": False},
        {"field": "to_phase", "source": "SGEN_TO_PHAS", "required": False},
        {"field": "p_mw", "source": "SGEN_P_MW", "required": False},
        {"field": "q_mvar", "source": "SGEN_Q_MVAR", "required": False},
        {"field": "sn_mva", "source": "SGEN_SN_MVA", "required": False},
        {"field": "scaling", "source": "SGEN_SCALING", "required": False},
        {"field": "in_service", "source": "SGEN_IN_SERVICE", "required": False},
        {"field": "name", "source": "CONDUCTING_EQUIPMENTID", "required": False},
        {"field": "type", "source": "SGEN_TYPE", "required": False}
    ],
    #cris 12/08: 1.5 change start
    "create_sgen_control": [
        {"field": "element", "source": "CALCULATED", "required": False},
        {"field": "qv_curve", "source": "CALCULATED", "required": False},
        {"field": "et", "source": "CALCULATED", "required": False},
        {"field": "mode", "source": "CALCULATED", "required": False},
        {"field": "damping_coef", "source": "CALCULATED", "required": False},
    ], 
    #cris 12/08: 1.5 change end        
}

# fmt: on


class SCECircuitDataTransformerHelper:
    def __init__(self):
        self.fmap = {}
        for create_method, mapping in sce_field_mapping.items():
            self.fmap[create_method] = {
                item["field"]: {"source": item["source"], "required": item["required"]}
                for item in mapping
                if item["source"] != ""
            }

    def is_number(self, value):
        return isinstance(value, numbers.Number)

    def get_arg(self, k, v):
        if k == "net":
            return "net"
        ret = f"{k} = '{v}'" if isinstance(v, str) else f"{k} = {v}"
        return ret

    def log_create_method(self, create_method, args):
        a = [self.get_arg(k, v) for k, v in args.items()]
        a_str = ", ".join(a)
        call = f"mc.{create_method}({a_str})\n"
        with open("method_calls.txt", "a") as f:
            f.write(call)

    def get_arguments(self, create_method, data_row, rt_context, net, control=False):
        if not create_method in self.fmap:
            raise f"{create_method} mapping to multiconductor schema is not defined"

        if control:
            args = {"mc_net": net}
        else:
            args = {"net": net}
        for field, mapping in self.fmap[create_method].items():
            if field in rt_context:
                args[field] = rt_context[field]
            else:
                source_field = mapping["source"]
                if source_field in data_row:
                    field_value = data_row[source_field]
                    if field_value is not None and field_value != "":
                        if not self.is_number(field_value):
                            args[field] = data_row[source_field]
                        elif not math.isnan(field_value):
                            args[field] = data_row[source_field]
        # self.log_create_method(create_method, args)
        return args


class SCECircuitDataTransformer(NetworkDataTransformer):
    def __init__(self):
        self.net = mc.create_empty_network(
            sn_mva=100, rho_ohmm=1e-10, f_hz=60, add_stdtypes=True
        )
        self.trasform_helper = SCECircuitDataTransformerHelper()
        self.bus_name_index_map = {}
        self.sgen_name_index_map = {} #cris 12/08: 1.5 change
        self.tx_name_index_map = {} #cris 12/08: 1.7 and 1.8 change        

    def phases_to_list(self, s: str, default_value=[]) -> list[int]:
        list = [int(digit) for digit in s] if s else default_value
        return list

    def to_list_f(self, decimal_string: str):
        return [float(s) for s in decimal_string.split(",")] if decimal_string else []

    def creat_buses(self, circuit):
        bus_dict = {
            key: value for key, value in circuit.items() if key.lower() == "bus"
        }
        item = next(iter(bus_dict.items()), None)

        if item:
            _, bus_rows = item
            for index, row in bus_rows.iterrows():
                rt_context = {}
                rt_context["num_phases"] = len(self.phases_to_list(row["BUS_PHASES"]))

                grounded_phases = self.phases_to_list(
                    row["BUS_GROUNDED_PHASES"], default_value=None
                )

                if not grounded_phases is None:
                    rt_context["grounded_phases"] = self.phases_to_list(
                        row["BUS_GROUNDED_PHASES"], default_value=None
                    )

                create_bus_args = self.trasform_helper.get_arguments(
                    "create_bus", row, rt_context, self.net
                )
                self.bus_name_index_map[create_bus_args["name"]] = mc.create_bus(
                    **create_bus_args
                )
                pass
        else:
            device_keys = ",".join(circuit.keys())
            raise f"bus data not found in {device_keys}"

    def get_row_value(self, row, column, default_value):
        return row[column] if row[column] else default_value

    def transform(self, circuit: dict) -> tuple[pp.pandapowerNet, dict]:
        tr_context = {}
        self.creat_buses(circuit)

        for device_type, device_data in circuit.items():
            match device_type.lower():
                case "ext_grid":
                    for index, row in device_data.iterrows():
                        from_phase = self.phases_to_list(row["EXTERNALGRID_FROM_PHASE"])
                        to_phase = self.phases_to_list(row["EXTERNALGRID_TO_PHASE"])
                        rt_context = {
                            "bus": self.bus_name_index_map[
                                row["DOWNSTREAM_CONNECTIVITYNODEID"]
                            ],
                            "from_phase": from_phase,
                            "to_phase": to_phase,
                        }

                        create_ext_grid_sequence_args = (
                            self.trasform_helper.get_arguments(
                                "create_ext_grid_sequence", row, rt_context, self.net
                            )
                        )
                        mc.create_ext_grid_sequence(**create_ext_grid_sequence_args)
                        pass

                case "line":
                    for index, row in device_data.iterrows():
                        line_connected_phase = self.get_row_value(
                            row, "LINE_CONNECTED_PHASE", ""
                        )
                        line_from_phase = self.get_row_value(row, "LINE_TO_PHASE", "")
                        line_to_phase = self.get_row_value(row, "LINE_TO_PHASE", "")

                        phase_count = len(line_connected_phase)
                        if (
                            "N" in line_connected_phase
                            or "0" in line_from_phase
                            or "0" in line_to_phase
                        ) and phase_count < 4:
                            std_type = (
                                self.get_row_value(row, "MATERIAL_CODE", "None") + "_" + str(phase_count) + "_N"
                            )
                        else:
                            std_type = self.get_row_value(row, "MATERIAL_CODE", "None") + "_" + str(phase_count)

                        from_phase = self.phases_to_list(row["LINE_FROM_PHASE"])
                        to_phase = self.phases_to_list(row["LINE_TO_PHASE"])
                        model_type = row["LINE_MODEL_TYPE"]
                        if model_type:
                            model_type = model_type.lower()

                        rt_context = {
                            "from_bus": self.bus_name_index_map[
                                row["UPSTREAM_CONNECTIVITYNODEID"]
                            ],
                            "to_bus": self.bus_name_index_map[
                                row["DOWNSTREAM_CONNECTIVITYNODEID"]
                            ],
                            "std_type": std_type,
                            "from_phase": from_phase,
                            "to_phase": to_phase,
                            "model_type": model_type,
                        }
                        self.create_line_std_type(circuit)
                        self.create_line_matrix_std_type(circuit)
                        create_line_args = self.trasform_helper.get_arguments(
                            "create_line", row, rt_context, self.net
                        )
                        mc.create_line(**create_line_args)
                        pass
                case "switch":
                    for index, row in device_data.iterrows():
                        #cris 12/08 change start
                        phase = self.phases_to_list(
                            row["SWITCH_FROM_PHASE"], default_value=[1, 2, 3]
                        )

                        rt_context = {
                            "bus": self.bus_name_index_map[
                                row["UPSTREAM_CONNECTIVITYNODEID"]
                            ],
                            "element": self.bus_name_index_map[
                                row["DOWNSTREAM_CONNECTIVITYNODEID"]
                            ],
                            "phase": phase,
                            "et": 'b', 
                            "closed": self.get_row_value(row, "SWITCH_CLOSED", True),
                        }
                        #cris 12/08 change end
                        create_switch_args = self.trasform_helper.get_arguments(
                            "create_switch", row, rt_context, self.net
                        )
                        mc.create_switch(**create_switch_args)
                        pass
                case "shunt":
                    for index, row in device_data.iterrows():
                        from_phase = self.phases_to_list(row["SHUNT_FROM_PHASE"])
                        to_phase = self.phases_to_list(row["SHUNT_TO_PHASE"])

                        shunt_connected_phase = row["SHUNT_CONNECTED_PHASE"]
                        shunt_p_mw_list = row["SHUNT_P_MW"] / len(shunt_connected_phase)
                        shunt_q_mvar_list = row["SHUNT_Q_MVAR"] / len(
                            shunt_connected_phase
                        )

                        if row["SHUNT_CONNECTION_TYPE"].upper() not in ["WYE", "DELTA"]:
                            raise ValueError(
                                f"{row['SHUNT_CONNECTION_TYPE']} is invalid, should be Wye or Delta."
                            )

                        cap_voltage = (
                            row["SHUNT_VN_KV"] / math.sqrt(3)
                            if row["SHUNT_CONNECTION_TYPE"].upper() == "WYE"
                            else row["SHUNT_VN_KV"]
                        )

                        rt_context = {
                            "bus": self.bus_name_index_map[
                                row["UPSTREAM_CONNECTIVITYNODEID"]
                            ],
                            "vn_kv": cap_voltage,
                            "p_mw": shunt_p_mw_list,
                            "q_mvar": shunt_q_mvar_list,
                            "from_phase": from_phase,
                            "to_phase": to_phase,
                            "control_mode": row["SHUNT_CONTROL_MODE"].lower() #cris 12/08: 1.4 change
                        }
                        create_create_asymmetric_shunt_args = (
                            self.trasform_helper.get_arguments(
                                "create_asymmetric_shunt", row, rt_context, self.net
                            )
                        )
                        mc.create_asymmetric_shunt(
                            **create_create_asymmetric_shunt_args
                        )
                        pass

                case "transformer":
                    for index, row in device_data.iterrows():
                        if not row["XFRMR_HV_CONNECTION_TYPE"]:
                            raise ValueError(f"XFRMR_HV_CONNECTION_TYPE is required")
                        if not row["XFRMR_LV_CONNECTION_TYPE"]:
                            raise ValueError(f"XFRMR_LV_CONNECTION_TYPE is required")
                        
                        ######################## cris update START ########################
                        conn_type_values = ["WYE", "DELTA", "D", "YN", "Y", "YG"]
                        ######################## cris update END ########################

                        for connection_type in [
                            row["XFRMR_HV_CONNECTION_TYPE"].upper(),
                            row["XFRMR_LV_CONNECTION_TYPE"].upper(),
                        ]:
                            if connection_type not in conn_type_values:
                                raise ValueError(
                                    f"{connection_type} is invalid, should be one of {str(conn_type_values)}."
                                )

                        hv_kv = (
                            row["XFRMR_VN_HV_KV"] / math.sqrt(3)
                            if row["XFRMR_HV_CONNECTION_TYPE"].upper() == "WYE" or "Y" in row["XFRMR_HV_CONNECTION_TYPE"].upper()
                            else row["XFRMR_VN_HV_KV"]
                        )

                        lv_kv = (
                            row["XFRMR_VN_LV_KV"] / math.sqrt(3)
                            if row["XFRMR_LV_CONNECTION_TYPE"].upper() == "WYE" or "Y" in row["XFRMR_LV_CONNECTION_TYPE"].upper()
                            else row["XFRMR_VN_LV_KV"]
                        )

                        ######################## cris update START ########################
                        match len(row["XFRMR_CONNECTED_PHASE"]):
                            case 1:
                                vn_kv = [hv_kv, lv_kv]

                                if row["XFRMR_CONNECTED_PHASE"] == "A":
                                    hv_from_phase = [1]
                                    hv_to_phase = [0]
                                    lv_from_phase = [1]
                                    lv_to_phase = [0]
                                elif row["XFRMR_CONNECTED_PHASE"] == "B":
                                    hv_from_phase = [2]
                                    hv_to_phase = [0]
                                    lv_from_phase = [2]
                                    lv_to_phase = [0]
                                else:
                                    hv_from_phase = [3]
                                    hv_to_phase = [0]
                                    lv_from_phase = [3]
                                    lv_to_phase = [0]
                            case 2:
                                vn_kv = [hv_kv, hv_kv, lv_kv, lv_kv]

                                #wye-wye
                                if row["XFRMR_HV_CONNECTION_TYPE"].upper() in ('WYE', 'YN', 'Y', 'YG') and row["XFRMR_LV_CONNECTION_TYPE"].upper() in ('WYE', 'YN', 'Y', 'YG'):
                                    if row["XFRMR_CONNECTED_PHASE"] == "AB" or row["XFRMR_CONNECTED_PHASE"] == "BA":
                                        hv_from_phase = [1, 2]
                                        hv_to_phase = [0, 0]
                                        lv_from_phase = [1, 2]
                                        lv_to_phase = [0, 0]
                                    elif row["XFRMR_CONNECTED_PHASE"] == "BC" or row["XFRMR_CONNECTED_PHASE"] == "CB":
                                        hv_from_phase = [2, 3]
                                        hv_to_phase = [0, 0]
                                        lv_from_phase = [2, 3]
                                        lv_to_phase = [0, 0]
                                    else:
                                        hv_from_phase = [3, 1]
                                        hv_to_phase = [0, 0]
                                        lv_from_phase = [3, 1]
                                        lv_to_phase = [0, 0]

                                #wye-delta
                                elif row["XFRMR_HV_CONNECTION_TYPE"].upper() in ('WYE', 'YN', 'Y', 'YG') and row["XFRMR_LV_CONNECTION_TYPE"].upper() in ('DELTA', 'D'):
                                    if row["XFRMR_CONNECTED_PHASE"] == "AB" or row["XFRMR_CONNECTED_PHASE"] == "BA":
                                        hv_from_phase = [1, 2]
                                        hv_to_phase = [0, 0]
                                        lv_from_phase = [1, 2]
                                        lv_to_phase = [2, 1]
                                    elif row["XFRMR_CONNECTED_PHASE"] == "BC" or row["XFRMR_CONNECTED_PHASE"] == "CB":
                                        hv_from_phase = [2, 3]
                                        hv_to_phase = [0, 0]
                                        lv_from_phase = [2, 3]
                                        lv_to_phase = [3, 2]
                                    else:
                                        hv_from_phase = [3, 1]
                                        hv_to_phase = [0, 0]
                                        lv_from_phase = [3, 1]
                                        lv_to_phase = [1, 3]

                                #delta-wye
                                elif row["XFRMR_HV_CONNECTION_TYPE"].upper() in ('DELTA', 'D') and row["XFRMR_LV_CONNECTION_TYPE"].upper() in ('WYE', 'YN', 'Y', 'YG'):
                                    if row["XFRMR_CONNECTED_PHASE"] == "AB" or row["XFRMR_CONNECTED_PHASE"] == "BA":
                                        hv_from_phase = [1, 2]
                                        hv_to_phase = [2, 1]
                                        lv_from_phase = [1, 2]
                                        lv_to_phase = [0, 0]
                                    elif row["XFRMR_CONNECTED_PHASE"] == "BC" or row["XFRMR_CONNECTED_PHASE"] == "CB":
                                        hv_from_phase = [2, 3]
                                        hv_to_phase = [3, 2]
                                        lv_from_phase = [2, 3]
                                        lv_to_phase = [0, 0]
                                    else:
                                        hv_from_phase = [3, 1]
                                        hv_to_phase = [1, 3]
                                        lv_from_phase = [3, 1]
                                        lv_to_phase = [0, 0]

                                #delta-delta
                                else:
                                    if row["XFRMR_CONNECTED_PHASE"] == "AB" or row["XFRMR_CONNECTED_PHASE"] == "BA":
                                        hv_from_phase = [1, 2]
                                        hv_to_phase = [2, 1]
                                        lv_from_phase = [1, 2]
                                        lv_to_phase = [2, 1]
                                    elif row["XFRMR_CONNECTED_PHASE"] == "BC" or row["XFRMR_CONNECTED_PHASE"] == "CB":
                                        hv_from_phase = [2, 3]
                                        hv_to_phase = [3, 2]
                                        lv_from_phase = [2, 3]
                                        lv_to_phase = [3, 2]
                                    else:
                                        hv_from_phase = [3, 1]
                                        hv_to_phase = [1, 3]
                                        lv_from_phase = [3, 1]
                                        lv_to_phase = [1, 3]
                            case 3:
                                vn_kv = [hv_kv, hv_kv, hv_kv, lv_kv, lv_kv, lv_kv]

                                #wye-wye
                                if row["XFRMR_HV_CONNECTION_TYPE"].upper() in ('WYE', 'YN', 'Y', 'YG') and row["XFRMR_LV_CONNECTION_TYPE"].upper() in ('WYE', 'YN', 'Y', 'YG'):
                                    hv_from_phase = [1, 2, 3]
                                    hv_to_phase = [0, 0, 0]
                                    lv_from_phase = [1, 2, 3]
                                    lv_to_phase = [0, 0, 0]

                                #wye-delta
                                elif row["XFRMR_HV_CONNECTION_TYPE"].upper() in ('WYE', 'YN', 'Y', 'YG') and row["XFRMR_LV_CONNECTION_TYPE"].upper() in ('DELTA', 'D'):
                                    hv_from_phase = [1, 2, 3]
                                    hv_to_phase = [0, 0, 0]
                                    lv_from_phase = [1, 2, 3]
                                    lv_to_phase = [2, 3, 1]

                                #delta-wye
                                elif row["XFRMR_HV_CONNECTION_TYPE"].upper() in ('DELTA', 'D') and row["XFRMR_LV_CONNECTION_TYPE"].upper() in ('WYE', 'YN', 'Y', 'YG'):
                                    hv_from_phase = [1, 2, 3]
                                    hv_to_phase = [2, 3, 1]
                                    lv_from_phase = [1, 2, 3]
                                    lv_to_phase = [0, 0, 0]

                                #delta-delta
                                else:
                                    hv_from_phase = [1, 2, 3]
                                    hv_to_phase = [2, 3, 1]
                                    lv_from_phase = [1, 2, 3]
                                    lv_to_phase = [2, 3, 1]
                        ######################## cris update END ########################

                        #cris 12/08: 1.6 change start
                        if row['XFRMR_BY_PHASE'] == 'TRUE':
                            sn_mva = row["XFRMR_SN_MVA"]
                        else:
                            sn_mva = row["XFRMR_SN_MVA"] / len(row["XFRMR_CONNECTED_PHASE"])
                        #cris 12/08: 1.6 change end

                        vk_percent = row["XFRMR_VK_PERCENT"] / 2

                        vkr_percent = row["XFRMR_VKR_PERCENT"] / 2

                        pfe_kw = row["XFRMR_PFE_KW"] / len(row["XFRMR_CONNECTED_PHASE"])

                        i0_percent = row["XFRMR_I0_PERCENT"]

                        ######################## cris update START ########################
                        rt_context = {
                            "buses": [
                                self.bus_name_index_map[
                                    row["UPSTREAM_CONNECTIVITYNODEID"]
                                ],
                                self.bus_name_index_map[
                                    row["DOWNSTREAM_CONNECTIVITYNODEID"]
                                ],
                            ],
                            "from_phase": hv_from_phase + lv_from_phase,
                            "to_phase": hv_to_phase + lv_to_phase,
                            "vn_kv": vn_kv,
                            "sn_mva": sn_mva,
                            "vk_percent": vk_percent,
                            "vkr_percent": vkr_percent,
                            "pfe_kw": pfe_kw,
                            "i0_percent": i0_percent,
                        }
                        ######################## cris update END ########################

                        control_mode = row["XFRMR_CONTROL_MODE"] 

                        if not math.isnan(row["XFRMR_TAP_NEUTRAL"]):
                            rt_context["tap_neutral"] = row["XFRMR_TAP_NEUTRAL"]

                        if not math.isnan(row["XFRMR_TAP_MAX"]):
                            rt_context["tap_max"] = row["XFRMR_TAP_MAX"]

                        if not math.isnan(row["XFRMR_TAP_MIN"]):
                            rt_context["tap_min"] = row["XFRMR_TAP_MIN"]

                        if not math.isnan(row["XFRMR_TAP_POS"]):
                            rt_context["tap_pos"] = row["XFRMR_TAP_POS"]

                        if not math.isnan(row["XFRMR_TAP_STEP_PERCENT"]):
                            rt_context["tap_step_percent"] = row[
                                "XFRMR_TAP_STEP_PERCENT"
                            ]

                        create_transformer_args = self.trasform_helper.get_arguments(
                            "create_transformer1ph", row, rt_context, self.net
                        )
                        #cris 12/08: 1.7 and 1.8 change start
                        self.tx_name_index_map[create_transformer_args["name"]] = mc.create_transformer1ph(**create_transformer_args)
                        
                        if control_mode in ['LTC', 'LDC'] and len(row["XFRMR_CONNECTED_PHASE"]) == 3:
                            vm_lower_pu = float(row["XFRMR_VM_LOWER_PU"])
                            vm_upper_pu = float(row["XFRMR_VM_UPPER_PU"])
                            control_side = row["XFRMR_CONTROL_SIDE"]
                            tol = float(row["XFRMR_TOL"])
                            pt_ratio = float(row["XFRMR_PT_RATIO"])
                            ct_rating_a = float(row["XFRMR_CT_RATING_A"])
                            r_ldc_forward_v = float(row["XFRMR_R_LDC_FORWARD_V"])
                            x_ldc_forward_v = float(row["XFRMR_X_LDC_FORWARD_V"])
                            r_ldc_reverse_v = float(row["XFRMR_R_LDC_REVERSE_V"])
                            x_ldc_reverse_v = float(row["XFRMR_X_LDC_REVERSE_V"])
                            bandwidth_forward_v = float(row["XFRMR_BANDWIDTH_FORWARD_V"])
                            bandwidth_reverse_v = float(row["XFRMR_BANDWIDTH_REVERSE_V"])
                            v_set_secondary_v = float(row["XFRMR_V_SET_SECONDARY_V"])
                            mode = 'locked_forward' if row["XFRMR_MODE"] in ['Locked Forward', 'N/A'] else 'locked_reverse'

                            transformer_index = self.tx_name_index_map[row["CONDUCTING_EQUIPMENTID"]]
                            self.net.trafo1ph.at[transformer_index, 'tap_min'] = -16
                            self.net.trafo1ph.at[transformer_index, 'tap_max'] = 16
                            self.net.trafo1ph.at[transformer_index, 'tap_step_percent'] = 0.625

                            #1.8
                            if control_mode == 'ltc':
                                sub_rt_context = {
                                        "trafo_top_level_index": transformer_index,
                                        "mode": mode,
                                        "vm_lower_pu": vm_lower_pu,
                                        "vm_upper_pu": vm_upper_pu,
                                        "detect_oscillation": False,
                                    }
                                create_ltc_control_args = self.trasform_helper.get_arguments(
                                                    "create_tap_control", row, sub_rt_context, self.net, True
                                                )
                                add_load_tap_changer_control(**create_ltc_control_args)

                            #1.7
                            else:
                                if mode == 'locked_forward':
                                    bandwidth_secondary_v = bandwidth_forward_v * 2
                                    r_ldc_v = r_ldc_forward_v * ct_rating_a/pt_ratio 
                                    x_ldc_v = x_ldc_forward_v * ct_rating_a/pt_ratio 
                                elif mode == 'locked_reverse':
                                    bandwidth_secondary_v = bandwidth_reverse_v * 2
                                    r_ldc_v = r_ldc_reverse_v * ct_rating_a/pt_ratio 
                                    x_ldc_v = x_ldc_reverse_v * ct_rating_a/pt_ratio 
                                else:
                                    raise (f"Mode {mode} not supported")
                                
                                sub_rt_context = {
                                        "trafo_top_level_index": transformer_index,
                                        "mode": mode,
                                        "v_set_secondary_v": v_set_secondary_v,
                                        "bandwidth_secondary_v": bandwidth_secondary_v,
                                        "pt_ratio": pt_ratio,
                                        "ct_primary_rating_a": ct_rating_a,
                                        "r_ldc_v": r_ldc_v,
                                        "x_ldc_v": x_ldc_v,
                                    }
                                
                                create_ldc_control_args = self.trasform_helper.get_arguments(
                                                    "create_regulator_control", row, sub_rt_context, self.net, True
                                                )
                                add_line_drop_control(**create_ldc_control_args)
                            
                            #TODO: run_control = True #send to run_pf(net, run_control=True)
                            tr_context['run_control'] = True
                        #cris 12/08: 1.7 and 1.8 change end
                        pass

                case "load":
                    for index, row in device_data.iterrows():
                        load_p_mw_list = self.to_list_f(row["LOAD_P_MW"])
                        load_q_mvar_list = self.to_list_f(row["LOAD_Q_MVAR"])
                        # from_phase = self.phases_to_list(row["LOAD_FROM_PHASE"])
                        # to_phase = self.phases_to_list(row["LOAD_TO_PHASE"])

                        ######################## cris update START ########################
                        total_load_p_mw = sum(load_p_mw_list)
                        total_load_q_mvar = sum(load_q_mvar_list)

                        match ''.join(sorted(row['LOAD_CONNECTED_PHASE'])):
                            case 'A':
                                from_phase = [1]
                                to_phase = [0]

                                p_mw = [total_load_p_mw]
                                q_mvar = [total_load_q_mvar]
                            case 'B':
                                from_phase = [2]
                                to_phase = [0]

                                p_mw = [total_load_p_mw]
                                q_mvar = [total_load_q_mvar]
                            case 'C':
                                from_phase = [3]
                                to_phase = [0]
                                
                                p_mw = [total_load_p_mw]
                                q_mvar = [total_load_q_mvar]
                            case 'AB':
                                from_phase = [1, 2]
                                if row["LOAD_CONNECTION_TYPE"].upper() in ["WYE", "YN", "Y", "YG"]:
                                    to_phase = [0, 0]
                                else:
                                    to_phase = [2, 1]
                                
                                p_mw = [total_load_p_mw/2 , total_load_p_mw/2]
                                q_mvar = [total_load_q_mvar/2 , total_load_q_mvar/2]
                            case 'AC':
                                from_phase = [3, 1]
                                if row["LOAD_CONNECTION_TYPE"].upper() in ["WYE", "YN", "Y", "YG"]:
                                    to_phase = [0, 0]
                                else:
                                    to_phase = [1, 3]     

                                p_mw = [total_load_p_mw/2 , total_load_p_mw/2]
                                q_mvar = [total_load_q_mvar/2 , total_load_q_mvar/2]
                            case 'BC':
                                from_phase = [2, 3]
                                if row["LOAD_CONNECTION_TYPE"].upper() in ["WYE", "YN", "Y", "YG"]:
                                    to_phase = [0, 0]
                                else:
                                    to_phase = [3, 2]     

                                p_mw = [total_load_p_mw/2 , total_load_p_mw/2]
                                q_mvar = [total_load_q_mvar/2 , total_load_q_mvar/2]
                            case 'ABC':
                                from_phase = [1, 2, 3]
                                if row["LOAD_CONNECTION_TYPE"].upper() in ["WYE", "YN", "Y", "YG"]:
                                    to_phase = [0, 0, 0]
                                else:
                                    to_phase = [2, 3, 1]
                                                                
                                p_mw = [total_load_p_mw/3 , total_load_p_mw/3 , total_load_p_mw/3]
                                q_mvar = [total_load_q_mvar/3 , total_load_q_mvar/3 , total_load_q_mvar/3]

                        rt_context = {
                            "bus": self.bus_name_index_map[
                                row["UPSTREAM_CONNECTIVITYNODEID"]
                            ],
                            "p_mw": p_mw,
                            "q_mvar": q_mvar,
                            "from_phase": from_phase,
                            "to_phase": to_phase,
                        }
                        ######################## cris update END ########################
                        create_load_args = self.trasform_helper.get_arguments(
                            "create_asymmetric_load", row, rt_context, self.net
                        )
                        try:
                            mc.create_asymmetric_load(**create_load_args)
                        except Exception as e:
                            df_from_series = row.to_frame()
                            styled_series = df_from_series.style.format({0: "{:,.0f}"})
                            lines = []
                            for key, value in create_load_args.items():
                                lines.append(f"{key}: {value}")
                            create_func_args = "\n".join(lines)

                            logger.info(f"Exception Occurred while creating `{device_type}`")
                            logger.info(create_func_args)
                            logger.info(f"Exception Message: {str(e)}")
                            raise 2

                # Temporarily disabled (12/4/25)
                # case "gen":
                #     for index, row in device_data.iterrows():
                #         from_phase = self.phases_to_list(row["GEN_FROM_PHASE"])
                #         to_phase = self.phases_to_list(row["GEN_TO_PHASE"])
                #         gen_p_mw_list = self.to_list_f(row["GEN_P_MW"])
                #         rt_context = {
                #             "bus": self.bus_name_index_map[
                #                 row["UPSTREAM_CONNECTIVITYNODEID"]
                #             ],
                #             "from_phase": from_phase,
                #             "to_phase": to_phase,
                #             "p_mw": gen_p_mw_list,
                #         }
                #         create_gen_args = self.trasform_helper.get_arguments(
                #             "create_asymmetric_gen", row, rt_context, self.net
                #         )
                #         mc.create_asymmetric_gen(**create_gen_args)

                case "sgen":
                    for index, row in device_data.iterrows():
                        # from_phase = self.phases_to_list(row["SGEN_FROM_PHASE"])
                        # to_phase = self.phases_to_list(row["SGEN_TO_PHASE"])

                        ######################## cris update START ########################
                        sgen_p_mw_list = self.to_list_f(row["SGEN_P_MW"])
                        sgen_q_mvar_list = self.to_list_f(row["SGEN_Q_MVAR"])

                        total_sgen_p_mw = sum(sgen_p_mw_list)
                        total_sgen_q_mvar = sum(sgen_q_mvar_list)

                        match ''.join(sorted(row['SGEN_CONNECTED_PHASE'])):
                            case 'A':
                                from_phase = [1]
                                to_phase = [0]

                                p_mw = [total_sgen_p_mw]
                                q_mvar = [total_sgen_q_mvar]
                            case 'B':
                                from_phase = [2]
                                to_phase = [0]
                                p_mw = [total_sgen_p_mw]
                                q_mvar = [total_sgen_q_mvar]
                            case 'C':
                                from_phase = [3]
                                to_phase = [0]
                                p_mw = [total_sgen_p_mw]
                                q_mvar = [total_sgen_q_mvar]
                            case 'AB':
                                from_phase = [1, 2]
                                if row["SGEN_CONNECTION_TYPE"].upper() in ["WYE", "YN", "Y", "YG"]:
                                    to_phase = [0, 0]
                                else:
                                    to_phase = [2, 1]
                                p_mw = [total_sgen_p_mw/2 , total_sgen_p_mw/2]
                                q_mvar = [total_sgen_q_mvar/2 , total_sgen_q_mvar/2]
                            case 'AC':
                                from_phase = [3, 1]
                                if row["SGEN_CONNECTION_TYPE"].upper() in ["WYE", "YN", "Y", "YG"]:
                                    to_phase = [0, 0]
                                else:
                                    to_phase = [1, 3]
                                p_mw = [total_sgen_p_mw/2 , total_sgen_p_mw/2]
                                q_mvar = [total_sgen_q_mvar/2 , total_sgen_q_mvar/2]
                            case 'BC':
                                from_phase = [2, 3]
                                if row["SGEN_CONNECTION_TYPE"].upper() in ["WYE", "YN", "Y", "YG"]:
                                    to_phase = [0, 0]
                                else:
                                    to_phase = [3, 2]
                                p_mw = [total_sgen_p_mw/2 , total_sgen_p_mw/2]
                                q_mvar = [total_sgen_q_mvar/2 , total_sgen_q_mvar/2]
                            case 'ABC':
                                from_phase = [1, 2, 3]
                                if row["SGEN_CONNECTION_TYPE"].upper() in ["WYE", "YN", "Y", "YG"]:
                                    to_phase = [0, 0, 0]
                                else:
                                    to_phase = [2, 3, 1]
                                p_mw = [total_sgen_p_mw/3 , total_sgen_p_mw/3 , total_sgen_p_mw/3]
                                q_mvar = [total_sgen_q_mvar/3 , total_sgen_q_mvar/3 , total_sgen_q_mvar/3]

                        rt_context = {
                            "bus": self.bus_name_index_map[
                                row["UPSTREAM_CONNECTIVITYNODEID"]
                            ],
                            "const_z_percent_p": 0,
                            "const_i_percent_p": 0,
                            "const_z_percent_q": 0,
                            "const_i_percent_q": 0,
                            "from_phase": from_phase,
                            "to_phase": to_phase,
                            "p_mw": p_mw,
                            "q_mvar": q_mvar,
                        }
                        create_sgen_args = self.trasform_helper.get_arguments(
                            "create_asymmetric_sgen", row, rt_context, self.net
                        )
                        try:
                            #cris 12/08: 1.5 change start
                            self.sgen_name_index_map[create_sgen_args["name"]] = mc.create_asymmetric_sgen(**create_sgen_args)

                            if row["SGEN_CONTROL_MODE"] is not None and len(row["SGEN_CONNECTED_PHASE"]) == 3:
                                # volt_var = self.to_list_f(row["SGEN_CONTROL_CURVE"].strip("[]"))
                                volt_var = [0.92, 0.967, 1.033, 1.07, 0.3, 0.0, 0.0, -0.3]
                                volt_curve = volt_var[:len(volt_var)//2]
                                var_curve = volt_var[len(volt_var)//2:]
                                qv_curve = QVCurve(vm_points_pu=volt_curve, q_points_pu=var_curve)
                                
                                sub_rt_context = {
                                        "element": self.sgen_name_index_map[
                                            row["CONDUCTING_EQUIPMENTID"]
                                        ],
                                        "qv_curve": qv_curve,
                                        "et": 'asymmetric_sgen',
                                        "mode": 'gang',
                                        "damping_coef": 2,
                                    }
                                create_sgen_control_args = self.trasform_helper.get_arguments(
                                                    "create_sgen_control", row, sub_rt_context, self.net, True
                                                )
                                add_volt_var_control(**create_sgen_control_args)

                                #TODO: run_control = True #send to run_pf(net, run_control=True)
                                tr_context['run_control'] = True
                            #cris 12/08: 1.5 change end
                        except Exception as e:
                            df_from_series = row.to_frame()
                            styled_series = df_from_series.style.format({0: "{:,.0f}"})

                            lines = []
                            for key, value in create_sgen_args.items():
                                lines.append(f"{key}: {value}")
                            create_func_args = "\n".join(lines)

                            logger.info(f"Exception Occurred while creating `{device_type}`")
                            logger.info(create_func_args)
                            logger.info(f"Exception Message: {str(e)}")
                            raise e

                case _:
                    pass
        return (self.net, tr_context)

    def create_line_matrix_std_type(self, circuit):
        std_type_dict = {
            key: value
            for key, value in circuit.items()
            if key.lower() == "line_std_type"
        }
        item = next(iter(std_type_dict.items()), None)

        if item:
            _, std_type_rows = item
            for index, row in std_type_rows.iterrows():
                material_code = self.get_row_value(row, "MATERIAL_CODE", "None")
                if "None" in material_code:
                    phase_conductor_values = {
                        "LINE_R_OHM_PER_KM": 0.00001,
                        "LINE_X_OHM_PER_KM": 0.00001,
                        "LINE_MAX_I_KA": 1,
                        "LINE_G_US_PER_KM": 0.0,
                        "LINE_B_US_PER_KM": 0.0,
                    }
                    neutral_conductor_values = {
                        "LINE_RN_OHM_PER_KM": 0.00001,
                        "LINE_XN_OHM_PER_KM": 0.00001,
                        "LINE_MAX_IN_KA": 1,
                        "LINE_GN_US_PER_KM": 0.0,
                        "LINE_BN_US_PER_KM": 0.0,
                    }
                else:
                    phase_conductor_values = {
                        "LINE_R_OHM_PER_KM": row["LINE_R_OHM_PER_KM"],
                        "LINE_X_OHM_PER_KM": row["LINE_X_OHM_PER_KM"],
                        "LINE_MAX_I_KA": row["LINE_MAX_I_KA"],
                        "LINE_G_US_PER_KM": row["LINE_G_US_PER_KM"],
                        "LINE_B_US_PER_KM": row["LINE_B_US_PER_KM"],
                    }
                    neutral_conductor_values = {
                        "LINE_RN_OHM_PER_KM": row["LINE_RN_OHM_PER_KM"],
                        "LINE_XN_OHM_PER_KM": row["LINE_XN_OHM_PER_KM"],
                        "LINE_MAX_IN_KA": row["LINE_MAX_IN_KA"],
                        "LINE_GN_US_PER_KM": row["LINE_GN_US_PER_KM"],
                        "LINE_BN_US_PER_KM": row["LINE_BN_US_PER_KM"],
                    }
                std_type = MultiConductorLineMatrixStdType(
                    material_code=material_code,
                    phase_conductor_values=phase_conductor_values,
                    neutral_conductor_values=neutral_conductor_values,
                )
                data = std_type.get_matrix_linetypes()
                mc.pycci.std_types.create_std_types(
                    self.net, data=data, element="matrix"
                )

    def create_line_std_type(self, circuit):
        std_type_dict = {
            key: value
            for key, value in circuit.items()
            if key.lower() == "line_std_type"
        }
        item = next(iter(std_type_dict.items()), None)

        if item:
            _, std_type_rows = item
            pass
            for index, row in std_type_rows.iterrows():
                line_std_types = self.trasform_helper.get_arguments(
                    "create_line_std_type", row, {}, self.net
                )

                if "name" in line_std_types:
                    linetypes = {
                        line_std_types["name"]: {
                            key: value
                            for key, value in line_std_types.items()
                            if key != "name" and key != "net"
                        }
                    }
                    mc.pycci.std_types.create_std_types(
                        self.net, data=linetypes, element="sequence"
                    )
                pass

class SCEResultsTransformer(ResultTransformer):

    def __init__(self):
        self.logger = logging.getLogger("SCEResultsTransformer_Logger")

    def rename_column(self, col):
        match col[0]:
            case "vm_pu":
                return f"SVVOLTAGE_V_{phase_map[col[1]]}_PU"       
            case "va_degree":
                return f"SVVOLTAGE_{phase_map[col[1]]}_ANGLE" 
            case "p_mw":
                return f"SVPOWERFLOW_P_{phase_map[col[1]]}_MW" 
            case "q_mvar":
                return f"SVPOWERFLOW_Q_{phase_map[col[1]]}_MVAR" 
            case _:
                raise f"Result mapping error: unknown column `{col[0]}`"            

    def transform_line_results(self, net: pp.pandapowerNet, load_data=None) -> dict:
        pass

    def transform_bus_results(self, net: pp.pandapowerNet, load_data=None) -> dict:
        columns = ["vm_pu", "va_degree", "p_mw", "q_mvar"]
        df_res = net.res_bus[columns]
        df_reset = df_res.reset_index()

        result = df_reset.pivot_table(
            index=[col for col in df_reset.columns if col not in ["phase"] + columns],
            columns="phase",
            values=columns,
            dropna=False,
        )

        result.columns = [self.rename_column(col).upper() for col in result.columns]

        bus_names = net.bus["name"].groupby(level="index").first()

        merged_df = pd.merge(
            result,
            bus_names,
            left_index=True,
            right_index=True,
            how="inner",
            suffixes=("_df1", "_df2"),
        )
        merged_df.rename(columns={"name": "CONDUCTING_EQUIPMENTID"}, inplace=True)
        merged_df["LINKED_EQUIPTYPE"] = "TERMINAL"
        merged_df["POWER_SYSTEM_RESOURCE_MRID"] = load_data[
            "POWER_SYSTEM_RESOURCE_MRID"
        ]
        merged_df["POWER_SYSTEM_RESOURCE_TYPE"] = "CIRCUIT"
        dt = pd.to_datetime(load_data["REPORTED_DTTM"])
        merged_df["REPORTED_DTTM"] = dt

        merged_df["REPORTED_DT"] = dt
        merged_df["HOUR_ID"] = dt.hour
        merged_df["YEAR_ID"] = load_data["YEAR_ID"]
        merged_df["MONTH_ID"] = load_data["MONTH_ID"]
        merged_df["STUDY_ID"] = load_data["TAG_NAME"]
        merged_df["EQUIPMENT_STATE"] = "INITIAL"
        merged_df["CHANGED_DATE"] = pd.to_datetime('now')

        extra_metadata = json.loads(load_data["EXTRA_METADATA"])
        merged_df["EDW_CREATED_DATE"] = datetime.strptime(extra_metadata["EDW_CREATED_DATE"], '%Y-%m-%dT%H:%M:%S.%fZ')
        merged_df["EDW_MODIFIED_DATE"] = datetime.strptime(extra_metadata["EDW_MODIFIED_DATE"], '%Y-%m-%dT%H:%M:%S.%fZ')
        merged_df["EDW_CREATED_BY"] = extra_metadata["EDW_CREATED_BY"]
        merged_df["EDW_MODIFIED_BY"] = extra_metadata["EDW_MODIFIED_BY"]

        merged_df["EDW_BATCH_ID"] = extra_metadata["EWD_BATCH_ID"]
        merged_df["EDW_BATCH_DETAIL_ID"] = extra_metadata["EWD_BATCH_DETAIL_ID"]
        merged_df["EDW_LAST_DML_CD"] = extra_metadata["EDW_LAST_DML_CD"]

        return merged_df

    def transform(self, net: pp.pandapowerNet, load_data: any = None) -> dict:
        bus_results = self.transform_bus_results(net, load_data)

        df = pd.DataFrame(columns=output_schema.keys()).astype(output_schema)
        result = pd.concat([df, bus_results], ignore_index=True)
        return result


class SCEPowerflowPipelineTemp(Pipeline):

    def before_run(self, net):
        if "ext_grid_sequence" in net:
            net["ext_grid_sequence"]["x_ohm"] = 1e-9
            net["ext_grid_sequence"]["r_ohm"] = 1e-9
        if "switch" in net:
            net['switch']['r_ohm'] = 0 
