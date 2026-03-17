import os

from datetime import datetime
from copy import deepcopy
from decimal import Decimal
import numpy as np
import pandapower as pp
import pandas as pd
from snowflake.snowpark.functions import pandas_udtf, PandasDataFrameType
from snowflake.snowpark import Session
import cloudpickle

from powerflow_analysis.data import OUTPUT_COLUMNS
from powerflow_analysis.exceptions import MissingLoadDataException

from pandapower.plotting import to_html as pp_to_html


class CircuitModel(object):
    pass

class PandapowerCircuitModel(CircuitModel):
    def __init__(self, circuit_data, *args, **kwargs):
        self.net = circuit_model.create_circuit()

class SCECircuitModel(CircuitModel):
    def __init__(self, circuit_data, circuit_key=None, capacitor_enabled=True, generator_enabled=True,
                 transformer_enabled=False, regulator_enabled=False, balanced_load=False, **kwargs):
        # TODO: a circuit analysis actually maybe should be created with a datasource instead of circit data drawn from a datasouce
        # because then when it needs to serialize, it will know how
        if type(circuit_data) is dict:
            self.circuit_data = circuit_data
        else:
            self.circuit_data = circuit_data.load_circuit_data()
        self.balanced_load = balanced_load
        self.circuit_key = circuit_key
        self.capacitor_enabled = capacitor_enabled
        self.transformer_enabled = transformer_enabled
        self.generator_enabled = generator_enabled
        self.regulator_enabled = regulator_enabled
        self.net = self._create_circuit()
        self.run_diagnostic()

    def _create_circuit(self):
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

    def run_diagnostic(self):
        self.diagnostic_results = pp.diagnostic(self.net, report_style=None)


    def diagnostic_to_dataframes(self):
        diagnostic_result = self.diagnostic_results
        dataframes = {}

        # Main Summary DataFrame
        # possible diagnostic keys fromhttps://pandapower.readthedocs.io/en/v2.6.0/_images/diag_results_dict.png
        summary_data = {k: k in self.diagnostic_results for k in ['buses_mult_gens_ext_grids',
                                                                  'deviating_nominal_voltages',
                                                                  'ext_grid',
                                                                  'inconsistent_voltages',
                                                                  'invalid_values',
                                                                  'isolated_sections',
                                                                  'lines_with_impedance_zero',
                                                                  'overload',
                                                                  'problematic_switches',
                                                                  'wrong_reference_system',
                                                                  'wrong_switch_configuration'
                                                                  ]}

        dataframes['Summary'] = pd.DataFrame(summary_data, index=[0])

        # these bits of additional processing are still not really right I think
        # Disconnected Elements DataFrame
        if 'disconnected_elements' in diagnostic_result and diagnostic_result['disconnected_elements']:
            disconnected = diagnostic_result['disconnected_elements'][0]  # Assuming there's only one dict in the list
            max_len = max(len(v) for v in disconnected.values())
            disconnected_data = {k: v + [np.nan] * (max_len - len(v)) for k, v in disconnected.items()}
            dataframes['Disconnected Elements'] = pd.DataFrame(disconnected_data)

        # Invalid Values DataFrame
        if 'invalid_values' in diagnostic_result:
            invalid_values = diagnostic_result['invalid_values']
            invalid_data = []
            for element_type, issues in invalid_values.items():
                for issue in issues:
                    invalid_data.append({
                        'Element Type': element_type,
                        'Element Index': issue[0],
                        'Attribute': issue[1],
                        'Value': issue[2],
                        'Expected': issue[3]
                    })
            dataframes['Invalid Values'] = pd.DataFrame(invalid_data)

        return dataframes

    def generate_html_report(self, out_dir):

        # Create the HTML report
        pp_to_html(self.net, filename=os.path.join(out_dir, f"{self.circuit_key}_pp_report.html"))



class CircuitAnalysis(object):
    def __init__(self, circuit_model, load_data_source):
        self.circuit_model = circuit_model
        if type(load_data_source) is pd.DataFrame:
            self.load_data = load_data_source
        else:
            self.load_data = load_data_source.get_timeseries_data()

class PandapowerCircuitAnalysis(CircuitAnalysis):
    def __init__(self, circuit_model):
        self.circuit_model = circuit_model

    def run_powerflow(self):
        pp.runpp(self.circuit_model.net)
        return self.circuit_model.net.res_bus

class SCECircuitAnalysis(CircuitAnalysis):


    def run_powerflow(self) -> pd.DataFrame:
        # Clear existing loads
        # self.net.load.drop(self.net.load.index, inplace=True)
        
        #SCE thinking pervades this codebase; maybe need to focus on removig SCE nonsense as part of refactor
        self.circuit_model.circuit_data["LOAD"] = self.load_data[self.load_data['measurement_type'] == 'GROSS']
        self.circuit_model.circuit_data["LOAD (GEN)"] = self.load_data[self.load_data['measurement_type'] == 'GEN']
        try:
            period = self.circuit_model.circuit_data["LOAD"]["reported_dttm"].values[0]
        except IndexError:
            raise MissingLoadDataException()

        if self.circuit_model.transformer_enabled:
            # if the transformer is enabled, we want to attach any loads on transformer nodes to the lv_bus, not the
            # hv_bus as will be the default.
            # todo: investigate this maybe?
            self.circuit_model.circuit_data['LOAD']['bus'] = [lvb if lvb in self.circuit_model.circuit_data['TRANSFORMER']['lv_bus'].values else b
                                                for
                                                b, lvb in self.circuit_model.circuit_data['LOAD'][['bus', 'lv_bus']].values]

        if self.circuit_model.generator_enabled:
            if self.circuit_model.transformer_enabled:
                self.circuit_model.circuit_data['LOAD (GEN)']['bus'] = self.circuit_model.circuit_data['LOAD (GEN)']['lv_bus']

        for index, row in self.circuit_model.circuit_data['LOAD'].iterrows():
            bus_idx = pp.get_element_index(self.circuit_model.net, "bus", row["bus"])
            if self.circuit_model.balanced_load:
                pp.create_load(self.circuit_model.net, bus_idx, p_mw=row['p_mw'], q_mvar=row['q_mvar'], name=row['name'],
                               linked_equiptype=row['linked_equiptype'])
            else:
                pp.create_asymmetric_load(self.circuit_model.net, bus_idx, p_mw=row['p_mw'], q_mvar=row['q_mvar'], name=row['name'],
                                          linked_equiptype=row['linked_equiptype'],
                                          p_a_mw=row['p_a_mw'], q_a_mvar=row['q_a_mvar'],
                                          p_b_mw=row['p_b_mw'], q_b_mvar=row['q_b_mvar'],
                                          p_c_mw=row['p_c_mw'], q_c_mvar=row['q_c_mvar'], type='wye')

        if self.circuit_model.generator_enabled:
            for index, row in self.circuit_model.circuit_data['LOAD (GEN)'].iterrows():
                bus_idx = pp.get_element_index(self.circuit_model.net, "bus", row['bus'])
                if self.circuit_model.balanced_load:
                    pp.create_sgen(self.circuit_model.net, bus_idx, p_mw=row['p_mw'])
                else:
                    pp.create_asymmetric_sgen(self.circuit_model.net, bus_idx,
                                              p_a_mw=row['p_a_mw'], p_b_mw=row['p_b_mw'], p_c_mw=row['p_c_mw'])

                    # Run power flow
        if self.circuit_model.balanced_load:
            pp.runpp(self.circuit_model.net)
        else:
            pp.add_zero_impedance_parameters(self.circuit_model.net)
            self.circuit_model.net.trafo["vector_group"] = 'Dyn'
            self.circuit_model.net.trafo["vk0_percent"] = self.circuit_model.net.trafo["vk_percent"]
            self.circuit_model.net.trafo["vkr0_percent"] = self.circuit_model.net.trafo["vkr_percent"]
            self.circuit_model.net.trafo["mag0_percent"] = 100
            self.circuit_model.net.trafo["mag0_rx"] = 0
            self.circuit_model.net.trafo["si0_hv_partial"] = 0.9
            pp.runpp_3ph(self.circuit_model.net, calculate_voltage_angles=True, max_iteration=50)

        results = self._process_results(self.circuit_model.circuit_data, self.circuit_model.net, period)
        self.circuit_model.circuit_data['RESULT'] = results['SCE_OUTPUT']
        return self.circuit_model.circuit_data['RESULT']

    def _process_results(self, data, net, period):

        results = {}
        output = data['BUS'].merge(data['METADATA'], on='bus', how='left')
        load_output = data['LOAD'][['bus', 'reported_dttm']]
        if self.circuit_model.balanced_load:
            output = pd.concat([output, net.res_bus], axis=1)
        else:
            trafo_output = pd.concat([data['TRANSFORMER'], net.res_trafo, net.res_trafo_3ph], axis=1)
            load_output = pd.concat([load_output, net.res_asymmetric_load, net.res_asymmetric_load_3ph], axis=1)
            output = pd.concat([output, net.res_bus, net.res_bus_3ph], axis=1)

            # we need to get the current values separately for transformers, lines, switches, etc. lord help us
            line_output = pd.concat([data['LINE'], net.res_line, net.res_line_3ph], axis=1)
            # swith_output = pd.concat([data['SWITCH'], net.res_switch_3ph], axis=1)
            trafo_cols = {'i_a_lv_ka': 'i_a_ka', 'i_b_lv_ka': 'i_b_ka', 'i_c_lv_ka': 'i_c_ka', 'lv_bus': 'bus',
                          'i_lv_ka': 'i_ka'}
            trafo_output = trafo_output.rename(columns=trafo_cols)

            line_output = line_output.rename(columns={'from_bus': 'bus'})
            # switch_output = line_output.rename(columns={'from_bus': 'bus'})
            current_cols = ['bus', 'i_a_ka', 'i_b_ka', 'i_c_ka', 'i_ka']
            currents = pd.concat([df[current_cols] for df in [line_output, trafo_output]])
            currents = currents.groupby('bus').first().reset_index()
            results['CURRENT'] = currents
            output = output.merge(currents, on='bus', how='left')
        results["OUTPUT"] = deepcopy(output)

        col_map = {
            "vn_kv": "SV_VOLTAGE_V_VALUE_KVLL",
            "bus": "CONNECTIVITY_NODEID",
            "va_degree": "SV_VOLTAGE_ANGLE",
            "vm_pu": "SV_VOLTAGE_V_PU_VALUE",
            "name": "CONDUCTING_EQUIPMENTID",
            "p_mw": "SV_POWERFLOW_P_MW",
            "q_mvar": "SV_POWERFLOW_Q_MVAR",
            "i_ka": "SV_CURRENT_CURRENT_VALUE",
            "linked_equiptype": "LINKED_EQUIPTYPE"}

        for x in ['A', 'B', 'C']:
            col_map['p_{}_mw'.format(x.lower())] = 'SV_POWERFLOW_P_{}_MW'.format(x)
            col_map['q_{}_mvar'.format(x.lower())] = 'SV_POWERFLOW_Q_{}_MVAR'.format(x)
            col_map['vm_{}_pu'.format(x.lower())] = 'SV_VOLTAGE_V_{}_PU_VALUE'.format(x)
            col_map['va_{}_degree'.format(x.lower())] = 'SV_VOLTAGE_{}_ANGLE'.format(x)
            col_map['i_{}_ka'.format(x.lower())] = 'SV_CURRENT_{}_CURRENT_VALUE'.format(x)

        # null fields for now
        for nf in ['SVNINJECTION_PINJECTION_MW', 'SVINJECTION_QINJECTION_MVAR',
                   'SV_POWERFLOW_PCAP_MW', 'SV_POWERFLOW_QCAP_MVAR',
                   'SV_POWERFLOW_PMOTOR_MW', 'SV_POWERFLOW_QMOTOR_MVAR']:
            output[nf] = None
        # these are ones I havent handled yet.
        for f in ['SV_CURRENT_ANGLE', 'SV_CURRENT_A_ANGLE', 'SV_CURRENT_B_ANGLE',
                  'SV_CURRENT_C_ANGLE']:
            output[f] = None

        ts = datetime.now()
        import numpy as np
        def to_datetime(date):
            timestamp = ((date - np.datetime64('1970-01-01T00:00:00'))
                         / np.timedelta64(1, 's'))
            return datetime.utcfromtimestamp(timestamp)

        pdt = to_datetime(period)
        output["REPORTED_DTTM"] = pdt
        output["REPORTED_DT"] = pdt
        output["HOUR_ID"] = pdt.hour
        output["YEAR_ID"] = pdt.year
        output["MONTH_ID"] = pdt.month
        output["FEEDER_MRID"] = data['METADATA']['feeder_mrid'].values[0]
        # NEED TO PULLTHE CIRCUIT HEAD VN_KV
        circuit_head = data['EXT_GRID']['bus'][0]
        bus_table = data['BUS']
        output["BASEVOLTAGE_VALUE_KVLL"] = bus_table['vn_kv'].values[0]  # * data['EXT_GRID']['vm_pu']
        output["BASEVOLTAGE_VALUE_KVLL"] = data['BUS'][data['BUS']['bus'] == circuit_head]['vn_kv'].values[0]
        output["BASEVOLTAGE_VALUE_KVLL"] = output['BASEVOLTAGE_VALUE_KVLL'].map(lambda x: Decimal("{:.6f}".format(x)))

        output.rename(columns=col_map, inplace=True)

        # TODO: it would be cool to automatically do the appropriate conversions based on the schema, but ah well.
        # it is possible that not all fields will have been created; so fill in any stragglers
        for c in [a for a in OUTPUT_COLUMNS if a not in output.columns]:
            output[c] = None

        output["SV_VOLTAGE_V_VALUE_KVLL"] = output['SV_VOLTAGE_V_PU_VALUE'].map(lambda x: Decimal("{:.6f}".format(x))) * \
                                            output['BASEVOLTAGE_VALUE_KVLL']
        if self.circuit_model.balanced_load is False:
            output["SV_VOLTAGE_V_A_VALUE_KVLL"] = output['SV_VOLTAGE_V_A_PU_VALUE'].map(
                lambda x: Decimal("{:.6f}".format(x))) * output['BASEVOLTAGE_VALUE_KVLL']
            output["SV_VOLTAGE_V_B_VALUE_KVLL"] = output['SV_VOLTAGE_V_B_PU_VALUE'].map(
                lambda x: Decimal("{:.6f}".format(x))) * output['BASEVOLTAGE_VALUE_KVLL']
            output["SV_VOLTAGE_V_C_VALUE_KVLL"] = output['SV_VOLTAGE_V_C_PU_VALUE'].map(
                lambda x: Decimal("{:.6f}".format(x))) * output['BASEVOLTAGE_VALUE_KVLL']

        results['SCE_OUTPUT'] = output[OUTPUT_COLUMNS]
        return results

    def save_to_excel(self, file_path):
        # this outputs the built-in to_excel serializtion that pandapower offerd
        pp.to_excel(self.circuit_model.net, file_path.replace('.', '_pp.'))

        # this is our spreadsheet for holding the data from the input queries and the specifically formatted output query.
        with pd.ExcelWriter(file_path) as writer:
            # Save circuit data
            for sheet_name, df in self.circuit_model.circuit_data.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)

            # Save diagnostic results
            if self.diagnostic_results:
                dfs = self.diagnostic_to_dataframes()
                for sheet_name, df in dfs.items():
                    df.to_excel(writer, sheet_name=sheet_name, index=False)

            # Save other attributes as needed
            pd.DataFrame([{
                'circuit_key': self.circuit_key,
                'capacitor_enabled': self.capacitor_enabled,
                'transformer_enabled': self.transformer_enabled,
                # Add other attributes as needed
            }]).to_excel(writer, sheet_name='Config', index=False)