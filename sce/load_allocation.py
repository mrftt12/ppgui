from datetime import datetime
import json
import logging
from powerflow_snowflake.load_allocation_wrapper import create_measurements_df
from sce.load_allocation_utils import get_asymmetric_load_by_phase
from powerflow_pipeline.powerflow import LoadAllocationMeasurement
import pandapower as pp
import multiconductor as mc
import pandas as pd
import numpy as np
from multiconductor.load_allocation.load_allocation import (
    build_measurement_graph,
    connected_capacity_allocation,
    get_rated_S,
    run_load_allocation,
)

logging.basicConfig(level=logging.INFO)

PHASE_MAP = {1: "A", 2: "B", 3: "C"}


def _phase_series(df):
    if "from_phase" in df.columns:
        return df["from_phase"]
    if isinstance(df.index, pd.MultiIndex):
        if "from_phase" in df.index.names:
            return df.index.get_level_values("from_phase")
        # fall back to last level if name missing
        return df.index.get_level_values(-1)
    return pd.Series(dtype=int)


def _sum_by_phase(df, value_col):
    if value_col not in df.columns:
        return {"A": np.nan, "B": np.nan, "C": np.nan}
    phases = _phase_series(df)
    if phases is None or len(phases) == 0:
        return {"A": np.nan, "B": np.nan, "C": np.nan}
    phase_vals = pd.to_numeric(phases, errors="coerce")
    if phase_vals.notna().any() and phase_vals.min() == 0 and phase_vals.max() <= 2:
        phase_vals = phase_vals + 1
    temp = pd.DataFrame(
        {
            "phase": phase_vals,
            "value": pd.to_numeric(df[value_col], errors="coerce"),
        }
    )
    grouped = temp.groupby("phase")["value"].sum()
    return {
        "A": float(grouped.get(1, np.nan)),
        "B": float(grouped.get(2, np.nan)),
        "C": float(grouped.get(3, np.nan)),
    }


def _imbalance_metrics(sums):
    vals = [v for v in sums.values() if pd.notna(v)]
    if len(vals) == 0:
        return {"avg": np.nan, "max_dev": np.nan, "imbalance_pct": np.nan}
    avg = float(np.mean(vals))
    max_dev = float(np.max([abs(v - avg) for v in vals]))
    imbalance_pct = float(max_dev / avg) if avg != 0 else np.nan
    return {"avg": avg, "max_dev": max_dev, "imbalance_pct": imbalance_pct}


def get_loading_by_phase(new_net):
    if (
        hasattr(new_net, "asymmetric_load")
        and new_net.asymmetric_load is not None
        and len(new_net.asymmetric_load) > 0
    ):
        load_df = new_net.asymmetric_load
        p_sums = _sum_by_phase(load_df, "p_mw")
        q_sums = _sum_by_phase(load_df, "q_mvar")

        p_imb = _imbalance_metrics(p_sums)
        q_imb = _imbalance_metrics(q_sums)

        p_total = float(np.nansum([p_sums["A"], p_sums["B"], p_sums["C"]]))
        q_total = float(np.nansum([q_sums["A"], q_sums["B"], q_sums["C"]]))
        summary = {
            "P_MW_TOTAL": p_total,
            "Q_MW_TOTAL": q_total,
            "P_A_MW": p_sums["A"],
            "P_B_MW": p_sums["B"],
            "P_C_MW": p_sums["C"],
            "Q_A_MVAR": q_sums["A"],
            "Q_B_MVAR": q_sums["B"],
            "Q_C_MVAR": q_sums["C"],
            "P_avg_MW": p_imb["avg"],
            "P_max_dev_MW": p_imb["max_dev"],
            "P_imbalance_pct": p_imb["imbalance_pct"],
            "Q_avg_MVAR": q_imb["avg"],
            "Q_max_dev_MVAR": q_imb["max_dev"],
            "Q_imbalance_pct": q_imb["imbalance_pct"],
        }
        # imbalance_df = pd.DataFrame([summary])
        return summary
    else:
        print("No asymmetric_load data found for phase sums/imbalance.")
        return {}


class SCELoadAllocationMeasurement(LoadAllocationMeasurement):
    def __init__(self):
        self.logger = logging.getLogger("SCELoadAllocationMeasurement_Logger")

    def run_ami_load_allocation(self, net: pp.pandapowerNet, profile_data: any) -> any:
        for reading in [r for r in profile_data["READINGS"] if r['RESOURCE_TYPE'] == 'STRUCTURE']:
            load_name = reading["NAME"]
            measurement_value = reading["MEASURE_VALUE"]

            bus = net.asymmetric_load[net.asymmetric_load['name'] == load_name].bus
            bus_id = bus.values[0]
            xfmr = net.trafo1ph[net.trafo1ph.index.get_level_values('bus') == bus_id]
            xfmr_index = xfmr.index.get_level_values(0).unique()
            for meas_idx, tidx in enumerate(xfmr_index):
                mc.create_measurement(
                            net,
                            measurement_type="p",
                            element_type="trafo1ph",
                            element=int(tidx),
                            value=measurement_value,
                            std_dev=0.01,
                            side="lv",
                            index=meas_idx,
                            name=f"P_trafo_{tidx}_lv",
                        )       

        mg = build_measurement_graph(net)
        meas_order = list(net.measurement.index)
        run_load_allocation(
            net, 
            mg,
            adjust_after_load_flow=False,
            tolerance=0.5,
            measurement_indices=meas_order,
            cap_to_load_rating=False,
            cap_to_transformer_rating=False,
            ignore_generators=False,
            verbose=False,
            max_iter=10,
        )
        asymmetric_load = get_asymmetric_load_by_phase(net)
        tmp_df = pd.DataFrame(profile_data['READINGS'], columns=['NAME', 'CONDUCTING_EQUIPMENTID'])

        merged_df = pd.merge(asymmetric_load, tmp_df, left_on='name', right_on='NAME', how='left')
        
        merged_df.rename(columns={'CONDUCTING_EQUIPMENTID': 'POWER_SYSTEM_RESOURCE_MRID'}, inplace=True)
        load_alloc_results = []
        extra_metadata = json.loads(profile_data["EXTRA_METADATA"])

        dt = pd.to_datetime(profile_data["REPORTED_DTTM"])

        for index, row in merged_df.iterrows():
            result_row = {}
            result_row['POWER_SYSTEM_RESOURCE_MRID'] = str(row['POWER_SYSTEM_RESOURCE_MRID'])
            result_row['REPORTED_DTTM'] = profile_data['REPORTED_DTTM']
            result_row["EDW_CREATED_DATE"] = datetime.strptime(extra_metadata["EDW_CREATED_DATE"], '%Y-%m-%dT%H:%M:%S.%fZ')
            result_row["EDW_MODIFIED_DATE"] = datetime.strptime(extra_metadata["EDW_MODIFIED_DATE"], '%Y-%m-%dT%H:%M:%S.%fZ')
            result_row["EDW_CREATED_BY"] = extra_metadata["EDW_CREATED_BY"]
            result_row["EDW_MODIFIED_BY"] = extra_metadata["EDW_MODIFIED_BY"]

            result_row['MEASURE_VALUE_A'] = row['p_mw_A']
            result_row['MEASURE_VALUE_B'] = row['p_mw_B']
            result_row['MEASURE_VALUE_C'] = row['p_mw_C']
            result_row['MEASURE_VALUE'] = row['p_mw_A'] + row['p_mw_B'] + row['p_mw_C']

            result_row['RDNG_MEAS_MVAR_A'] = row['q_mvar_A']
            result_row['RDNG_MEAS_MVAR_B'] = row['q_mvar_B']
            result_row['RDNG_MEAS_MVAR_C'] = row['q_mvar_C']
            result_row['RDNG_MEAS_MVAR'] = row['q_mvar_A'] + row['q_mvar_B'] + row['q_mvar_C']

            result_row['POWER_SYSTEM_RESOURCE_KEY'] = profile_data['CIRCUIT_KEY']
            result_row['YEAR_ID'] = profile_data['YEAR_ID']
            result_row['MONTH_ID'] = profile_data['MONTH_ID']
            result_row['PROFILE_STATE'] = 'EDITED'
            result_row['POWER_SYSTEM_RESOURCE_TYPE'] = 'LOAD'
            result_row['PROFILE_TYPE'] = 'P_Q'
            result_row['MEASUREMENT_TYPE'] = 'GROSS'
            result_row['UNIT_SYMBOL'] = 'MW-MVAr'
            result_row['REPORTED_DT'] = dt
            result_row['POWER_SYSTEM_RESOURCE_NUM'] = row['NAME']
            
            result_row['EDW_BATCH_ID'] = extra_metadata['EDW_BATCH_ID']
            result_row['EDW_BATCH_DETAIL_ID'] = extra_metadata['EDW_BATCH_DETAIL_ID']
            result_row['EDW_LAST_DML_CD'] = extra_metadata['EDW_LAST_DML_CD']

            load_alloc_results.append(result_row)

        return load_alloc_results
        
    def run_cc_allocation(self, net: pp.pandapowerNet, measurement_data: any) -> any:
        ret = []
        extra_metadata = json.loads(measurement_data["EXTRA_METADATA"])
        for data in measurement_data["READINGS"]:
            resource_type = data["RESOURCE_TYPE"]
            # unit_symbol = data["UNIT_SYMBOL"]
            if True:
                match resource_type:
                    case "CIRCUIT":
                        p_mw = data["MEASURE_VALUE"]
                        res: list = self.cc_load_allocation(net, p_mw)
                        for item in res:
                            for param in [
                                "POWER_SYSTEM_RESOURCE_MRID",
                                "POWER_SYSTEM_RESOURCE_KEY",
                                "REPORTED_DTTM",
                                "YEAR_ID",
                                "MONTH_ID",
                            ]:
                                item[param] = measurement_data[param]
                            for param in [
                                "POWER_SYSTEM_RESOURCE_TYPE",
                                "UNIT_SYMBOL",
                                "PROFILE_TYPE",
                                "REPORTED_DT",
                                "POWER_SYSTEM_RESOURCE_NUM",
                            ]:
                                item[param] = data[param] 

                            # TODO: ask SCE to make the column wider
                            item['MEASUREMENT_TYPE'] = data[param][:5]   

                            item["PROFILE_STATE"] = "EDITED"

                            item["EDW_CREATED_DATE"] = datetime.strptime(
                                extra_metadata["EDW_CREATED_DATE"],
                                "%Y-%m-%dT%H:%M:%S.%fZ",
                            )
                            item["EDW_MODIFIED_DATE"] = datetime.strptime(
                                extra_metadata["EDW_MODIFIED_DATE"],
                                "%Y-%m-%dT%H:%M:%S.%fZ",
                            )
                            item["EDW_CREATED_BY"] = extra_metadata["EDW_CREATED_BY"]
                            item["EDW_MODIFIED_BY"] = extra_metadata["EDW_MODIFIED_BY"]

                            item["EDW_BATCH_ID"] = extra_metadata["EWD_BATCH_ID"]
                            item["EDW_BATCH_DETAIL_ID"] = extra_metadata[
                                "EWD_BATCH_DETAIL_ID"
                            ]
                            item["EDW_LAST_DML_CD"] = extra_metadata["EDW_LAST_DML_CD"]
                        ret.extend(res)

        return ret

    def add_trafo_p_measurement_value(
        self,
        net,
        trafo_idx,
        side="hv",
        value_mw=0.0,
        meas_index=None,
        std_dev=0.01,
        name=None,
    ):
        mc.create_measurement(
            net,
            measurement_type="p",
            element_type="trafo1ph",
            element=trafo_idx,
            value=float(value_mw),
            std_dev=std_dev,
            side=side,
            index=meas_index,
            name=name,
        )

    def add_ext_grid_p_measurement_value(
        self, net, element, value_mw, meas_index, name
    ):
        """
        Create a P measurement at an ext_grid element.
        'element' = ext_grid index (integer)
        """
        if "measurement" not in net:
            raise RuntimeError("net.measurement table missing")

        net.measurement.loc[
            meas_index,
            ["element_type", "element", "measurement_type", "side", "value", "name"],
        ] = ["ext_grid", element, "p", "from", float(value_mw), name]

    def add_line_p_measurement_value(
        self,
        net,
        line_idx,
        value_mw,
        side="from",
        meas_index=None,
        std_dev=0.01,
        name=None,
    ):

        mc.create_measurement(
            net,
            measurement_type="p",
            element_type="line",
            element=line_idx,
            value=float(value_mw),
            std_dev=std_dev,
            side=side,
            name=name,
            index=meas_index,
        )

    def cc_load_allocation(self, net, p_mw):
        orig_index = net.asymmetric_load.index
        net.asymmetric_load = net.asymmetric_load.reset_index(drop=True)

        mg = build_measurement_graph(net)
        load_indices = list(net.asymmetric_load.index)
        rated_S = get_rated_S(net, mg, load_indices, include_trafo=True)

        connected_capacity_allocation(net, load_indices, rated_S, p_mw)
        res: dict = get_loading_by_phase(net)
        ret = []
        p_q_values = {}
        p_q_values["MEASURE_VALUE"] = res["P_MW_TOTAL"]
        p_q_values["MEASURE_VALUE_A"] = res["P_A_MW"]
        p_q_values["MEASURE_VALUE_B"] = res["P_B_MW"]
        p_q_values["MEASURE_VALUE_C"] = res["P_C_MW"]
        #
        p_q_values["RDNG_MEAS_MVAR"] = res["Q_MW_TOTAL"]
        p_q_values["RDNG_MEAS_MVAR_A"] = res["Q_A_MVAR"]
        p_q_values["RDNG_MEAS_MVAR_B"] = res["Q_B_MVAR"]
        p_q_values["RDNG_MEAS_MVAR_C"] = res["Q_C_MVAR"]

        ret.append(p_q_values)

        # Restore original MultiIndex so downstream code (e.g. run_pf) works
        net.asymmetric_load.index = orig_index

        return ret
