"""CYME hosting-capacity workflow using temporary ECG injections."""

from __future__ import annotations

import math

import pandas as pd

from cyme._common import (
    close_study,
    ensure_study,
    query_node_text,
    run_load_flow,
    study_name_from_path,
)
from cyme.pf.powerflow import _bus_results


def _max_voltage(bus_df: pd.DataFrame) -> float:
    if bus_df.empty:
        return 0.0
    return float(bus_df["vm_pu"].max())


def _min_voltage(bus_df: pd.DataFrame) -> float:
    if bus_df.empty:
        return 999.0
    non_zero = bus_df.loc[bus_df["vm_pu"] > 0.01, "vm_pu"]
    if non_zero.empty:
        return 999.0
    return float(non_zero.min())


def _add_temp_generator(cympy, circuit_name: str, bus_row: pd.Series, kw: float, pf: float):
    dev_type = cympy.enums.DeviceType.ElectronicConverterGenerator
    node_id = bus_row["bus"]
    phase_labels = bus_row.get("phase_labels") or "ABC"
    phase_count = max(int(bus_row.get("num_phases") or len(phase_labels) or 1), 1)
    base_kv = float(bus_row.get("kv_base") or 0.12)
    sn_kva = abs(kw / pf) if pf not in (0, None) else abs(kw)
    dev_name = "HC_TEST"
    section_id = f"{dev_name}-G"

    cympy.study.AddSection(section_id, circuit_name, dev_name, dev_type, node_id)
    section = cympy.study.GetSection(section_id)
    if section is not None:
        section.SetValue(phase_labels, "Phase")

    device = cympy.study.GetDevice(dev_name, dev_type)
    device.SetValue("Connected", "ConnectionStatus")
    device.SetValue(float(kw), "GenerationModels.Get(1).ActiveGeneration")
    device.SetValue(float(-100.0 * pf), "GenerationModels.Get(1).PowerFactor")
    device.SetValue("SinglePhase" if phase_count == 1 else "ThreePhase", "Inverter.ACDCConverterSettings.Type")
    device.SetValue(float(sn_kva), "Inverter.ACDCConverterSettings.ConverterRating")
    device.SetValue(float(base_kv), "Inverter.ACDCConverterSettings.NominalACVoltage")
    device.SetValue(float(sn_kva), "Inverter.ACDCConverterSettings.ActivePowerRating")
    device.SetValue(float(sn_kva), "Inverter.ACDCConverterSettings.ReactivePowerRating")


def _evaluate_bus_capacity(
    study_file: str,
    bus_row: pd.Series,
    circuit_name: str,
    v_upper: float,
    v_lower: float,
    max_kw: float,
    tol_kw: float,
    pf: float,
    disable_active_controls: bool,
    cyme_python_path: str | None,
):
    lo = 0.0
    hi = float(max_kw)
    hc = 0.0
    binding = "none"

    while hi - lo > tol_kw:
        mid = (lo + hi) / 2.0
        cympy, opened_here, _ = ensure_study(study_file, cyme_python_path)
        try:
            _add_temp_generator(cympy, circuit_name, bus_row, mid, pf)
            _, converged, _ = run_load_flow(cympy, disable_active_controls=disable_active_controls)
            if not converged:
                hi = mid
                binding = "diverged"
                continue

            bus_df = _bus_results(cympy)
            vmax = _max_voltage(bus_df)
            vmin = _min_voltage(bus_df)

            if vmax > v_upper:
                hi = mid
                binding = "overvoltage"
            elif vmin < v_lower:
                hi = mid
                binding = "undervoltage"
            else:
                lo = mid
                hc = mid
                binding = "none"
        finally:
            if opened_here:
                close_study(cympy)

    return round(hc, 1), binding


def run_hosting_capacity(
    study_file,
    v_upper: float = 1.05,
    v_lower: float = 0.95,
    max_kw: float = 5000.0,
    tol_kw: float = 100.0,
    pf: float = 1.0,
    exclude_slack: bool = True,
    verbose: bool = True,
    buses=None,
    disable_active_controls: bool = True,
    cyme_python_path: str | None = None,
):
    """Binary-search hosting capacity using temporary CYME ECG devices.

    This CYME implementation mirrors the OpenDSS workflow structure and checks
    voltage constraints directly. Thermal constraints can be added later once a
    stable CYME query surface is confirmed for the target version.
    """
    study_file = str(study_file)
    circuit_name = study_name_from_path(study_file)

    cympy, opened_here, _ = ensure_study(study_file, cyme_python_path)
    try:
        _, converged, _ = run_load_flow(cympy, disable_active_controls=disable_active_controls)
        if not converged:
            raise RuntimeError("Base-case CYME load flow did not converge")
        bus_df = _bus_results(cympy)
    finally:
        if opened_here:
            close_study(cympy)

    if exclude_slack and not bus_df.empty:
        slack_mask = bus_df["bus"].astype(str).str.lower() == circuit_name.lower()
        if slack_mask.any():
            bus_df = bus_df.loc[~slack_mask].reset_index(drop=True)
        else:
            bus_df = bus_df.iloc[1:].reset_index(drop=True)

    if buses is not None:
        requested = {str(bus_name).lower() for bus_name in buses}
        bus_df = bus_df.loc[bus_df["bus"].astype(str).str.lower().isin(requested)].reset_index(drop=True)

    results = []
    total = len(bus_df)
    for idx, (_, row) in enumerate(bus_df.iterrows(), start=1):
        hc_kw, binding = _evaluate_bus_capacity(
            study_file=study_file,
            bus_row=row,
            circuit_name=circuit_name,
            v_upper=v_upper,
            v_lower=v_lower,
            max_kw=max_kw,
            tol_kw=tol_kw,
            pf=pf,
            disable_active_controls=disable_active_controls,
            cyme_python_path=cyme_python_path,
        )
        results.append({
            "bus": row["bus"],
            "kv_base": row["kv_base"],
            "num_phases": row["num_phases"],
            "base_vpu": row["vm_pu"],
            "hosting_capacity_kw": hc_kw,
            "binding_constraint": binding,
        })
        if verbose and ((idx % 100 == 0) or idx == total):
            print(f"  {idx}/{total} buses processed")

    return {"res_hc": pd.DataFrame(results)}
