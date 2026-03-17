"""CYME load-allocation workflow for SpotLoad devices."""

from __future__ import annotations

from cyme._common import (
    close_study,
    ensure_study,
    get_device_name,
    get_value_if_possible,
    list_devices,
    query_device_float,
    run_load_flow,
    set_value_if_possible,
)
from cyme.pf.powerflow import _bus_results, _line_results, _transformer_results


LOAD_VALUE_PATHS = tuple(
    (
        f"CustomerLoads[0].CustomerLoadModels[0].CustomerLoadValues[{idx}].LoadValue.KW",
        f"CustomerLoads[0].CustomerLoadModels[0].CustomerLoadValues[{idx}].LoadValue.KVAR",
    )
    for idx in range(3)
)


def _capture_spot_loads(cympy):
    loads = []
    for device, device_type, _ in list_devices(cympy, (("SpotLoad", None),)):
        values = []
        for kw_path, kvar_path in LOAD_VALUE_PATHS:
            kw = get_value_if_possible(device, kw_path)
            kvar = get_value_if_possible(device, kvar_path)
            if kw is None and kvar is None:
                continue
            values.append(
                {
                    "kw_path": kw_path,
                    "kvar_path": kvar_path,
                    "kw": float(kw or 0.0),
                    "kvar": float(kvar or 0.0),
                }
            )
        if values:
            loads.append({
                "device": device,
                "device_type": device_type,
                "name": get_device_name(device),
                "values": values,
            })
    return loads


def _set_scaled_loads(loads, kw_scale: float, kvar_scale: float | None = None):
    kvar_scale = kw_scale if kvar_scale is None else kvar_scale
    for load in loads:
        device = load["device"]
        for entry in load["values"]:
            set_value_if_possible(device, entry["kw"] * kw_scale, entry["kw_path"])
            set_value_if_possible(device, entry["kvar"] * kvar_scale, entry["kvar_path"])


def _get_total_load_power(cympy, loads):
    total_kw = 0.0
    total_kvar = 0.0
    for load in loads:
        device = load["device"]
        device_number = getattr(device, "DeviceNumber", load["name"])
        total_kw += query_device_float(cympy, "KWTOT", device_number, load["device_type"]) or 0.0
        total_kvar += query_device_float(cympy, "KVARTOT", device_number, load["device_type"]) or 0.0
    return total_kw, total_kvar


def run_load_allocation(
    study_file_or_module,
    target_kw: float | None = None,
    target_kvar: float | None = None,
    tolerance_kw: float = 0.5,
    max_iter: int = 15,
    adjust_power_factor: bool = True,
    disable_active_controls: bool = True,
    cyme_python_path: str | None = None,
    close_on_finish: bool = True,
):
    """Scale CYME SpotLoad models until total load matches the requested target.

    This mirrors the OpenDSS workflow shape, but on CYME studies the target is
    matched against the aggregate SpotLoad demand because the root notebook only
    established a load-flow and result-query pattern, not a feeder-head meter
    query.
    """
    if target_kw is None:
        raise ValueError("target_kw is required for load allocation")

    cympy, opened_here, _ = ensure_study(study_file_or_module, cyme_python_path)
    try:
        loads = _capture_spot_loads(cympy)
        if not loads:
            raise RuntimeError("No CYME SpotLoad devices were found in the study")

        cumulative_scale = 1.0
        kvar_scale = None

        for iteration in range(1, max_iter + 1):
            run_load_flow(cympy, disable_active_controls=disable_active_controls)
            p_total, q_total = _get_total_load_power(cympy, loads)
            mismatch = target_kw - p_total

            if abs(mismatch) <= tolerance_kw:
                break

            if abs(p_total) > 1e-6:
                cumulative_scale *= target_kw / p_total
            else:
                cumulative_scale *= 2.0

            if adjust_power_factor and target_kvar is not None and abs(q_total) > 1e-6:
                kvar_scale = target_kvar / q_total

            _set_scaled_loads(loads, cumulative_scale, kvar_scale)
        else:
            iteration = max_iter

        run_load_flow(cympy, disable_active_controls=disable_active_controls)
        p_final, _ = _get_total_load_power(cympy, loads)

        return {
            "converged": abs(target_kw - p_final) <= tolerance_kw,
            "iterations": iteration,
            "mismatch_kw": target_kw - p_final,
            "scale_factor": cumulative_scale,
            "res_bus": _bus_results(cympy),
            "res_line": _line_results(cympy),
            "res_trafo": _transformer_results(cympy),
            "cympy": cympy,
        }
    finally:
        if opened_here and close_on_finish:
            close_study(cympy)
