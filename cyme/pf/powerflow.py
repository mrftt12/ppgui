"""CYME power-flow workflow wrapper built from the root CYME notebook."""

from __future__ import annotations

from typing import Any

from cyme._common import (
    build_phase_vectors,
    close_study,
    dataframe_or_empty,
    ensure_study,
    first_available_device_text,
    get_device_name,
    get_device_number,
    list_devices,
    query_device_float,
    query_device_text,
    query_node_float,
    query_node_text,
    run_load_flow,
)


def _bus_results(cympy):
    rows = []
    node_type = cympy.enums.NodeType.All
    for node in cympy.study.ListNodes(node_type):
        node_id = getattr(node, "ID", str(node))
        phase_text = query_node_text(cympy, "Phase", node_id) or ""
        vm_by_phase = {
            phase: query_node_float(cympy, f"Vpu{phase}", node_id)
            for phase in ("A", "B", "C")
        }
        va_by_phase = {
            phase: query_node_float(cympy, f"VAngle{phase}", node_id)
            for phase in ("A", "B", "C")
        }
        vbase_by_phase = {
            phase: query_node_float(cympy, f"VBase{phase}", node_id)
            for phase in ("A", "B", "C")
        }

        vm_pu_per_phase = build_phase_vectors(vm_by_phase)
        va_degree_per_phase = build_phase_vectors(va_by_phase)
        kv_candidates = [value / 1000.0 for value in vbase_by_phase.values() if value is not None]
        vm_pu = max(vm_pu_per_phase) if vm_pu_per_phase else 0.0
        va_degree = 0.0
        if vm_pu_per_phase and va_degree_per_phase:
            va_degree = va_degree_per_phase[vm_pu_per_phase.index(vm_pu)]

        rows.append({
            "bus": node_id,
            "kv_base": max(kv_candidates) if kv_candidates else None,
            "num_phases": len(vm_pu_per_phase),
            "phase_labels": phase_text,
            "vm_pu": vm_pu,
            "va_degree": va_degree,
            "vm_pu_per_phase": vm_pu_per_phase,
            "va_degree_per_phase": va_degree_per_phase,
        })

    return dataframe_or_empty(
        rows,
        [
            "bus",
            "kv_base",
            "num_phases",
            "phase_labels",
            "vm_pu",
            "va_degree",
            "vm_pu_per_phase",
            "va_degree_per_phase",
        ],
    )


def _line_results(cympy):
    rows = []
    type_specs = (("UnbalancedLine", 12), ("Switch", None))
    for device, device_type, type_name in list_devices(cympy, type_specs):
        device_id = get_device_number(device)
        currents = [
            query_device_float(cympy, f"I{phase}", device_id, device_type)
            for phase in ("A", "B", "C")
        ]
        currents = [value for value in currents if value is not None]
        rows.append({
            "line": get_device_name(device),
            "device_type": type_name,
            "phases": query_device_text(cympy, "Phase", device_id, device_type),
            "from_bus": first_available_device_text(
                cympy,
                ("FromNodeID", "FromNode", "Node1", "HeadNode"),
                device_id,
                device_type,
            ),
            "to_bus": first_available_device_text(
                cympy,
                ("ToNodeID", "ToNode", "Node2", "TailNode"),
                device_id,
                device_type,
            ),
            "i_from_a": currents,
            "i_to_a": [],
            "i_ka": max(currents) / 1000.0 if currents else 0.0,
            "p_from_kw": query_device_float(cympy, "KWTOT", device_id, device_type),
            "q_from_kvar": query_device_float(cympy, "KVARTOT", device_id, device_type),
            "p_to_kw": None,
            "q_to_kvar": None,
        })

    return dataframe_or_empty(
        rows,
        [
            "line",
            "device_type",
            "phases",
            "from_bus",
            "to_bus",
            "i_from_a",
            "i_to_a",
            "i_ka",
            "p_from_kw",
            "q_from_kvar",
            "p_to_kw",
            "q_to_kvar",
        ],
    )


def _transformer_results(cympy):
    rows = []
    type_specs = (("Transformer", 1), ("TransformerByPhase", 33), ("Regulator", None))
    for device, device_type, type_name in list_devices(cympy, type_specs):
        device_id = get_device_number(device)
        currents = [
            query_device_float(cympy, f"I{phase}", device_id, device_type)
            for phase in ("A", "B", "C")
        ]
        currents = [value for value in currents if value is not None]
        rows.append({
            "transformer": get_device_name(device),
            "device_type": type_name,
            "phases": query_device_text(cympy, "Phase", device_id, device_type),
            "p_hv_kw": query_device_float(cympy, "KWTOT", device_id, device_type),
            "q_hv_kvar": query_device_float(cympy, "KVARTOT", device_id, device_type),
            "p_lv_kw": None,
            "q_lv_kvar": None,
            "i_hv_ka": max(currents) / 1000.0 if currents else 0.0,
        })

    return dataframe_or_empty(
        rows,
        [
            "transformer",
            "device_type",
            "phases",
            "p_hv_kw",
            "q_hv_kvar",
            "p_lv_kw",
            "q_lv_kvar",
            "i_hv_ka",
        ],
    )


def run_pf(
    study_file_or_module: Any,
    disable_active_controls: bool = True,
    voltage_tolerance: float = 0.001,
    analysis_mode: str = "VoltageDropUnbalanced",
    cyme_python_path: str | None = None,
    close_on_finish: bool = True,
):
    """Run a CYME load flow on an ``.sxst`` or ``.sxrt`` study.

    Parameters mirror the notebook configuration rather than exposing the full
    CYME load-flow parameter tree. Additional tuning can be added later once
    a target CYME version is fixed.
    """
    cympy, opened_here, study_file = ensure_study(study_file_or_module, cyme_python_path)
    try:
        _, converged, raw_result = run_load_flow(
            cympy,
            disable_active_controls=disable_active_controls,
            voltage_tolerance=voltage_tolerance,
            analysis_mode=analysis_mode,
        )

        return {
            "converged": bool(converged),
            "raw_result": raw_result,
            "res_bus": _bus_results(cympy),
            "res_line": _line_results(cympy),
            "res_trafo": _transformer_results(cympy),
            "study_file": study_file,
            "cympy": cympy,
        }
    finally:
        if opened_here and close_on_finish:
            close_study(cympy)
