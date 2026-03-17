"""Best-effort CYME short-circuit workflow wrapper.

CYME short-circuit automation is more version-sensitive than the notebook-based
load-flow workflow already present in this repository. This wrapper therefore
keeps the interface in place, probes the ``cympy`` simulator for a compatible
short-circuit object, and returns the available node-level results when the
target CYME build exposes them.
"""

from __future__ import annotations

import pandas as pd

from cyme._common import close_study, ensure_study, query_node_float, query_node_text


FAULT_NAME_MAP = {
    "3ph": "ThreePhase",
    "lll": "ThreePhase",
    "1ph": "SingleLineToGround",
    "lg": "SingleLineToGround",
    "slg": "SingleLineToGround",
    "ll": "LineToLine",
    "2ph": "LineToLine",
    "llg": "DoubleLineToGround",
}


def _create_short_circuit_sim(cympy):
    for class_name in ("ShortCircuit", "ShortCircuitDuty", "FaultStudy"):
        simulator_class = getattr(cympy.sim, class_name, None)
        if simulator_class is not None:
            return simulator_class(), class_name
    raise RuntimeError(
        "The installed cympy build does not expose a recognized short-circuit "
        "simulation class. Update the candidate class names in cyme/scd/short_circuit.py "
        "for your CYME version."
    )


def _set_first_path(target, value, paths):
    for path in paths:
        try:
            target.SetValue(value, path)
            return path
        except Exception:
            continue
    return None


def calc_sc(
    study_file_or_module,
    bus=None,
    fault: str = "3ph",
    r_fault_ohm: float = 0.0,
    x_fault_ohm: float = 0.0,
    cyme_python_path: str | None = None,
    close_on_finish: bool = True,
):
    cympy, opened_here, _ = ensure_study(study_file_or_module, cyme_python_path)
    try:
        sc, simulator_name = _create_short_circuit_sim(cympy)

        fault_name = FAULT_NAME_MAP.get(fault.lower().strip(), fault)
        _set_first_path(sc, fault_name, (
            "ParametersConfigurations[0].FaultType",
            "ParametersConfigurations[0].FaultedConnectionType",
        ))
        _set_first_path(sc, r_fault_ohm, (
            "ParametersConfigurations[0].FaultResistance",
            "ParametersConfigurations[0].FaultResistanceOhm",
        ))
        _set_first_path(sc, x_fault_ohm, (
            "ParametersConfigurations[0].FaultReactance",
            "ParametersConfigurations[0].FaultReactanceOhm",
        ))
        if isinstance(bus, str):
            _set_first_path(sc, bus, (
                "ParametersConfigurations[0].FaultedNodeId",
                "ParametersConfigurations[0].FaultedNodeID",
            ))

        raw_result = sc.Run()

        target_buses = None
        if isinstance(bus, str):
            target_buses = {bus}
        elif bus is not None:
            target_buses = set(bus)

        rows = []
        for node in cympy.study.ListNodes(cympy.enums.NodeType.All):
            node_id = getattr(node, "ID", str(node))
            if target_buses is not None and node_id not in target_buses:
                continue

            ik_candidates = (
                query_node_float(cympy, "IfaultA", node_id),
                query_node_float(cympy, "ShortCircuitCurrentA", node_id),
                query_node_float(cympy, "AvailableFaultCurrentA", node_id),
            )
            ik_amp = next((value for value in ik_candidates if value is not None), None)
            z_candidates = (
                query_node_float(cympy, "TheveninImpedance", node_id),
                query_node_float(cympy, "Zth", node_id),
            )
            z_ohm = next((value for value in z_candidates if value is not None), None)

            rows.append({
                "bus": node_id,
                "kv_base": (query_node_float(cympy, "VBaseA", node_id) or 0.0) / 1000.0,
                "fault_type": fault,
                "ikss_ka": ik_amp / 1000.0 if ik_amp is not None else None,
                "i_fault_a": [value for value in ik_candidates if value is not None],
                "v_fault_pu": query_node_float(cympy, "VpuA", node_id),
                "z_ohm": z_ohm,
                "r_ohm": query_node_float(cympy, "Rth", node_id),
                "x_ohm": query_node_float(cympy, "Xth", node_id),
                "phase_labels": query_node_text(cympy, "Phase", node_id),
            })

        return {
            "res_bus_sc": pd.DataFrame(rows),
            "raw_result": raw_result,
            "simulator": simulator_name,
            "cympy": cympy,
        }
    finally:
        if opened_here and close_on_finish:
            close_study(cympy)
