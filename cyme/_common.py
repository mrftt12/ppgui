"""Shared helpers for CYME workflow wrappers.

The root notebooks in this repository demonstrate a consistent CYME load-flow
setup using ``cympy``. This module lifts that setup into reusable helpers while
keeping imports lazy so the package can still be imported on machines where
CYME is not installed.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


DEFAULT_CYME_PYTHON_PATHS = (
    os.environ.get("CYME_PYTHON_PATH"),
    r"C:\CYME\CYME",
)


def _prepend_cyme_path(cyme_python_path: str | None = None) -> None:
    candidates = []
    if cyme_python_path:
        candidates.append(cyme_python_path)
    candidates.extend(path for path in DEFAULT_CYME_PYTHON_PATHS if path)

    for candidate in candidates:
        if candidate and candidate not in sys.path:
            sys.path.insert(0, candidate)
            break


def import_cympy(cyme_python_path: str | None = None):
    _prepend_cyme_path(cyme_python_path)
    try:
        import cympy  # type: ignore
    except ImportError as exc:
        raise ImportError(
            "cympy is required for the cyme package. Set CYME_PYTHON_PATH or "
            "pass cyme_python_path to point at the CYME Python installation."
        ) from exc
    return cympy


def ensure_study(study_file_or_module: Any, cyme_python_path: str | None = None):
    """Return a ready cympy module and whether this call opened the study."""
    cympy = import_cympy(cyme_python_path)

    if hasattr(study_file_or_module, "study") and hasattr(study_file_or_module, "sim"):
        return study_file_or_module, False, None

    if isinstance(study_file_or_module, (str, os.PathLike)):
        study_file = os.fspath(study_file_or_module)
        cympy.study.Open(study_file)
        return cympy, True, study_file

    raise TypeError(
        "study_file_or_module must be a CYME study path or an imported cympy module"
    )


def close_study(cympy, ask_for_save: bool = False) -> None:
    try:
        cympy.study.Close(AskForSave=ask_for_save)
    except Exception:
        pass


def set_value_if_possible(target: Any, value: Any, path: str) -> bool:
    try:
        target.SetValue(value, path)
        return True
    except Exception:
        return False


def get_value_if_possible(target: Any, path: str) -> Any:
    try:
        return target.GetValue(path)
    except Exception:
        return None


def safe_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        value = stripped
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def query_node(cympy, key: str, node_id: str, precision: int | None = None) -> Any:
    if precision is None:
        return cympy.study.QueryInfoNode(key, node_id)
    return cympy.study.QueryInfoNode(key, node_id, precision)


def query_node_float(cympy, key: str, node_id: str, precision: int = 7) -> float | None:
    try:
        return safe_float(query_node(cympy, key, node_id, precision))
    except Exception:
        return None


def query_node_text(cympy, key: str, node_id: str) -> str | None:
    try:
        value = query_node(cympy, key, node_id)
    except Exception:
        return None
    if value is None:
        return None
    return str(value)


def query_device(cympy, key: str, device_id: Any, device_type: Any, precision: int | None = None) -> Any:
    if precision is None:
        return cympy.study.QueryInfoDevice(key, device_id, device_type)
    return cympy.study.QueryInfoDevice(key, device_id, device_type, precision)


def query_device_float(
    cympy,
    key: str,
    device_id: Any,
    device_type: Any,
    precision: int = 6,
) -> float | None:
    try:
        return safe_float(query_device(cympy, key, device_id, device_type, precision))
    except Exception:
        return None


def query_device_text(cympy, key: str, device_id: Any, device_type: Any) -> str | None:
    try:
        value = query_device(cympy, key, device_id, device_type)
    except Exception:
        return None
    if value is None:
        return None
    return str(value)


def first_available_device_text(cympy, keys: Iterable[str], device_id: Any, device_type: Any) -> str | None:
    for key in keys:
        value = query_device_text(cympy, key, device_id, device_type)
        if value not in (None, ""):
            return value
    return None


def first_available_device_float(
    cympy,
    keys: Iterable[str],
    device_id: Any,
    device_type: Any,
    precision: int = 6,
) -> float | None:
    for key in keys:
        value = query_device_float(cympy, key, device_id, device_type, precision=precision)
        if value is not None:
            return value
    return None


def get_device_number(device: Any) -> Any:
    for attr in ("DeviceNumber", "ID", "Name"):
        if hasattr(device, attr):
            return getattr(device, attr)
    return device


def get_device_name(device: Any) -> str:
    for attr in ("Name", "ID", "DeviceNumber"):
        if hasattr(device, attr):
            return str(getattr(device, attr))
    return str(device)


def get_enum_value(cympy, enum_name: str, fallback: Any = None) -> Any:
    try:
        return getattr(cympy.enums.DeviceType, enum_name)
    except Exception:
        return fallback


def list_devices(cympy, type_specs: Iterable[tuple[str, Any]]):
    """Yield ``(device, device_type, type_name)`` tuples for known CYME types."""
    seen: set[tuple[Any, Any]] = set()
    for type_name, fallback in type_specs:
        device_type = get_enum_value(cympy, type_name, fallback)
        if device_type is None:
            continue
        try:
            devices = cympy.study.ListDevices(device_type)
        except Exception:
            continue
        for device in devices or []:
            key = (get_device_number(device), device_type)
            if key in seen:
                continue
            seen.add(key)
            yield device, device_type, type_name


def configure_load_flow(
    lf: Any,
    disable_active_controls: bool = True,
    voltage_tolerance: float = 0.001,
    analysis_mode: str = "VoltageDropUnbalanced",
) -> None:
    """Apply the notebook-derived CYME load-flow setup."""
    settings = {
        "ParametersConfigurations[0].VoltageTolerance": voltage_tolerance,
        "ParametersConfigurations[0].AnalysisMode": analysis_mode,
        "ParametersConfigurations[0].IncludeSourceImpedance": 0,
        "ParametersConfigurations[0].IncludeLineCharging": 0,
        "ParametersConfigurations[0].AssumeLineTransposition": 0,
        "ParametersConfigurations[0].TemperatureAdjustment.EnableTemperatureAdjustment": 0,
        "ParametersConfigurations[0].LoadFlowVoltageSensitivityLoadModel.Mode": "Global",
        "ParametersConfigurations[0].LoadFlowVoltageSensitivityLoadModel.V": 0.0,
        "ParametersConfigurations[0].LockMultiStageSwitchableShunts": 1,
        "ParametersConfigurations[0].LockSwitchedCapacitors": 1,
        "ParametersConfigurations[0].EquipmentStatusParameters.FixedCapStatus": 1,
    }
    for path, value in settings.items():
        set_value_if_possible(lf, value, path)

    if not disable_active_controls:
        return

    off_paths = (
        "ParametersConfigurations[0].EquipmentStatusParameters.VoltageCapStatus",
        "ParametersConfigurations[0].EquipmentStatusParameters.CurrentCapStatus",
        "ParametersConfigurations[0].EquipmentStatusParameters.ReactiveCurrentCapStatus",
        "ParametersConfigurations[0].EquipmentStatusParameters.PowerFactorCapStatus",
        "ParametersConfigurations[0].EquipmentStatusParameters.TemperatureCapStatus",
        "ParametersConfigurations[0].EquipmentStatusParameters.TimeCapStatus",
        "ParametersConfigurations[0].EquipmentStatusParameters.KVARCapStatus",
        "ParametersConfigurations[0].EquipmentStatusParameters.PythonScriptCapStatus",
        "ParametersConfigurations[0].EquipmentStatusParameters.MultiStageSwitchableShuntStatus",
        "ParametersConfigurations[0].EquipmentStatusParameters.SVCStatus",
        "ParametersConfigurations[0].EquipmentStatusParameters.VARCompensatorStatus",
        "ParametersConfigurations[0].EquipmentStatusParameters.SynchronousMotorStatus",
        "ParametersConfigurations[0].EquipmentStatusParameters.InductionMotorStatus",
        "ParametersConfigurations[0].EquipmentStatusParameters.GeneratorStatus",
        "ParametersConfigurations[0].EquipmentStatusParameters.SynchronousGeneratorStatus",
        "ParametersConfigurations[0].EquipmentStatusParameters.InductionGeneratorStatus",
        "ParametersConfigurations[0].EquipmentStatusParameters.ElectronicallyCoupledGeneratorStatus",
        "ParametersConfigurations[0].EquipmentStatusParameters.WecsStatus",
        "ParametersConfigurations[0].EquipmentStatusParameters.BESSStatus",
        "ParametersConfigurations[0].EquipmentStatusParameters.SofcStatus",
        "ParametersConfigurations[0].EquipmentStatusParameters.PhotovoltaicStatus",
        "ParametersConfigurations[0].EquipmentStatusParameters.MicroTurbineStatus",
    )
    for path in off_paths:
        set_value_if_possible(lf, 0, path)


def run_load_flow(
    cympy,
    disable_active_controls: bool = True,
    voltage_tolerance: float = 0.001,
    analysis_mode: str = "VoltageDropUnbalanced",
):
    lf = cympy.sim.LoadFlow()
    configure_load_flow(
        lf,
        disable_active_controls=disable_active_controls,
        voltage_tolerance=voltage_tolerance,
        analysis_mode=analysis_mode,
    )
    raw_result = lf.Run()
    converged = raw_result not in (False, 0, "0")
    return lf, converged, raw_result


def build_phase_vectors(values: dict[str, float | None]) -> list[float]:
    phases = []
    for phase in ("A", "B", "C"):
        value = values.get(phase)
        if value is not None:
            phases.append(float(value))
    return phases


def dataframe_or_empty(rows: list[dict[str, Any]], columns: list[str]) -> pd.DataFrame:
    if rows:
        return pd.DataFrame(rows)
    return pd.DataFrame(columns=columns)


def study_name_from_path(study_file: str | os.PathLike[str]) -> str:
    return Path(study_file).stem
