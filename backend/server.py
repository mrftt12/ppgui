from fastapi import FastAPI, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any, Union
from pathlib import Path
from datetime import datetime
import pandapower as pp
import multiconductor as mc
import sce_wrapper.general as sce_general
import pandas as pd
import numpy as np
import matplotlib

matplotlib.use("Agg")  # non-GUI backend for containers
import matplotlib.pyplot as plt
import seaborn as sns
import json
import uuid
from copy import deepcopy
import io
import base64
import traceback
import logging

try:
    from database import Database
except ModuleNotFoundError:
    from backend.database import Database
from multiconductor.load_allocation.load_allocation import (
    build_measurement_graph,
    run_load_allocation,
    get_simulated_measurement_value,
)


# Valid logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Multiconductor API", version="1.0.0", description="Power System Analysis API"
)

# CORS Configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory storage for networks (session-based)
networks_store: Dict[str, Any] = {}
# Persistent storage
db = Database()

# ---------- TCGM GIS Support ----------
TCGM_GIS_PATH = Path(__file__).resolve().parent.parent / "tcgm-gis.json"
_tcgm_cache: Optional[Dict[str, Any]] = None


def _load_tcgm_cache() -> Dict[str, Any]:
    """Load tcgm-gis.json once and cache."""
    global _tcgm_cache
    if _tcgm_cache is None:
        if not TCGM_GIS_PATH.exists():
            logger.warning("tcgm-gis.json not found at %s", TCGM_GIS_PATH)
            _tcgm_cache = {}
        else:
            logger.info("Loading tcgm-gis.json ...")
            with open(TCGM_GIS_PATH, "r") as f:
                _tcgm_cache = json.load(f)
            logger.info(
                "Loaded tcgm-gis.json with %d objects",
                len(_tcgm_cache.get("objects", [])),
            )
    return _tcgm_cache


def _tcgm_nodes_by_group(group_filter: Optional[str] = None) -> Dict[str, Any]:
    """Return mapping of groupid -> list of nodes with lat/lon."""
    data = _load_tcgm_cache()
    objs = data.get("objects", [])
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for obj in objs:
        if obj.get("name") != "powerflow.node":
            continue
        attrs = obj.get("attributes", {})
        gid = attrs.get("groupid")
        if not gid:
            continue
        if group_filter and gid != group_filter:
            continue
        lat = attrs.get("lat", obj.get("lat"))
        lon = attrs.get("lon", obj.get("lon"))
        if lat is None or lon is None:
            continue
        groups.setdefault(gid, []).append(
            {
                "name": attrs.get("name"),
                "groupid": gid,
                "nominal_voltage": attrs.get("nominal_voltage"),
                "phases": attrs.get("phases"),
                "lat": float(lat),
                "lon": float(lon),
            }
        )

    return {
        "updated_at": datetime.utcnow().isoformat() + "Z",
        "groups": [
            {"groupid": gid, "nodes": nodes}
            for gid, nodes in sorted(groups.items(), key=lambda x: x[0])
        ],
    }


# TCGM GIS cache (tcgm-gis.json in repo root)
TCGM_GIS_PATH = Path(__file__).resolve().parent.parent / "tcgm-gis.json"
_tcgm_cache: Optional[Dict[str, Any]] = None


# Helper function to convert pandas DataFrames to JSON-serializable format
def network_to_dict(net):
    """Convert multiconductor network to JSON-serializable dictionary."""
    result: Dict[str, Any] = {}
    safe_keys = {
        "bus",
        "line",
        "trafo1ph",
        "switch",
        "ext_grid",
        "ext_grid_sequence",
        "asymmetric_load",
        "asymmetric_sgen",
        "asymmetric_gen",
        "measurement",
        "bus_geodata",
        "line_geodata",
        "res_bus",
        "res_line",
        "res_asymmetric_load",
        "res_asymmetric_sgen",
        "res_ext_grid",
    }

    def df_to_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
        df = df.copy()
        df = df.reset_index()
        for col in df.columns:
            if df[col].dtype == "object":
                try:
                    df[col] = df[col].astype(str)
                except Exception:
                    df[col] = df[col].apply(lambda x: str(x) if x is not None else None)
        return df.replace({np.nan: None}).to_dict(orient="records")

    for key in net.keys():
        try:
            if isinstance(net[key], pd.DataFrame):
                if key in safe_keys:
                    result[key] = df_to_records(net[key]) if not net[key].empty else []
            elif isinstance(net[key], (int, float, str, bool, type(None))):
                result[key] = net[key]
            elif isinstance(net[key], np.integer):
                result[key] = int(net[key])
            elif isinstance(net[key], np.floating):
                result[key] = float(net[key])
        except Exception:
            pass

    result["std_types"] = getattr(net, "std_types", {})
    result["name"] = net.name if hasattr(net, "name") else ""
    result["f_hz"] = float(net.f_hz) if hasattr(net, "f_hz") else 50.0
    result["sn_mva"] = float(net.sn_mva) if hasattr(net, "sn_mva") else 1.0
    return result


def dict_to_dataframe_safe(data: List[Dict]) -> pd.DataFrame:
    """Safely convert list of dicts to DataFrame"""
    if not data:
        return pd.DataFrame()
    return pd.DataFrame(data)


def restore_network_data(net, data: Dict[str, Any]):
    """Restore a multiconductor network from dictionary data."""
    if "std_types" in data:
        net.std_types = data["std_types"]

    def restore_table(key: str) -> None:
        records = data.get(key)
        if records is None:
            return
        df = pd.DataFrame(records)
        if df.empty:
            net[key] = net[key].iloc[0:0].copy()
            return

        index_cols = []
        for candidate in [
            ("index", "bus", "circuit"),
            ("index", "phase"),
            ("index", "circuit"),
        ]:
            if all(col in df.columns for col in candidate):
                index_cols = list(candidate)
                break
        if not index_cols and "index" in df.columns:
            index_cols = ["index"]
        if index_cols:
            df = df.set_index(index_cols)

        if key in net:
            template_cols = list(net[key].columns)
            for col in template_cols:
                if col not in df.columns:
                    df[col] = np.nan
            df = df[template_cols]
        net[key] = df

    for key in [
        "bus",
        "line",
        "trafo1ph",
        "switch",
        "ext_grid",
        "ext_grid_sequence",
        "asymmetric_load",
        "asymmetric_sgen",
        "asymmetric_gen",
        "measurement",
        "bus_geodata",
        "line_geodata",
    ]:
        restore_table(key)


DEFAULT_PHASES = [1, 2, 3]


def _normalize_phase_list(
    value: Optional[Union[int, List[int]]], default: List[int]
) -> List[int]:
    if value is None:
        return list(default)
    if isinstance(value, list):
        return value
    return [int(value)]


def _normalize_to_phase(
    value: Optional[Union[int, List[int]]], default: int
) -> Union[int, List[int]]:
    if value is None:
        return default
    if isinstance(value, list):
        return value
    return int(value)


def _split_three_phase(value: float, phases: List[int]) -> List[float]:
    if not phases:
        return []
    return [value / len(phases)] * len(phases)


def _ensure_sequence_line_std_type(net, params: Dict[str, Any]) -> str:
    std_name = params.get("std_type") or f"seq_line_{uuid.uuid4().hex[:8]}"
    if std_name in net.std_types.get("sequence", {}):
        return std_name
    r = params.get("r_ohm_per_km", 0.1)
    x = params.get("x_ohm_per_km", 0.1)
    c = params.get("c_nf_per_km", 0.0)
    data = {
        "r_ohm_per_km": r,
        "x_ohm_per_km": x,
        "r0_ohm_per_km": params.get("r0_ohm_per_km", r * 3),
        "x0_ohm_per_km": params.get("x0_ohm_per_km", x * 3),
        "c_nf_per_km": c,
        "c0_nf_per_km": params.get("c0_nf_per_km", c),
        "max_i_ka": params.get("max_i_ka", 1.0),
    }
    mc.create_std_type(net, data, std_name, element="sequence", overwrite=True)
    return std_name


def _ensure_trafo_std_type(net, params: Dict[str, Any]) -> str:
    std_name = params.get("std_type") or f"trafo_{uuid.uuid4().hex[:8]}"
    if std_name in net.std_types.get("trafo", {}):
        return std_name
    data = {
        "sn_mva": params.get("sn_mva", 1.0),
        "vn_hv_kv": params.get("vn_hv_kv", 20.0),
        "vn_lv_kv": params.get("vn_lv_kv", 0.4),
        "vk_percent": params.get("vk_percent", 6.0),
        "vkr_percent": params.get("vkr_percent", 0.5),
        "pfe_kw": params.get("pfe_kw", 0.0),
        "i0_percent": params.get("i0_percent", 0.0),
        "vector_group": params.get("vector_group", "Yy0"),
        "tap_side": params.get("tap_side", "hv"),
        "tap_neutral": params.get("tap_neutral", 0),
        "tap_min": params.get("tap_min", -2),
        "tap_max": params.get("tap_max", 2),
        "tap_step_degree": params.get("tap_step_degree", 0.0),
        "tap_step_percent": params.get("tap_step_percent", 2.5),
        "shift_degree": params.get("shift_degree", 0.0),
    }
    mc.create_std_type(net, data, std_name, element="trafo", overwrite=True)
    return std_name


def _create_simple_network(name: str = "Sample Network"):
    net = mc.create_empty_network(name=name)
    mc.create_bus(net, vn_kv=20.0, name="Bus 0")
    mc.create_bus(net, vn_kv=0.4, name="Bus 1")
    std_type = _ensure_sequence_line_std_type(
        net,
        {"r_ohm_per_km": 0.1, "x_ohm_per_km": 0.1, "c_nf_per_km": 0.0, "max_i_ka": 1.0},
    )
    mc.create_line(
        net,
        std_type=std_type,
        model_type="sequence",
        from_bus=0,
        from_phase=DEFAULT_PHASES,
        to_bus=1,
        to_phase=DEFAULT_PHASES,
        length_km=0.1,
        name="Line 0-1",
    )
    mc.create_ext_grid(
        net,
        bus=0,
        from_phase=DEFAULT_PHASES,
        to_phase=0,
        vm_pu=1.0,
        va_degree=0.0,
        r_ohm=0.0,
        x_ohm=0.0,
    )
    mc.create_asymmetric_load(
        net,
        bus=1,
        from_phase=DEFAULT_PHASES,
        to_phase=0,
        p_mw=_split_three_phase(0.3, DEFAULT_PHASES),
        q_mvar=_split_three_phase(0.1, DEFAULT_PHASES),
        name="Load 1",
    )
    return net


def _phase_suffix_results_bus(net: Any) -> List[Dict[str, Any]]:
    if net.res_bus is None or net.res_bus.empty:
        return []
    df = net.res_bus.reset_index()
    bus_col = "index" if "index" in df.columns else df.columns[0]
    phase_col = "phase" if "phase" in df.columns else df.columns[1]
    phase_map = {1: "a", 2: "b", 3: "c"}
    value_cols = [c for c in df.columns if c not in {bus_col, phase_col}]
    results: List[Dict[str, Any]] = []
    for bus_id, group in df.groupby(bus_col):
        row: Dict[str, Any] = {"bus": int(bus_id), "index": int(bus_id)}
        for col in value_cols:
            phase_values = []
            for phase, suffix in phase_map.items():
                phase_row = group[group[phase_col] == phase]
                value = phase_row.iloc[0][col] if not phase_row.empty else None
                key = "{}_{}".format(col, suffix)
                row[key] = None if pd.isna(value) else value
                if value is not None and not pd.isna(value):
                    phase_values.append(float(value))
            if phase_values:
                if col in {
                    "p_mw",
                    "q_mvar",
                    "p_load_mw",
                    "q_load_mvar",
                    "p_gen_mw",
                    "q_gen_mvar",
                }:
                    row[col] = float(sum(phase_values))
                elif col in {"i_ka", "im_from_source_ka"}:
                    row[col] = float(max(phase_values))
                else:
                    row[col] = float(sum(phase_values)) / len(phase_values)
        results.append(row)
    return results


def _phase_suffix_results_line(net: Any) -> List[Dict[str, Any]]:
    if net.res_line is None or net.res_line.empty:
        return []
    df = net.res_line.reset_index()
    line_col = "index" if "index" in df.columns else df.columns[0]
    circuit_col = "circuit" if "circuit" in df.columns else df.columns[1]
    phase_map = {0: "a", 1: "b", 2: "c"}
    value_cols = [c for c in df.columns if c not in {line_col, circuit_col}]
    results: List[Dict[str, Any]] = []
    for line_id, group in df.groupby(line_col):
        row: Dict[str, Any] = {"line": int(line_id), "index": int(line_id)}
        for col in value_cols:
            phase_values = []
            for circuit, suffix in phase_map.items():
                phase_row = group[group[circuit_col] == circuit]
                value = phase_row.iloc[0][col] if not phase_row.empty else None
                key = "{}_{}".format(col, suffix)
                row[key] = None if pd.isna(value) else value
                if value is not None and not pd.isna(value):
                    phase_values.append(float(value))
            if phase_values:
                if col in {
                    "p_from_mw",
                    "p_to_mw",
                    "q_from_mvar",
                    "q_to_mvar",
                    "pl_mw",
                    "ql_mvar",
                }:
                    row[col] = float(sum(phase_values))
                elif col in {"i_from_ka", "i_to_ka", "i_ka"}:
                    row[col] = float(max(phase_values))
                else:
                    row[col] = float(sum(phase_values)) / len(phase_values)
        results.append(row)
    return results


def _table_records(net: Any, name: str) -> List[Dict[str, Any]]:
    table = getattr(net, name, pd.DataFrame())
    if not isinstance(table, pd.DataFrame) or table.empty:
        return []
    return table.replace({np.nan: None}).reset_index().to_dict(orient="records")


def prepare_network_for_hc(network_id: Optional[str]) -> Any:
    """Return a fresh copy of the network to use for hosting capacity analysis."""
    if network_id:
        if network_id not in networks_store:
            raise HTTPException(status_code=404, detail="Network not found")
        return deepcopy(networks_store[network_id])
    return _create_simple_network(name="HC Default")


# Pydantic Models
class NetworkCreate(BaseModel):
    name: str = "New Network"
    f_hz: float = 50.0
    sn_mva: float = 1.0


class BusCreate(BaseModel):
    vn_kv: float
    name: Optional[str] = None
    index: Optional[int] = None
    geodata: Optional[List[float]] = None
    type: str = "b"
    zone: Optional[str] = None
    in_service: bool = True
    max_vm_pu: float = 1.1
    min_vm_pu: float = 0.9
    num_phases: int = 4
    grounded_phases: Optional[List[int]] = None
    grounding_r_ohm: float = 0.0
    grounding_x_ohm: float = 0.0


class LineCreate(BaseModel):
    from_bus: int
    to_bus: int
    length_km: float
    std_type: Optional[str] = None
    model_type: str = "sequence"
    from_phase: Optional[Union[int, List[int]]] = None
    to_phase: Optional[Union[int, List[int]]] = None
    name: Optional[str] = None
    r_ohm_per_km: Optional[float] = None
    x_ohm_per_km: Optional[float] = None
    c_nf_per_km: Optional[float] = None
    max_i_ka: Optional[float] = None
    in_service: bool = True
    parallel: int = 1


class TransformerCreate(BaseModel):
    hv_bus: int
    lv_bus: int
    std_type: Optional[str] = None
    name: Optional[str] = None
    sn_mva: Optional[float] = None
    vn_hv_kv: Optional[float] = None
    vn_lv_kv: Optional[float] = None
    vk_percent: Optional[float] = None
    vkr_percent: Optional[float] = None
    pfe_kw: Optional[float] = None
    i0_percent: Optional[float] = None
    tap_pos: Optional[int] = None
    in_service: bool = True
    parallel: int = 1


class LoadCreate(BaseModel):
    bus: int
    p_mw: float
    q_mvar: float = 0.0
    name: Optional[str] = None
    scaling: float = 1.0
    in_service: bool = True
    type: Optional[str] = None
    from_phase: Optional[Union[int, List[int]]] = None
    to_phase: Optional[Union[int, List[int]]] = None


class GeneratorCreate(BaseModel):
    bus: int
    p_mw: float
    vm_pu: float = 1.0
    name: Optional[str] = None
    max_q_mvar: Optional[float] = None
    min_q_mvar: Optional[float] = None
    min_p_mw: Optional[float] = None
    max_p_mw: Optional[float] = None
    scaling: float = 1.0
    slack: bool = False
    in_service: bool = True
    type: Optional[str] = None
    from_phase: Optional[Union[int, List[int]]] = None
    to_phase: Optional[Union[int, List[int]]] = None


class StaticGeneratorCreate(BaseModel):
    bus: int
    p_mw: float
    q_mvar: float = 0.0
    name: Optional[str] = None
    scaling: float = 1.0
    in_service: bool = True
    type: Optional[str] = None
    from_phase: Optional[Union[int, List[int]]] = None
    to_phase: Optional[Union[int, List[int]]] = None


class ExternalGridCreate(BaseModel):
    bus: int
    vm_pu: float = 1.0
    va_degree: float = 0.0
    name: Optional[str] = None
    s_sc_max_mva: Optional[float] = None
    s_sc_min_mva: Optional[float] = None
    rx_max: Optional[float] = None
    rx_min: Optional[float] = None
    in_service: bool = True
    from_phase: Optional[Union[int, List[int]]] = None
    to_phase: Optional[Union[int, List[int]]] = None


class ShuntCreate(BaseModel):
    bus: int
    q_mvar: float
    p_mw: float = 0.0
    name: Optional[str] = None
    step: int = 1
    max_step: int = 1
    in_service: bool = True


class SwitchCreate(BaseModel):
    bus: int
    element: int
    et: str  # 'l' for line, 't' for transformer, 'b' for bus
    type: Optional[str] = None
    closed: bool = True
    name: Optional[str] = None
    phase: Optional[Union[int, List[int]]] = None


class PowerFlowOptions(BaseModel):
    algorithm: str = "nr"  # nr, bfsw, gs, fdbx, fdxb
    init: str = "auto"  # auto, flat, dc, results
    max_iteration: int = 50
    tolerance_mva: float = 1e-8
    trafo_model: str = "t"  # t, pi
    trafo_loading: str = "current"  # current, power
    enforce_q_lims: bool = False
    check_connectivity: bool = True
    voltage_depend_loads: bool = True
    calculate_voltage_angles: bool = True


class ShortCircuitOptions(BaseModel):
    fault: str = "3ph"  # 3ph, 2ph, 1ph
    case: str = "max"  # max, min
    lv_tol_percent: float = 10.0
    topology: str = "auto"  # auto, radial, meshed
    ip: bool = False
    ith: bool = False
    tk_s: float = 1.0


class OPFOptions(BaseModel):
    verbose: bool = False
    suppress_warnings: bool = True
    OPF_FLOW_LIM: str = "I"  # S, I


class LoadAllocationOptions(BaseModel):
    tolerance: float = 0.5
    max_iter: int = 8
    adjust_after_load_flow: bool = True
    ignore_generators: bool = False
    ignore_fixed_capacitors: bool = False
    ignore_controlled_capacitors: bool = False
    cap_to_transformer_rating: bool = False
    cap_to_load_rating: bool = False
    trafo_overload_factor: Optional[float] = None
    adjust_power_factor: bool = True
    measurement_indices: Optional[List[int]] = None
    verbose: bool = False


class NetworkImport(BaseModel):
    network_data: Dict[str, Any]
    name: str = "Imported Network"


class HostingCapacityOptions(BaseModel):
    """Options for hosting capacity analysis."""

    iterations: int = 50
    voltage_limit: float = 1.04
    loading_limit: float = 50.0
    plant_mean_mw: float = 0.5
    plant_std_mw: float = 0.05
    network_id: Optional[str] = (
        None  # Use an existing network; if None, use mv_oberrhein
    )
    seed: Optional[int] = None


class TimeSeriesOptions(BaseModel):
    """Options for time series simulation."""

    timesteps: int = Field(default=24, ge=1, le=168)
    seed: Optional[int] = None
    # future: allow user-provided profiles
    profiles: Optional[Dict[str, List[float]]] = None


class SaveVersionRequest(BaseModel):
    description: Optional[str] = None


# Health Check
@app.get("/api/health")
async def health_check():
    return {"status": "healthy", "service": "Multiconductor API"}


# ---------- TCGM GIS Endpoints ----------


@app.get("/api/tcgm/nodes")
async def get_tcgm_nodes(groupid: Optional[str] = None):
    """
    Return tcgm-gis node coordinates grouped by groupid.
    Optional query param `groupid` to filter.
    """
    return _tcgm_nodes_by_group(groupid)


# ==================== NETWORK MANAGEMENT ====================


@app.post("/api/networks")
async def create_network(network: NetworkCreate):
    """Create a new empty power network"""
    network_id = str(uuid.uuid4())
    net = mc.create_empty_network(
        name=network.name, f_hz=network.f_hz, sn_mva=network.sn_mva
    )
    # Persist
    db.create_network(network_id, network.name)
    networks_store[network_id] = net
    # Save initial version
    db.save_version(network_id, network_to_dict(net), "Initial creation")
    return {
        "network_id": network_id,
        "name": network.name,
        "message": "Network created successfully",
    }


@app.get("/api/networks/{network_id}")
async def get_network(network_id: str):
    """Get network details"""
    if network_id not in networks_store:
        # Try to load from DB
        data = db.load_version(network_id)
        if data:
            net = mc.create_empty_network(name=data.get("name", "Loaded Network"))
            restore_network_data(net, data)
            networks_store[network_id] = net
        else:
            raise HTTPException(status_code=404, detail="Network not found")

    net = networks_store[network_id]
    return {"network_id": network_id, "data": network_to_dict(net)}


@app.delete("/api/networks/{network_id}")
async def delete_network(network_id: str):
    """Delete a network"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    del networks_store[network_id]
    return {"message": "Network deleted successfully"}


@app.get("/api/networks")
async def list_networks():
    """List all networks"""
    networks = db.list_networks()
    # Fill in counts from active memory if available?
    # For now just list them. To get stats we'd need to load them or store stats in DB.
    # We'll just return the list from DB.
    return {"networks": networks}


@app.post("/api/networks/{network_id}/copy")
async def copy_network(network_id: str, name: Optional[str] = None):
    """Create a copy of an existing network"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    new_id = str(uuid.uuid4())
    networks_store[new_id] = deepcopy(networks_store[network_id])
    if name:
        networks_store[new_id].name = name

    # Persist copy
    db.create_network(new_id, networks_store[new_id].name)
    db.save_version(
        new_id, network_to_dict(networks_store[new_id]), f"Copy of {network_id}"
    )

    return {"network_id": new_id, "message": "Network copied successfully"}


@app.post("/api/networks/{network_id}/save")
async def save_network_version(network_id: str, request: SaveVersionRequest):
    """Save current network state as a new version"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")

    net = networks_store[network_id]
    data = network_to_dict(net)
    version_num = db.save_version(network_id, data, request.description)

    return {
        "network_id": network_id,
        "version": version_num,
        "message": "Network version saved",
    }


@app.get("/api/networks/{network_id}/history")
async def get_network_history(network_id: str):
    """Get history of network versions"""
    # Check if exists (in store or DB)
    if network_id not in networks_store and not db.get_network(network_id):
        raise HTTPException(status_code=404, detail="Network not found")

    history = db.get_history(network_id)
    return {"network_id": network_id, "history": history}


@app.post("/api/networks/{network_id}/load/{version_id}")
async def load_network_version(network_id: str, version_id: str):
    """Load a specific version of the network"""
    try:
        data = db.load_version(network_id, version_id=version_id)
        if not data:
            raise HTTPException(status_code=404, detail="Version not found")

        net = mc.create_empty_network(name=data.get("name", "Loaded Network"))
        restore_network_data(net, data)
        networks_store[network_id] = net

        return {
            "network_id": network_id,
            "message": "Network version loaded",
            "data": network_to_dict(net),
        }
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error loading version {version_id}: {str(e)}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@app.post("/api/networks/import")
async def import_network(data: NetworkImport):
    """Import a network from JSON data"""
    try:
        network_id = str(uuid.uuid4())
        net = mc.create_empty_network(name=data.name)

        # Restore
        restore_network_data(net, data.network_data)

        # Persist
        db.create_network(network_id, data.name)
        networks_store[network_id] = net
        db.save_version(network_id, network_to_dict(net), "Imported Network")

        return {
            "network_id": network_id,
            "name": data.name,
            "message": "Network imported successfully",
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Import failed: {str(e)}")


@app.get("/api/networks/{network_id}/export")
async def export_network(network_id: str):
    """Export network to JSON format"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]
    return {"network_id": network_id, "data": network_to_dict(net)}


# ==================== SAMPLE NETWORKS ====================


@app.get("/api/sample-networks")
async def list_sample_networks():
    """List available multiconductor sample networks."""
    samples = [
        {
            "id": "simple_radial_2bus",
            "name": "Simple Radial 2-Bus",
            "description": "Two-bus radial multiconductor example",
        },
        {
            "id": "simple_radial_4bus",
            "name": "Simple Radial 4-Bus",
            "description": "Four-bus radial multiconductor example",
        },
    ]
    return {"samples": samples}


@app.post("/api/sample-networks/{sample_id}/load")
async def load_sample_network(sample_id: str):
    """Load a sample multiconductor network."""
    network_id = str(uuid.uuid4())
    try:
        if sample_id == "simple_radial_2bus":
            net = _create_simple_network(name="Simple Radial 2-Bus")
        elif sample_id == "simple_radial_4bus":
            net = mc.create_empty_network(name="Simple Radial 4-Bus")
            for idx in range(4):
                mc.create_bus(net, vn_kv=0.4 if idx else 20.0, name=f"Bus {idx}")
            std_type = _ensure_sequence_line_std_type(
                net,
                {
                    "r_ohm_per_km": 0.1,
                    "x_ohm_per_km": 0.1,
                    "c_nf_per_km": 0.0,
                    "max_i_ka": 1.0,
                },
            )
            for idx in range(3):
                mc.create_line(
                    net,
                    std_type=std_type,
                    model_type="sequence",
                    from_bus=idx,
                    from_phase=DEFAULT_PHASES,
                    to_bus=idx + 1,
                    to_phase=DEFAULT_PHASES,
                    length_km=0.1,
                    name=f"Line {idx}-{idx + 1}",
                )
            mc.create_ext_grid(
                net,
                bus=0,
                from_phase=DEFAULT_PHASES,
                to_phase=0,
                vm_pu=1.0,
                va_degree=0.0,
                r_ohm=0.0,
                x_ohm=0.0,
            )
            mc.create_asymmetric_load(
                net,
                bus=3,
                from_phase=DEFAULT_PHASES,
                to_phase=0,
                p_mw=_split_three_phase(0.5, DEFAULT_PHASES),
                q_mvar=_split_three_phase(0.15, DEFAULT_PHASES),
                name="Load 3",
            )
        else:
            raise HTTPException(status_code=404, detail="Sample network not found")

        networks_store[network_id] = net
        return {
            "network_id": network_id,
            "name": sample_id,
            "message": "Sample network loaded",
            "data": network_to_dict(net),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load sample: {str(e)}")


# ==================== BUS MANAGEMENT ====================


@app.post("/api/networks/{network_id}/buses")
async def create_bus(network_id: str, bus: BusCreate):
    """Create a new bus"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]
    try:
        bus_idx = mc.create_bus(
            net,
            vn_kv=bus.vn_kv,
            num_phases=bus.num_phases,
            name=bus.name,
            grounded_phases=bus.grounded_phases or [0],
            grounding_r_ohm=bus.grounding_r_ohm,
            grounding_x_ohm=bus.grounding_x_ohm,
            in_service=bus.in_service,
            type=bus.type,
            zone=bus.zone,
            index=bus.index,
        )
        if bus.geodata:
            net.bus_geodata.loc[bus_idx, ["x", "y"]] = bus.geodata[:2]
        return {"bus_id": int(bus_idx), "message": "Bus created successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/networks/{network_id}/buses")
async def get_buses(network_id: str):
    """Get all buses in the network"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]
    if net.bus.empty:
        return {"buses": []}
    df = net.bus.reset_index()
    bus_col = "index" if "index" in df.columns else df.columns[0]
    phase_col = "phase" if "phase" in df.columns else df.columns[1]
    buses = []
    for bus_id, group in df.groupby(bus_col):
        row = group.iloc[0].to_dict()
        row["index"] = int(bus_id)
        row["bus"] = int(bus_id)
        row["phases"] = [int(p) for p in group[phase_col].tolist()]
        buses.append(row)
    return {
        "buses": pd.DataFrame(buses).replace({np.nan: None}).to_dict(orient="records")
    }


@app.put("/api/networks/{network_id}/buses/{bus_id}")
async def update_bus(network_id: str, bus_id: int, bus: BusCreate):
    """Update a bus"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]
    if bus_id not in net.bus.index.get_level_values(0):
        raise HTTPException(status_code=404, detail="Bus not found")

    mask = net.bus.index.get_level_values(0) == bus_id
    net.bus.loc[mask, "vn_kv"] = bus.vn_kv
    net.bus.loc[mask, "name"] = bus.name
    net.bus.loc[mask, "type"] = bus.type
    net.bus.loc[mask, "in_service"] = bus.in_service
    return {"message": "Bus updated successfully"}


@app.delete("/api/networks/{network_id}/buses/{bus_id}")
async def delete_bus(network_id: str, bus_id: int):
    """Delete a bus"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]
    if bus_id not in net.bus.index.get_level_values(0):
        raise HTTPException(status_code=404, detail="Bus not found")
    try:
        net.bus = net.bus[net.bus.index.get_level_values(0) != bus_id]
        if not net.line.empty:
            net.line = net.line[
                (net.line["from_bus"] != bus_id) & (net.line["to_bus"] != bus_id)
            ]
        for table in [
            "asymmetric_load",
            "asymmetric_sgen",
            "asymmetric_gen",
            "ext_grid",
            "switch",
            "trafo1ph",
        ]:
            if table in net and not net[table].empty and "bus" in net[table].columns:
                net[table] = net[table][net[table]["bus"] != bus_id]
            elif (
                table in net
                and not net[table].empty
                and "element" in net[table].columns
            ):
                net[table] = net[table][net[table]["element"] != bus_id]
        return {"message": "Bus deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# ==================== LINE MANAGEMENT ====================


@app.post("/api/networks/{network_id}/lines")
async def create_line(network_id: str, line: LineCreate):
    """Create a new line"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]
    try:
        from_phase = _normalize_phase_list(line.from_phase, DEFAULT_PHASES)
        to_phase = _normalize_phase_list(line.to_phase, DEFAULT_PHASES)
        std_type = _ensure_sequence_line_std_type(
            net,
            {
                "std_type": line.std_type,
                "r_ohm_per_km": line.r_ohm_per_km,
                "x_ohm_per_km": line.x_ohm_per_km,
                "c_nf_per_km": line.c_nf_per_km,
                "max_i_ka": line.max_i_ka,
            },
        )
        line_idx = mc.create_line(
            net,
            std_type=std_type,
            model_type=line.model_type,
            from_bus=line.from_bus,
            from_phase=from_phase,
            to_bus=line.to_bus,
            to_phase=to_phase,
            length_km=line.length_km,
            name=line.name,
            in_service=line.in_service,
        )
        return {"line_id": int(line_idx), "message": "Line created successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/networks/{network_id}/lines")
async def get_lines(network_id: str):
    """Get all lines in the network"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]
    if net.line.empty:
        return {"lines": []}
    df = net.line.reset_index()
    line_col = "index" if "index" in df.columns else df.columns[0]
    circuit_col = "circuit" if "circuit" in df.columns else df.columns[1]
    lines = []
    for line_id, group in df.groupby(line_col):
        row = group.iloc[0].to_dict()
        row["index"] = int(line_id)
        row["line"] = int(line_id)
        row["circuits"] = [int(c) for c in group[circuit_col].tolist()]
        lines.append(row)
    return {
        "lines": pd.DataFrame(lines).replace({np.nan: None}).to_dict(orient="records")
    }


@app.delete("/api/networks/{network_id}/lines/{line_id}")
async def delete_line(network_id: str, line_id: int):
    """Delete a line"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]
    if line_id not in net.line.index.get_level_values(0):
        raise HTTPException(status_code=404, detail="Line not found")
    try:
        net.line = net.line[net.line.index.get_level_values(0) != line_id]
        return {"message": "Line deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# ==================== TRANSFORMER MANAGEMENT ====================


@app.post("/api/networks/{network_id}/transformers")
async def create_transformer(network_id: str, trafo: TransformerCreate):
    """Create a new transformer"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]
    try:
        existing = (
            set(net.trafo1ph.index.get_level_values(0))
            if hasattr(net, "trafo1ph") and not net.trafo1ph.empty
            else set()
        )
        std_type = _ensure_trafo_std_type(
            net,
            {
                "std_type": trafo.std_type,
                "sn_mva": trafo.sn_mva,
                "vn_hv_kv": trafo.vn_hv_kv,
                "vn_lv_kv": trafo.vn_lv_kv,
                "vk_percent": trafo.vk_percent,
                "vkr_percent": trafo.vkr_percent,
                "pfe_kw": trafo.pfe_kw,
                "i0_percent": trafo.i0_percent,
            },
        )
        trafo_idx = mc.create_transformer_3ph(
            net,
            hv_bus=trafo.hv_bus,
            lv_bus=trafo.lv_bus,
            std_type=std_type,
            tap_pos=trafo.tap_pos if trafo.tap_pos is not None else np.nan,
            name=trafo.name or "trafo",
            in_service=trafo.in_service,
        )
        if trafo_idx is None and hasattr(net, "trafo1ph") and not net.trafo1ph.empty:
            created = set(net.trafo1ph.index.get_level_values(0)) - existing
            trafo_idx = (
                max(created) if created else max(net.trafo1ph.index.get_level_values(0))
            )
        return {
            "transformer_id": int(trafo_idx),
            "message": "Transformer created successfully",
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/networks/{network_id}/transformers")
async def get_transformers(network_id: str):
    """Get all transformers in the network"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]
    if net.trafo1ph.empty:
        return {"transformers": []}
    df = net.trafo1ph.reset_index()
    trafo_col = "index" if "index" in df.columns else df.columns[0]
    trafos = []
    for trafo_id, group in df.groupby(trafo_col):
        row = group.iloc[0].to_dict()
        row["index"] = int(trafo_id)
        trafos.append(row)
    return {
        "transformers": pd.DataFrame(trafos)
        .replace({np.nan: None})
        .to_dict(orient="records")
    }


@app.delete("/api/networks/{network_id}/transformers/{trafo_id}")
async def delete_transformer(network_id: str, trafo_id: int):
    """Delete a transformer"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]
    if trafo_id not in net.trafo1ph.index.get_level_values(0):
        raise HTTPException(status_code=404, detail="Transformer not found")
    try:
        net.trafo1ph = net.trafo1ph[net.trafo1ph.index.get_level_values(0) != trafo_id]
        return {"message": "Transformer deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# ==================== LOAD MANAGEMENT ====================


@app.post("/api/networks/{network_id}/loads")
async def create_load(network_id: str, load: LoadCreate):
    """Create a new load"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]
    try:
        from_phase = _normalize_phase_list(load.from_phase, DEFAULT_PHASES)
        to_phase = _normalize_to_phase(load.to_phase, 0)
        p_vals = _split_three_phase(load.p_mw, from_phase)
        q_vals = _split_three_phase(load.q_mvar, from_phase)
        load_idx = mc.create_asymmetric_load(
            net,
            bus=load.bus,
            from_phase=from_phase,
            to_phase=to_phase,
            p_mw=p_vals,
            q_mvar=q_vals,
            name=load.name,
            scaling=load.scaling,
            in_service=load.in_service,
            type=load.type,
        )
        return {"load_id": int(load_idx), "message": "Load created successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/networks/{network_id}/loads")
async def get_loads(network_id: str):
    """Get all loads in the network"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]
    if net.asymmetric_load.empty:
        return {"loads": []}
    df = net.asymmetric_load.reset_index()
    load_col = "index" if "index" in df.columns else df.columns[0]
    loads = []
    for load_id, group in df.groupby(load_col):
        row = group.iloc[0].to_dict()
        row["index"] = int(load_id)
        loads.append(row)
    return {
        "loads": pd.DataFrame(loads).replace({np.nan: None}).to_dict(orient="records")
    }


@app.delete("/api/networks/{network_id}/loads/{load_id}")
async def delete_load(network_id: str, load_id: int):
    """Delete a load"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]
    if load_id not in net.asymmetric_load.index.get_level_values(0):
        raise HTTPException(status_code=404, detail="Load not found")
    try:
        net.asymmetric_load = net.asymmetric_load[
            net.asymmetric_load.index.get_level_values(0) != load_id
        ]
        return {"message": "Load deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# ==================== GENERATOR MANAGEMENT ====================


@app.post("/api/networks/{network_id}/generators")
async def create_generator(network_id: str, gen: GeneratorCreate):
    """Create a new generator"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]
    try:
        from_phase = _normalize_phase_list(gen.from_phase, DEFAULT_PHASES)
        to_phase = _normalize_to_phase(gen.to_phase, 0)
        p_vals = _split_three_phase(gen.p_mw, from_phase)
        vm_vals = [gen.vm_pu] * len(from_phase)
        gen_idx = mc.create_asymmetric_gen(
            net,
            bus=gen.bus,
            from_phase=from_phase,
            to_phase=to_phase,
            p_mw=p_vals,
            vm_pu=vm_vals,
            scaling=gen.scaling,
            in_service=gen.in_service,
            name=gen.name,
            type=gen.type,
            slack=gen.slack,
        )
        return {
            "generator_id": int(gen_idx),
            "message": "Generator created successfully",
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/networks/{network_id}/generators")
async def get_generators(network_id: str):
    """Get all generators in the network"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]
    if net.asymmetric_gen.empty:
        return {"generators": []}
    df = net.asymmetric_gen.reset_index()
    gen_col = "index" if "index" in df.columns else df.columns[0]
    generators = []
    for gen_id, group in df.groupby(gen_col):
        row = group.iloc[0].to_dict()
        row["index"] = int(gen_id)
        generators.append(row)
    return {
        "generators": pd.DataFrame(generators)
        .replace({np.nan: None})
        .to_dict(orient="records")
    }


@app.delete("/api/networks/{network_id}/generators/{gen_id}")
async def delete_generator(network_id: str, gen_id: int):
    """Delete a generator"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]
    if gen_id not in net.asymmetric_gen.index.get_level_values(0):
        raise HTTPException(status_code=404, detail="Generator not found")
    try:
        net.asymmetric_gen = net.asymmetric_gen[
            net.asymmetric_gen.index.get_level_values(0) != gen_id
        ]
        return {"message": "Generator deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# ==================== STATIC GENERATOR MANAGEMENT ====================


@app.post("/api/networks/{network_id}/static-generators")
async def create_static_generator(network_id: str, sgen: StaticGeneratorCreate):
    """Create a new static generator"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]
    try:
        from_phase = _normalize_phase_list(sgen.from_phase, DEFAULT_PHASES)
        to_phase = _normalize_to_phase(sgen.to_phase, 0)
        p_vals = _split_three_phase(sgen.p_mw, from_phase)
        q_vals = _split_three_phase(sgen.q_mvar, from_phase)
        sgen_idx = mc.create_asymmetric_sgen(
            net,
            bus=sgen.bus,
            from_phase=from_phase,
            to_phase=to_phase,
            p_mw=p_vals,
            q_mvar=q_vals,
            name=sgen.name,
            scaling=sgen.scaling,
            in_service=sgen.in_service,
            type=sgen.type,
        )
        return {
            "sgen_id": int(sgen_idx),
            "message": "Static generator created successfully",
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/networks/{network_id}/static-generators")
async def get_static_generators(network_id: str):
    """Get all static generators in the network"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]
    if net.asymmetric_sgen.empty:
        return {"static_generators": []}
    df = net.asymmetric_sgen.reset_index()
    sgen_col = "index" if "index" in df.columns else df.columns[0]
    sgens = []
    for sgen_id, group in df.groupby(sgen_col):
        row = group.iloc[0].to_dict()
        row["index"] = int(sgen_id)
        sgens.append(row)
    return {
        "static_generators": pd.DataFrame(sgens)
        .replace({np.nan: None})
        .to_dict(orient="records")
    }


@app.delete("/api/networks/{network_id}/static-generators/{sgen_id}")
async def delete_static_generator(network_id: str, sgen_id: int):
    """Delete a static generator"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]
    if sgen_id not in net.asymmetric_sgen.index.get_level_values(0):
        raise HTTPException(status_code=404, detail="Static generator not found")
    try:
        net.asymmetric_sgen = net.asymmetric_sgen[
            net.asymmetric_sgen.index.get_level_values(0) != sgen_id
        ]
        return {"message": "Static generator deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# ==================== EXTERNAL GRID MANAGEMENT ====================


@app.post("/api/networks/{network_id}/external-grids")
async def create_external_grid(network_id: str, ext_grid: ExternalGridCreate):
    """Create a new external grid connection"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]
    try:
        from_phase = _normalize_phase_list(ext_grid.from_phase, DEFAULT_PHASES)
        to_phase = _normalize_to_phase(ext_grid.to_phase, 0)
        eg_idx = mc.create_ext_grid(
            net,
            bus=ext_grid.bus,
            from_phase=from_phase,
            to_phase=to_phase,
            vm_pu=ext_grid.vm_pu,
            va_degree=ext_grid.va_degree,
            r_ohm=0.0,
            x_ohm=0.0,
            name=ext_grid.name,
            in_service=ext_grid.in_service,
        )
        return {
            "ext_grid_id": int(eg_idx),
            "message": "External grid created successfully",
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/networks/{network_id}/external-grids")
async def get_external_grids(network_id: str):
    """Get all external grids in the network"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]
    if net.ext_grid.empty:
        return {"external_grids": []}
    df = net.ext_grid.reset_index()
    eg_col = "index" if "index" in df.columns else df.columns[0]
    grids = []
    for eg_id, group in df.groupby(eg_col):
        row = group.iloc[0].to_dict()
        row["index"] = int(eg_id)
        grids.append(row)
    return {
        "external_grids": pd.DataFrame(grids)
        .replace({np.nan: None})
        .to_dict(orient="records")
    }


@app.delete("/api/networks/{network_id}/external-grids/{eg_id}")
async def delete_external_grid(network_id: str, eg_id: int):
    """Delete an external grid"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]
    if eg_id not in net.ext_grid.index.get_level_values(0):
        raise HTTPException(status_code=404, detail="External grid not found")
    try:
        net.ext_grid = net.ext_grid[net.ext_grid.index.get_level_values(0) != eg_id]
        return {"message": "External grid deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# ==================== SHUNT MANAGEMENT ====================


@app.post("/api/networks/{network_id}/shunts")
async def create_shunt(network_id: str, shunt: ShuntCreate):
    """Create a new shunt"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    raise HTTPException(
        status_code=501, detail="Shunt elements are not supported in multiconductor yet"
    )


@app.get("/api/networks/{network_id}/shunts")
async def get_shunts(network_id: str):
    """Get all shunts in the network"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    return {"shunts": []}


@app.delete("/api/networks/{network_id}/shunts/{shunt_id}")
async def delete_shunt(network_id: str, shunt_id: int):
    """Delete a shunt"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    raise HTTPException(
        status_code=501, detail="Shunt elements are not supported in multiconductor yet"
    )


# ==================== SWITCH MANAGEMENT ====================


@app.post("/api/networks/{network_id}/switches")
async def create_switch(network_id: str, switch: SwitchCreate):
    """Create a new switch"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]
    try:
        phase = _normalize_phase_list(switch.phase, DEFAULT_PHASES)
        sw_idx = mc.create_switch(
            net,
            bus=switch.bus,
            phase=phase,
            element=switch.element,
            et=switch.et,
            type=switch.type,
            closed=switch.closed,
            name=switch.name,
        )
        return {"switch_id": int(sw_idx), "message": "Switch created successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/networks/{network_id}/switches")
async def get_switches(network_id: str):
    """Get all switches in the network"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]
    if net.switch.empty:
        return {"switches": []}
    df = net.switch.reset_index()
    sw_col = "index" if "index" in df.columns else df.columns[0]
    switches = []
    for sw_id, group in df.groupby(sw_col):
        row = group.iloc[0].to_dict()
        row["index"] = int(sw_id)
        switches.append(row)
    return {
        "switches": pd.DataFrame(switches)
        .replace({np.nan: None})
        .to_dict(orient="records")
    }


@app.put("/api/networks/{network_id}/switches/{sw_id}/toggle")
async def toggle_switch(network_id: str, sw_id: int):
    """Toggle a switch open/closed"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]
    if sw_id not in net.switch.index.get_level_values(0):
        raise HTTPException(status_code=404, detail="Switch not found")
    mask = net.switch.index.get_level_values(0) == sw_id
    net.switch.loc[mask, "closed"] = ~net.switch.loc[mask, "closed"].astype(bool)
    closed = bool(net.switch.loc[mask, "closed"].iloc[0])
    return {"message": "Switch toggled", "closed": closed}


@app.delete("/api/networks/{network_id}/switches/{sw_id}")
async def delete_switch(network_id: str, sw_id: int):
    """Delete a switch"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]
    if sw_id not in net.switch.index.get_level_values(0):
        raise HTTPException(status_code=404, detail="Switch not found")
    try:
        net.switch = net.switch[net.switch.index.get_level_values(0) != sw_id]
        return {"message": "Switch deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# ==================== STANDARD TYPE LIBRARIES ====================


@app.get("/api/standard-types/lines")
async def get_line_standard_types():
    """Get available line standard types"""
    net = mc.create_empty_network()
    types = list(net.std_types["sequence"].keys())
    return {"line_types": types, "model_types": ["sequence", "configuration", "matrix"]}


@app.get("/api/standard-types/transformers")
async def get_transformer_standard_types():
    """Get available transformer standard types"""
    net = mc.create_empty_network()
    types = list(net.std_types["trafo"].keys())
    return {"transformer_types": types}


@app.get("/api/standard-types/transformers-3w")
async def get_transformer3w_standard_types():
    """Get available 3-winding transformer standard types"""
    net = mc.create_empty_network()
    types = list(net.std_types["trafo3w"].keys()) if "trafo3w" in net.std_types else []
    return {"transformer3w_types": types}


# ==================== TIME SERIES ANALYSIS ====================


def _generate_time_series_profiles(
    net,
    timesteps: int,
    seed: Optional[int],
    custom_profiles: Optional[Dict[str, List[float]]],
) -> pd.DataFrame:
    """Create basic time-series profiles for loads and static generators."""
    if custom_profiles:
        df = pd.DataFrame(custom_profiles)
        if len(df) != timesteps:
            raise HTTPException(
                status_code=400,
                detail="Provided profiles length does not match requested timesteps",
            )
        return df.reset_index(drop=True)

    rng = np.random.default_rng(seed)
    profiles = pd.DataFrame(index=range(timesteps))

    load_indices = (
        sorted(set(net.asymmetric_load.index.get_level_values(0)))
        if not net.asymmetric_load.empty
        else []
    )
    sgen_indices = (
        sorted(set(net.asymmetric_sgen.index.get_level_values(0)))
        if not net.asymmetric_sgen.empty
        else []
    )

    if not load_indices and not sgen_indices:
        raise HTTPException(
            status_code=400,
            detail="Add at least one load or static generator before running time series",
        )

    # Smooth load variation
    load_shape = 0.7 + 0.3 * np.sin(np.linspace(-np.pi / 2, 3 * np.pi / 2, timesteps))
    for li in load_indices:
        base_p = (
            float(
                net.asymmetric_load.loc[
                    net.asymmetric_load.index.get_level_values(0) == li, "p_mw"
                ].sum()
            )
            if "p_mw" in net.asymmetric_load.columns
            else 0.0
        )
        noise = rng.normal(1.0, 0.05, timesteps)
        profiles[f"load_{li}_p"] = np.maximum(0, base_p * load_shape * noise)

    # Solar-like curve for static generators
    solar_shape = np.clip(
        np.sin(np.linspace(-np.pi / 2, 3 * np.pi / 2, timesteps)), 0, None
    )
    for si in sgen_indices:
        base_p = (
            float(
                net.asymmetric_sgen.loc[
                    net.asymmetric_sgen.index.get_level_values(0) == si, "p_mw"
                ].sum()
            )
            if "p_mw" in net.asymmetric_sgen.columns
            else 0.0
        )
        cloud = rng.normal(0.9, 0.08, timesteps)
        profiles[f"sgen_{si}_p"] = np.maximum(0, base_p * solar_shape * cloud)

    return profiles.fillna(0.0).reset_index(drop=True)


def _run_time_series_steps(net, profiles: pd.DataFrame) -> Dict[str, Any]:
    """Iterate through timesteps, updating injections and running power flow."""
    net_copy = deepcopy(net)
    time_steps = list(range(len(profiles)))

    outputs: Dict[str, Any] = {
        "res_bus": [],
        "res_line": [],
        "res_load": [],
        "res_sgen": [],
        "res_ext_grid": [],
        "convergence": [],
        "errors": [],
    }

    load_cols = [c for c in profiles.columns if c.startswith("load_")]
    sgen_cols = [c for c in profiles.columns if c.startswith("sgen_")]
    load_indices = [int(c.split("_")[1]) for c in load_cols]
    sgen_indices = [int(c.split("_")[1]) for c in sgen_cols]

    for t in time_steps:
        # Update injections
        for idx, col in zip(load_indices, load_cols):
            mask = net_copy.asymmetric_load.index.get_level_values(0) == idx
            phases = net_copy.asymmetric_load.loc[mask, "from_phase"].tolist()
            per_phase = _split_three_phase(float(profiles.at[t, col]), phases)
            net_copy.asymmetric_load.loc[mask, "p_mw"] = per_phase
        for idx, col in zip(sgen_indices, sgen_cols):
            mask = net_copy.asymmetric_sgen.index.get_level_values(0) == idx
            phases = net_copy.asymmetric_sgen.loc[mask, "from_phase"].tolist()
            per_phase = _split_three_phase(float(profiles.at[t, col]), phases)
            net_copy.asymmetric_sgen.loc[mask, "p_mw"] = per_phase

        converged = True
        error_msg = None
        try:
            mc.run_pf(net_copy, MaxIter=50, run_control=True)
            sce_general.additional_res_bus_columns(net_copy)
        except Exception as e:
            converged = False
            error_msg = str(e)

        outputs["convergence"].append({"timestep": t, "converged": converged})
        if not converged:
            outputs["errors"].append({"timestep": t, "error": error_msg})
            continue

        # Collect results for this step
        if not net_copy.res_bus.empty:
            for row in _phase_suffix_results_bus(net_copy):
                row["timestep"] = t
                outputs["res_bus"].append(row)

        if not net_copy.res_line.empty:
            for row in _phase_suffix_results_line(net_copy):
                row["timestep"] = t
                outputs["res_line"].append(row)

        if not net_copy.res_asymmetric_load.empty:
            load_df = (
                net_copy.res_asymmetric_load.replace({np.nan: None})
                .reset_index()
                .rename(columns={"index": "load"})
            )
            load_df["timestep"] = t
            outputs["res_load"].extend(load_df.to_dict(orient="records"))

        if not net_copy.res_asymmetric_sgen.empty:
            sgen_df = (
                net_copy.res_asymmetric_sgen.replace({np.nan: None})
                .reset_index()
                .rename(columns={"index": "sgen"})
            )
            sgen_df["timestep"] = t
            outputs["res_sgen"].extend(sgen_df.to_dict(orient="records"))

        if not net_copy.res_ext_grid.empty:
            eg_df = (
                net_copy.res_ext_grid.replace({np.nan: None})
                .reset_index()
                .rename(columns={"index": "ext_grid"})
            )
            eg_df["timestep"] = t
            outputs["res_ext_grid"].extend(eg_df.to_dict(orient="records"))

    outputs["converged"] = all(item["converged"] for item in outputs["convergence"])
    return outputs


@app.post("/api/networks/{network_id}/analysis/time-series")
async def run_time_series(
    network_id: str, options: TimeSeriesOptions = TimeSeriesOptions()
):
    """Run a lightweight time series simulation using generated profiles."""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")

    net = networks_store[network_id]
    profiles = _generate_time_series_profiles(
        net, options.timesteps, options.seed, options.profiles
    )
    results = _run_time_series_steps(net, profiles)

    profiles_payload = profiles.reset_index().rename(columns={"index": "timestep"})
    profiles_payload = profiles_payload.replace({np.nan: None}).to_dict(
        orient="records"
    )

    return {
        "success": results["converged"],
        "type": "time_series",
        "inputs": {
            "timesteps": options.timesteps,
            "seed": options.seed,
            "profiles": profiles_payload,
            "columns": list(profiles.columns),
        },
        "results": results,
        "error": results["errors"][0]["error"] if results["errors"] else None,
    }


# ==================== POWER FLOW ANALYSIS ====================


@app.post("/api/networks/{network_id}/analysis/powerflow")
async def run_power_flow(network_id: str, options: Optional[PowerFlowOptions] = None):
    """Run AC power flow analysis"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]

    if options is None:
        options = PowerFlowOptions()

    try:
        mc.run_pf(net, MaxIter=options.max_iteration, run_control=True)
        sce_general.additional_res_bus_columns(net)

        results = {
            "converged": bool(getattr(net, "converged", True)),
            "res_bus": _phase_suffix_results_bus(net),
            "res_line": _phase_suffix_results_line(net),
            "res_load": net.res_asymmetric_load.replace({np.nan: None})
            .reset_index()
            .to_dict(orient="records")
            if hasattr(net, "res_asymmetric_load") and not net.res_asymmetric_load.empty
            else [],
            "res_sgen": net.res_asymmetric_sgen.replace({np.nan: None})
            .reset_index()
            .to_dict(orient="records")
            if hasattr(net, "res_asymmetric_sgen") and not net.res_asymmetric_sgen.empty
            else [],
            "res_ext_grid": net.res_ext_grid.replace({np.nan: None})
            .reset_index()
            .to_dict(orient="records")
            if hasattr(net, "res_ext_grid") and not net.res_ext_grid.empty
            else [],
        }

        return {"success": True, "results": results}
    except Exception as e:
        return {"success": False, "error": str(e), "converged": False}


@app.post("/api/networks/{network_id}/analysis/load-allocation")
async def run_load_allocation_endpoint(
    network_id: str, options: Optional[LoadAllocationOptions] = None
):
    """Run load allocation based on measurement data."""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]

    if options is None:
        options = LoadAllocationOptions()

    if not hasattr(net, "measurement") or net.measurement.empty:
        raise HTTPException(status_code=400, detail="No measurements found in network")

    try:
        mc.run_pf(net, MaxIter=50, run_control=True)
        mg = build_measurement_graph(net)
        run_load_allocation(
            net,
            mg,
            adjust_after_load_flow=options.adjust_after_load_flow,
            tolerance=options.tolerance,
            ignore_fixed_capacitors=options.ignore_fixed_capacitors,
            ignore_controlled_capacitors=options.ignore_controlled_capacitors,
            ignore_generators=options.ignore_generators,
            cap_to_transformer_rating=options.cap_to_transformer_rating,
            cap_to_load_rating=options.cap_to_load_rating,
            trafo_overload_factor=options.trafo_overload_factor,
            adjust_power_factor=options.adjust_power_factor,
            measurement_indices=options.measurement_indices,
            max_iter=options.max_iter,
            verbose=options.verbose,
        )
        mc.run_pf(net, MaxIter=50, run_control=True)
        sce_general.additional_res_bus_columns(net)

        measurements = []
        meas_df = net.measurement.reset_index()
        for _, row in meas_df.iterrows():
            meas_record = row.to_dict()
            try:
                simulated = get_simulated_measurement_value(net, row)
                target = float(row.get("value", np.nan))
                delta = None if not np.isfinite(target) else float(target - simulated)
                meas_record["simulated"] = simulated
                meas_record["delta"] = delta
            except Exception as exc:
                meas_record["simulated"] = None
                meas_record["delta"] = None
                meas_record["error"] = str(exc)
            measurements.append(meas_record)

        results = {
            "converged": bool(getattr(net, "converged", True)),
            "res_bus": _phase_suffix_results_bus(net),
            "res_line": _phase_suffix_results_line(net),
            "res_load": net.res_asymmetric_load.replace({np.nan: None})
            .reset_index()
            .to_dict(orient="records")
            if hasattr(net, "res_asymmetric_load") and not net.res_asymmetric_load.empty
            else [],
            "res_sgen": net.res_asymmetric_sgen.replace({np.nan: None})
            .reset_index()
            .to_dict(orient="records")
            if hasattr(net, "res_asymmetric_sgen") and not net.res_asymmetric_sgen.empty
            else [],
            "res_ext_grid": net.res_ext_grid.replace({np.nan: None})
            .reset_index()
            .to_dict(orient="records")
            if hasattr(net, "res_ext_grid") and not net.res_ext_grid.empty
            else [],
            "measurements": measurements,
        }

        return {"success": True, "results": results}
    except Exception as e:
        return {"success": False, "error": str(e), "converged": False}


@app.post("/api/networks/{network_id}/analysis/powerflow-dc")
async def run_dc_power_flow(network_id: str):
    """Run DC power flow analysis"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    raise HTTPException(
        status_code=501, detail="DC power flow is not supported in multiconductor"
    )


# ==================== SHORT CIRCUIT ANALYSIS ====================


def _short_circuit_runner(network_id: str, options: Optional[ShortCircuitOptions]):
    """Shared logic so both POST and GET (fallback) can execute the analysis."""
    raise HTTPException(
        status_code=501,
        detail="Short-circuit analysis is not supported in multiconductor yet",
    )


@app.post("/api/networks/{network_id}/analysis/short-circuit")
async def run_short_circuit(
    network_id: str, options: Optional[ShortCircuitOptions] = None
):
    """Run short circuit analysis (POST)."""
    return _short_circuit_runner(network_id, options)


# Some environments accidentally call this endpoint with GET (e.g., misconfigured proxies).
# Provide a safe fallback that still runs the analysis instead of returning 405.
@app.get("/api/networks/{network_id}/analysis/short-circuit")
async def run_short_circuit_get(network_id: str):
    return _short_circuit_runner(network_id, None)


# ==================== OPTIMAL POWER FLOW ====================

# ==================== HOSTING CAPACITY ANALYSIS ====================


def _run_hosting_capacity(options: HostingCapacityOptions):
    """Compute hosting capacity and return statistics plus chart image."""
    net_id = options.network_id
    rng = np.random.default_rng(options.seed)
    installed_vals: List[float] = []
    violations: List[str] = []

    for _ in range(options.iterations):
        net = prepare_network_for_hc(net_id)
        installed = 0.0

        while True:
            try:
                mc.run_pf(net, MaxIter=50, run_control=True)
                sce_general.additional_res_bus_columns(net)
            except Exception as e:
                return {"success": False, "error": f"Power flow failed: {str(e)}"}

            if not net.res_line.empty and "i_ka" in net.res_line.columns:
                line_loading = float(net.res_line["i_ka"].max())
            else:
                line_loading = 0.0
            trafo_loading = 0.0
            if not net.res_bus.empty and "vm_pu" in net.res_bus.columns:
                vm_max = float(net.res_bus["vm_pu"].max())
            else:
                vm_max = 1.0

            violation_type = None
            if line_loading > options.loading_limit:
                violation_type = "Line Overloading"
            elif trafo_loading > options.loading_limit:
                violation_type = "Transformer Overloading"
            elif vm_max > options.voltage_limit:
                violation_type = "Voltage Violation"

            if violation_type:
                installed_vals.append(float(installed))
                violations.append(violation_type)
                break

            # Add a new PV plant
            plant_size = float(
                max(0.0, rng.normal(options.plant_mean_mw, options.plant_std_mw))
            )
            candidate_buses = (
                net.asymmetric_load["bus"].unique()
                if not net.asymmetric_load.empty
                else net.bus.index.get_level_values(0).unique().values
            )
            chosen_bus = int(rng.choice(candidate_buses))
            mc.create_asymmetric_sgen(
                net,
                bus=chosen_bus,
                from_phase=DEFAULT_PHASES,
                to_phase=0,
                p_mw=_split_three_phase(plant_size, DEFAULT_PHASES),
                q_mvar=_split_three_phase(0.0, DEFAULT_PHASES),
                name="HC Plant",
            )
            installed += plant_size

    if len(installed_vals) == 0:
        return {"success": False, "error": "No results generated"}

    # Build statistics
    stats = {
        "mean": float(np.mean(installed_vals)),
        "median": float(np.median(installed_vals)),
        "max": float(np.max(installed_vals)),
        "min": float(np.min(installed_vals)),
        "iterations": len(installed_vals),
    }
    violation_counts = pd.Series(violations).value_counts().to_dict()

    # Create chart image (boxplot + pie)
    sns.set_style("whitegrid", {"axes.grid": False})
    fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(10, 5))

    sns.boxplot(y=installed_vals, width=0.2, ax=axes[0], orient="v", color="#3b82f6")
    axes[0].set_xticklabels([""])
    axes[0].set_ylabel("Installed Capacity [MW]")

    axes[1].axis("equal")
    pd.Series(violations).value_counts().plot(
        kind="pie", ax=axes[1], autopct=lambda x: f"{x:.0f} %"
    )
    axes[1].set_ylabel("")
    axes[1].set_xlabel("")
    plt.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight", dpi=160)
    plt.close(fig)
    image_base64 = base64.b64encode(buf.getvalue()).decode("ascii")

    return {
        "success": True,
        "stats": stats,
        "installed": installed_vals,
        "violation_counts": violation_counts,
        "violations": violations,
        "used_network": net_id or "default",
        "image_base64": image_base64,
        "image_mime": "image/png",
    }


@app.post("/api/analysis/hosting-capacity")
async def hosting_capacity(options: HostingCapacityOptions):
    """Run hosting capacity analysis. If network_id is omitted, uses a built-in multiconductor demo network."""
    return _run_hosting_capacity(options)


@app.post("/api/networks/{network_id}/analysis/hosting-capacity")
async def hosting_capacity_for_network(
    network_id: str, options: HostingCapacityOptions
):
    """Run hosting capacity analysis on a specific stored network."""
    options.network_id = network_id
    return _run_hosting_capacity(options)


@app.post("/api/networks/{network_id}/analysis/opf")
async def run_optimal_power_flow(network_id: str, options: Optional[OPFOptions] = None):
    """Run optimal power flow analysis"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    raise HTTPException(
        status_code=501, detail="Optimal power flow is not supported in multiconductor"
    )


# ==================== TOPOLOGY ANALYSIS ====================


@app.get("/api/networks/{network_id}/topology")
async def get_network_topology(network_id: str):
    """Get network topology for visualization"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]

    # Build nodes (buses)
    nodes = []
    if not net.bus.empty:
        df = net.bus.reset_index()
        bus_col = "index" if "index" in df.columns else df.columns[0]
        for bus_id, group in df.groupby(bus_col):
            bus = group.iloc[0]
            node = {
                "id": int(bus_id),
                "name": str(bus.get("name", f"Bus {bus_id}")),
                "vn_kv": float(bus["vn_kv"]),
                "type": str(bus.get("type", "b")),
                "in_service": bool(bus.get("in_service", True)),
            }

            # Add geodata if available
            if bus_id in net.bus_geodata.index:
                geo = net.bus_geodata.loc[bus_id]
                node["x"] = float(geo["x"]) if "x" in geo else 0
                node["y"] = float(geo["y"]) if "y" in geo else 0

            # Check for connected elements
            node["has_load"] = (
                int(bus_id) in net.asymmetric_load["bus"].values
                if not net.asymmetric_load.empty
                else False
            )
            node["has_gen"] = (
                int(bus_id) in net.asymmetric_gen["bus"].values
                if not net.asymmetric_gen.empty
                else False
            )
            node["has_sgen"] = (
                int(bus_id) in net.asymmetric_sgen["bus"].values
                if not net.asymmetric_sgen.empty
                else False
            )
            node["has_ext_grid"] = (
                int(bus_id) in net.ext_grid["bus"].values
                if not net.ext_grid.empty
                else False
            )
            nodes.append(node)

    # Build edges (lines and transformers)
    edges = []

    # Lines
    if not net.line.empty:
        df = net.line.reset_index()
        line_col = "index" if "index" in df.columns else df.columns[0]
        for line_id, group in df.groupby(line_col):
            line = group.iloc[0]
            phases_val = line.get("from_phase")
            if isinstance(phases_val, (list, tuple, np.ndarray, pd.Series)):
                phase_count = len(phases_val)
            else:
                phase_count = 1
            edges.append(
                {
                    "id": f"line_{line_id}",
                    "type": "line",
                    "from": int(line["from_bus"]),
                    "to": int(line["to_bus"]),
                    "name": str(line.get("name", f"Line {line_id}")),
                    "in_service": bool(line.get("in_service", True)),
                    "length_km": float(line.get("length_km", 0)),
                    "phase_count": int(phase_count),
                }
            )

    # Transformers
    if hasattr(net, "trafo1ph") and not net.trafo1ph.empty:
        df = net.trafo1ph.reset_index()
        trafo_col = "index" if "index" in df.columns else df.columns[0]
        for trafo_id, group in df.groupby(trafo_col):
            trafo = group.iloc[0]
            edges.append(
                {
                    "id": f"trafo_{trafo_id}",
                    "type": "transformer",
                    "from": int(trafo["bus"])
                    if "bus" in trafo
                    else int(trafo["from_bus"]),
                    "to": int(trafo["bus"])
                    if "bus" in trafo
                    else int(trafo.get("to_bus", 0)),
                    "name": str(trafo.get("name", f"Trafo {trafo_id}")),
                    "in_service": bool(trafo.get("in_service", True)),
                }
            )

    return {"nodes": nodes, "edges": edges}


# ==================== NETWORK STATISTICS ====================


@app.get("/api/networks/{network_id}/statistics")
async def get_network_statistics(network_id: str):
    """Get network statistics and summary"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]

    stats = {
        "name": net.name if hasattr(net, "name") else "Unnamed",
        "f_hz": float(net.f_hz) if hasattr(net, "f_hz") else 50.0,
        "sn_mva": float(net.sn_mva) if hasattr(net, "sn_mva") else 1.0,
        "element_counts": {
            "bus": len(net.bus.index.get_level_values(0).unique())
            if not net.bus.empty
            else 0,
            "line": len(net.line.index.get_level_values(0).unique())
            if not net.line.empty
            else 0,
            "trafo": len(net.trafo1ph.index.get_level_values(0).unique())
            if hasattr(net, "trafo1ph") and not net.trafo1ph.empty
            else 0,
            "trafo3w": 0,
            "load": len(net.asymmetric_load.index.get_level_values(0).unique())
            if not net.asymmetric_load.empty
            else 0,
            "gen": len(net.asymmetric_gen.index.get_level_values(0).unique())
            if not net.asymmetric_gen.empty
            else 0,
            "sgen": len(net.asymmetric_sgen.index.get_level_values(0).unique())
            if not net.asymmetric_sgen.empty
            else 0,
            "ext_grid": len(net.ext_grid.index.get_level_values(0).unique())
            if not net.ext_grid.empty
            else 0,
            "shunt": 0,
            "switch": len(net.switch.index.get_level_values(0).unique())
            if not net.switch.empty
            else 0,
        },
        "total_load_mw": float(net.asymmetric_load["p_mw"].sum())
        if not net.asymmetric_load.empty
        else 0,
        "total_load_mvar": float(net.asymmetric_load["q_mvar"].sum())
        if not net.asymmetric_load.empty
        else 0,
        "total_gen_mw": float(net.asymmetric_gen["p_mw"].sum())
        if not net.asymmetric_gen.empty
        else 0,
        "total_sgen_mw": float(net.asymmetric_sgen["p_mw"].sum())
        if not net.asymmetric_sgen.empty
        else 0,
        "voltage_levels": sorted(net.bus["vn_kv"].unique().tolist())
        if not net.bus.empty
        else [],
    }

    return stats


# ==================== VALIDATION ====================


@app.get("/api/networks/{network_id}/validate")
async def validate_network(network_id: str):
    """Validate network for power flow"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]

    issues = []

    # Check for buses
    if net.bus.empty:
        issues.append({"severity": "error", "message": "Network has no buses"})

    # Check for slack bus / ext_grid
    if net.ext_grid.empty and (
        net.asymmetric_gen.empty
        or not net.asymmetric_gen.get("slack", pd.Series(False)).any()
    ):
        issues.append(
            {
                "severity": "error",
                "message": "Network has no slack bus (external grid or slack generator)",
            }
        )

    # Check for isolated buses
    connected_buses = set()
    if not net.line.empty:
        connected_buses.update(net.line["from_bus"].values)
        connected_buses.update(net.line["to_bus"].values)
    if (
        hasattr(net, "trafo1ph")
        and not net.trafo1ph.empty
        and "bus" in net.trafo1ph.columns
    ):
        connected_buses.update(net.trafo1ph["bus"].values)

    bus_ids = (
        set(net.bus.index.get_level_values(0).unique()) if not net.bus.empty else set()
    )
    isolated = bus_ids - connected_buses
    if isolated and len(bus_ids) > 1:
        issues.append(
            {
                "severity": "warning",
                "message": f"Isolated buses found: {list(isolated)}",
            }
        )

    # Check for loads without generation
    if (
        not net.asymmetric_load.empty
        and net.ext_grid.empty
        and net.asymmetric_gen.empty
        and net.asymmetric_sgen.empty
    ):
        issues.append(
            {
                "severity": "warning",
                "message": "Network has loads but no generation sources",
            }
        )

    is_valid = not any(i["severity"] == "error" for i in issues)

    return {"valid": is_valid, "issues": issues}


# ==================== THREE WINDING TRANSFORMER ====================


@app.get("/api/networks/{network_id}/trafo3w")
async def get_trafo3w(network_id: str):
    """Get all 3-winding transformers"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]
    items = _table_records(net, "trafo3w")
    for i, item in enumerate(items):
        item["index"] = i
    return {"trafo3w": items}


# ==================== MOTOR ====================


@app.get("/api/networks/{network_id}/motors")
async def get_motors(network_id: str):
    """Get all motors"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]
    items = _table_records(net, "motor")
    for i, item in enumerate(items):
        item["index"] = i
    return {"motors": items}


# ==================== ASYMMETRIC LOAD ====================


@app.get("/api/networks/{network_id}/asymmetric-loads")
async def get_asymmetric_loads(network_id: str):
    """Get all asymmetric loads"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]
    items = _table_records(net, "asymmetric_load")
    for i, item in enumerate(items):
        item["index"] = i
    return {"asymmetric_loads": items}


# ==================== ASYMMETRIC SGEN ====================


@app.get("/api/networks/{network_id}/asymmetric-sgens")
async def get_asymmetric_sgens(network_id: str):
    """Get all asymmetric static generators"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]
    items = _table_records(net, "asymmetric_sgen")
    for i, item in enumerate(items):
        item["index"] = i
    return {"asymmetric_sgens": items}


# ==================== MEASUREMENTS ====================


@app.get("/api/networks/{network_id}/measurements")
async def get_measurements(network_id: str):
    """Get all measurements"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]
    items = _table_records(net, "measurement")
    for i, item in enumerate(items):
        if "index" not in item:
            item["index"] = i
    return {"measurements": items}


# ==================== DC LINE ====================


@app.get("/api/networks/{network_id}/dclines")
async def get_dclines(network_id: str):
    """Get all DC lines"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]
    items = _table_records(net, "dcline")
    for i, item in enumerate(items):
        item["index"] = i
    return {"dclines": items}


# ==================== STORAGE ====================


@app.get("/api/networks/{network_id}/storages")
async def get_storages(network_id: str):
    """Get all storage elements"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]
    items = _table_records(net, "storage")
    for i, item in enumerate(items):
        item["index"] = i
    return {"storages": items}


# ==================== SVC (Static Var Compensator) ====================


@app.get("/api/networks/{network_id}/svcs")
async def get_svcs(network_id: str):
    """Get all SVCs"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]
    items = _table_records(net, "svc")
    for i, item in enumerate(items):
        item["index"] = i
    return {"svcs": items}


# ==================== TCSC (Thyristor-Controlled Series Capacitor) ====================


@app.get("/api/networks/{network_id}/tcscs")
async def get_tcscs(network_id: str):
    """Get all TCSCs"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]
    items = _table_records(net, "tcsc")
    for i, item in enumerate(items):
        item["index"] = i
    return {"tcscs": items}


# ==================== SSC (Static Synchronous Compensator) ====================


@app.get("/api/networks/{network_id}/sscs")
async def get_sscs(network_id: str):
    """Get all SSCs"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]
    items = _table_records(net, "ssc")
    for i, item in enumerate(items):
        item["index"] = i
    return {"sscs": items}


# ==================== WARD / XWARD ====================


@app.get("/api/networks/{network_id}/wards")
async def get_wards(network_id: str):
    """Get all ward equivalents"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]
    items = _table_records(net, "ward")
    for i, item in enumerate(items):
        item["index"] = i
    return {"wards": items}


@app.get("/api/networks/{network_id}/xwards")
async def get_xwards(network_id: str):
    """Get all extended ward equivalents"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]
    items = _table_records(net, "xward")
    for i, item in enumerate(items):
        item["index"] = i
    return {"xwards": items}


# ==================== IMPEDANCE ====================


@app.get("/api/networks/{network_id}/impedances")
async def get_impedances(network_id: str):
    """Get all impedance elements"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]
    items = _table_records(net, "impedance")
    for i, item in enumerate(items):
        item["index"] = i
    return {"impedances": items}


# ==================== ALL ELEMENTS SUMMARY ====================


@app.get("/api/networks/{network_id}/elements")
async def get_all_elements(network_id: str):
    """Get counts of all element types"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]

    def get_count(df):
        if isinstance(df, pd.DataFrame) and not df.empty:
            if isinstance(df.index, pd.MultiIndex):
                return len(df.index.get_level_values(0).unique())
            return len(df)
        return 0

    elements = {
        # Sources
        "ext_grid": get_count(getattr(net, "ext_grid", pd.DataFrame())),
        # Equipment
        "bus": get_count(getattr(net, "bus", pd.DataFrame())),
        "line": get_count(getattr(net, "line", pd.DataFrame())),
        "switch": get_count(getattr(net, "switch", pd.DataFrame())),
        "trafo": get_count(getattr(net, "trafo1ph", pd.DataFrame())),
        "trafo3w": 0,
        # Generators
        "gen": get_count(getattr(net, "asymmetric_gen", pd.DataFrame())),
        "sgen": get_count(getattr(net, "asymmetric_sgen", pd.DataFrame())),
        "motor": 0,
        # Loads
        "load": get_count(getattr(net, "asymmetric_load", pd.DataFrame())),
        "asymmetric_load": get_count(getattr(net, "asymmetric_load", pd.DataFrame())),
        # DC
        "dcline": get_count(getattr(net, "dcline", pd.DataFrame())),
        "storage": get_count(getattr(net, "storage", pd.DataFrame())),
        # Other/FACTS
        "shunt": 0,
        "asymmetric_sgen": get_count(getattr(net, "asymmetric_sgen", pd.DataFrame())),
        "svc": get_count(getattr(net, "svc", pd.DataFrame())),
        "tcsc": get_count(getattr(net, "tcsc", pd.DataFrame())),
        "ssc": get_count(getattr(net, "ssc", pd.DataFrame())),
        "ward": get_count(getattr(net, "ward", pd.DataFrame())),
        "xward": get_count(getattr(net, "xward", pd.DataFrame())),
        "impedance": get_count(getattr(net, "impedance", pd.DataFrame())),
        # Measurements
        "measurement": get_count(getattr(net, "measurement", pd.DataFrame())),
    }

    return {"elements": elements}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8001)

# ==================== GENERIC ELEMENT MANAGEMENT ====================


@app.post("/api/networks/{network_id}/elements/{element_type}")
async def create_element_generic(
    network_id: str, element_type: str, data: Dict[str, Any] = Body(...)
):
    """Generic endpoint to create any element type"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]

    try:
        # Dynamically get the create function
        create_func_name = f"create_{element_type}"
        if not hasattr(mc, create_func_name):
            raise HTTPException(
                status_code=400, detail=f"Unsupported element type: {element_type}"
            )

        create_func = getattr(mc, create_func_name)

        # Remove 'index' if present to let pandapower handle it (or use it if needed)
        # Usually for creation we let pp assign index unless specified
        idx = data.pop("index", None)

        # Call the create function with network and data
        # We pass data as kwargs. We assume frontend sends correct keys.
        element_index = create_func(net, index=idx, **data)

        # Get the serialized data of the created element
        # element_type is usually singular (e.g., 'bus'), table is generic (e.g., 'bus')
        # Some exceptions exist, so we might need mapping if pp names differ from table names
        # But generally pp.create_X adds to net[X]

        # Handle some naming inconsistencies if any
        table_name = element_type

        if table_name in net and hasattr(net[table_name], "loc"):
            element_data = net[table_name].loc[element_index].to_dict()
            # handle nan/none
            for k, v in element_data.items():
                if isinstance(v, float) and np.isnan(v):
                    element_data[k] = None

            # Add index to result
            element_data["index"] = int(element_index)

            return {
                "message": f"{element_type} created",
                "index": int(element_index),
                "data": element_data,
            }

        return {"message": f"{element_type} created", "index": int(element_index)}

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(
            status_code=500, detail=f"Failed to create element: {str(e)}"
        )


@app.delete("/api/networks/{network_id}/elements/{element_type}/{index}")
async def delete_element_generic(network_id: str, element_type: str, index: int):
    """Generic endpoint to delete any element type"""
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]

    try:
        # Check if table exists
        if element_type not in net:
            raise HTTPException(
                status_code=400, detail=f"Table {element_type} not found in network"
            )

        # Check if index exists
        if isinstance(net[element_type].index, pd.MultiIndex):
            exists = index in net[element_type].index.get_level_values(0)
        else:
            exists = index in net[element_type].index
        if not exists:
            raise HTTPException(
                status_code=404,
                detail=f"Element {element_type} at index {index} not found",
            )

        # Drop element
        if isinstance(net[element_type].index, pd.MultiIndex):
            net[element_type] = net[element_type][
                net[element_type].index.get_level_values(0) != index
            ]
        else:
            net[element_type] = net[element_type].drop(index)

        return {"message": f"{element_type} {index} deleted"}

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(
            status_code=500, detail=f"Failed to delete element: {str(e)}"
        )


@app.put("/api/networks/{network_id}/layout")
async def update_network_layout(
    network_id: str,
    layout_data: Dict[str, Dict[str, float]] = Body(...),
):
    """
    Update bus geodata (x, y coordinates).
    layout_data format: { "bus_id": { "x": 123.4, "y": 567.8 }, ... }
    """
    if network_id not in networks_store:
        raise HTTPException(status_code=404, detail="Network not found")
    net = networks_store[network_id]

    try:
        updated_count = 0
        if "bus_geodata" not in net:
            net.bus_geodata = pd.DataFrame(columns=["x", "y"])

        # iterate over provided bus updates
        for bus_id_str, coords in layout_data.items():
            bus_idx = int(bus_id_str)
            if bus_idx in net.bus.index.get_level_values(0):
                # Ensure bus_geodata exists for this bus, create if needed
                if bus_idx not in net.bus_geodata.index:
                    # Initialize with 0 or provided coords if missing row
                    net.bus_geodata.loc[bus_idx, "x"] = coords.get("x", 0)
                    net.bus_geodata.loc[bus_idx, "y"] = coords.get("y", 0)
                else:
                    if "x" in coords:
                        net.bus_geodata.loc[bus_idx, "x"] = float(coords["x"])
                    if "y" in coords:
                        net.bus_geodata.loc[bus_idx, "y"] = float(coords["y"])
                updated_count += 1

        return {"message": f"Updated layout for {updated_count} buses"}

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(
            status_code=500, detail=f"Failed to update layout: {str(e)}"
        )


# ==================== PANDAPOWER MODEL MANAGEMENT ====================

OUTPUT_PANDAPOWER_PATH = Path(__file__).resolve().parent.parent / "output_pandapower"


@app.get("/api/pandapower/models")
async def list_pandapower_models():
    """List available generated Pandapower models"""
    if not OUTPUT_PANDAPOWER_PATH.exists():
        return {"models": []}

    models = []
    for f in OUTPUT_PANDAPOWER_PATH.glob("*.json"):
        models.append(f.stem)  # filename without extension

    return {"models": sorted(models)}


@app.post("/api/pandapower/load/{model_name}")
async def load_pandapower_model(model_name: str):
    """Load a specific Pandapower model file"""
    file_path = OUTPUT_PANDAPOWER_PATH / f"{model_name}.json"

    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"Model {model_name} not found")

    try:
        # Load from JSON
        # Note: pp.from_json typically returns a pandapowerNet object
        net = pp.from_json(str(file_path))

        # Ensure it has basic multiconductor structure if needed?
        # The output files were created with multiconductor so they should be fine.

        # We need a new ID for this session
        network_id = str(uuid.uuid4())

        # Name it
        if not hasattr(net, "name") or not net.name:
            net.name = model_name

        # Store in session
        networks_store[network_id] = net

        # Optionally persist to DB?
        # For now, treat as ephemeral load unless user explicitly saves?
        # But front-end expects it to behave like a loaded network.
        # Let's persist it so it shows up in lists?
        # Actually, let's keep it ephemeral for "Preview" unless imported.
        # Wait, the prompt says "load the model into the UI".
        # If we just put it in networks_store, the UI can access it via get_network.

        # Persist to DB to be safe and consistent with other networks
        db.create_network(network_id, net.name)
        db.save_version(network_id, network_to_dict(net), f"Loaded from {model_name}")

        return {
            "network_id": network_id,
            "name": net.name,
            "message": f"Loaded model {model_name}",
            "data": network_to_dict(net),
        }

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to load model: {str(e)}")
