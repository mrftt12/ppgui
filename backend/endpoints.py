from fastapi import APIRouter, HTTPException, Body
import os
import sys
import pandapower as pp
import json
import logging
import pickle
import re

# Add parent directory to path to allow importing multiconductor
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from multiconductor.pycci import cci_ica
from multiconductor.pycci import cci_thermal_model
from .network_converters import (
    pandapower_ieee118_to_ui,
    pandapower_ieee9_to_ui,
    pandapower_ieee30_to_ui,
    run_mc_loadflow,
    multiconductor_net_to_ui,
)

router = APIRouter()
logger = logging.getLogger(__name__)

MODELS_DIR = os.path.join(PROJECT_ROOT, "output_pandapower")
WORKSPACE_NETWORKS_DIR = os.path.join(PROJECT_ROOT, "networks")
LEGACY_NETWORKS_DIR = os.path.join(os.path.dirname(__file__), "networks")
NETWORKS_DIR = WORKSPACE_NETWORKS_DIR if os.path.exists(WORKSPACE_NETWORKS_DIR) else LEGACY_NETWORKS_DIR

# Ensure models directory exists
if not os.path.exists(MODELS_DIR):
    try:
        os.makedirs(MODELS_DIR)
    except OSError:
        pass

# Ensure networks directory exists
if not os.path.exists(NETWORKS_DIR):
    try:
        os.makedirs(NETWORKS_DIR)
    except OSError:
        pass


@router.get("/models")
async def list_models():
    """List available network models (JSON files)."""
    if not os.path.exists(MODELS_DIR):
        return {"models": []}

    files = [f for f in os.listdir(MODELS_DIR) if f.endswith(".json")]
    models = [{"id": f, "name": f} for f in files]
    return {"models": models}


@router.get("/models/{model_id}")
async def get_model(model_id: str):
    """Load and return a network model."""
    file_path = os.path.join(MODELS_DIR, model_id)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Model not found")

    try:
        with open(file_path, "r") as f:
            data = json.load(f)
        return {"id": model_id, "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/analyze")
async def analyze_network(request: dict = Body(...)):
    """Run load flow analysis on a model."""
    model_id = request.get("modelId")
    if not model_id:
        raise HTTPException(status_code=400, detail="modelId required")

    file_path = os.path.join(MODELS_DIR, model_id)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Model not found")

    try:
        # Load net
        net = pp.from_json(file_path)

        # Run ICA Streamlined
        results_df = cci_ica.run_ica_streamlined(net)

        if results_df is None:
            raise HTTPException(status_code=500, detail="Analysis failed")

        # Convert to records
        results = results_df.to_dict(orient="records")

        return {"results": results}

    except Exception as e:
        logger.error(f"Analysis error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/loadflow")
async def run_loadflow(request: dict = Body(...)):
    """
    Run load flow analysis on UI elements.

    This endpoint converts the UI element representation to a pandapower network
    and runs power flow analysis using the multiconductor package.
    """
    network_id = request.get("networkId", "unknown")
    elements = request.get("elements", [])
    connections = request.get("connections", [])

    logger.info(
        f"Running load flow for network {network_id} with {len(elements)} elements"
    )

    try:
        result = run_mc_loadflow(elements, connections, network_id)
        logger.info(
            f"Load flow completed: converged={result.get('converged')}, buses={len(result.get('busResults', []))}, branches={len(result.get('branchResults', []))}"
        )
        return result
    except Exception as e:
        logger.error(f"Load flow error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/ductbank/steady-state")
async def ductbank_steady_state(request: dict = Body(...)):
    """
    Calculate ductbank steady-state temperatures using cci_thermal_model.

    Expects:
      ductbank: { rows, columns, depth, verticalSpacing, horizontalSpacing, soilResistivity, ducts: [{row, column, load, loadFactor, diameter, thickness}] }
      cableType: optional list of cable parameters for cci_thermal_model
    """
    ductbank = request.get("ductbank") or {}
    rows = int(ductbank.get("rows", 1))
    cols = int(ductbank.get("columns", 1))
    ducts = ductbank.get("ducts") or []

    depth = float(ductbank.get("depth", 36))
    vert_spacing = float(ductbank.get("verticalSpacing", 6))
    horiz_spacing = float(ductbank.get("horizontalSpacing", 6))
    soil_rho = float(ductbank.get("soilResistivity", 90))

    # Build CurrentArray in row-major order (row 1..rows, col 1..cols)
    current_array = []
    for row in range(1, rows + 1):
        for col in range(1, cols + 1):
            match = next(
                (d for d in ducts if int(d.get("row", 0)) == row and int(d.get("column", 0)) == col),
                None,
            )
            load = float(match.get("load", 0)) if match else 0.0
            load_factor = float(match.get("loadFactor", 1)) if match else 1.0
            current_array.append(load * load_factor)

    # DuctBank array mapping based on original algorithm expectations
    # [0,1] unused, [2] fill rho, [4] depth, [5] vert spacing, [6] horiz spacing, [7] n_factor, [8] earth rho
    ductbank_list = [0.0, 0.0, soil_rho, 0.0, depth, vert_spacing, horiz_spacing, 1.0, soil_rho]

    cable_type = request.get("cableType")
    if not cable_type:
        # Default cable type template (indices used by calculate_steady_state)
        cable_type = [0.0] * 32
        cable_type[0] = float(ductbank.get("ductDiameterIn", 5)) * 0.2
        cable_type[2] = 0.03
        cable_type[3] = 0.25
        cable_type[4] = 0.03
        cable_type[7] = 16
        cable_type[10] = 1.0
        cable_type[11] = 25.0
        cable_type[13] = 13.8
        cable_type[14] = 3.5
        cable_type[15] = 6.0
        cable_type[19] = soil_rho
        cable_type[31] = depth

    try:
        temps = cci_thermal_model.calculate_steady_state(
            current_array,
            cable_type,
            ductbank_list,
            cols,
        )
        return {
            "rows": rows,
            "columns": cols,
            "temperatures": temps,
        }
    except Exception as e:
        logger.error(f"Ductbank steady-state error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/samples/ieee-118")
async def sample_ieee_118():
    try:
        return pandapower_ieee118_to_ui()
    except Exception as e:
        logger.error(f"IEEE 118 sample generation error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/samples/ieee-9")
async def sample_ieee_9():
    try:
        return pandapower_ieee9_to_ui()
    except Exception as e:
        logger.error(f"IEEE 9 sample generation error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/samples/ieee-30")
async def sample_ieee_30():
    try:
        return pandapower_ieee30_to_ui()
    except Exception as e:
        logger.error(f"IEEE 30 sample generation error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Display-name mapping for geospatial networks
NETWORK_DISPLAY_NAMES = {
    "st_charles_geo": "St. Charles",
    "barcelona_geo": "Barcelona",
}


@router.get("/api/getnetworks")
async def list_test_networks():
    try:
        if not os.path.exists(NETWORKS_DIR):
            return {"networks": []}
        files = [f for f in os.listdir(NETWORKS_DIR) if f.endswith(".pkl")]
        names = []
        for f in sorted(files):
            file_path = os.path.join(NETWORKS_DIR, f)
            try:
                with open(file_path, "rb") as fh:
                    net = pickle.load(fh)
                has_bus_geo = (
                    hasattr(net, "bus_geodata")
                    and net.bus_geodata is not None
                    and not net.bus_geodata.empty
                )
                has_line_geo = (
                    hasattr(net, "line_geodata")
                    and net.line_geodata is not None
                    and not net.line_geodata.empty
                )
                has_bus = (
                    hasattr(net, "bus")
                    and net.bus is not None
                    and not net.bus.empty
                )
                if (has_bus_geo and has_line_geo) or has_bus:
                    stem = os.path.splitext(f)[0]
                    names.append({
                        "name": stem,
                        "displayName": NETWORK_DISPLAY_NAMES.get(stem, stem),
                        "hasGeodata": has_bus_geo and has_line_geo,
                    })
            except Exception:
                # Skip files that can't be loaded
                continue
        return {"networks": names}
    except Exception as e:
        logger.error(f"Test network listing error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/test-networks/{network_name}")
async def load_test_network(network_name: str):
    try:
        cleaned = network_name[:-4] if network_name.lower().endswith(".pkl") else network_name
        if not re.match(r"^[A-Za-z0-9_\\-\\.]+$", cleaned):
            raise HTTPException(status_code=400, detail="Invalid network name")
        file_path = os.path.join(NETWORKS_DIR, f"{cleaned}.pkl")
        if not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail="Network not found")
        with open(file_path, "rb") as f:
            net = pickle.load(f)
        payload = multiconductor_net_to_ui(net)
        payload["name"] = NETWORK_DISPLAY_NAMES.get(cleaned, cleaned)
        payload["description"] = "Multiconductor test network"
        return payload
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Test network load error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
