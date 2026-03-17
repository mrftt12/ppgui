"""Fix strategies for all diagnostic categories.

Each fix function follows the signature::

    def fix_xxx(net) -> str:
        '''Mutate *net* in-place, return a description of what changed.'''

Fix functions are grouped into a registry that maps category names to
ordered lists of (fix_id, description, fix_fn, scope_categories) tuples.
"""
from __future__ import annotations

import math
from typing import Any

import pandas as pd

from ._common import (
    get_table,
    unique_bus_ids,
    elem_id,
    get_bus_vn_kv,
    is_in_service,
    safe_float,
    dedup_iter,
    get_element_circuits,
    trafo1ph_bus_pair,
    build_network_graph,
    bfs_from_sources,
)


# ===================================================================
# VOLTAGE BASE fixes
# ===================================================================

def fix_vb_01_set_missing_vn_kv_from_transformer(net: Any) -> str:
    """For buses with vn_kv=0/NaN that are on a transformer winding,
    copy the transformer winding vn_kv to the bus."""
    bus = get_table(net, "bus")
    trafo1ph = get_table(net, "trafo1ph")
    if bus.empty or trafo1ph.empty or "vn_kv" not in bus.columns:
        return "No bus or trafo1ph table."
    if not isinstance(trafo1ph.index, pd.MultiIndex) or "bus" not in trafo1ph.index.names:
        return "trafo1ph missing MultiIndex with bus level."

    bus_lv = trafo1ph.index.names.index("bus")
    idx_level = trafo1ph.index.names.index("index") if "index" in trafo1ph.index.names else 0

    # Build map: bus_id -> winding vn_kv from trafo1ph
    trafo_bus_vn: dict[Any, float] = {}
    if "vn_kv" in trafo1ph.columns:
        for tid in trafo1ph.index.get_level_values(idx_level).unique():
            try:
                sub = trafo1ph.xs(tid, level=idx_level)
                for tb in sub.index.get_level_values(
                    sub.index.names.index("bus") if "bus" in sub.index.names else 0
                ).unique():
                    winding = sub.xs(tb, level=sub.index.names.index("bus") if "bus" in sub.index.names else 0)
                    vn = safe_float(winding["vn_kv"].iloc[0] if hasattr(winding["vn_kv"], "iloc") else winding["vn_kv"])
                    if vn and vn > 0:
                        trafo_bus_vn[tb] = vn
            except Exception:
                continue

    fixed = 0
    for bus_id in unique_bus_ids(bus):
        vn = get_bus_vn_kv(bus, bus_id)
        if vn is not None and vn > 0:
            continue
        if bus_id not in trafo_bus_vn:
            continue
        new_vn = trafo_bus_vn[bus_id]
        if isinstance(bus.index, pd.MultiIndex):
            mask = bus.index.get_level_values(0) == bus_id
            net.bus.loc[mask, "vn_kv"] = new_vn
        else:
            net.bus.at[bus_id, "vn_kv"] = new_vn
        fixed += 1

    return f"Set vn_kv on {fixed} buses from transformer winding data."


def fix_vb_04_unit_confusion_volts_to_kv(net: Any) -> str:
    """Convert vn_kv < 0.1 to kV (× 1000) — likely stored in Volts."""
    bus = get_table(net, "bus")
    if bus.empty or "vn_kv" not in bus.columns:
        return "No bus table."

    fixed = 0
    for bus_id in unique_bus_ids(bus):
        vn = get_bus_vn_kv(bus, bus_id)
        if vn is None or vn <= 0 or vn >= 0.1:
            continue
        new_vn = vn * 1000.0
        if isinstance(bus.index, pd.MultiIndex):
            mask = bus.index.get_level_values(0) == bus_id
            net.bus.loc[mask, "vn_kv"] = new_vn
        else:
            net.bus.at[bus_id, "vn_kv"] = new_vn
        fixed += 1

    return f"Converted {fixed} bus vn_kv from Volts to kV (× 1000)."


# ===================================================================
# TRANSFORMER fixes
# ===================================================================

def fix_tx_01_default_zero_impedance(net: Any) -> str:
    """Set default vk_percent (4.0) where it is zero, and default
    vkr_percent (1.0) where it is missing/NaN."""
    trafo1ph = get_table(net, "trafo1ph")
    if trafo1ph.empty:
        return "No trafo1ph table."

    fixed = 0
    DEFAULT_VK = 4.0
    DEFAULT_VKR = 1.0

    if "vk_percent" in trafo1ph.columns:
        mask_zero = trafo1ph["vk_percent"].apply(lambda v: safe_float(v) == 0)
        if mask_zero.any():
            net.trafo1ph.loc[mask_zero, "vk_percent"] = DEFAULT_VK
            fixed += int(mask_zero.sum())

    if "vkr_percent" in trafo1ph.columns:
        mask_nan = trafo1ph["vkr_percent"].isna()
        if mask_nan.any():
            net.trafo1ph.loc[mask_nan, "vkr_percent"] = DEFAULT_VKR
            fixed += int(mask_nan.sum())

    return f"Set default impedance on {fixed} trafo1ph rows."


def fix_tx_02_winding_vn_kv_from_bus(net: Any) -> str:
    """Set transformer winding vn_kv to match the connected bus vn_kv
    when they differ by more than 10%. The bus voltage is authoritative."""
    trafo1ph = get_table(net, "trafo1ph")
    bus = get_table(net, "bus")
    if trafo1ph.empty or bus.empty or "vn_kv" not in trafo1ph.columns:
        return "No trafo1ph or bus table."
    if not isinstance(trafo1ph.index, pd.MultiIndex) or "bus" not in trafo1ph.index.names:
        return "trafo1ph missing MultiIndex with bus level."

    idx_level = trafo1ph.index.names.index("index") if "index" in trafo1ph.index.names else 0
    bus_level = trafo1ph.index.names.index("bus")

    fixed = 0
    for full_idx, row in trafo1ph.iterrows():
        winding_vn = safe_float(row.get("vn_kv"))
        if winding_vn is None or winding_vn <= 0:
            continue
        # Get the bus id from the MultiIndex tuple
        tb = full_idx[bus_level]
        bus_vn = get_bus_vn_kv(bus, tb)
        if bus_vn is None or bus_vn <= 0:
            continue
        ratio = abs(bus_vn - winding_vn) / max(bus_vn, winding_vn)
        if ratio > 0.10:
            net.trafo1ph.at[full_idx, "vn_kv"] = bus_vn
            fixed += 1

    return f"Updated vn_kv on {fixed} trafo1ph winding rows to match bus."


def fix_tx_04_tap_at_extreme_reset(net: Any) -> str:
    """Reset tap_pos to tap_neutral when tap is at min or max."""
    trafo1ph = get_table(net, "trafo1ph")
    if trafo1ph.empty:
        return "No trafo1ph table."

    cols_needed = {"tap_pos", "tap_neutral", "tap_min", "tap_max"}
    if not cols_needed.issubset(trafo1ph.columns):
        return "Missing tap columns."

    fixed = 0
    for idx, row in trafo1ph.iterrows():
        tap = safe_float(row.get("tap_pos"))
        neutral = safe_float(row.get("tap_neutral"))
        tap_min = safe_float(row.get("tap_min"))
        tap_max = safe_float(row.get("tap_max"))
        if tap is None or neutral is None or tap_min is None or tap_max is None:
            continue
        if tap == tap_min or tap == tap_max:
            net.trafo1ph.at[idx, "tap_pos"] = neutral
            fixed += 1

    return f"Reset {fixed} trafo1ph tap positions from extreme to neutral."


# ===================================================================
# GROUNDING fixes
# ===================================================================

def fix_gnd_03_ground_source_bus_neutral(net: Any) -> str:
    """Set grounded=True and grounding_r_ohm=1e-6 on source bus neutral phases."""
    bus = get_table(net, "bus")
    if bus.empty or not isinstance(bus.index, pd.MultiIndex):
        return "No MultiIndex bus table."
    if "grounded" not in bus.columns:
        return "No grounded column."

    source_buses: set = set()
    for tbl_name in ("ext_grid", "ext_grid_sequence"):
        eg = get_table(net, tbl_name)
        if not eg.empty and "bus" in eg.columns:
            source_buses.update(eg["bus"].dropna().unique().tolist())

    fixed = 0
    for sb in source_buses:
        try:
            sub = bus.xs(sb, level=0)
        except KeyError:
            continue
        grounded_any = any(sub["grounded"].tolist())
        if grounded_any:
            continue
        # Find neutral phase rows (phase=0) and ground them
        for idx in bus.index:
            if idx[0] != sb:
                continue
            phase = idx[1]
            if int(phase) == 0:
                net.bus.at[idx, "grounded"] = True
                if "grounding_r_ohm" in bus.columns:
                    net.bus.at[idx, "grounding_r_ohm"] = 1e-6
                if "grounding_x_ohm" in bus.columns:
                    net.bus.at[idx, "grounding_x_ohm"] = 0.0
                fixed += 1

    return f"Grounded neutral at {fixed} source bus phases."


# ===================================================================
# IMPEDANCE fixes
# ===================================================================

def fix_imp_01_set_small_positive_resistance(net: Any) -> str:
    """Replace zero/negative resistance with a small positive default (0.001 Ω/km)."""
    line = get_table(net, "line")
    if line.empty:
        return "No line table."

    DEFAULT_R = 0.001
    r_col = None
    for col in ("r_ohm_per_km", "r_ohm"):
        if col in line.columns:
            r_col = col
            break
    if r_col is None:
        return "No resistance column found."

    mask = line[r_col].apply(lambda v: (safe_float(v) or 0) <= 0)
    fixed = int(mask.sum())
    if fixed > 0:
        net.line.loc[mask, r_col] = DEFAULT_R

    return f"Set {r_col}={DEFAULT_R} on {fixed} lines with zero/negative resistance."


def fix_imp_01_set_small_positive_reactance(net: Any) -> str:
    """Replace zero reactance with a small positive default (0.001 Ω/km)."""
    line = get_table(net, "line")
    if line.empty:
        return "No line table."

    DEFAULT_X = 0.001
    x_col = None
    for col in ("x_ohm_per_km", "x_ohm"):
        if col in line.columns:
            x_col = col
            break
    if x_col is None:
        return "No reactance column found."

    mask = line[x_col].apply(lambda v: safe_float(v) == 0)
    fixed = int(mask.sum())
    if fixed > 0:
        net.line.loc[mask, x_col] = DEFAULT_X

    return f"Set {x_col}={DEFAULT_X} on {fixed} lines with zero reactance."


# ===================================================================
# DUPLICATES fixes
# ===================================================================

def fix_dup_01_remove_duplicate_multiindex_rows(net: Any) -> str:
    """Drop fully-duplicated MultiIndex rows, keeping first occurrence."""
    TABLES = (
        "bus", "line", "trafo1ph", "switch",
        "asymmetric_load", "asymmetric_sgen", "asymmetric_shunt",
        "ext_grid", "ext_grid_sequence",
    )
    total = 0
    for tbl_name in TABLES:
        table = get_table(net, tbl_name)
        if table.empty:
            continue
        if isinstance(table.index, pd.MultiIndex):
            dup_mask = table.index.duplicated(keep="first")
        else:
            dup_mask = table.index.duplicated(keep="first")
        n_dup = int(dup_mask.sum())
        if n_dup > 0:
            setattr(net, tbl_name, table[~dup_mask])
            total += n_dup

    return f"Removed {total} duplicate rows across all tables."


# ===================================================================
# LOAD MODEL fixes
# ===================================================================

def fix_ld_02_clamp_power_factor(net: Any) -> str:
    """Clamp loads with power factor < 0.70 by reducing q_mvar
    to bring PF to exactly 0.70."""
    load = get_table(net, "asymmetric_load")
    if load.empty or "p_mw" not in load.columns or "q_mvar" not in load.columns:
        return "No asymmetric_load table with p_mw/q_mvar."

    MIN_PF = 0.70
    fixed = 0
    for idx, row in load.iterrows():
        if not is_in_service(row):
            continue
        p = safe_float(row.get("p_mw"))
        q = safe_float(row.get("q_mvar"))
        if p is None or q is None:
            continue
        s = math.sqrt(p**2 + q**2)
        if s < 1e-12:
            continue
        pf = abs(p) / s
        if pf < MIN_PF:
            # q_max for PF=0.70: |q| = |p| * tan(acos(0.70))
            q_max = abs(p) * math.tan(math.acos(MIN_PF))
            sign = 1.0 if q >= 0 else -1.0
            net.asymmetric_load.at[idx, "q_mvar"] = sign * q_max
            fixed += 1

    return f"Clamped q_mvar on {fixed} load rows to achieve PF ≥ {MIN_PF}."


# ===================================================================
# CONTROLS fixes
# ===================================================================

def fix_ctrl_02_clamp_vm_pu(net: Any) -> str:
    """Clamp source vm_pu to [0.95, 1.05] if outside [0.90, 1.10]."""
    fixed = 0
    for tbl_name in ("ext_grid", "ext_grid_sequence"):
        table = get_table(net, tbl_name)
        if table.empty or "vm_pu" not in table.columns:
            continue
        for idx, row in table.iterrows():
            if not is_in_service(row):
                continue
            vm = safe_float(row.get("vm_pu"))
            if vm is None:
                continue
            if vm < 0.90:
                getattr(net, tbl_name).at[idx, "vm_pu"] = 0.95
                fixed += 1
            elif vm > 1.10:
                getattr(net, tbl_name).at[idx, "vm_pu"] = 1.05
                fixed += 1

    return f"Clamped vm_pu on {fixed} source rows to [0.95, 1.05]."


def fix_ctrl_03_swap_inverted_cap_thresholds(net: Any) -> str:
    """Swap v_threshold_on and v_threshold_off when inverted (on >= off)."""
    fixed = 0
    for tbl_name in ("shunt", "asymmetric_shunt"):
        table = get_table(net, tbl_name)
        if table.empty:
            continue
        if "v_threshold_on" not in table.columns or "v_threshold_off" not in table.columns:
            continue
        for idx, row in table.iterrows():
            if not is_in_service(row):
                continue
            v_on = safe_float(row.get("v_threshold_on"))
            v_off = safe_float(row.get("v_threshold_off"))
            if v_on is not None and v_off is not None and v_on >= v_off:
                getattr(net, tbl_name).at[idx, "v_threshold_on"] = v_off
                getattr(net, tbl_name).at[idx, "v_threshold_off"] = v_on
                fixed += 1

    return f"Swapped inverted cap thresholds on {fixed} shunt rows."


# ===================================================================
# TOPOLOGY fixes
# ===================================================================

def fix_top_01_mark_unreachable_out_of_service(net: Any) -> str:
    """Mark buses not reachable from any source as out-of-service."""
    bus = get_table(net, "bus")
    if bus.empty or "in_service" not in bus.columns:
        return "No bus table or in_service column."

    graph = build_network_graph(net)
    reachable = bfs_from_sources(net, graph)
    bus_ids = unique_bus_ids(bus)
    unreachable = bus_ids - reachable

    fixed = 0
    for b in unreachable:
        if isinstance(bus.index, pd.MultiIndex):
            mask = bus.index.get_level_values(0) == b
            if net.bus.loc[mask, "in_service"].any():
                net.bus.loc[mask, "in_service"] = False
                fixed += 1
        else:
            if net.bus.at[b, "in_service"]:
                net.bus.at[b, "in_service"] = False
                fixed += 1

    return f"Marked {fixed} unreachable buses out-of-service."


def fix_gnd_01_mark_orphan_buses_out_of_service(net: Any) -> str:
    """Mark completely isolated buses (no connected elements) as out-of-service."""
    bus = get_table(net, "bus")
    if bus.empty or "in_service" not in bus.columns:
        return "No bus table or in_service column."

    bus_ids = unique_bus_ids(bus)
    connected_buses: set = set()

    _BRANCH = ("line", "trafo", "trafo1ph")
    _SHUNT = ("asymmetric_load", "asymmetric_sgen", "asymmetric_shunt", "ext_grid", "ext_grid_sequence")

    for tbl_name in _BRANCH:
        table = get_table(net, tbl_name)
        if table.empty:
            continue
        if tbl_name == "trafo1ph" and isinstance(table.index, pd.MultiIndex) and "bus" in table.index.names:
            connected_buses.update(table.index.get_level_values(table.index.names.index("bus")).unique().tolist())
            continue
        for col in ("from_bus", "to_bus", "hv_bus", "lv_bus", "bus"):
            if col in table.columns:
                connected_buses.update(table[col].dropna().unique().tolist())

    for tbl_name in _SHUNT:
        table = get_table(net, tbl_name)
        if not table.empty and "bus" in table.columns:
            connected_buses.update(table["bus"].dropna().unique().tolist())

    orphans = bus_ids - connected_buses
    fixed = 0
    for b in orphans:
        if isinstance(bus.index, pd.MultiIndex):
            mask = bus.index.get_level_values(0) == b
            if net.bus.loc[mask, "in_service"].any():
                net.bus.loc[mask, "in_service"] = False
                fixed += 1
        else:
            if net.bus.at[b, "in_service"]:
                net.bus.at[b, "in_service"] = False
                fixed += 1

    return f"Marked {fixed} orphan buses out-of-service."


# ===================================================================
# FIX REGISTRY — ordered list of (fix_id, description, fix_fn, scope_categories)
# ===================================================================

FIX_REGISTRY: list[tuple[str, str, Any, str | list[str]]] = [
    # --- duplicates (run first — cleanest foundation) ---
    ("dup_01_dedup", "Remove duplicate MultiIndex rows",
     fix_dup_01_remove_duplicate_multiindex_rows, "duplicates"),

    # --- voltage_base ---
    ("vb_01_from_trafo", "Set missing bus vn_kv from transformer winding data",
     fix_vb_01_set_missing_vn_kv_from_transformer, "voltage_base"),
    ("vb_04_unit_fix", "Convert vn_kv from Volts to kV (× 1000)",
     fix_vb_04_unit_confusion_volts_to_kv, "voltage_base"),

    # --- transformer ---
    ("tx_01_default_z", "Set default vk/vkr_percent where zero or missing",
     fix_tx_01_default_zero_impedance, "transformer"),
    ("tx_02_winding_vn", "Correct trafo winding vn_kv to match bus vn_kv",
     fix_tx_02_winding_vn_kv_from_bus, "transformer"),

    # --- grounding ---
    ("gnd_03_source_gnd", "Ground neutral at source buses",
     fix_gnd_03_ground_source_bus_neutral, "grounding"),

    # --- impedance ---
    ("imp_01_r_fix", "Set small positive resistance where zero/negative",
     fix_imp_01_set_small_positive_resistance, "impedance"),
    ("imp_01_x_fix", "Set small positive reactance where zero",
     fix_imp_01_set_small_positive_reactance, "impedance"),

    # --- load model ---
    ("ld_02_pf_clamp", "Clamp load power factor to ≥ 0.70",
     fix_ld_02_clamp_power_factor, "load_model"),

    # --- controls ---
    ("ctrl_02_vm_clamp", "Clamp source vm_pu to [0.95, 1.05]",
     fix_ctrl_02_clamp_vm_pu, "controls"),
    ("ctrl_03_cap_swap", "Swap inverted capacitor thresholds",
     fix_ctrl_03_swap_inverted_cap_thresholds, "controls"),

    # --- topology ---
    ("gnd_01_orphan_oos", "Mark orphan buses out-of-service",
     fix_gnd_01_mark_orphan_buses_out_of_service, ["grounding", "topology"]),
    ("top_01_unreach_oos", "Mark unreachable buses out-of-service",
     fix_top_01_mark_unreachable_out_of_service, "topology"),
]
