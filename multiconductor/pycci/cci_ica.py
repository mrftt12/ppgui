import pandapower as pp
import pandapower.topology
import multiconductor.shortcircuit as sc
import networkx as nx
import numpy as np
import pandas as pd
from multiconductor.pycci import cci_powerflow
import logging
import math
import copy

try:
    import multiconductor.shortcircuit as sc

    HAS_SC = True
except ImportError:
    HAS_SC = False

logger = logging.getLogger(__name__)


def get_rating(net, element_type, element_idx):
    """
    Return the ampacity (kA) of the given element.
    """
    if element_type == "line":
        # pandapower stores rating in kA in max_i_ka
        return net.line.at[element_idx, "max_i_ka"] * 1000.0  # convert to A
    elif element_type == "trafo":
        # Approx rating from sn_mva
        if "sn_mva" in net.trafo.columns:
            sn_mva = net.trafo.at[element_idx, "sn_mva"]
            # Rated current on LV side (destination usually)
            vn_lv_kv = net.trafo.at[element_idx, "vn_lv_kv"]
            if vn_lv_kv > 0:
                return (sn_mva * 1e6) / (np.sqrt(3) * vn_lv_kv * 1000.0) * 1000.0
    return 99999.0


def get_active_power_flow(net, etype, idx, from_node, to_node):
    """
    Get active power flow (MW) from u to v.
    Positive means flow is u -> v.
    """
    p_mw = 0.0

    if etype == "line":
        if idx not in net.res_line.index:
            return 0.0
        # Check orientation
        l = net.line.loc[idx]
        if l.from_bus == from_node:
            p_mw = net.res_line.at[idx, "p_from_mw"]
        else:
            p_mw = net.res_line.at[idx, "p_to_mw"]

    elif etype == "trafo":
        if idx not in net.res_trafo.index:
            return 0.0
        t = net.trafo.loc[idx]
        # Trafo usually hv -> lv
        if t.hv_bus == from_node:
            p_mw = net.res_trafo.at[idx, "p_hv_mw"]
        else:
            p_mw = net.res_trafo.at[idx, "p_lv_mw"]

    return p_mw


def run_ica_streamlined(net, output_file=None):
    """
    Main function to run Streamlined ICA.
    """
    logger.info("Running Power Flow...")
    try:
        cci_powerflow.run_pf(net)
    except Exception as e:
        logger.error(f"Power Flow failed: {e}")
        return None

    if HAS_SC:
        logger.info("Running Short Circuit...")
        try:
            # Ensure indices are correct?
            # SC calc requires standard elements
            sc.calc_sc(net, case="max", ip=True, ith=True, branch_results=False)
        except Exception as e:
            logger.warning(f"Sort Circuit calculation failed: {e}")

    # Trace Graph
    # pandapower's create_nxgraph expects simple integer indices, not MultiIndex
    # so we create a shallow copy with flattened indices for graph creation
    net_graph = net
    if isinstance(net.bus.index, pd.MultiIndex):
        import copy as _copy
        net_graph = _copy.copy(net)
        for key in list(net_graph.keys()):
            val = net_graph[key]
            if isinstance(val, pd.DataFrame) and isinstance(val.index, pd.MultiIndex):
                flat = val.copy()
                flat.index = flat.index.get_level_values(0)
                flat = flat[~flat.index.duplicated(keep="first")]
                flat.index.name = None
                net_graph[key] = flat
    # Ensure standard pandapower tables exist (create_nxgraph accesses them by key)
    for tbl in ("impedance", "dcline", "tcsc", "vsc", "line_dc", "trafo3w", "switch", "trafo"):
        if tbl not in net_graph:
            net_graph[tbl] = pd.DataFrame()
    g = pp.topology.create_nxgraph(
        net_graph, respect_switches=True, include_lines=True, include_trafos=True
    )

    # Find Slack
    if hasattr(net, "ext_grid") and not net.ext_grid.empty:
        slack_bus = net.ext_grid.bus.iloc[0]
    elif hasattr(net, "ext_grid_sequence") and not net.ext_grid_sequence.empty:
        # multiconductor nets store ext_grid data in ext_grid_sequence
        slack_bus = net.ext_grid_sequence.bus.iloc[0]
    else:
        logger.error("No external grid found.")
        return None

    edge_map = trace_downstream_pp(g, slack_bus, net)

    results = []

    # helper for reverse lookup
    def get_strat_p(strat_str):
        if not strat_str or strat_str == "source":
            return 99999.0  # Infinite reverse capacity at source
        parts = strat_str.split("_")
        if len(parts) == 2:
            stype, sidx = parts[0], int(parts[1])
            # We need the flow through this device.
            # But we don't know the nodes clearly here without lookup.
            # We assume forward flow (positive P) is the load.
            # We'll fetch P_hv (for trafo) or P_from (line) and take abs?
            # Or assume standard orientation?
            # Let's take the max of P_hv/P_lv or P_from/P_to magnitude to represent 'Loading'.
            if stype == "trafo":
                if sidx in net.res_trafo.index:
                    return max(
                        abs(net.res_trafo.at[sidx, "p_hv_mw"]),
                        abs(net.res_trafo.at[sidx, "p_lv_mw"]),
                    )
            elif stype == "line":
                if sidx in net.res_line.index:
                    return max(
                        abs(net.res_line.at[sidx, "p_from_mw"]),
                        abs(net.res_line.at[sidx, "p_to_mw"]),
                    )
        return 0.0

    for (etype, idx), data in edge_map.items():
        if etype not in ["line", "trafo"]:
            continue

        to_node = data.get("to_node")
        from_node = data.get("from_node")  # Computed during trace?

        # Power Flow Result
        # Get P flow to determine Thermal Capacity
        # Get Current Magnitude
        i_ka = 0.0
        p_flow_mw = 0.0  # Flow in direction from_node -> to_node

        if etype == "line":
            if idx in net.res_line.index:
                i_ka = net.res_line.at[idx, "i_ka"]
                p_flow_mw = get_active_power_flow(net, etype, idx, from_node, to_node)
        elif etype == "trafo":
            if idx in net.res_trafo.index:
                i_ka = net.res_trafo.at[idx, "i_hv_ka"]  # Use HV current or max?
                p_flow_mw = get_active_power_flow(net, etype, idx, from_node, to_node)

        # Voltage
        vm_pu = (
            net.res_bus.at[to_node, "vm_pu"] if to_node in net.res_bus.index else 1.0
        )
        vn_kv = net.bus.at[to_node, "vn_kv"]
        v_actual_kv = vm_pu * vn_kv
        v_ll_actual = v_actual_kv * np.sqrt(3)

        # 1. Thermal
        # Capacity (kW).
        min_amp = data["min_ampacity"]

        # Check Direction for Thermal
        # If p_flow_mw > 0 (Forward), we can add (Rating + Load).
        # If p_flow_mw < 0 (Reverse), we can add (Rating - Load).

        # i_ka is magnitude.
        # capacity_A:
        if p_flow_mw >= 0:
            # Forward flow. Gen reduces flow.
            # Limit is usually `Rating + Current` ?
            # Example: Rating 100A. Load 50A (Forward).
            # Gen 0 -> Flow 50A.
            # Gen 50 -> Flow 0A.
            # Gen 150 -> Flow 100A (Reverse).
            # Total Gen = 150. (Rating 100 + Load 50).
            cap_A = min_amp / 1000.0 + i_ka
        else:
            # Reverse flow (already Gen).
            # Rating 100A. Reverse Flow 50A.
            # Can add 50A more Gen.
            cap_A = max(0, min_amp / 1000.0 - i_ka)

        str_thermal_kw = cap_A * np.sqrt(3) * vn_kv * 1000.0

        # 2. SSV (Steady State Voltage)
        headroom = 1.05 - vm_pu
        str_ssv_kw = 0.0
        if (
            headroom > 0
            and HAS_SC
            and "res_bus_sc" in net
            and to_node in net.res_bus_sc.index
        ):
            r_sc = net.res_bus_sc.at[to_node, "r_ohm"]
            if r_sc > 0:
                str_ssv_kw = 1000.0 * headroom * (v_ll_actual**2) / r_sc

        # 3. Voltage Variation
        str_volt_var_kw = 0.0
        if HAS_SC and "res_bus_sc" in net and to_node in net.res_bus_sc.index:
            r_sc = net.res_bus_sc.at[to_node, "r_ohm"]
            if r_sc > 0:
                str_volt_var_kw = 1000.0 * 0.03 * (v_ll_actual**2) / r_sc

        # 4. Protection
        str_prot_kw = 0.0
        if HAS_SC and "res_bus_sc" in net and to_node in net.res_bus_sc.index:
            ikss = net.res_bus_sc.at[to_node, "ikss_ka"]
            sc_mva = np.sqrt(3) * vn_kv * ikss
            str_prot_kw = 0.1 * sc_mva * 1000.0

        # 5. Reverse Flow
        # Limit is the Load at the Strategic Device.
        # "Don't backfeed past the Strat Device".
        # So we can generate up to the Strat Device's Forward Flow.
        strat_load_mw = get_strat_p(data["strat_device"])
        str_rev_kw = strat_load_mw * 1000.0

        # Final ICA
        ica_kw = min(
            str_thermal_kw,
            str_ssv_kw if str_ssv_kw > 0 else 9e9,
            str_volt_var_kw if str_volt_var_kw > 0 else 9e9,
            str_prot_kw if str_prot_kw > 0 else 9e9,
            str_rev_kw,
        )

        results.append(
            {
                "element_type": etype,
                "element_index": idx,
                "from_node": from_node,
                "to_node": to_node,
                "ica_kw": ica_kw,
                "ica_thermal_kw": str_thermal_kw,
                "ica_ssv_kw": str_ssv_kw,
                "ica_volt_var_kw": str_volt_var_kw,
                "ica_protection_kw": str_prot_kw,
                "ica_reverse_kw": str_rev_kw,
                "min_ampacity_A": min_amp,
                "strat_device": data["strat_device"],
                "bus_vm_pu": vm_pu,
                "bus_vn_kv": vn_kv,
            }
        )

    df = pd.DataFrame(results)
    if output_file:
        df.to_csv(output_file, index=False)
    return df


def trace_downstream_pp(graph, start_bus, net):
    element_map = {}

    # Queue: (bus, upstream_rating, strat_device)
    queue = [(start_bus, 999999.0, "source")]
    visited = set([start_bus])

    while queue:
        u, u_rat, u_strat = queue.pop(0)

        if u not in graph:
            continue

        for v in graph[u]:
            if v in visited:
                continue

            # Identify edge
            # graph[u][v] might be dict-of-dicts for MultiGraph
            edges = graph[u][v]
            # Since edges is a dict with key=edge_index
            for k in edges:
                attr = edges[k]
                etype_code = attr.get("type")  # 'l' or 't'
                idx = attr.get("idx")  # Assuming standard pp graph keys

                # If keys missing (older pp?), fallback
                if idx is None:
                    continue

                etype = (
                    "line"
                    if etype_code == "l"
                    else "trafo"
                    if etype_code == "t"
                    else "other"
                )

                local_rat = get_rating(net, etype, idx)
                path_rat = min(u_rat, local_rat)

                new_strat = u_strat
                if etype == "trafo":
                    new_strat = f"trafo_{idx}"

                element_map[(etype, idx)] = {
                    "min_ampacity": path_rat,
                    "strat_device": new_strat,
                    "from_node": u,
                    "to_node": v,
                }

                # Add to queue
                if v not in visited:
                    visited.add(v)
                    queue.append((v, path_rat, new_strat))

    return element_map


def check_violations(net, max_v_pu=1.05, min_v_pu=0.95):
    """
    Check if the network state has any violations.
    Returns True if violations exist.
    """
    # 1. Voltage Violation
    # We check all buses with in_service=True
    # res_bus might check phases separately?
    # cci_powerflow writes standard res_bus average?
    # If multiphase, we might need to check internal variables?
    # Assuming standard res_bus accounts for violations if 3ph is balanced or average.
    # For rigorous multiphase, we should check net.res_bus_3ph if available?
    # cci_powerflow standard `res_bus` vm_pu is usually max(phasors) or similar?
    # Let's assume standard `res_bus`.

    if net.res_bus.vm_pu.max() > max_v_pu:
        return True
    if net.res_bus.vm_pu.min() < min_v_pu:
        return True

    # 2. Thermal Violation
    # Lines
    # loading_percent is relative to max_i_ka
    # Checks if any line > 100%
    if "loading_percent" in net.res_line.columns:
        if net.res_line.loading_percent.max() > 100.0:
            return True
    # Trafos
    if "loading_percent" in net.res_trafo.columns:
        if net.res_trafo.loading_percent.max() > 100.0:
            return True

    # 3. Reverse Flow at Source (ext_grid)
    # Usually we don't allow P_export > 0? Or limit?
    # ICA usually assumes "No Reverse Power Flow at scada" if that is constraint.
    # We will assume NO export at substation (P > 0 means import, P < 0 export).
    # If p_mw < 0, check if we allow it.
    # Simplified: assume strict 'No Reverse Flow' for now as per Streamlined?
    # Reference says VerifyReverseFlow=True.
    if net.res_ext_grid.p_mw.min() < -0.001:  # Small tolerance
        return True

    return False


def run_ica_iterative(net, nodes=None):
    """
    Perform Iterative ICA on specified nodes (or all nodes in streamlined results).
    """
    logger.info("Starting Iterative ICA...")

    # 1. Prepare Net for Iteration
    # We use a copy
    if net is None:
        return None
    try:
        net_iter = copy.deepcopy(net)
    except Exception:
        import pickle
        net_iter = pickle.loads(pickle.dumps(net, protocol=pickle.HIGHEST_PROTOCOL))

    # Optimization: Add dummy sgens at ALL buses (for all phases?)
    # cci_powerflow uses `asymmetric_sgen`.
    # We need to add 1 sgen per bus (3 phases?) or 1 sgen with phases=[0,1,2]?
    # cci_powerflow handles individual phase entries in asymmetric_sgen.
    # To test capacity properly, we should inject balanced? Or single phase?
    # Tutorial `ica.AddGeneration` usually adds balanced 3ph generation for 3ph nodes.
    # We will add a 3-phase sgen (3 rows in asymmetric_sgen) for each bus.

    # Identify 3-phase buses or all buses?
    # Let's add safely for all buses.

    # List of buses to test
    if nodes is None:
        if isinstance(net_iter.bus.index, pd.MultiIndex):
            nodes = sorted(net_iter.bus.index.get_level_values("index").unique())
        else:
            nodes = net_iter.bus.index.tolist()

    # Map bus -> sgen index in net_iter (MultiIndex uses (index, circuit))
    sgen_map = {}

    from multiconductor.create import create_asymmetric_sgen

    for bus in nodes:
        sgen_idx = create_asymmetric_sgen(
            net_iter,
            bus=bus,
            from_phase=(1, 2, 3),
            to_phase=0,
            p_mw=(0.0, 0.0, 0.0),
            q_mvar=(0.0, 0.0, 0.0),
            name=f"ica_dummy_bus{bus}",
        )
        sgen_map[bus] = sgen_idx

    # Initialize Model ONCE
    # This prepares Y-bus including the new zero-injection sgens.
    logger.info("Initializing Model for Iteration...")
    try:
        # Using private init to ensure it's forced
        # But run_pf calls it.
        # If we use cci_powerflow, we can call _initialize_model
        from multiconductor.pycci.model import _initialize_model
        from multiconductor.file_io import _set_multiindex

        try:
            _initialize_model(net_iter)
        except AttributeError as e:
            if "codes" in str(e):
                _set_multiindex(net_iter)
                _initialize_model(net_iter)
            else:
                raise

        # Initial PF to get Base Case Voltages
        cci_powerflow._init_pf(net_iter)
        cci_powerflow.snap_pf(net_iter, 1e-5, 1e-5, 100)  # Base run

        # Store Base Voltages for Variation Check
        base_vm_pu = net_iter.res_bus.vm_pu.copy()

    except Exception as e:
        logger.error(f"Initialization failed: {e}")
        return None

    results = []

    # Binary Search Parameters
    MAX_GEN_MW = 10.0  # Upper bound for search (per phase? or total?)
    # Tutorial usually 20MW max total?
    # If we inject 3-phase, total MW = 3 * P_phase.
    # Let's define Max Total = 10 MW.

    for bus in nodes:
        # Binary Search
        low = 0.0
        high = MAX_GEN_MW
        valid_mw = 0.0

        # We perform search on Total MW.
        # Per phase = Total / 3.

        # Check if 0 is valid (Base Case)
        # If 0 is violation, then 0 capacity.
        # Ideally we check base violations first.
        # Assuming base is valid or we just report 0.

        # Iteration
        for _ in range(5):  # 5 steps binary search gives ~3% precision on 10MW (0.3MW)
            mid = (low + high) / 2.0

            # Apply Gen
            p_phase = mid / 3.0
            idx = sgen_map[bus]
            net_iter.asymmetric_sgen.loc[(idx, slice(None)), "p_mw"] = p_phase

            # Run PF
            # We must NOT re-call run_pf because it re-inits model
            # call snap_pf directly
            try:
                cci_powerflow.snap_pf(net_iter, 1e-5, 1e-5, 20)

                # Check Violations
                is_viol = check_violations(net_iter)

                # Check Voltage Variation (Base Case comparison)
                # Max deviation > 3%
                if not is_viol:
                    # Deviation = abs(V_new - V_base) / V_base?
                    # Usually just abs(V_new - V_base) if scaling?
                    # let's use abs diff > 0.03 pu
                    dev = (net_iter.res_bus.vm_pu - base_vm_pu).abs().max()
                    if dev > 0.03:
                        is_viol = True

                if is_viol:
                    high = mid
                else:
                    valid_mw = mid
                    low = mid

            except Exception:
                # Non-convergence -> unsafe
                high = mid

        # Reset Gen to 0 for next bus
        idx = sgen_map[bus]
        net_iter.asymmetric_sgen.loc[(idx, slice(None)), "p_mw"] = 0.0

        results.append({"node": bus, "iterative_ica_mw": valid_mw})

        logger.info(f"Bus {bus}: ICA={valid_mw:.3f} MW")

    return pd.DataFrame(results)
