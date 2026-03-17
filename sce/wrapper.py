"""
Power Flow Wrapper - Unified interface for running power flow with auto-fix.

This module wraps the multiconductor power flow solver with automatic data
quality fixes based on the Newton Rules Engine validation.

Usage:
    from sce.wrapper import run_pf
    
    # Run power flow with auto-fix enabled (default)
    run_pf(net)
    
    # Run power flow without auto-fix
    run_pf(net, auto_fix=False)

Copyright 2026, iTron.
Authors: Frank M Gonzales, Ajith Joseph
"""

import multiconductor as mc
from multiconductor.pycci.cci_powerflow import run_pf as mc_run_pf

from sce.newton_rules_engine import get_pf_rule_mismatches, new_pf_field_mapping
from sce import nr_iteration_scrubber as scrubber


# Mapping of element types to their fix functions
ELEMENT_FIX_MAPPING = {
    "trafo": scrubber.fix_zero_sn_mva,
    "trafo1ph": scrubber.fix_zero_sn_mva,
    "asymmetric_load": [
        scrubber.fix_asymmetric_load_sn_mva,
        scrubber.fix_load_p_mw,
    ],
    "load": scrubber.fix_load_p_mw,
    "asymmetric_gen": scrubber.fix_negative_gen_power,
    "asymmetric_sgen": scrubber.fix_negative_gen_power,
    "shunt": scrubber.fix_shunt_p_mw,
    "asymmetric_shunt": scrubber.fix_shunt_p_mw,
    "switch": scrubber.fix_switch_r_ohm,
    "ext_grid_sequence": scrubber.fix_ext_grid_impedances,
    "line": scrubber.update_std_type_impedances,
    "bus": [
        scrubber.coerce_boolean_columns,
        scrubber.normalize_in_service_columns,
    ],
}


def _apply_fix_for_element(net, element: str, fix_func_or_list):
    """Apply fix function(s) for a specific element type."""
    if isinstance(fix_func_or_list, list):
        for func in fix_func_or_list:
            try:
                func(net)
            except Exception:
                pass
    else:
        try:
            fix_func_or_list(net)
        except Exception:
            pass


def run_pf(
    net,
    auto_fix: bool = True,
    tol_vmag_pu: float = 1e-5,
    tol_vang_rad: float = 1e-5,
    max_iter: int = 100,
    run_control: bool = True,
    verbose: bool = False,
):
    """
    Run power flow with optional automatic data quality fixes.
    
    If auto_fix is True, this function will:
    1. Run the Newton Rules Engine to identify mismatches
    2. Apply appropriate scrubber fixes for each element category
    3. Apply default grounding and switch values
    4. Run the power flow solver
    
    Parameters:
        net: pandapower/multiconductor network object
        auto_fix: Whether to apply automatic fixes before running power flow (default True)
        tol_vmag_pu: Voltage magnitude tolerance in p.u. (default 1e-5)
        tol_vang_rad: Voltage angle tolerance in radians (default 1e-5)
        max_iter: Maximum iterations (default 100)
        run_control: Whether to run control loop (default True)
        verbose: Print detailed information (default False)
    
    Returns:
        net: The network object after power flow (check net.converged for status)
    """
    # Attach auto_fix flag to network
    scrubber.attach_auto_fix(net, auto_fix)
    
    if auto_fix:
        if verbose:
            print("Running Newton Rules Engine validation...")
        
        # Get mismatches before fixing
        mismatch_df = get_pf_rule_mismatches(net, new_pf_field_mapping)
        
        if verbose and not mismatch_df.empty:
            elements_with_issues = mismatch_df['element'].unique()
            print(f"Found {len(mismatch_df)} mismatches in: {list(elements_with_issues)}")
        
        # Get unique elements with issues
        if not mismatch_df.empty:
            elements_with_issues = set(mismatch_df['element'].unique())
            
            # Apply element-specific fixes
            for element in elements_with_issues:
                if element in ELEMENT_FIX_MAPPING:
                    _apply_fix_for_element(net, element, ELEMENT_FIX_MAPPING[element])
        
        # Always apply baseline scrubbers
        if verbose:
            print("Applying baseline scrubbers...")
        
        # Apply grounding defaults
        scrubber.apply_grounding_defaults(net)
        
        # Apply switch defaults
        scrubber.apply_switch_defaults(net)
        
        # Always apply these critical fixes
        scrubber.fix_zero_sn_mva(net)
        scrubber.coerce_boolean_columns(net)
        scrubber.normalize_in_service_columns(net)
        scrubber.update_std_type_impedances(net)
        scrubber.fix_ext_grid_impedances(net)
        
        if verbose:
            # Validate after fixing
            remaining = get_pf_rule_mismatches(net, new_pf_field_mapping)
            print(f"Remaining mismatches after fix: {len(remaining)}")
    
    # Run power flow
    if verbose:
        print("Running power flow...")
    
    mc_run_pf(
        net,
        tol_vmag_pu=tol_vmag_pu,
        tol_vang_rad=tol_vang_rad,
        MaxIter=max_iter,
        run_control=run_control
    )
    
    if verbose:
        print(f"Converged: {net.converged}")
    
    return net


def run_pf_batch(
    nets: list,
    auto_fix: bool = True,
    tol_vmag_pu: float = 1e-5,
    tol_vang_rad: float = 1e-5,
    max_iter: int = 100,
    run_control: bool = True,
    verbose: bool = False,
) -> dict:
    """
    Run power flow on multiple networks.
    
    Parameters:
        nets: List of network objects
        auto_fix: Whether to apply automatic fixes (default True)
        tol_vmag_pu: Voltage magnitude tolerance
        tol_vang_rad: Voltage angle tolerance
        max_iter: Maximum iterations
        run_control: Whether to run control loop
        verbose: Print detailed information
    
    Returns:
        dict: Summary with converged/failed counts and lists
    """
    converged = []
    failed = []
    
    for i, net in enumerate(nets):
        if verbose:
            print(f"Processing network {i+1}/{len(nets)}...")
        
        try:
            run_pf(
                net,
                auto_fix=auto_fix,
                tol_vmag_pu=tol_vmag_pu,
                tol_vang_rad=tol_vang_rad,
                max_iter=max_iter,
                run_control=run_control,
                verbose=False
            )
            
            if net.converged:
                converged.append(i)
            else:
                failed.append(i)
        except Exception as e:
            failed.append(i)
            if verbose:
                print(f"  Error: {e}")
    
    return {
        "total": len(nets),
        "converged_count": len(converged),
        "failed_count": len(failed),
        "converged_indices": converged,
        "failed_indices": failed,
    }


def set_load(net, p_mw: float = 1.5, pf: float = 0.95, verbose: bool = False):
    """
    Perform load allocation using connected capacity (sn_mva).
    
    Distributes the total load across individual loads proportionally based on
    their connected capacity (sn_mva). The reactive power is calculated from
    the power factor.
    
    Parameters:
        net: pandapower/multiconductor network object
        p_mw: Total active power to allocate in MW (default 1.5)
        pf: Power factor (default 0.95)
        verbose: Print allocation details (default False)
    
    Returns:
        dict: Summary of load allocation with total_p_mw, total_q_mvar, and load_count
    
    Example:
        from sce.wrapper import set_load
        
        # Allocate 2 MW at 0.9 power factor
        result = set_load(net, p_mw=2.0, pf=0.9)
        print(f"Allocated {result['total_p_mw']:.3f} MW across {result['load_count']} loads")
    """
    import math
    import pandas as pd
    import numpy as np
    
    # Calculate q_mvar from power factor
    # pf = cos(theta), so theta = acos(pf), tan(theta) = sin/cos = sqrt(1-pf^2)/pf
    if pf >= 1.0:
        q_mvar_ratio = 0.0
    else:
        q_mvar_ratio = math.sqrt(1 - pf**2) / pf
    
    total_q_mvar = p_mw * q_mvar_ratio
    
    result = {
        "total_p_mw": 0.0,
        "total_q_mvar": 0.0,
        "load_count": 0,
        "allocations": []
    }
    
    # Process asymmetric_load table
    if "asymmetric_load" in net and isinstance(net["asymmetric_load"], pd.DataFrame):
        table = net["asymmetric_load"]
        
        if len(table) > 0 and "sn_mva" in table.columns:
            # Get connected capacity for each load
            sn_values = pd.to_numeric(table["sn_mva"], errors="coerce").fillna(0)
            total_sn = sn_values.sum()
            
            if total_sn > 0:
                # Calculate allocation factor for each load
                allocation_factors = sn_values / total_sn
                
                for idx in table.index:
                    factor = allocation_factors[idx]
                    load_p_mw = p_mw * factor
                    load_q_mvar = total_q_mvar * factor
                    
                    # Handle per-phase allocation if p_mw is a list
                    current_p = table.at[idx, "p_mw"] if "p_mw" in table.columns else None
                    
                    if isinstance(current_p, list):
                        # Distribute equally across phases
                        num_phases = len(current_p)
                        if num_phases > 0:
                            p_per_phase = load_p_mw / num_phases
                            q_per_phase = load_q_mvar / num_phases
                            net["asymmetric_load"].at[idx, "p_mw"] = [p_per_phase] * num_phases
                            if "q_mvar" in table.columns:
                                net["asymmetric_load"].at[idx, "q_mvar"] = [q_per_phase] * num_phases
                    else:
                        # Single value
                        net["asymmetric_load"].at[idx, "p_mw"] = load_p_mw
                        if "q_mvar" in table.columns:
                            net["asymmetric_load"].at[idx, "q_mvar"] = load_q_mvar
                    
                    result["allocations"].append({
                        "index": idx,
                        "sn_mva": float(sn_values[idx]),
                        "factor": float(factor),
                        "p_mw": float(load_p_mw),
                        "q_mvar": float(load_q_mvar)
                    })
                    result["total_p_mw"] += load_p_mw
                    result["total_q_mvar"] += load_q_mvar
                    result["load_count"] += 1
    
    # Process standard load table if present
    if "load" in net and isinstance(net["load"], pd.DataFrame):
        table = net["load"]
        
        if len(table) > 0 and "sn_mva" in table.columns:
            sn_values = pd.to_numeric(table["sn_mva"], errors="coerce").fillna(0)
            total_sn = sn_values.sum()
            
            if total_sn > 0:
                allocation_factors = sn_values / total_sn
                
                for idx in table.index:
                    factor = allocation_factors[idx]
                    load_p_mw = p_mw * factor
                    load_q_mvar = total_q_mvar * factor
                    
                    net["load"].at[idx, "p_mw"] = load_p_mw
                    if "q_mvar" in table.columns:
                        net["load"].at[idx, "q_mvar"] = load_q_mvar
                    
                    result["allocations"].append({
                        "index": idx,
                        "sn_mva": float(sn_values[idx]),
                        "factor": float(factor),
                        "p_mw": float(load_p_mw),
                        "q_mvar": float(load_q_mvar)
                    })
                    result["total_p_mw"] += load_p_mw
                    result["total_q_mvar"] += load_q_mvar
                    result["load_count"] += 1
    
    if verbose:
        print(f"Load Allocation Summary:")
        print(f"  Target P: {p_mw:.3f} MW")
        print(f"  Power Factor: {pf:.3f}")
        print(f"  Target Q: {total_q_mvar:.3f} Mvar")
        print(f"  Loads Allocated: {result['load_count']}")
        print(f"  Actual Total P: {result['total_p_mw']:.3f} MW")
        print(f"  Actual Total Q: {result['total_q_mvar']:.3f} Mvar")
    
    return result


def set_gen(net, percent_sn_mva: float = 100.0, pf: float = 1.0, verbose: bool = False):
    """
    Set asymmetric_gen output to a percentage of nameplate capacity (sn_mva).
    
    Parameters:
        net: pandapower/multiconductor network object
        percent_sn_mva: Percentage of sn_mva to set as output (default 100%)
        pf: Power factor for reactive power calculation (default 1.0 = unity)
        verbose: Print allocation details (default False)
    
    Returns:
        dict: Summary with gen_count and total_p_mw
    
    Example:
        from sce.wrapper import set_gen
        
        # Set all generators to 80% of nameplate
        result = set_gen(net, percent_sn_mva=80.0)
    """
    import math
    import pandas as pd
    
    # Calculate q_mvar ratio from power factor
    if pf >= 1.0:
        q_mvar_ratio = 0.0
    else:
        q_mvar_ratio = math.sqrt(1 - pf**2) / pf
    
    result = {
        "gen_count": 0,
        "total_p_mw": 0.0,
        "total_q_mvar": 0.0,
        "allocations": []
    }
    
    if "asymmetric_gen" not in net or not isinstance(net["asymmetric_gen"], pd.DataFrame):
        return result
    
    table = net["asymmetric_gen"]
    if len(table) == 0:
        return result
    
    if "sn_mva" not in table.columns:
        return result
    
    factor = percent_sn_mva / 100.0
    
    for idx in table.index:
        sn_mva = pd.to_numeric(table.at[idx, "sn_mva"], errors="coerce")
        if pd.isna(sn_mva) or sn_mva <= 0:
            continue
        
        gen_p_mw = sn_mva * factor
        gen_q_mvar = gen_p_mw * q_mvar_ratio
        
        # Handle per-phase allocation if p_mw is a list
        current_p = table.at[idx, "p_mw"] if "p_mw" in table.columns else None
        
        if isinstance(current_p, list):
            num_phases = len(current_p)
            if num_phases > 0:
                p_per_phase = gen_p_mw / num_phases
                q_per_phase = gen_q_mvar / num_phases
                net["asymmetric_gen"].at[idx, "p_mw"] = [p_per_phase] * num_phases
                if "q_mvar" in table.columns:
                    net["asymmetric_gen"].at[idx, "q_mvar"] = [q_per_phase] * num_phases
        else:
            net["asymmetric_gen"].at[idx, "p_mw"] = gen_p_mw
            if "q_mvar" in table.columns:
                net["asymmetric_gen"].at[idx, "q_mvar"] = gen_q_mvar
        
        result["allocations"].append({
            "index": idx,
            "sn_mva": float(sn_mva),
            "p_mw": float(gen_p_mw),
            "q_mvar": float(gen_q_mvar)
        })
        result["total_p_mw"] += gen_p_mw
        result["total_q_mvar"] += gen_q_mvar
        result["gen_count"] += 1
    
    if verbose:
        print(f"Generator Output Summary (asymmetric_gen):")
        print(f"  Percent of Nameplate: {percent_sn_mva:.1f}%")
        print(f"  Power Factor: {pf:.3f}")
        print(f"  Generators Set: {result['gen_count']}")
        print(f"  Total P: {result['total_p_mw']:.3f} MW")
        print(f"  Total Q: {result['total_q_mvar']:.3f} Mvar")
    
    return result


def set_sgen(net, percent_sn_mva: float = 100.0, pf: float = 1.0, verbose: bool = False):
    """
    Set asymmetric_sgen output to a percentage of nameplate capacity (sn_mva).
    
    Parameters:
        net: pandapower/multiconductor network object
        percent_sn_mva: Percentage of sn_mva to set as output (default 100%)
        pf: Power factor for reactive power calculation (default 1.0 = unity)
        verbose: Print allocation details (default False)
    
    Returns:
        dict: Summary with sgen_count and total_p_mw
    
    Example:
        from sce.wrapper import set_sgen
        
        # Set all static generators to 50% of nameplate
        result = set_sgen(net, percent_sn_mva=50.0)
    """
    import math
    import pandas as pd
    
    # Calculate q_mvar ratio from power factor
    if pf >= 1.0:
        q_mvar_ratio = 0.0
    else:
        q_mvar_ratio = math.sqrt(1 - pf**2) / pf
    
    result = {
        "sgen_count": 0,
        "total_p_mw": 0.0,
        "total_q_mvar": 0.0,
        "allocations": []
    }
    
    if "asymmetric_sgen" not in net or not isinstance(net["asymmetric_sgen"], pd.DataFrame):
        return result
    
    table = net["asymmetric_sgen"]
    if len(table) == 0:
        return result
    
    if "sn_mva" not in table.columns:
        return result
    
    factor = percent_sn_mva / 100.0
    
    for idx in table.index:
        sn_mva = pd.to_numeric(table.at[idx, "sn_mva"], errors="coerce")
        if pd.isna(sn_mva) or sn_mva <= 0:
            continue
        
        sgen_p_mw = sn_mva * factor
        sgen_q_mvar = sgen_p_mw * q_mvar_ratio
        
        # Handle per-phase allocation if p_mw is a list
        current_p = table.at[idx, "p_mw"] if "p_mw" in table.columns else None
        
        if isinstance(current_p, list):
            num_phases = len(current_p)
            if num_phases > 0:
                p_per_phase = sgen_p_mw / num_phases
                q_per_phase = sgen_q_mvar / num_phases
                net["asymmetric_sgen"].at[idx, "p_mw"] = [p_per_phase] * num_phases
                if "q_mvar" in table.columns:
                    net["asymmetric_sgen"].at[idx, "q_mvar"] = [q_per_phase] * num_phases
        else:
            net["asymmetric_sgen"].at[idx, "p_mw"] = sgen_p_mw
            if "q_mvar" in table.columns:
                net["asymmetric_sgen"].at[idx, "q_mvar"] = sgen_q_mvar
        
        result["allocations"].append({
            "index": idx,
            "sn_mva": float(sn_mva),
            "p_mw": float(sgen_p_mw),
            "q_mvar": float(sgen_q_mvar)
        })
        result["total_p_mw"] += sgen_p_mw
        result["total_q_mvar"] += sgen_q_mvar
        result["sgen_count"] += 1
    
    if verbose:
        print(f"Static Generator Output Summary (asymmetric_sgen):")
        print(f"  Percent of Nameplate: {percent_sn_mva:.1f}%")
        print(f"  Power Factor: {pf:.3f}")
        print(f"  Static Generators Set: {result['sgen_count']}")
        print(f"  Total P: {result['total_p_mw']:.3f} MW")
        print(f"  Total Q: {result['total_q_mvar']:.3f} Mvar")
    
    return result

def add_calculated_results(net):
    """
    Add calculated power flow result tables to net.res_pf_bus and net.res_pf_line.

    Multiconductor uses MultiIndex (element_idx, phase).

    This function pivots per-phase data into separate bus-level and line-level
    output tables with the requested output specification order.
    """
    import numpy as np
    import pandas as pd

    phase_map = {0: 'N', 1: 'A', 2: 'B', 3: 'C', 4: 'N'}

    # ---------------------------------------------------------------------------
    # Helper: get vn_kv for a bus index from net.bus  (needed for current calc)
    # ---------------------------------------------------------------------------
    def _vn_kv_for_bus(bus_idx):
        """Return nominal voltage in kV for a given bus index."""
        try:
            bus_df = net.bus if isinstance(net.bus, pd.DataFrame) else None
            if bus_df is not None and 'vn_kv' in bus_df.columns:
                if isinstance(bus_df.index, pd.MultiIndex):
                    # first level is the bus id
                    vals = bus_df.loc[bus_idx, 'vn_kv']
                    return float(vals.iloc[0]) if hasattr(vals, 'iloc') else float(vals)
                elif bus_idx in bus_df.index:
                    return float(bus_df.at[bus_idx, 'vn_kv'])
        except Exception:
            pass
        return np.nan

    # ===== Process res_bus =====
    if hasattr(net, 'res_bus') and net.res_bus is not None and len(net.res_bus) > 0:
        res_bus = net.res_bus.copy()

        if isinstance(res_bus.index, pd.MultiIndex):
            if 'vm_pu' in res_bus.columns and res_bus.index.nlevels > 1:
                phase_values = res_bus.index.get_level_values(1)
                neutral_mask = phase_values == 0
                if neutral_mask.any():
                    res_bus.loc[neutral_mask, 'vm_pu'] = np.nan
                    net.res_bus.loc[neutral_mask, 'vm_pu'] = np.nan

            bus_indices = res_bus.index.get_level_values(0).unique()

            result_rows = []
            for bus_idx in bus_indices:
                row = {'bus_idx': bus_idx}

                # Get bus name from net.bus
                try:
                    if isinstance(net.bus.index, pd.MultiIndex):
                        bus_name = net.bus.loc[bus_idx, 'name'].iloc[0] if hasattr(net.bus.loc[bus_idx, 'name'], 'iloc') else net.bus.loc[bus_idx, 'name']
                    else:
                        bus_name = net.bus.at[bus_idx, 'name'] if bus_idx in net.bus.index else None
                except Exception:
                    bus_name = None
                row['bus_name'] = bus_name

                try:
                    bus_data = res_bus.loc[bus_idx]
                except KeyError:
                    continue

                vn_kv = _vn_kv_for_bus(bus_idx)
                vm_values = {}
                va_values = {}
                p_values = {}
                q_values = {}

                if isinstance(bus_data.index, pd.Index):
                    present_phases = [
                        phase_map[p] for p in bus_data.index
                        if p in phase_map and p in (1, 2, 3)
                    ]
                    row['phase'] = ''.join(present_phases) if present_phases else np.nan
                else:
                    row['phase'] = np.nan

                for phase_num, phase_letter in phase_map.items():
                    try:
                        if phase_num not in bus_data.index:
                            continue
                        phase_data = bus_data.loc[phase_num]

                        # --- Voltage magnitude & angle ---
                        vm = phase_data.get('vm_pu', np.nan)
                        va = phase_data.get('va_degree', np.nan)

                        if pd.notna(vm):
                            row[f'SVVOLTAGE_V_{phase_letter}_PU'] = vm
                            vm_values[phase_letter] = vm
                        if pd.notna(va):
                            row[f'SVVOLTAGE_{phase_letter}_ANGLE'] = va
                            va_values[phase_letter] = va

                        # --- Total power at bus per phase ---
                        p_val = phase_data.get('p_mw', np.nan)
                        q_val = phase_data.get('q_mvar', np.nan)

                        if pd.notna(p_val):
                            row[f'SVPOWERFLOW_P_{phase_letter}_MW'] = p_val
                            if phase_letter in ('A', 'B', 'C'):
                                p_values[phase_letter] = p_val
                        if pd.notna(q_val):
                            row[f'SVPOWERFLOW_Q_{phase_letter}_MVAR'] = q_val
                            if phase_letter in ('A', 'B', 'C'):
                                q_values[phase_letter] = q_val

                        # --- Load / Gen split (negative P/Q = load) ---
                        if pd.notna(p_val):
                            if p_val < 0:
                                row[f'SVPOWERFLOW_P_LOAD_{phase_letter}_MW'] = abs(p_val)
                                row[f'SVPOWERFLOW_P_GEN_{phase_letter}_MW'] = 0.0
                            else:
                                row[f'SVPOWERFLOW_P_LOAD_{phase_letter}_MW'] = 0.0
                                row[f'SVPOWERFLOW_P_GEN_{phase_letter}_MW'] = p_val

                        if pd.notna(q_val):
                            if q_val < 0:
                                row[f'SVPOWERFLOW_Q_LOAD_{phase_letter}_MVAR'] = abs(q_val)
                                row[f'SVPOWERFLOW_Q_GEN_{phase_letter}_MVAR'] = 0.0
                            else:
                                row[f'SVPOWERFLOW_Q_LOAD_{phase_letter}_MVAR'] = 0.0
                                row[f'SVPOWERFLOW_Q_GEN_{phase_letter}_MVAR'] = q_val

                        # --- Bus current magnitude & angle ---
                        # I = S* / V  →  |I| = |S| / |V|,  angle(I) = -atan2(Q,P) - va_rad
                        if pd.notna(p_val) and pd.notna(q_val) and pd.notna(vm) and pd.notna(vn_kv) and vm * vn_kv > 0:
                            s_mag = np.sqrt(p_val ** 2 + q_val ** 2)  # MVA
                            i_ka = s_mag / (vm * vn_kv)               # kA
                            row[f'SVCURRENT_CURRENT_{phase_letter}_KA'] = i_ka

                            if pd.notna(va):
                                va_rad = np.deg2rad(va)
                                s_angle = np.arctan2(q_val, p_val)    # angle of S
                                i_angle = np.rad2deg(-s_angle - va_rad)
                                row[f'SVCURRENT_{phase_letter}_ANGLE'] = i_angle

                    except (KeyError, TypeError):
                        pass

                # --- Aggregate voltage stats (mean of A, B, C) ---
                abc_vm = [vm_values.get(p) for p in ['A', 'B', 'C'] if vm_values.get(p) is not None]
                abc_va = [va_values.get(p) for p in ['A', 'B', 'C'] if va_values.get(p) is not None]
                if abc_vm:
                    row['SVVOLTAGE_VM_PU'] = np.mean(abc_vm)
                if abc_va:
                    row['SVVOLTAGE_VM_DEG'] = np.mean(abc_va)
                if p_values:
                    row['SVPOWERFLOW_P_MW'] = float(np.sum(list(p_values.values())))
                if q_values:
                    row['SVPOWERFLOW_Q_MVAR'] = float(np.sum(list(q_values.values())))

                # --- Voltage unbalance (IEEE definition) ---
                if len(abc_vm) >= 2:
                    vm_avg = np.mean(abc_vm)
                    if vm_avg > 0:
                        vm_max_dev = max(abs(v - vm_avg) for v in abc_vm)
                        row['UNBALANCEVOLTAGEPERCENTAGE'] = (vm_max_dev / vm_avg) * 100
                    else:
                        row['UNBALANCEVOLTAGEPERCENTAGE'] = 0.0

                result_rows.append(row)

            # Build ordered bus-level calculated output table
            if result_rows:
                calc_df = pd.DataFrame(result_rows).set_index('bus_idx')
                bus_ordered_cols = [
                    'phase',
                    'bus_name',
                    'SVVOLTAGE_VM_PU',
                    'SVVOLTAGE_VM_DEG',
                    'SVPOWERFLOW_P_MW',
                    'SVPOWERFLOW_Q_MVAR',
                    'SVVOLTAGE_V_A_PU',
                    'SVVOLTAGE_A_ANGLE',
                    'SVVOLTAGE_V_B_PU',
                    'SVVOLTAGE_B_ANGLE',
                    'SVVOLTAGE_V_C_PU',
                    'SVVOLTAGE_C_ANGLE',
                    'SVPOWERFLOW_P_A_MW',
                    'SVPOWERFLOW_Q_A_MVAR',
                    'SVPOWERFLOW_P_B_MW',
                    'SVPOWERFLOW_Q_B_MVAR',
                    'SVPOWERFLOW_P_C_MW',
                    'SVPOWERFLOW_Q_C_MVAR',
                    'SVCURRENT_CURRENT_A_KA',
                    'SVCURRENT_A_ANGLE',
                    'SVCURRENT_CURRENT_B_KA',
                    'SVCURRENT_B_ANGLE',
                    'SVCURRENT_CURRENT_C_KA',
                    'SVCURRENT_C_ANGLE',
                    'SVPOWERFLOW_P_LOAD_A_MW',
                    'SVPOWERFLOW_Q_LOAD_A_MVAR',
                    'SVPOWERFLOW_P_LOAD_B_MW',
                    'SVPOWERFLOW_Q_LOAD_B_MVAR',
                    'SVPOWERFLOW_P_LOAD_C_MW',
                    'SVPOWERFLOW_Q_LOAD_C_MVAR',
                    'SVPOWERFLOW_P_GEN_A_MW',
                    'SVPOWERFLOW_Q_GEN_A_MVAR',
                    'SVPOWERFLOW_P_GEN_B_MW',
                    'SVPOWERFLOW_Q_GEN_B_MVAR',
                    'SVPOWERFLOW_P_GEN_C_MW',
                    'SVPOWERFLOW_Q_GEN_C_MVAR',
                    'UNBALANCEVOLTAGEPERCENTAGE',
                ]
                net.res_pf_bus = calc_df.reindex(columns=bus_ordered_cols)

    # ===== Process res_line =====
    if hasattr(net, 'res_line') and net.res_line is not None and len(net.res_line) > 0:
        res_line = net.res_line.copy()

        if isinstance(res_line.index, pd.MultiIndex):
            line_indices = res_line.index.get_level_values(0).unique()

            # Look up max_i_ka per line from net.line or line_std_types for loading %
            def _max_i_ka_for_line(line_idx):
                try:
                    line_df = net.line if isinstance(net.line, pd.DataFrame) else None
                    if line_df is not None and 'max_i_ka' in line_df.columns and line_idx in line_df.index:
                        return float(line_df.at[line_idx, 'max_i_ka'])
                except Exception:
                    pass
                return np.nan

            result_rows = []
            for line_idx in line_indices:
                row = {'line_idx': line_idx}

                try:
                    line_data = res_line.loc[line_idx]
                except KeyError:
                    continue

                max_i = _max_i_ka_for_line(line_idx)

                for phase_num, phase_letter in phase_map.items():
                    try:
                        if phase_num not in line_data.index:
                            continue
                        phase_data = line_data.loc[phase_num]

                        # --- Power flows ---
                        if 'p_from_mw' in phase_data.index:
                            row[f'SVPOWERFLOW_P_FROMBUS_{phase_letter}_MW'] = phase_data['p_from_mw']
                        if 'q_from_mvar' in phase_data.index:
                            row[f'SVPOWERFLOW_Q_FROMBUS_{phase_letter}_MVAR'] = phase_data['q_from_mvar']
                        if 'p_to_mw' in phase_data.index:
                            row[f'SVPOWERFLOW_P_TOBUS_{phase_letter}_MW'] = phase_data['p_to_mw']
                        if 'q_to_mvar' in phase_data.index:
                            row[f'SVPOWERFLOW_Q_TOBUS_{phase_letter}_MVAR'] = phase_data['q_to_mvar']
                        if 'pl_mw' in phase_data.index:
                            row[f'SVPOWERFLOW_P_LOSS_{phase_letter}_MW'] = phase_data['pl_mw']
                        if 'ql_mvar' in phase_data.index:
                            row[f'SVPOWERFLOW_Q_LOSS_{phase_letter}_MVAR'] = phase_data['ql_mvar']

                        # --- Current magnitudes ---
                        if 'i_from_ka' in phase_data.index:
                            i_from = phase_data['i_from_ka']
                            row[f'SVCURRENT_CURRENT_FROMBUS_{phase_letter}_KA'] = i_from
                        if 'i_to_ka' in phase_data.index:
                            i_to = phase_data['i_to_ka']
                            row[f'SVCURRENT_CURRENT_TOBUS_{phase_letter}_KA'] = i_to

                        # --- Loading percent per phase ---
                        if 'loading_percent' in phase_data.index:
                            row[f'SVPOWERFLOW_PERCENT_LOAD_{phase_letter}'] = phase_data['loading_percent']
                        elif pd.notna(max_i) and max_i > 0:
                            # Derive from from-side current
                            i_val = row.get(f'SVCURRENT_CURRENT_FROMBUS_{phase_letter}_KA', np.nan)
                            if pd.notna(i_val):
                                row[f'SVPOWERFLOW_PERCENT_LOAD_{phase_letter}'] = (i_val / max_i) * 100

                    except (KeyError, TypeError):
                        pass

                result_rows.append(row)

            if result_rows:
                calc_df = pd.DataFrame(result_rows).set_index('line_idx')
                line_ordered_cols = [
                    'SVPOWERFLOW_P_FROMBUS_A_MW',
                    'SVPOWERFLOW_Q_FROMBUS_A_MVAR',
                    'SVPOWERFLOW_P_FROMBUS_B_MW',
                    'SVPOWERFLOW_Q_FROMBUS_B_MVAR',
                    'SVPOWERFLOW_P_FROMBUS_C_MW',
                    'SVPOWERFLOW_Q_FROMBUS_C_MVAR',
                    'SVPOWERFLOW_P_FROMBUS_N_MW',
                    'SVPOWERFLOW_Q_FROMBUS_N_MVAR',
                    'SVPOWERFLOW_P_TOBUS_A_MW',
                    'SVPOWERFLOW_Q_TOBUS_A_MVAR',
                    'SVPOWERFLOW_P_TOBUS_B_MW',
                    'SVPOWERFLOW_Q_TOBUS_B_MVAR',
                    'SVPOWERFLOW_P_TOBUS_C_MW',
                    'SVPOWERFLOW_Q_TOBUS_C_MVAR',
                    'SVPOWERFLOW_P_TOBUS_N_MW',
                    'SVPOWERFLOW_Q_TOBUS_N_MVAR',
                    'SVPOWERFLOW_P_LOSS_A_MW',
                    'SVPOWERFLOW_Q_LOSS_A_MVAR',
                    'SVPOWERFLOW_P_LOSS_B_MW',
                    'SVPOWERFLOW_Q_LOSS_B_MVAR',
                    'SVPOWERFLOW_P_LOSS_C_MW',
                    'SVPOWERFLOW_Q_LOSS_C_MVAR',
                    'SVPOWERFLOW_P_LOSS_N_MW',
                    'SVPOWERFLOW_Q_LOSS_N_MVAR',
                    'SVCURRENT_CURRENT_FROMBUS_A_KA',
                    'SVCURRENT_CURRENT_FROMBUS_B_KA',
                    'SVCURRENT_CURRENT_FROMBUS_C_KA',
                    'SVCURRENT_CURRENT_FROMBUS_N_KA',
                    'SVCURRENT_CURRENT_TOBUS_A_KA',
                    'SVCURRENT_CURRENT_TOBUS_B_KA',
                    'SVCURRENT_CURRENT_TOBUS_C_KA',
                    'SVCURRENT_CURRENT_TOBUS_N_KA',
                    'SVPOWERFLOW_PERCENT_LOAD_A',
                    'SVPOWERFLOW_PERCENT_LOAD_B',
                    'SVPOWERFLOW_PERCENT_LOAD_C',
                    'SVPOWERFLOW_PERCENT_LOAD_N',
                ]
                net.res_pf_line = calc_df.reindex(columns=line_ordered_cols)

    return net