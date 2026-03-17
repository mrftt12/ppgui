"""
Usage:
    net = <load your multiconductor network>
    mc_to_cyme(net, circuit_name="MY_CIRCUIT")
"""
import sys
sys.path.insert(0, r"C:\CYME\CYME")
import cympy
import cympy.eq
import cympy.rm

import math
import numpy as np
import pandas as pd

import logging

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
PHASE_MAP = {0: '', 1: 'A', 2: 'B', 3: 'C'}
PHASE_INT_TO_LETTER = {0: 'N', 1: 'A', 2: 'B', 3: 'C'}


def _phases_for_bus(net, bus_idx):
    """Return sorted phase-letter string (e.g. 'ABC') for a bus index."""
    bus_data = net.bus.loc[bus_idx]
    phases = sorted(set(bus_data.index.get_level_values(-1)) - {0})
    return "".join(PHASE_INT_TO_LETTER.get(p, '') for p in phases)


def _mc_phase_to_cyme(from_phase):
    """Map multiconductor from_phase int to CYME phase letter."""
    return PHASE_INT_TO_LETTER.get(int(from_phase), '')


def _collect_element_phases(net, table_name, element_idx):
    """Collect sorted phase string for a multi-circuit element."""
    df = getattr(net, table_name)
    if element_idx not in df.index.get_level_values(0):
        return ''
    rows = df.loc[element_idx]
    phases = set()
    if 'from_phase' in rows.columns if hasattr(rows, 'columns') else 'from_phase' in rows.index:
        if hasattr(rows, 'iterrows'):
            for _, r in rows.iterrows():
                p = _mc_phase_to_cyme(r['from_phase'])
                if p and p != 'N':
                    phases.add(p)
        else:
            p = _mc_phase_to_cyme(rows['from_phase'])
            if p and p != 'N':
                phases.add(p)
    return "".join(sorted(phases))


def _vector_group_to_connection(trafo_rows):
    """Derive CYME transformer connection string from trafo1ph from/to phases."""
    from_phases = set()
    to_phases = set()
    for idx, row in trafo_rows.iterrows():
        fp = int(row['from_phase']) if not pd.isna(row['from_phase']) else 0
        tp = int(row['to_phase']) if not pd.isna(row['to_phase']) else 0
        from_phases.add(fp)
        to_phases.add(tp)

    has_delta = any(tp != 0 for tp in to_phases)
    if has_delta:
        return "D_Yg"
    return "Yg_Yg"


def _get_ext_grid_bus(net):
    """Return the bus index of the first ext_grid."""
    if 'ext_grid_sequence' in net and not net.ext_grid_sequence.empty:
        eg = net.ext_grid_sequence
        first_idx = eg.index.get_level_values(0)[0]
        return int(eg.loc[first_idx].iloc[0]['bus'])
    elif not net.ext_grid.empty:
        eg = net.ext_grid
        first_idx = eg.index.get_level_values(0)[0]
        eg_data = eg.loc[first_idx]
        return int(eg_data.iloc[0]['bus']) if hasattr(eg_data, 'iloc') else int(eg_data['bus'])
    return None


def _node_id(bus_idx, circuit_name, head_bus):
    """Map a bus index to a CYME node ID.

    The ext_grid (head) bus maps to the circuit head node (circuit_name)
    so that sections are connected to the source.
    """
    if bus_idx == head_bus:
        return circuit_name
    return f"BUS_{bus_idx}"


# ---------------------------------------------------------------------------
# Source
# ---------------------------------------------------------------------------
def _create_source(net, circuit_name):
    """Set CYME equivalent source from ext_grid / ext_grid_sequence."""
    if 'ext_grid_sequence' in net and not net.ext_grid_sequence.empty:
        eg = net.ext_grid_sequence
        first_idx = eg.index.get_level_values(0)[0]
        eg_data = eg.loc[first_idx]
        bus_idx = int(eg_data.iloc[0]['bus'])
        vm_pu = float(eg_data.iloc[0]['vm_pu'])
    elif not net.ext_grid.empty:
        eg = net.ext_grid
        first_idx = eg.index.get_level_values(0)[0]
        eg_data = eg.loc[first_idx]
        bus_idx = int(eg_data.iloc[0]['bus']) if hasattr(eg_data, 'iloc') else int(eg_data['bus'])
        vm_pu = float(eg_data.iloc[0]['vm_pu']) if hasattr(eg_data, 'iloc') else float(eg_data['vm_pu'])
    else:
        logger.warning("No ext_grid found in network")
        return

    bus_vn_kv = float(net.bus.loc[(bus_idx, 1)]['vn_kv']) if (bus_idx, 1) in net.bus.index else float(net.bus.loc[bus_idx].iloc[0]['vn_kv'])
    operating_kv = bus_vn_kv * vm_pu
    voltage_ln = operating_kv / math.sqrt(3.0)

    cympy.study.SetValueTopo(bus_vn_kv, 'Sources[0].EquivalentSourceModels[0].EquivalentSource.KVLL', circuit_name)
    cympy.study.SetValueTopo(voltage_ln, 'Sources[0].EquivalentSourceModels[0].EquivalentSource.OperatingVoltage1', circuit_name)
    cympy.study.SetValueTopo(voltage_ln, 'Sources[0].EquivalentSourceModels[0].EquivalentSource.OperatingVoltage2', circuit_name)
    cympy.study.SetValueTopo(voltage_ln, 'Sources[0].EquivalentSourceModels[0].EquivalentSource.OperatingVoltage3', circuit_name)
    logger.info(f"Source set: {bus_vn_kv} kVLL, {vm_pu} p.u.")


# ---------------------------------------------------------------------------
# Lines
# ---------------------------------------------------------------------------
def _create_lines(net, circuit_name, head_bus):
    """Create CYME line equipment and devices from mc line table."""
    if net.line.empty:
        return

    line_indices = net.line.index.get_level_values(0).unique()

    for line_idx in line_indices:
        line_data = net.line.loc[line_idx]
        if isinstance(line_data, pd.Series):
            line_data = line_data.to_frame().T

        first_row = line_data.iloc[0]
        from_bus = int(first_row['from_bus'])
        to_bus = int(first_row['to_bus'])
        length_km = float(first_row['length_km'])
        in_service = bool(first_row['in_service'])

        # Collect phases from all circuits
        phases = set()
        for _, row in line_data.iterrows():
            p = _mc_phase_to_cyme(row['from_phase'])
            if p and p != 'N':
                phases.add(p)
        phase_str = "".join(sorted(phases))

        std_type = str(first_row.get('std_type', ''))
        model_type = str(first_row.get('model_type', ''))
        eq_id = f"MC_LINE_{line_idx}"

        # Create unbalanced line equipment
        equip_type = 19  # UnbalancedLine
        check_eq = cympy.eq.GetEquipment(eq_id, equip_type)
        if check_eq is None:
            cympy.eq.Add(eq_id, equip_type)

        eq = cympy.eq.GetEquipment(eq_id, equip_type)
        eq.SetValue(1, 'UserDefinedImpedances')

        # Try to extract impedance from std_type tables
        if model_type == 'sequence' and std_type in net.sequence_std_type.index:
            st = net.sequence_std_type.loc[std_type]
            r_ohm_km = float(st['r_ohm_per_km'])
            x_ohm_km = float(st['x_ohm_per_km'])
            max_i_ka = float(st.get('max_i_ka', 0.4))

            for ph in ('A', 'B', 'C'):
                if ph in phase_str:
                    eq.SetValue(r_ohm_km * length_km, f'SelfResistance{ph}')
                    eq.SetValue(x_ohm_km * length_km, f'SelfReactance{ph}')
                    eq.SetValue(max_i_ka * 1000, f'NominalRating{ph}')
                else:
                    eq.SetValue(0.0, f'SelfResistance{ph}')
                    eq.SetValue(0.0, f'SelfReactance{ph}')
                    eq.SetValue(0.0, f'NominalRating{ph}')

            # Zero-sequence mutual impedances (simplified)
            r0 = float(st.get('r0_ohm_per_km', 0)) * length_km
            x0 = float(st.get('x0_ohm_per_km', 0)) * length_km
            # Mutual = (Z0 - Z1) / 3 approximation
            r_mut = (r0 - r_ohm_km * length_km) / 3.0
            x_mut = (x0 - x_ohm_km * length_km) / 3.0
            for pair in ('AB', 'BC', 'CA'):
                eq.SetValue(r_mut, f'MutualResistance{pair}')
                eq.SetValue(x_mut, f'MutualReactance{pair}')
        elif model_type == 'matrix' and std_type:
            # Matrix model: read impedance matrix directly
            if std_type in net.matrix_std_type.index.get_level_values(0):
                mat = net.matrix_std_type.loc[std_type]
                # Self impedances from diagonal
                for i, ph in enumerate(('A', 'B', 'C'), 1):
                    if ph in phase_str:
                        r_val = float(mat.iloc[0].get(f'r_{i}', 0)) * length_km if f'r_{i}' in mat.columns else 0
                        x_val = float(mat.iloc[0].get(f'x_{i}', 0)) * length_km if f'x_{i}' in mat.columns else 0
                        eq.SetValue(r_val, f'SelfResistance{ph}')
                        eq.SetValue(x_val, f'SelfReactance{ph}')
                        eq.SetValue(400, f'NominalRating{ph}')
                    else:
                        eq.SetValue(0.0, f'SelfResistance{ph}')
                        eq.SetValue(0.0, f'SelfReactance{ph}')
                        eq.SetValue(0.0, f'NominalRating{ph}')
        else:
            # Fallback: set minimal impedance so CYME doesn't error
            for ph in ('A', 'B', 'C'):
                if ph in phase_str:
                    eq.SetValue(0.001, f'SelfResistance{ph}')
                    eq.SetValue(0.001, f'SelfReactance{ph}')
                    eq.SetValue(400, f'NominalRating{ph}')
                else:
                    eq.SetValue(0.0, f'SelfResistance{ph}')
                    eq.SetValue(0.0, f'SelfReactance{ph}')
                    eq.SetValue(0.0, f'NominalRating{ph}')

        # Zero out shunt/mutual values not yet set
        for ph in ('A', 'B', 'C'):
            eq.SetValue(0.0, f'ShuntSusceptance{ph}')
            eq.SetValue(0.0, f'ShuntConductance{ph}')
        for pair in ('AB', 'BC', 'CA'):
            if not (model_type == 'sequence' and std_type in net.sequence_std_type.index):
                eq.SetValue(0.0, f'MutualResistance{pair}')
                eq.SetValue(0.0, f'MutualReactance{pair}')
            eq.SetValue(0.0, f'MutualShuntSusceptance{pair}')
            eq.SetValue(0.0, f'MutualShuntConductance{pair}')

        # Create the section and line device in CYME
        section_id = f"LINE_{line_idx}"
        from_node_id = _node_id(from_bus, circuit_name, head_bus)
        to_node_id = _node_id(to_bus, circuit_name, head_bus)
        dev_type = 12  # UnbalancedLine

        try:
            cympy.study.AddSection(section_id, circuit_name, section_id, dev_type, from_node_id, to_node_id)
            section = cympy.study.GetSection(section_id)
            if section is not None:
                section.SetValue(phase_str, 'Phase')
            cympy.study.ReplaceDevice(section_id, dev_type, dev_type, eq_id)
            cympy.study.SetValueDevice(length_km * 1000, "Length", section_id, dev_type)
        except cympy.err.CymError as e:
            logger.warning(f"Line {line_idx}: {e}")

    logger.info(f"Created {len(line_indices)} lines")


# ---------------------------------------------------------------------------
# Transformers
# ---------------------------------------------------------------------------
def _create_transformers(net, circuit_name, head_bus):
    """Create CYME transformer equipment and devices from mc trafo1ph table."""
    if net.trafo1ph.empty:
        return

    trafo_indices = net.trafo1ph.index.get_level_values(0).unique()

    for trafo_idx in trafo_indices:
        trafo_data = net.trafo1ph.loc[trafo_idx]
        if isinstance(trafo_data, pd.Series):
            trafo_data = trafo_data.to_frame().T

        # Get HV and LV bus indices
        bus_levels = trafo_data.index.get_level_values('bus').unique() if 'bus' in trafo_data.index.names else [0]
        if len(bus_levels) < 2:
            buses = sorted(set(trafo_data.reset_index()['bus'].values))
        else:
            buses = sorted(bus_levels)

        hv_bus = buses[0]
        lv_bus = buses[1] if len(buses) > 1 else buses[0]

        # Get transformer parameters from the first winding
        flat_data = trafo_data.reset_index() if isinstance(trafo_data.index, pd.MultiIndex) else trafo_data
        first_row = flat_data.iloc[0]

        sn_mva = float(first_row.get('sn_mva', 0.05))
        vk_pct = float(first_row.get('vk_percent', 4.0))
        vkr_pct = float(first_row.get('vkr_percent', 1.0))
        tap_pos = float(first_row.get('tap_pos', 0))
        tap_min = float(first_row.get('tap_min', -16))
        tap_max = float(first_row.get('tap_max', 16))
        tap_step_pct = float(first_row.get('tap_step_percent', 0.625))

        hv_vn_kv = float(net.bus.loc[(hv_bus, 1)]['vn_kv']) if (hv_bus, 1) in net.bus.index else float(net.bus.loc[hv_bus].iloc[0]['vn_kv'])
        lv_vn_kv = float(net.bus.loc[(lv_bus, 1)]['vn_kv']) if (lv_bus, 1) in net.bus.index else float(net.bus.loc[lv_bus].iloc[0]['vn_kv'])

        # Collect phases
        phases = set()
        for _, row in flat_data.iterrows():
            p = _mc_phase_to_cyme(row['from_phase'])
            if p and p != 'N':
                phases.add(p)
        phase_str = "".join(sorted(phases))

        # Determine connection type
        connection = _vector_group_to_connection(flat_data)

        # Determine device type (by-phase vs 3-phase)
        n_windings = len(flat_data)
        is_by_phase = (n_windings <= 3 and len(phases) <= 3)
        dev_type = 33 if is_by_phase else 1  # 33=TransformerByPhase, 1=Transformer

        rating_kva = sn_mva * 1000
        z1_pct = vk_pct
        x1r1_ratio = math.sqrt(max(0, (vk_pct / vkr_pct) ** 2 - 1)) if vkr_pct > 0 else 10.0

        eq_id = f"MC_TX_{trafo_idx}"

        # Create equipment
        check_eq = cympy.eq.GetEquipment(eq_id, cympy.enums.EquipmentType.Transformer)
        if check_eq is None:
            cympy.eq.Add(eq_id, cympy.enums.EquipmentType.Transformer)
        tx_eq = cympy.eq.GetEquipment(eq_id, cympy.enums.EquipmentType.Transformer)

        if dev_type == 33:
            tx_eq.SetValue('SinglePhase', 'TransfoType')
        else:
            tx_eq.SetValue('ThreePhase', 'TransfoType')
            tx_eq.SetValue(0, 'ZeroSequenceImpedancePercent')
            tx_eq.SetValue(0, 'XR0Ratio')

        tx_eq.SetValue(rating_kva, 'NominalRatingKVA')
        tx_eq.SetValue(hv_vn_kv, 'PrimaryVoltage')
        tx_eq.SetValue(lv_vn_kv, 'SecondaryVoltage')
        tx_eq.SetValue(z1_pct, 'PositiveSequenceImpedancePercent')
        tx_eq.SetValue(x1r1_ratio, 'XRRatio')
        tx_eq.SetValue(connection, 'TransformerConnection')

        # Create section
        section_id = f"TX_{trafo_idx}-XFO"
        from_node = _node_id(hv_bus, circuit_name, head_bus)
        to_node = _node_id(lv_bus, circuit_name, head_bus)
        dev_name = f"TX_{trafo_idx}"

        try:
            cympy.study.AddSection(section_id, circuit_name, dev_name, dev_type, from_node, to_node)
            section = cympy.study.GetSection(section_id)
            if section is not None:
                section.SetValue(phase_str, 'Phase')

            # Set per-phase transformer IDs for by-phase type
            if dev_type == 33:
                for ph in ('A', 'B', 'C'):
                    ph_idx = {'A': 1, 'B': 2, 'C': 3}[ph]
                    if ph in phase_str:
                        cympy.study.SetValueDevice(eq_id, f"PhaseTransformerID{ph_idx}", dev_name, dev_type)
                    else:
                        cympy.study.SetValueDevice('', f"PhaseTransformerID{ph_idx}", dev_name, dev_type)
                cympy.study.SetValueDevice(connection, "Connection", dev_name, dev_type)
            else:
                cympy.study.SetValueDevice(eq_id, "DeviceID", dev_name, dev_type)
                cympy.study.SetValueDevice(connection, "TransformerConnection", dev_name, dev_type)

            # Set phase shift
            shift_deg = 30 if 'D' in connection else 0
            cympy.study.SetValueDevice(f"{shift_deg}deg", "PhaseShift", dev_name, dev_type)

            # Set LV bus voltage
            cympy.study.SetValueNode(lv_vn_kv, "UserDefinedBaseVoltage", to_node)

        except cympy.err.CymError as e:
            logger.warning(f"Transformer {trafo_idx}: {e}")

    logger.info(f"Created {len(trafo_indices)} transformers")


# ---------------------------------------------------------------------------
# Loads
# ---------------------------------------------------------------------------
def _create_loads(net, circuit_name, head_bus):
    """Create CYME spot loads from mc asymmetric_load table."""
    if net.asymmetric_load.empty:
        return

    load_indices = net.asymmetric_load.index.get_level_values(0).unique()

    for load_idx in load_indices:
        load_data = net.asymmetric_load.loc[load_idx]
        if isinstance(load_data, pd.Series):
            load_data = load_data.to_frame().T

        first_row = load_data.iloc[0]
        bus_idx = int(first_row['bus'])

        # Collect per-phase P and Q
        phase_pq = {}
        for _, row in load_data.iterrows():
            ph = _mc_phase_to_cyme(row['from_phase'])
            if ph and ph != 'N':
                phase_pq[ph] = {
                    'kw': float(row['p_mw']) * 1000,
                    'kvar': float(row['q_mvar']) * 1000
                }

        phase_str = "".join(sorted(phase_pq.keys()))
        section_id = f"LOAD_{load_idx}-L"
        dev_name = f"LOAD_{load_idx}"
        from_node = _node_id(bus_idx, circuit_name, head_bus)

        try:
            cympy.study.AddSection(section_id, circuit_name, dev_name,
                                   cympy.enums.DeviceType.SpotLoad, from_node)
            section = cympy.study.GetSection(section_id)
            if section is not None:
                section.SetValue(phase_str, 'Phase')

            spot_load = cympy.study.GetDevice(dev_name, cympy.enums.DeviceType.SpotLoad)
            spot_load.SetValue("KW_KVAR", "CustomerLoads[0].CustomerLoadModels[0].LoadValueType")

            # Map phases to CYME load value indices (0=A/first, 1=B/second, 2=C/third)
            sorted_phases = sorted(phase_pq.keys())
            for i, ph in enumerate(sorted_phases):
                pq = phase_pq[ph]
                spot_load.SetValue(pq['kw'], f"CustomerLoads[0].CustomerLoadModels[0].CustomerLoadValues[{i}].LoadValue.KW")
                spot_load.SetValue(pq['kvar'], f"CustomerLoads[0].CustomerLoadModels[0].CustomerLoadValues[{i}].LoadValue.KVAR")

        except cympy.err.CymError as e:
            logger.warning(f"Load {load_idx}: {e}")

    logger.info(f"Created {len(load_indices)} loads")


# ---------------------------------------------------------------------------
# Generators (asymmetric_sgen -> ECG)
# ---------------------------------------------------------------------------
def _create_sgens(net, circuit_name, head_bus):
    """Create CYME ECG devices from mc asymmetric_sgen table."""
    if net.asymmetric_sgen.empty:
        return

    sgen_indices = net.asymmetric_sgen.index.get_level_values(0).unique()

    for sgen_idx in sgen_indices:
        sgen_data = net.asymmetric_sgen.loc[sgen_idx]
        if isinstance(sgen_data, pd.Series):
            sgen_data = sgen_data.to_frame().T

        first_row = sgen_data.iloc[0]
        bus_idx = int(first_row['bus'])
        sn_mva = float(first_row.get('sn_mva', 0.1))
        in_service = bool(first_row.get('in_service', True))

        # Sum active power across phases
        total_p_kw = sum(float(r['p_mw']) * 1000 for _, r in sgen_data.iterrows())

        phases = set()
        for _, row in sgen_data.iterrows():
            p = _mc_phase_to_cyme(row['from_phase'])
            if p and p != 'N':
                phases.add(p)
        phase_str = "".join(sorted(phases))

        section_id = f"SGEN_{sgen_idx}-G"
        dev_name = f"SGEN_{sgen_idx}"
        from_node = _node_id(bus_idx, circuit_name, head_bus)
        dev_type = cympy.enums.DeviceType.ElectronicConverterGenerator

        try:
            cympy.study.AddSection(section_id, circuit_name, dev_name, dev_type, from_node)
            section = cympy.study.GetSection(section_id)
            if section is not None:
                section.SetValue(phase_str, 'Phase')

            gen_dev = cympy.study.GetDevice(dev_name, dev_type)
            gen_dev.SetValue('Connected' if in_service else 'Disconnected', 'ConnectionStatus')
            gen_dev.SetValue(total_p_kw, 'GenerationModels.Get(1).ActiveGeneration')
            gen_dev.SetValue(-100, 'GenerationModels.Get(1).PowerFactor')

            # Inverter ratings
            bus_vn_kv = float(net.bus.loc[(bus_idx, 1)]['vn_kv']) if (bus_idx, 1) in net.bus.index else 0
            gen_dev.SetValue('SinglePhase', 'Inverter.ACDCConverterSettings.Type')
            gen_dev.SetValue(sn_mva * 1000, 'Inverter.ACDCConverterSettings.ConverterRating')
            gen_dev.SetValue(bus_vn_kv, 'Inverter.ACDCConverterSettings.NominalACVoltage')
            gen_dev.SetValue(sn_mva * 1000, 'Inverter.ACDCConverterSettings.ActivePowerRating')
            gen_dev.SetValue(sn_mva * 1000, 'Inverter.ACDCConverterSettings.ReactivePowerRating')

            # Control mode
            control_mode = str(first_row.get('control_mode', ''))
            if control_mode == 'Volt/Var':
                logger.info(f"SGEN {sgen_idx}: Volt/Var control noted (apply separately via control scripts)")

        except cympy.err.CymError as e:
            logger.warning(f"Sgen {sgen_idx}: {e}")

    logger.info(f"Created {len(sgen_indices)} sgens")


# ---------------------------------------------------------------------------
# Shunt Capacitors
# ---------------------------------------------------------------------------
def _create_shunts(net, circuit_name, head_bus):
    """Create CYME shunt capacitors from mc asymmetric_shunt table."""
    if net.asymmetric_shunt.empty:
        return

    shunt_indices = net.asymmetric_shunt.index.get_level_values(0).unique()

    for shunt_idx in shunt_indices:
        shunt_data = net.asymmetric_shunt.loc[shunt_idx]
        if isinstance(shunt_data, pd.Series):
            shunt_data = shunt_data.to_frame().T

        first_row = shunt_data.iloc[0]
        bus_idx = int(first_row['bus'])
        vn_kv = float(first_row.get('vn_kv', 0))
        control_mode = str(first_row.get('control_mode', 'fixed'))

        # Collect per-phase Q
        phase_q = {}
        for _, row in shunt_data.iterrows():
            ph = _mc_phase_to_cyme(row['from_phase'])
            if ph and ph != 'N':
                phase_q[ph] = float(row.get('max_q_mvar', row.get('q_mvar', 0))) * 1000  # kvar

        phase_str = "".join(sorted(phase_q.keys()))
        section_id = f"SHUNT_{shunt_idx}-CAP"
        dev_name = f"SHUNT_{shunt_idx}"
        from_node = _node_id(bus_idx, circuit_name, head_bus)

        try:
            cympy.study.AddSection(section_id, circuit_name, dev_name,
                                   cympy.enums.DeviceType.ShuntCapacitor, from_node)
            section = cympy.study.GetSection(section_id)
            if section is not None:
                section.SetValue(phase_str, 'Phase')

            cap_dev = cympy.study.GetDevice(dev_name, cympy.enums.DeviceType.ShuntCapacitor)
            setkvl = vn_kv / math.sqrt(3) if vn_kv > 0 else 0
            cap_dev.SetValue(setkvl, "KVLN")

            if control_mode == 'fixed':
                for ph in ('A', 'B', 'C'):
                    kvar = phase_q.get(ph, 0)
                    cap_dev.SetValue(kvar, f"FixedKVAR{ph}")
                    cap_dev.SetValue(0.0, f"SwitchedKVAR{ph}")
            else:
                v_on = float(first_row.get('v_threshold_on', 0.95)) * 120
                v_off = float(first_row.get('v_threshold_off', 1.05)) * 120
                for ph in ('A', 'B', 'C'):
                    kvar = phase_q.get(ph, 0)
                    cap_dev.SetValue(0.0, f"FixedKVAR{ph}")
                    cap_dev.SetValue(kvar, f"SwitchedKVAR{ph}")
                cap_dev.Execute('CapacitorControl.SetType(VoltageControlled)')
                cap_dev.SetValue('ThreePhase', 'CapacitorControl.SwitchingMode')
                cap_dev.SetValue(phase_str, 'CapacitorControl.InitiallyClosedPhase')
                cap_dev.SetValue(phase_str, 'CapacitorControl.CurrentClosedPhase')
                cap_dev.SetValue(phase_str, 'CapacitorControl.ControlledPhase')
                cap_dev.SetValue('Capacitor', 'CapacitorControl.SensorLocation')
                for ph in ('A', 'B', 'C'):
                    cap_dev.SetValue(v_on, f'CapacitorControl.OnValue{ph}')
                    cap_dev.SetValue(v_off, f'CapacitorControl.OffValue{ph}')

        except cympy.err.CymError as e:
            logger.warning(f"Shunt {shunt_idx}: {e}")

    logger.info(f"Created {len(shunt_indices)} shunts")


# ---------------------------------------------------------------------------
# Switches
# ---------------------------------------------------------------------------
def _create_switches(net, circuit_name, head_bus):
    """Create CYME switches from mc switch table."""
    if net.switch.empty:
        return

    switch_indices = net.switch.index.get_level_values(0).unique()

    for sw_idx in switch_indices:
        sw_data = net.switch.loc[sw_idx]
        if isinstance(sw_data, pd.Series):
            sw_data = sw_data.to_frame().T

        first_row = sw_data.iloc[0]
        bus_idx = int(first_row['bus'])
        element_idx = int(first_row['element'])
        et = str(first_row['et'])
        closed = bool(first_row['closed'])

        phases = set()
        for _, row in sw_data.iterrows():
            p = _mc_phase_to_cyme(row['phase'])
            if p and p != 'N':
                phases.add(p)
        phase_str = "".join(sorted(phases))

        section_id = f"SW_{sw_idx}"
        dev_name = f"SW_{sw_idx}"
        from_node = _node_id(bus_idx, circuit_name, head_bus)

        # Determine to-node based on element type
        if et == 'l':  # line
            to_node = f"BUS_{element_idx}_SW"
        elif et == 'b':  # bus
            to_node = _node_id(element_idx, circuit_name, head_bus)
        else:
            to_node = _node_id(element_idx, circuit_name, head_bus)

        try:
            cympy.study.AddSection(section_id, circuit_name, dev_name,
                                   cympy.enums.DeviceType.Switch, from_node, to_node)
            section = cympy.study.GetSection(section_id)
            if section is not None:
                section.SetValue(phase_str, 'Phase')

            sw_dev = cympy.study.GetDevice(dev_name, cympy.enums.DeviceType.Switch)
            sw_dev.SetValue(phase_str if closed else 'None', 'ClosedPhase')

        except cympy.err.CymError as e:
            logger.warning(f"Switch {sw_idx}: {e}")

    logger.info(f"Created {len(switch_indices)} switches")


# ---------------------------------------------------------------------------
# Controllers (map mc controllers to CYME regulator tap settings)
# ---------------------------------------------------------------------------
def _create_controllers(net, circuit_name):
    """Map mc controller objects to CYME device settings."""
    if not hasattr(net, 'controller') or net.controller.empty:
        logger.info("No controllers to map")
        return

    from multiconductor.control.line_drop_control import LineDropControl, LineDropControlExtended
    from multiconductor.control.load_tap_changer_control import LoadTapChangerControl
    from multiconductor.control.shunt_controller import MulticonductorBinaryShuntController
    from multiconductor.control.volt_var_control import VoltVarController

    for i, ctr_row in net.controller.iterrows():
        ctr = ctr_row['object']
        if not ctr_row.get('in_service', True):
            continue

        if isinstance(ctr, (LineDropControl, LineDropControlExtended)):
            trafo_idx = ctr.trafo_top_level_index
            dev_name = f"TX_{trafo_idx}"
            logger.info(f"Controller {i}: LineDropControl mapped to regulator for {dev_name}")
            if hasattr(ctr, 'tap_pos') and ctr.tap_pos is not None:
                for j, tap in enumerate(ctr.tap_pos):
                    ph = ('A', 'B', 'C')[j] if j < 3 else ''
                    logger.info(f"  Phase {ph}: tap_pos={tap}")

        elif isinstance(ctr, LoadTapChangerControl):
            trafo_idx = ctr.trafo_top_level_index
            dev_name = f"TX_{trafo_idx}"
            logger.info(f"Controller {i}: LoadTapChangerControl for {dev_name}, mode={ctr.mode}")

        elif isinstance(ctr, MulticonductorBinaryShuntController):
            logger.info(f"Controller {i}: ShuntController for indices {ctr.shunt_indices}")

        elif isinstance(ctr, VoltVarController):
            logger.info(f"Controller {i}: VoltVarController for {ctr.element} {ctr.element_index}")

        else:
            logger.info(f"Controller {i}: {type(ctr).__name__} (no CYME mapping)")


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def mc_to_cyme(net, circuit_name, sxst_path=None, create_new_study=False):
    """Convert a multiconductor network to a CYME network model."""
    if create_new_study:
        cympy.study.New()

    # Create the network in the study
    cympy.study.AddNetwork(circuit_name, cympy.enums.NetworkType.Feeder)

    # Determine the ext_grid (head) bus so sections connect to the source node
    head_bus = _get_ext_grid_bus(net)
    logger.info(f"Converting mc network to CYME circuit: {circuit_name} (head_bus={head_bus})")

    # Build the CYME network from mc tables
    _create_source(net, circuit_name)
    _create_lines(net, circuit_name, head_bus)
    _create_transformers(net, circuit_name, head_bus)
    _create_loads(net, circuit_name, head_bus)
    _create_sgens(net, circuit_name, head_bus)
    _create_shunts(net, circuit_name, head_bus)
    _create_switches(net, circuit_name, head_bus)
    _create_controllers(net, circuit_name)

    if sxst_path:
        try:
            cympy.study.Save(str(sxst_path), True)
            logger.info(f"Study saved to {sxst_path}")
        except cympy.err.CymError as e:
            logger.warning(f"Failed to save study: {e}")

    logger.info("Conversion complete")
    return True