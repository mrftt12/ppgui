import locale
import cmath

# Line Drop Control (LDC) - Mimics multiconductor LineDropControlExtended
# Estimates voltage at a remote bus via PT/CT sensing and line drop compensation,
# then adjusts the regulator tap one step at a time.

# Parameters Definition
# PARAMETER: V_SET_SECONDARY_V, NUMERIC, 122.0
# PARAMETER: BANDWIDTH_SECONDARY_V, NUMERIC, 2.0
# PARAMETER: PT_RATIO, NUMERIC, 60.0
# PARAMETER: CT_PRIMARY_RATING_A, NUMERIC, 600.0
# PARAMETER: R_LDC_V, NUMERIC, 8.0
# PARAMETER: X_LDC_V, NUMERIC, 10.0
# PARAMETER: MODE, TEXT, bidirectional

# Retrieve parameters
V_SET = cympy.GetInputParameter('V_SET_SECONDARY_V')
BANDWIDTH = cympy.GetInputParameter('BANDWIDTH_SECONDARY_V')
PT_RATIO = cympy.GetInputParameter('PT_RATIO')
CT_PRIMARY = cympy.GetInputParameter('CT_PRIMARY_RATING_A')
R_LDC = cympy.GetInputParameter('R_LDC_V')
X_LDC = cympy.GetInputParameter('X_LDC_V')
MODE = cympy.GetInputParameter('MODE')  # 'locked_forward', 'locked_reverse', or 'bidirectional'

Regulator = cympy.study.GetCurrentDevice()

if Regulator is not None:
    Ph = cympy.study.QueryInfoDevice('Phase', Regulator.DeviceNumber, Regulator.DeviceType)

    # Determine if regulator is RegulatorByPhase or Regulator
    is_by_phase = (Regulator.DeviceType == cympy.enums.DeviceType.RegulatorByPhase)
    dev_type = cympy.enums.DeviceType.RegulatorByPhase if is_by_phase else cympy.enums.DeviceType.Regulator

    # Collect per-phase voltage (kV), current (A), and power for power-flow direction
    phase_data = {}
    for ph in ('A', 'B', 'C'):
        if ph not in Ph:
            continue
        # Voltage magnitude and angle at regulator output (secondary side)
        v_mag = locale.atof(cympy.study.QueryInfoDevice(f'VBase{ph}', Regulator.DeviceNumber, dev_type, 6))
        v_ang = locale.atof(cympy.study.QueryInfoDevice(f'VAngle{ph}', Regulator.DeviceNumber, dev_type, 6))
        # Current magnitude and angle
        i_mag = locale.atof(cympy.study.QueryInfoDevice(f'I{ph}', Regulator.DeviceNumber, dev_type, 6))
        i_ang = locale.atof(cympy.study.QueryInfoDevice(f'IAngle{ph}', Regulator.DeviceNumber, dev_type, 6))
        # Active power for direction detection
        p_kw = locale.atof(cympy.study.QueryInfoDevice(f'KW{ph}', Regulator.DeviceNumber, dev_type, 6))
        phase_data[ph] = {
            'v_mag': v_mag, 'v_ang': v_ang,
            'i_mag': i_mag, 'i_ang': i_ang,
            'p_kw': p_kw
        }

    # Determine power flow direction (forward = power flowing from source to load)
    total_p = sum(d['p_kw'] for d in phase_data.values())
    pf_forward = total_p >= 0

    # Mode check: skip control if mode restricts action based on power direction
    if (MODE == 'locked_forward' and not pf_forward) or (MODE == 'locked_reverse' and pf_forward):
        # Do not adjust tap - output current tap positions unchanged
        for ph in ('A', 'B', 'C'):
            if ph in Ph:
                tap_key = f'RegTap{ph}' if is_by_phase or len(Ph) > 1 else 'RegTap'
                actual_tap = locale.atof(cympy.study.QueryInfoDevice(tap_key, Regulator.DeviceNumber, dev_type))
                cympy.results.Add(f'TAP{ph}', actual_tap)
    else:
        # Line drop compensation voltage estimation and tap adjustment per phase
        Z_ldc = complex(R_LDC, X_LDC)

        for ph in ('A', 'B', 'C'):
            if ph not in Ph:
                continue
            d = phase_data[ph]

            # Convert voltage and current to complex phasors
            V_sec_kv = (d['v_mag'] / PT_RATIO) * cmath.exp(1j * cmath.pi * d['v_ang'] / 180.0)
            I_prim_a = d['i_mag'] * cmath.exp(1j * cmath.pi * d['i_ang'] / 180.0)

            # Estimated voltage at remote bus (secondary volts)
            # V_est = V_secondary - Z_ldc * (I_primary / CT_rating)
            V_est = V_sec_kv - Z_ldc * (I_prim_a / CT_PRIMARY)
            V_est_mag = abs(V_est) * 1000.0  # convert to volts

            # Voltage bandwidth limits (in secondary volts)
            V_lower = V_SET - BANDWIDTH
            V_upper = V_SET + BANDWIDTH

            # Get tap parameters
            if is_by_phase:
                eq_key = f'RegByPhaseEqId{ph}'
                eq_id = cympy.study.QueryInfoDevice(eq_key, Regulator.DeviceNumber, dev_type)
                eq = cympy.eq.GetEquipment(eq_id, cympy.enums.EquipmentType.Regulator)
                NbTap = int(eq.GetValue('NumberOfTaps'))
                MaxBoost = locale.atof(cympy.study.QueryInfoDevice(f'RegByPhaseBoost{ph}', Regulator.DeviceNumber, dev_type))
                MaxBuck = locale.atof(cympy.study.QueryInfoDevice(f'RegByPhaseBuck{ph}', Regulator.DeviceNumber, dev_type))
                tap_key = f'RegTap{ph}'
            else:
                NbTap = int(cympy.study.QueryInfoDevice('RegNbtap', Regulator.DeviceNumber, dev_type))
                MaxBoost = locale.atof(cympy.study.QueryInfoDevice('RegBoost', Regulator.DeviceNumber, dev_type))
                MaxBuck = locale.atof(cympy.study.QueryInfoDevice('RegBuck', Regulator.DeviceNumber, dev_type))
                tap_key = f'RegTap{ph}' if len(Ph) > 1 else 'RegTap'

            ActualTap = locale.atof(cympy.study.QueryInfoDevice(tap_key, Regulator.DeviceNumber, dev_type))
            TapMin = -NbTap / 2.0
            TapMax = NbTap / 2.0

            # Discrete one-step tap adjustment
            if V_est_mag < V_lower and ActualTap < TapMax:
                new_tap = ActualTap + 1
            elif V_est_mag > V_upper and ActualTap > TapMin:
                new_tap = ActualTap - 1
            else:
                new_tap = ActualTap

            cympy.results.Add(f'TAP{ph}', new_tap)