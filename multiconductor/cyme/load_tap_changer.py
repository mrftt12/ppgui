import locale

# Load Tap Changer Control (LTC) - Mimics multiconductor LoadTapChangerControl
# Discrete tap changer with gang and phase modes, plus oscillation detection.

# Parameters Definition
# PARAMETER: VM_LOWER_PU, NUMERIC, 0.97
# PARAMETER: VM_UPPER_PU, NUMERIC, 1.03
# PARAMETER: LTC_MODE, TEXT, gang
# PARAMETER: DETECT_OSCILLATION, NUMERIC, 0

# Retrieve parameters
VM_LOWER = cympy.GetInputParameter('VM_LOWER_PU')
VM_UPPER = cympy.GetInputParameter('VM_UPPER_PU')
LTC_MODE = cympy.GetInputParameter('LTC_MODE')  # 'gang' or 'phase'
DETECT_OSC = int(cympy.GetInputParameter('DETECT_OSCILLATION'))  # 0=False, 1=True

Regulator = cympy.study.GetCurrentDevice()

if Regulator is not None:
    Ph = cympy.study.QueryInfoDevice('Phase', Regulator.DeviceNumber, Regulator.DeviceType)
    is_by_phase = (Regulator.DeviceType == cympy.enums.DeviceType.RegulatorByPhase)
    dev_type = cympy.enums.DeviceType.RegulatorByPhase if is_by_phase else cympy.enums.DeviceType.Regulator

    # Collect per-phase voltage in p.u. and current tap positions
    vm_pu = {}
    actual_taps = {}
    tap_mins = {}
    tap_maxs = {}

    for ph in ('A', 'B', 'C'):
        if ph not in Ph:
            continue
        vm_pu[ph] = locale.atof(cympy.study.QueryInfoDevice(
            f'VBase{ph}', Regulator.DeviceNumber, dev_type, 4)) / 120.0

        if is_by_phase:
            eq_id = cympy.study.QueryInfoDevice(f'RegByPhaseEqId{ph}', Regulator.DeviceNumber, dev_type)
            eq = cympy.eq.GetEquipment(eq_id, cympy.enums.EquipmentType.Regulator)
            NbTap = int(eq.GetValue('NumberOfTaps'))
            actual_taps[ph] = locale.atof(cympy.study.QueryInfoDevice(f'RegTap{ph}', Regulator.DeviceNumber, dev_type))
        else:
            NbTap = int(cympy.study.QueryInfoDevice('RegNbtap', Regulator.DeviceNumber, dev_type))
            actual_taps[ph] = locale.atof(cympy.study.QueryInfoDevice(
                f'RegTap{ph}' if len(Ph) > 1 else 'RegTap', Regulator.DeviceNumber, dev_type))

        tap_mins[ph] = -NbTap / 2.0
        tap_maxs[ph] = NbTap / 2.0

    if LTC_MODE == 'gang':
        # Gang mode: use min and max voltage across all phases for decision
        voltages = list(vm_pu.values())
        v_min = min(voltages)
        v_max = max(voltages)

        # Single tap decision applied to all phases
        if v_min < VM_LOWER:
            increment = 1  # raise voltage
        elif v_max > VM_UPPER:
            increment = -1  # lower voltage
        else:
            increment = 0

        for ph in vm_pu:
            new_tap = actual_taps[ph] + increment
            new_tap = max(tap_mins[ph], min(tap_maxs[ph], new_tap))
            cympy.results.Add(f'TAP{ph}', new_tap)

    else:
        # Phase mode: independent per-phase tap adjustment
        for ph in vm_pu:
            v = vm_pu[ph]
            if v < VM_LOWER and actual_taps[ph] < tap_maxs[ph]:
                new_tap = actual_taps[ph] + 1
            elif v > VM_UPPER and actual_taps[ph] > tap_mins[ph]:
                new_tap = actual_taps[ph] - 1
            else:
                new_tap = actual_taps[ph]
            cympy.results.Add(f'TAP{ph}', new_tap)