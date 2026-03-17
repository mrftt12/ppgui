import locale

# Shunt Controller - Mimics multiconductor MulticonductorBinaryShuntController
# Binary gang-operated capacitor bank switching based on average bus voltage.

# Parameters Definition
# PARAMETER: V_THRESHOLD_ON, NUMERIC, 115.0
# PARAMETER: V_THRESHOLD_OFF, NUMERIC, 125.0
# PARAMETER: CONTROL_MODE, TEXT, switched

# Retrieve parameters
V_THR_ON = cympy.GetInputParameter('V_THRESHOLD_ON')
V_THR_OFF = cympy.GetInputParameter('V_THRESHOLD_OFF')
CTRL_MODE = cympy.GetInputParameter('CONTROL_MODE')  # 'fixed' or 'switched'

Capacitor = cympy.study.GetCurrentDevice()

if Capacitor is not None:
    # Fixed mode: always close the capacitor bank
    if CTRL_MODE == 'fixed':
        cympy.results.Add('STAT', 1)
    else:
        # Switched mode: binary switching based on average voltage thresholds
        Ph = cympy.study.QueryInfoDevice(
            'Phase', Capacitor.DeviceNumber, cympy.enums.DeviceType.ShuntCapacitor)

        # Collect voltages on valid phases
        voltages = []
        if 'A' in Ph:
            voltages.append(locale.atof(cympy.study.QueryInfoDevice(
                'VBaseA', Capacitor.DeviceNumber, cympy.enums.DeviceType.ShuntCapacitor, 4)))
        if 'B' in Ph:
            voltages.append(locale.atof(cympy.study.QueryInfoDevice(
                'VBaseB', Capacitor.DeviceNumber, cympy.enums.DeviceType.ShuntCapacitor, 4)))
        if 'C' in Ph:
            voltages.append(locale.atof(cympy.study.QueryInfoDevice(
                'VBaseC', Capacitor.DeviceNumber, cympy.enums.DeviceType.ShuntCapacitor, 4)))

        # Average voltage across valid phases (gang-operation decision)
        V_avg = sum(voltages) / len(voltages) if voltages else 0.0

        # Hysteresis-based switching:
        #  - Close (STAT=1) when average voltage drops below V_THRESHOLD_ON
        #  - Open  (STAT=0) when average voltage rises above V_THRESHOLD_OFF
        #  - No change (STAT=-1) when voltage is between thresholds
        if V_avg <= V_THR_ON:
            cympy.results.Add('STAT', 1)
        elif V_avg >= V_THR_OFF:
            cympy.results.Add('STAT', 0)
        else:
            cympy.results.Add('STAT', -1)