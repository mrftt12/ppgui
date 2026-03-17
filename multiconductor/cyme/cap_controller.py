import locale
# Parameters Definition
# PARAMETER: CLOSE_AT_V, NUMERIC, 115.0
# PARAMETER: TRIP_AT_V, NUMERIC, 120.0

# Retrieve the script parameters
CLOSE_AT = cympy.GetInputParameter('CLOSE_AT_V')
TRIP_AT = cympy.GetInputParameter('TRIP_AT_V')

# Get the shunt capacitor
Capacitor = cympy.study.GetCurrentDevice()

if Capacitor is not None:
    ValA = 0.0
    ValB = 0.0
    ValC = 0.0

    # Valid phase at the capacitor location
    Ph = cympy.study.QueryInfoDevice('Phase', Capacitor.DeviceNumber, cympy.enums.DeviceType.ShuntCapacitor)
    
    # Find the voltage on each phase
    if 'A' in Ph:
        ValA = locale.atof(cympy.study.QueryInfoDevice('VBaseA', Capacitor.DeviceNumber, cympy.enums.DeviceType.ShuntCapacitor, 4))
    if 'B' in Ph:
        ValB = locale.atof(cympy.study.QueryInfoDevice('VBaseB', Capacitor.DeviceNumber, cympy.enums.DeviceType.ShuntCapacitor, 4))
    if 'C' in Ph:
        ValC = locale.atof(cympy.study.QueryInfoDevice('VBaseC', Capacitor.DeviceNumber, cympy.enums.DeviceType.ShuntCapacitor, 4))

    # Compute the average voltage
    Val = (ValA + ValB + ValC) / len(Ph)

    # Compare to the threshold and choose the action to be taken
    if Val < CLOSE_AT:
        cympy.results.Add('STAT', 1)
    elif Val > TRIP_AT:
        cympy.results.Add('STAT', 0)
    else:
        cympy.results.Add('STAT', -1)