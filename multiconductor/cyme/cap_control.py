import locale
# Parameters Definition
# PARAMETER: CLOSE_AT_A, NUMERIC, 115.0
# PARAMETER: CLOSE_AT_B, NUMERIC, 115.0
# PARAMETER: CLOSE_AT_C, NUMERIC, 115.0
# PARAMETER: TRIP_AT_A, NUMERIC, 120.0
# PARAMETER: TRIP_AT_B, NUMERIC, 120.0
# PARAMETER: TRIP_AT_C, NUMERIC, 120.0

# Retrieve the script parameters
CLOSE_AT_A = cympy.GetInputParameter('CLOSE_AT_A')
CLOSE_AT_B = cympy.GetInputParameter('CLOSE_AT_B')
CLOSE_AT_C = cympy.GetInputParameter('CLOSE_AT_C')
TRIP_AT_A = cympy.GetInputParameter('TRIP_AT_A')
TRIP_AT_B = cympy.GetInputParameter('TRIP_AT_B')
TRIP_AT_C = cympy.GetInputParameter('TRIP_AT_C')

# Get the shunt capacitor
Capacitor = cympy.study.GetCurrentDevice()

if Capacitor is not None:

    ValA = 0.0
    ValB = 0.0
    ValC = 0.0

    # Valid phase at the capacitor location
    Ph = cympy.study.QueryInfoDevice('Phase', Capacitor.DeviceNumber, cympy.enums.DeviceType.ShuntCapacitor)
    
    # Verify voltage on phase A and choose the action to be taken
    if 'A' in Ph:
        ValA = locale.atof(cympy.study.QueryInfoDevice('VBaseA', Capacitor.DeviceNumber, cympy.enums.DeviceType.ShuntCapacitor, 4))
        if ValA < CLOSE_AT_A:
            cympy.results.Add('STATA', 1)
        elif  ValA > TRIP_AT_A:
            cympy.results.Add('STATA', 0)
        else:
            cympy.results.Add('STATA', -1)

    # Verify voltage on phase B and choose the action to be taken
    if 'B' in Ph:
        ValB = locale.atof(cympy.study.QueryInfoDevice('VBaseB', Capacitor.DeviceNumber, cympy.enums.DeviceType.ShuntCapacitor, 4))
        if ValB < CLOSE_AT_B:
            cympy.results.Add('STATB', 1)
        elif  ValB > TRIP_AT_B:
            cympy.results.Add('STATB', 0)
        else:
            cympy.results.Add('STATB', -1)

    # Verify voltage on phase C and choose the action to be taken
    if 'C' in Ph:
        ValC = locale.atof(cympy.study.QueryInfoDevice('VBaseC', Capacitor.DeviceNumber, cympy.enums.DeviceType.ShuntCapacitor, 4))
        if ValC < CLOSE_AT_C:
            cympy.results.Add('STATC', 1)
        elif  ValC > TRIP_AT_C:
            cympy.results.Add('STATC', 0)
        else:
            cympy.results.Add('STATC', -1)