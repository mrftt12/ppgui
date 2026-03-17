import locale
# Target voltage with default values
# PARAMETER: TARGET_V, NUMERIC, 122

# Retrieve the parameter
TargetVoltage = cympy.GetInputParameter('TARGET_V')

# Get the actual regulator
Regulator = cympy.study.GetCurrentDevice()

ValA = 0.0
ValB = 0.0
ValC = 0.0

# Valid Phase
Ph = cympy.study.QueryInfoDevice('Phase', Regulator.DeviceNumber, cympy.enums.DeviceType.Regulator)

# Get the voltage on each valid phase
if 'A' in Ph:
    ValA = locale.atof(cympy.study.QueryInfoDevice('VBaseA', Regulator.DeviceNumber, cympy.enums.DeviceType.Regulator, 6))
if 'B' in Ph:
    ValB = locale.atof(cympy.study.QueryInfoDevice('VBaseB', Regulator.DeviceNumber, cympy.enums.DeviceType.Regulator, 6))
if 'C' in Ph:
    ValC = locale.atof(cympy.study.QueryInfoDevice('VBaseC', Regulator.DeviceNumber, cympy.enums.DeviceType.Regulator, 6))

# Calculate the average output voltage
Val = (ValA + ValB + ValC) / len(Ph)

# Difference between the actual average voltage and the target
Delta = ( TargetVoltage - Val ) / TargetVoltage

# Get the number of taps of the regulator
NbTap = int(cympy.study.QueryInfoDevice('RegNbtap', Regulator.DeviceNumber, cympy.enums.DeviceType.Regulator))

# Find the regulator actual tap
ActualTap = locale.atof(cympy.study.QueryInfoDevice('RegTap', Regulator.DeviceNumber, cympy.enums.DeviceType.Regulator))

# Maximum Boost and Buck
MaxBoost = locale.atof(cympy.study.QueryInfoDevice('RegBoost', Regulator.DeviceNumber, cympy.enums.DeviceType.Regulator))
MaxBuck = locale.atof(cympy.study.QueryInfoDevice('RegBuck', Regulator.DeviceNumber, cympy.enums.DeviceType.Regulator))

# Calculate the required tap change
regSteps = ( MaxBoost + MaxBuck ) / float(NbTap) / 100.0
NbSteps = Delta / regSteps

# Return the calculated value to CYME
cympy.results.Add('TAP', NbSteps + ActualTap )