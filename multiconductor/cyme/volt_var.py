import locale
# Parameters Definition
# PARAMETER: LOWER_LIMIT_V, NUMERIC, 115.0
# PARAMETER: UPPER_LIMIT_V, NUMERIC, 120.0
# PARAMETER: NODE_ID, TEXT

# Get the parameters
LowerLimit = cympy.GetInputParameter('LOWER_LIMIT_V')
UpperLimit = cympy.GetInputParameter('UPPER_LIMIT_V')
NodeID = cympy.GetInputParameter('NODE_ID')

# Find the Centralized Capacitor Control System
CCCS = cympy.study.GetCurrentInstrument()

# Get the valid phase at the controlled node
Phase = cympy.study.QueryInfoNode('Phase', NodeID)

# Find the minimal voltage on the valid phase

MinV = 999999

if 'A' in Phase:
    ValA = locale.atof(cympy.study.QueryInfoNode('VBaseA', NodeID, 4))
    if ValA < MinV:
        MinV = ValA
        
if 'B' in Phase:
    ValB = locale.atof(cympy.study.QueryInfoNode('VBaseB', NodeID, 4))
    if ValB < MinV:
        MinV = ValB
        
if 'C' in Phase:
    ValC = locale.atof(cympy.study.QueryInfoNode('VBaseC', NodeID, 4))
    if ValC < MinV:
        MinV = ValC

# Choose the action to be taken by the CCCS

if CCCS is not None:
    if MinV < LowerLimit:
        cympy.results.Add('ACT', 1)
    elif MinV > UpperLimit:
        cympy.results.Add('ACT', 0)
    else:
        cympy.results.Add('ACT', -1)