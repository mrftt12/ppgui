import locale
# Power Driven Control done using a Python script

# Script default input parameters info
#PARAMETER: Spot Load Number, text,
#PARAMETER: Discharge Trigger Power, numeric, 100.0, kW
#PARAMETER: Discharge Power, numeric, 100.0, %, Must be between 0.0 and 100.0 %
#PARAMETER: Discharge Reference,  numeric, 2, 0 = 'Max Discharging Power', 1 = 'Converter Rating' and 2 = 'Difference'
#PARAMETER: Charge Trigger Power, numeric, 50.0, kW
#PARAMETER: Charge Power, numeric, 100.0, %, Must be between 0.0 and 100.0 %
#PARAMETER: Charge Reference, numeric, 2, 0 = 'Max Charging Power', 1 = 'Converter Rating' and 2 = 'Difference'

# Get and verify the input parameters
try:
    spotLoadID = cympy.GetInputParameter('Spot Load Number')
except ValueError as e:
    # Catching string to float conversion error
    # Setting default value
    raise (e)

try:
    discTriggerPower = cympy.GetInputParameter('Discharge Trigger Power')
except ValueError as e:
    # Catching string to float conversion error
    # Setting default value
    raise (e)

try:
    discPower = cympy.GetInputParameter('Discharge Power')
	
    if discPower < 0.0 or discPower > 100.0:
        raise ( Exception('Discharge Power is not within 0.0 and 100.0 %') )
except ValueError as e:
    # Catching string to float conversion error
    # Setting default value
    raise (e)

try:
    discReference = cympy.GetInputParameter('Discharge Reference')
	
    if discReference != 0 and discReference != 1 and discReference != 2:
        raise ( Exception('Selected Discharge Reference is not valid') )
except ValueError as e:
    # Catching string to float conversion error
    # Setting default value
    raise (e)

try:
    chaTriggerPower = cympy.GetInputParameter('Charge Trigger Power')
except ValueError as e:
    # Catching string to float conversion error
    # Setting default value
    raise (e)

try:
    chaPower = cympy.GetInputParameter('Charge Power')
	
    if chaPower < 0.0 or chaPower > 100.0:
        raise ( Exception('Charge Power is not within 0.0 and 100.0 %') )
except ValueError as e:
    # Catching string to float conversion error
    # Setting default value
    raise (e)

try:
    chaReference = cympy.GetInputParameter('Charge Reference')
	
    if chaReference != 0 and chaReference != 1 and chaReference != 2:
        raise ( Exception('Selected Charge Reference is not valid') )
except ValueError as e:
    # Catching string to float conversion error
    # Setting default value
    raise (e)

# Verify the Spot Load ID Number is valid
spotLoads = cympy.study.ListDevices(cympy.enums.DeviceType.SpotLoad)
exist = False
for spotLoad in spotLoads:
    if spotLoad.DeviceNumber == spotLoadID:
        exist = True

if not exist:
    raise ( Exception('The spot load ' + spotLoadID + ' does not exist') )

# Find the current BESS
BESS = cympy.study.GetCurrentDevice()

# Get the parameters of the current BESS
BESSSOC = locale.atof( cympy.study.QueryInfoDevice('BESSSOC', BESS.DeviceNumber, cympy.enums.DeviceType.BESS, 6 ) )
BESSMaxSOC = locale.atof( cympy.study.QueryInfoDevice('BESSMaxSOC', BESS.DeviceNumber, cympy.enums.DeviceType.BESS, 6 ) )
BESSMinSOC = locale.atof( cympy.study.QueryInfoDevice('BESSMinSOC', BESS.DeviceNumber, cympy.enums.DeviceType.BESS, 6 ) )
BESSMaxChargPower = locale.atof( cympy.study.QueryInfoDevice('BESSMaxChargPower', BESS.DeviceNumber, cympy.enums.DeviceType.BESS, 6 ) )
BESSMaxDiscPower = locale.atof( cympy.study.QueryInfoDevice('BESSMaxDiscPower', BESS.DeviceNumber, cympy.enums.DeviceType.BESS, 6 ) )
BESSConverterRating = locale.atof( BESS.GetValue('Converter.ActivePowerRating') )

# Get the monitored active power of the selected Spot Load
Pload = locale.atof(cympy.study.QueryInfoDevice('KWTOT', spotLoadID, cympy.enums.DeviceType.SpotLoad, 6 ) )

# Determine the output active power of the BESS in function of the 
# monitored active power
if Pload < chaTriggerPower and BESSSOC+0.00001 < BESSMaxSOC:
    if chaReference == 0:
        reqP = -chaPower/100.0*BESSMaxChargPower
    elif chaReference == 1:
        reqP = -chaPower/100.0*BESSConverterRating
    else:
        reqP = -chaPower/100.0*(chaTriggerPower - Pload)

elif Pload > discTriggerPower and BESSSOC-0.00001 > BESSMinSOC:
    if discReference == 0:
        reqP = discPower/100.0*BESSMaxDiscPower
    elif discReference == 1:
        reqP = discPower/100.0*BESSConverterRating
    else:
        reqP = discPower/100.0*(Pload - discTriggerPower)

else:
    reqP = 0.0

# Verification that the requested active power isn't over the 
# battery charging/discharging limits. Those conditions are possible 
# when the charging/discharging references are set to DIFFERENCE
if reqP > BESSMaxDiscPower:
    reqP = BESSMaxDiscPower
elif reqP < -BESSMaxChargPower:
    reqP = -BESSMaxChargPower

# Output the requested active and reactive power
cympy.results.Add('P', reqP)
cympy.results.Add('Q', 0.0)