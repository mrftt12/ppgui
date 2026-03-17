import locale
# Target voltages with default values
#PARAMETER: TARGET_A_V, NUMERIC, 122
#PARAMETER: TARGET_B_V, NUMERIC, 122
#PARAMETER: TARGET_C_V, NUMERIC, 122

# Get the parameter value
TargetVoltageA = cympy.GetInputParameter('TARGET_A_V')
TargetVoltageB = cympy.GetInputParameter('TARGET_B_V')
TargetVoltageC = cympy.GetInputParameter('TARGET_C_V')

# Find the actual regulator
Regulator = cympy.study.GetCurrentDevice()

# Get the valid phase
Ph = cympy.study.QueryInfoDevice('Phase', Regulator.DeviceNumber, Regulator.DeviceType)

if ( Regulator.DeviceType == cympy.enums.DeviceType.RegulatorByPhase ):

	# Calculate new tap for phase A
	if 'A' in Ph:
		# Get the regulator information
		EqIDA = cympy.study.QueryInfoDevice('RegByPhaseEqIdA', Regulator.DeviceNumber, cympy.enums.DeviceType.RegulatorByPhase)
		EqA = cympy.eq.GetEquipment(EqIDA, cympy.enums.EquipmentType.Regulator)
	
		NbTap = int(EqA.GetValue('NumberOfTaps'))
		MaxBoost = locale.atof(cympy.study.QueryInfoDevice('RegByPhaseBoostA', Regulator.DeviceNumber, cympy.enums.DeviceType.RegulatorByPhase))
		MaxBuck = locale.atof(cympy.study.QueryInfoDevice('RegByPhaseBuckA', Regulator.DeviceNumber, cympy.enums.DeviceType.RegulatorByPhase))
		regSteps = ( MaxBoost + MaxBuck ) / float(NbTap) / 100.0

		ValA = locale.atof(cympy.study.QueryInfoDevice('VBaseA', Regulator.DeviceNumber, cympy.enums.DeviceType.RegulatorByPhase, 6))
		ActualTapA = locale.atof(cympy.study.QueryInfoDevice('RegTapA', Regulator.DeviceNumber, cympy.enums.DeviceType.RegulatorByPhase))
	
		Delta = ( TargetVoltageA - ValA ) / TargetVoltageA
		NbSteps = Delta / regSteps
		cympy.results.Add('TAPA', NbSteps + ActualTapA )

	# Calculate new tap for phase B
	if 'B' in Ph:
		# Get the regulator information
		EqIDB = cympy.study.QueryInfoDevice('RegByPhaseEqIdB', Regulator.DeviceNumber, cympy.enums.DeviceType.RegulatorByPhase)
		EqB = cympy.eq.GetEquipment(EqIDB, cympy.enums.EquipmentType.Regulator)

		NbTap = int(EqB.GetValue('NumberOfTaps'))
		MaxBoost = locale.atof(cympy.study.QueryInfoDevice('RegByPhaseBoostB', Regulator.DeviceNumber, cympy.enums.DeviceType.RegulatorByPhase))
		MaxBuck = locale.atof(cympy.study.QueryInfoDevice('RegByPhaseBuckB', Regulator.DeviceNumber, cympy.enums.DeviceType.RegulatorByPhase))
		regSteps = ( MaxBoost + MaxBuck ) / float(NbTap) / 100.0

		ValB = locale.atof(cympy.study.QueryInfoDevice('VBaseB', Regulator.DeviceNumber, cympy.enums.DeviceType.RegulatorByPhase, 6))
		ActualTapB = locale.atof(cympy.study.QueryInfoDevice('RegTapB', Regulator.DeviceNumber, cympy.enums.DeviceType.RegulatorByPhase))
	
		Delta = ( TargetVoltageB - ValB ) / TargetVoltageB
		NbSteps = Delta / regSteps
		cympy.results.Add('TAPB', NbSteps + ActualTapB )

	# Calculate new tap for phase C
	if 'C' in Ph:
		# Get the regulator information
		EqIDC = cympy.study.QueryInfoDevice('RegByPhaseEqIdC', Regulator.DeviceNumber, cympy.enums.DeviceType.RegulatorByPhase)
		EqC = cympy.eq.GetEquipment(EqIDC, cympy.enums.EquipmentType.Regulator)

		NbTap = int(EqC.GetValue('NumberOfTaps'))
		MaxBoost = locale.atof(cympy.study.QueryInfoDevice('RegByPhaseBoostC', Regulator.DeviceNumber, cympy.enums.DeviceType.RegulatorByPhase))
		MaxBuck = locale.atof(cympy.study.QueryInfoDevice('RegByPhaseBuckC', Regulator.DeviceNumber, cympy.enums.DeviceType.RegulatorByPhase))
		regSteps = ( MaxBoost + MaxBuck ) / float(NbTap) / 100.0

		ValC = locale.atof(cympy.study.QueryInfoDevice('VBaseC', Regulator.DeviceNumber, cympy.enums.DeviceType.RegulatorByPhase, 6))
		ActualTapC = locale.atof(cympy.study.QueryInfoDevice('RegTapC', Regulator.DeviceNumber, cympy.enums.DeviceType.RegulatorByPhase))
	
		Delta = ( TargetVoltageC - ValC ) / TargetVoltageC
		NbSteps = Delta / regSteps
		cympy.results.Add('TAPC', NbSteps + ActualTapC )

else:	# Regulator

	# Get the regulator information
	NbTap = int(cympy.study.QueryInfoDevice('RegNbtap', Regulator.DeviceNumber, cympy.enums.DeviceType.Regulator))
	MaxBoost = locale.atof(cympy.study.QueryInfoDevice('RegBoost', Regulator.DeviceNumber, cympy.enums.DeviceType.Regulator))
	MaxBuck = locale.atof(cympy.study.QueryInfoDevice('RegBuck', Regulator.DeviceNumber, cympy.enums.DeviceType.Regulator))
	regSteps = ( MaxBoost + MaxBuck ) / float(NbTap) / 100.0

	# Calculate new tap for phase A
	if 'A' in Ph:
	    ValA = locale.atof(cympy.study.QueryInfoDevice('VBaseA', Regulator.DeviceNumber, cympy.enums.DeviceType.Regulator, 6))
	    ActualTapA = locale.atof(cympy.study.QueryInfoDevice('RegTapA', Regulator.DeviceNumber, cympy.enums.DeviceType.Regulator))
	    Delta = ( TargetVoltageA - ValA ) / TargetVoltageA
	    NbSteps = Delta / regSteps
	    cympy.results.Add('TAPA', NbSteps + ActualTapA )

	# Calculate new tap for phase B
	if 'B' in Ph:
	    ValB = locale.atof(cympy.study.QueryInfoDevice('VBaseB', Regulator.DeviceNumber, cympy.enums.DeviceType.Regulator, 6))
	    ActualTapB = locale.atof(cympy.study.QueryInfoDevice('RegTapB', Regulator.DeviceNumber, cympy.enums.DeviceType.Regulator))
	    Delta = ( TargetVoltageB - ValB ) / TargetVoltageB
	    NbSteps = Delta / regSteps
	    cympy.results.Add('TAPB', NbSteps + ActualTapB )

	# Calculate new tap for phase C
	if 'C' in Ph:
	    ValC = locale.atof(cympy.study.QueryInfoDevice('VBaseC', Regulator.DeviceNumber, cympy.enums.DeviceType.Regulator, 6))
	    ActualTapC = locale.atof(cympy.study.QueryInfoDevice('RegTapC', Regulator.DeviceNumber, cympy.enums.DeviceType.Regulator))
	    Delta = ( TargetVoltageC - ValC ) / TargetVoltageC
	    NbSteps = Delta / regSteps
	    cympy.results.Add('TAPC', NbSteps + ActualTapC )