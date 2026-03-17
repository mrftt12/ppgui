import locale
import cmath
import math

# Volt-Var Control - Mimics multiconductor VoltVarController with QVCurve
# Piecewise-linear Q(V) characteristic applied to an ECG, using worst-case
# phase-to-phase voltage. Includes apparent power saturation and damping.

# Parameters Definition
# PARAMETER: SN_MVA, NUMERIC, 0.5
# PARAMETER: DAMPING_COEF, NUMERIC, 2.0
# PARAMETER: V1_PU, NUMERIC, 0.92
# PARAMETER: V2_PU, NUMERIC, 0.98
# PARAMETER: V3_PU, NUMERIC, 0.99
# PARAMETER: V4_PU, NUMERIC, 1.01
# PARAMETER: V5_PU, NUMERIC, 1.02
# PARAMETER: V6_PU, NUMERIC, 1.08
# PARAMETER: Q1_PU, NUMERIC, 0.44
# PARAMETER: Q2_PU, NUMERIC, 0.44
# PARAMETER: Q3_PU, NUMERIC, 0.0
# PARAMETER: Q4_PU, NUMERIC, 0.0
# PARAMETER: Q5_PU, NUMERIC, -0.44
# PARAMETER: Q6_PU, NUMERIC, -0.44

# Retrieve parameters
SN_MVA = cympy.GetInputParameter('SN_MVA')
DAMPING = cympy.GetInputParameter('DAMPING_COEF')

# QV curve breakpoints (voltage in p.u., Q in p.u. of sn_mva)
vm_points = [
    cympy.GetInputParameter('V1_PU'), cympy.GetInputParameter('V2_PU'),
    cympy.GetInputParameter('V3_PU'), cympy.GetInputParameter('V4_PU'),
    cympy.GetInputParameter('V5_PU'), cympy.GetInputParameter('V6_PU')
]
q_points = [
    cympy.GetInputParameter('Q1_PU'), cympy.GetInputParameter('Q2_PU'),
    cympy.GetInputParameter('Q3_PU'), cympy.GetInputParameter('Q4_PU'),
    cympy.GetInputParameter('Q5_PU'), cympy.GetInputParameter('Q6_PU')
]

def qv_curve_interp(vm_pu, vm_pts, q_pts):
    """Piecewise-linear interpolation of Q(V) curve (mimics QVCurve.step)."""
    if vm_pu <= vm_pts[0]:
        return q_pts[0]
    if vm_pu >= vm_pts[-1]:
        return q_pts[-1]
    for i in range(len(vm_pts) - 1):
        if vm_pts[i] <= vm_pu <= vm_pts[i + 1]:
            t = (vm_pu - vm_pts[i]) / (vm_pts[i + 1] - vm_pts[i])
            return q_pts[i] + t * (q_pts[i + 1] - q_pts[i])
    return 0.0

def convert_v_p2g_to_p2p(va_mag, va_ang, vb_mag, vb_ang, vc_mag, vc_ang):
    """Convert phase-to-ground voltages to phase-to-phase magnitudes (p.u.)."""
    s = 1.0 / math.sqrt(3.0)
    U1 = va_mag * s * cmath.exp(1j * math.radians(va_ang))
    U2 = vb_mag * s * cmath.exp(1j * math.radians(vb_ang))
    U3 = vc_mag * s * cmath.exp(1j * math.radians(vc_ang))
    return [abs(U1 - U2), abs(U2 - U3), abs(U3 - U1)]

# Get the current ECG device
ECG = cympy.study.GetCurrentDevice()

if ECG is not None:
    dev_type = cympy.enums.DeviceType.ElectronicConverterGenerator
    Ph = cympy.study.QueryInfoDevice('Phase', ECG.DeviceNumber, dev_type)

    # Get per-phase voltage magnitude (p.u.) and angle at ECG bus
    va_mag, va_ang = 0.0, 0.0
    vb_mag, vb_ang = 0.0, 0.0
    vc_mag, vc_ang = 0.0, 0.0

    if 'A' in Ph:
        va_mag = locale.atof(cympy.study.QueryInfoDevice('VpuA', ECG.DeviceNumber, dev_type, 6))
        va_ang = locale.atof(cympy.study.QueryInfoDevice('VAngleA', ECG.DeviceNumber, dev_type, 6))
    if 'B' in Ph:
        vb_mag = locale.atof(cympy.study.QueryInfoDevice('VpuB', ECG.DeviceNumber, dev_type, 6))
        vb_ang = locale.atof(cympy.study.QueryInfoDevice('VAngleB', ECG.DeviceNumber, dev_type, 6))
    if 'C' in Ph:
        vc_mag = locale.atof(cympy.study.QueryInfoDevice('VpuC', ECG.DeviceNumber, dev_type, 6))
        vc_ang = locale.atof(cympy.study.QueryInfoDevice('VAngleC', ECG.DeviceNumber, dev_type, 6))

    # Calculate phase-to-phase voltages and find worst-case (max deviation from 1.0)
    v_p2p = convert_v_p2g_to_p2p(va_mag, va_ang, vb_mag, vb_ang, vc_mag, vc_ang)
    vm_worst = max(v_p2p, key=lambda v: abs(v - 1.0))

    # Look up Q target from QV curve (in p.u. of sn_mva)
    q_target_pu = qv_curve_interp(vm_worst, vm_points, q_points)

    # Get current P output (kW -> MW)
    p_current_kw = locale.atof(cympy.study.QueryInfoDevice('KWTOT', ECG.DeviceNumber, dev_type, 6))
    p_mw = p_current_kw / 1000.0

    # Get current Q output (kvar -> Mvar)
    q_current_kvar = locale.atof(cympy.study.QueryInfoDevice('KVARTOT', ECG.DeviceNumber, dev_type, 6))
    q_current_mvar = q_current_kvar / 1000.0

    # Target Q in Mvar
    q_target_mvar = q_target_pu * SN_MVA

    # Apply damping (mimics multiconductor damping_coef)
    q_damped_mvar = q_current_mvar + (q_target_mvar - q_current_mvar) / DAMPING

    # Apparent power saturation (Q priority: curtail P if S > Sn)
    s_check = math.sqrt(p_mw ** 2 + q_damped_mvar ** 2)
    if s_check > SN_MVA:
        q_damped_mvar = max(-SN_MVA, min(SN_MVA, q_damped_mvar))
        p_mw = math.sqrt(max(0, SN_MVA ** 2 - q_damped_mvar ** 2))

    # Output P (kW) and Q (kvar)
    cympy.results.Add('P', p_mw * 1000.0)
    cympy.results.Add('Q', q_damped_mvar * 1000.0)