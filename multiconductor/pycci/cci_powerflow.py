import numpy as np
from scipy.sparse import csc_matrix, eye, linalg, coo_matrix
from numba import njit
import multiconductor
from multiconductor.pycci.pf_results import _bus_results_pf, _line_results_pf, _trafo_results_pf, _shunt_results_pf
from .model import _initialize_model, get_bus_terminal, find_islands

# TEMP fix
import pandapower.control
import inspect
source = inspect.getsource(pandapower.control.run_control).replace("ctrl_variables = prepare_run_ctrl(net, ctrl_variables)", "ctrl_variables = prepare_run_ctrl(net, ctrl_variables, **kwargs)")
exec(source, pandapower.control.__dict__)



# @njit(cache=True)
# def infclean(I):
#     for k in range(len(I)):
#         if np.isinf(I[k,0]):
#             I[k,0]=0
#     return I

def set_controllers_in_service(net, ctrtype, in_service):
    # ctrtype: ['All', 'LoadTapChangerControl','LineDropControl', VoltVarController', 'BinaryShuntController']
    for i,co in net.controller.object.items():
        if ctrtype=="All" or ctrtype in str(type(co)):
            net.controller.at[i,'in_service']=in_service
            

def run_pf(net, tol_vmag_pu=1e-5, tol_vang_rad=1e-5, MaxIter=100, run_control=False, debug_level=0, **kwargs):
    """
    Run a snapshot power flow calculation in a generic multi-conductor grid using the Correction-Current-Injection
    method.

    INPUT:
        **net** - The pandapower-like format network where elements are defined

    OPTIONAL:
    
        **tol_vmag_pu** (float, 1e-5) - Tolerance of iterative process on voltage magnitude in pu. Default is 1e-5

        **tol_vang_rad** (float, 1e-5) - Tolerance of iterative process on voltage angle in rad. Default is 1e-5

        **MaxIter** (int, 100) - Max number of iterations in the iterative process. Default is 100.
        
        **kwargs** - Additional keyword arguments. Effective ones may be: \
            * init_model (True), False - specify if model has to be initialized
            * run_capacitor_control (boolean, True) - if True, capacitor controllers are used. If False, they are deactivated.  
            * run_ltc_control (boolean, True) - if True, LTC controllers are used. If False, they are deactivated.
            * run_ldc_control (boolean, True) - if True, LDC controllers are used. If False, they are deactivated.
            * run_voltvar_control (boolean, True) - if True, volt/var controllers are used. If False, they are deactivated.    
            * NOTE: the run_x_control flags modify the in_service property of controllers. If no flag is given, this property will remain unchanged.
            * If, on the other side, any flag is given, all in_service properties will be changed (eventually to default True for missing flags). 
            * In-service properties will not be re-set after run_pp. This needs to be considered when using run_pp multiple times on the same grid.           
    """
    
    if net.switch.closed.dtype != np.dtype("bool"):
        net.switch.closed = net.switch.closed.astype(bool)

    if run_control and any([flag in kwargs.keys() for flag in ['run_capacitor_control','run_ltc_control','run_ldc_control','run_voltvar_control'] ]):         
        set_controllers_in_service(net, 'BinaryShuntController', kwargs.get('run_capacitor_control', True))
        set_controllers_in_service(net, 'LoadTapChangerControl', kwargs.get('run_ltc_control', True))
        set_controllers_in_service(net, 'LineDropControl', kwargs.get('run_ldc_control', True))
        set_controllers_in_service(net, 'VoltVarController', kwargs.get('run_voltvar_control', True))                    
        
    if run_control and net.controller.in_service.any():
        def f(net, **kwargs):
            _initialize_model(net)
            _init_pf(net)
            snap_pf(net, tol_vmag_pu, tol_vang_rad, MaxIter, **kwargs)
            net['_control_steps'] += 1
            net['converged'] = True

        net['_control_steps'] = 0
        pandapower.control.run_control(net, run=f, max_iter=60)
    else:
        _initialize_model(net, debug_level=debug_level)
        _init_pf(net)
        snap_pf(net, tol_vmag_pu, tol_vang_rad, MaxIter)


def _init_pf(net):
    """
        Initialize the power flow results for first calculation.
        Voltage in all terminals is assumed as the corresponding phase at the slack bus.

        INPUT:
            **net** - The pandapower-like format network where elements are defined
            
    """
    net.model.solved = False
    model = net.model

    Y_pass = (model.Y_tran + model.Y_network + model.Y_ground + model.Y_source +
              model.Y_shunt + model.Y_switch)

    # Defining the set-point for the voltage source(s) at the slack bus
    if net.ext_grid.shape[0] > 0:
        slack = net.ext_grid
        Vslack = slack['vm_pu'].values * np.exp(1j * slack['va_degree'].values * np.pi / 180)
    elif net.ext_grid_sequence.shape[0] > 0:
        alpha = np.exp(1j * np.pi * 2 / 3)
        T = np.array([[1, 1, 1], [1, alpha ** 2, alpha], [1, alpha, alpha ** 2]])
        slack = net.ext_grid_sequence
        Vslack = T @ (slack['vm_pu'] * np.exp(1j * slack['va_degree'] * np.pi / 180))

    y_slack = model.terminal_to_y_lookup[model.terminal_is_slack]
    model.y_fixed_voltage[y_slack] = Vslack.reshape(-1, 1)

    C = Y_pass.copy()
    for tname in ["asymmetric_load", "asymmetric_sgen"]:
        shunt = net[tname]
        buses = shunt["bus"].values
        from_phases = shunt["from_phase"].values
        to_phases = shunt["to_phase"].values
        y_from = net.model.terminal_to_y_lookup[buses * 4 + from_phases]
        y_to = net.model.terminal_to_y_lookup[buses * 4 + to_phases]
        C += coo_matrix((np.ones(2 * len(y_from)), (np.hstack([y_from, y_to]), np.hstack([y_to, y_from]))),
                        shape=(model.y_size, model.y_size)).tocsr()
    y_connected = find_islands(net.model.y_size, y_slack, C.indptr, C.indices)
    y_isolated = np.where(y_connected == -1)[0]
    net.model.y_fixed_voltage[y_isolated] = np.nan

    y_fix = np.where((net.model.y_fixed_voltage != -1) )[0] # & (y_connected != -1) TODO
    y_nonslack = np.where(net.model.y_fixed_voltage == -1)[0]
    E_fix = net.model.y_fixed_voltage[y_fix]

    Ytot_0 = Y_pass + eye(net.model.y_size) * 1e-6
    Y_from_nonslack = Ytot_0[y_nonslack, :]
    Yna = Y_from_nonslack[:, y_fix]
    Ynn = Y_from_nonslack[:, y_nonslack]
    rhs = Yna @ E_fix

    Ynn_solver = linalg.splu(csc_matrix(Ynn))
    En = -Ynn_solver.solve(rhs)

    E0 = np.zeros((net.model.y_size, 1), dtype=complex)
    E0[y_fix] = E_fix
    E0[y_nonslack] = En
    
    net.model.E0 = E0
    net.model.E_fix = E_fix
    net.model.Y_tot = Y_pass
    net.model.y_fix = y_fix
    net.model.y_nonslack = y_nonslack
    net.model.y_isolated = y_isolated


def snap_pf(net, tol_vmag_pu, tol_vang_rad, MaxIter,**kwargs):
    _cci_pf(net, tol_vmag_pu, tol_vang_rad, MaxIter)

    _bus_results_pf(net)
    _shunt_results_pf(net)
    _line_results_pf(net)
    _trafo_results_pf(net)

    return net


def _cci_pf(net,Tol_EM,Tol_EA,MaxIter):
    model = net["model"]
    E0 = model.E0
    E_fix = model.E_fix
    y_fix = model.y_fix
    y_nonslack = net.model.y_nonslack
    Ytot = model.Y_tot
    
    if len(y_fix) == model.y_size:
        model.E = E0
        return

    En = E0[y_nonslack.astype(int)]

    Eph = abs(E0) > 0.2
    E0[Eph] = E0[Eph] / abs(E0[Eph])

    Y1 = Ytot[y_nonslack, :]
    Ylg = Y1[:, y_fix]
    Yll = Y1[:, y_nonslack]
    

    model.Yll = Yll
    model.y_nonslack = y_nonslack

    Yll_1 = linalg.splu(csc_matrix(Yll))

    it = 0
    solved = False

    sbase = net.sn_mva * 1e6

    def ding(tname):
        shunt = net[tname]
        buses = shunt["bus"].values
        from_phases = shunt["from_phase"].values
        to_phases = shunt["to_phase"].values
        y_from = model.terminal_to_y_lookup[buses * 4 + from_phases]
        connected_y = ~np.isin(y_from, net.model.y_isolated)
        y_to = model.terminal_to_y_lookup[buses * 4 + to_phases]
        absEsh0 = np.abs(E0[y_from] - E0[y_to]).flatten()
        if any(absEsh0 == 0):
            if model["debug_level"] > 0:
                print(f"disconnected {sum(absEsh0 == 0)} {tname}")
            absEsh0[absEsh0 == 0] = 1
        S = (shunt["p_mw"].values + 1j * shunt["q_mvar"].values) * 1e6 * shunt["in_service"]
        kIp = shunt["const_i_percent_p"].values / 100.
        kZp = shunt["const_z_percent_p"].values / 100.
        kPp = 1 - (kIp + kZp)
        kIq = shunt["const_i_percent_q"].values / 100.
        kZq = shunt["const_z_percent_q"].values / 100.
        kPq = 1 - (kIq + kZq)
        S_const_power = kPp * np.real(S) + kPq * 1j*np.imag(S)
        S_const_current = kIp * np.real(S) + kIq * 1j*np.imag(S)
        S_const_impediance = kZp * np.real(S) + kZq * 1j*np.imag(S)
        return (y_from[connected_y], y_to[connected_y], absEsh0[connected_y], S_const_power[connected_y],
                S_const_current[connected_y], S_const_impediance[connected_y])
    
    load_y_from, load_y_to, load_absEsh0, load_S_const_power, load_S_const_current, load_S_const_impediance = ding("asymmetric_load")
    sgen_y_from, sgen_y_to, sgen_absEsh0, sgen_S_const_power, sgen_S_const_current, sgen_S_const_impediance = ding("asymmetric_sgen")

    y_from = np.hstack([load_y_from, sgen_y_from])
    y_to = np.hstack([load_y_to, sgen_y_to])
    absEsh0 = np.hstack([load_absEsh0, sgen_absEsh0])
    S_const_power = np.hstack([load_S_const_power, -sgen_S_const_power])
    S_const_current = np.hstack([load_S_const_current, -sgen_S_const_current])
    S_const_impediance = np.hstack([load_S_const_impediance, -sgen_S_const_impediance])

    def sum_currents(y_from, y_to, I_corr_sh):
        Icorr = np.zeros((model.y_size, 1), dtype=np.complex128)
        np.add.at(Icorr[:, 0], y_from, I_corr_sh)
        np.add.at(Icorr[:, 0], y_to, -I_corr_sh)
        return Icorr

    En_old = np.zeros((len(En), 1), dtype=complex)
    E = E0.copy()
    IFIX = Ylg @ E_fix
    while True:
        solved = np.max(np.abs(np.abs(En) - np.abs(En_old))) <= Tol_EM

        if solved:
            significant = np.abs(En) > 0.01
            if np.any(significant) and np.max(np.abs(np.angle(En[significant]) - np.angle(En_old[significant]))) > Tol_EA:
                 solved = False

        if solved or it == MaxIter:
            break

        En_old = En.copy()
        E_shunt = E[y_from] - E[y_to]
        E_shunt[E_shunt == 0] = 1

        absEsh = np.abs(E_shunt).flatten()
        S_act = S_const_power + \
                S_const_current * absEsh / absEsh0 + \
                S_const_impediance  * absEsh**2 / absEsh0**2
        I_corr_sh = -np.conj(S_act / sbase / E_shunt.flatten())
        I = sum_currents(y_from, y_to, I_corr_sh)
        Il = I[y_nonslack]
        En = Yll_1.solve(Il - IFIX)
        
        E[y_nonslack] = En
        it = it + 1

    net.model.E = E

#    net.model.S_load = S_act[:len(load_y_from)]
#    net.model.I_load = sum_currents(load_y_from, load_y_to, I_corr_sh[:len(load_y_from)])

#    net.model.S_sgen = S_act[len(load_y_from):]
#    net.model.I_sgen = sum_currents(sgen_y_from, sgen_y_to, I_corr_sh[len(load_y_from):])

    net.model.iterations = it
    net.model.solved = solved


def _CorrCurr_PV(net,E,nph_G,nph_SG,CVa,D,kx):
    # Costruction of the correction current array for PV elements
    sbase=net.sn_mva
    #Y_index=net.model.Y_index
    Gen=net.asymmetric_gen
    
    Icorr = np.zeros((net.model.y_size, 1), dtype=np.complex128)
    # Eg=np.zeros((nph_G.shape[0],1),dtype=complex)
    # for ng in range(Gen.shape[0]):
    #     N=Gen.iloc[ng]['bus']
    #     ph1=Gen.iloc[ng]['from_phase']
    #     ph2=Gen.iloc[ng]['to_phase']
    #     # Position in the overall system of equations of the terminals
    #     ph1_g=Y_index.loc[(N,ph1)]['yrow'].astype(int)
    #     ph2_g=Y_index.loc[(N,ph2)]['yrow'].astype(int)
    #     # Voltage set-point
    #     vm=Gen.iloc[ng]['vm_pu']
        
    #     # Get the voltage angle at the generator's terminals
    #     Eg[nph_G==ph1_g,0]=vm*(E[ph1_g]-E[ph2_g])/abs(E[ph1_g]-E[ph2_g])
    #     Eg[nph_G==ph2_g,0]=E[ph2_g]
        
    #     # Calculate the active power exchanged according to const admittance        
        
    # Ix=CVa + D@Eg + kx[np.in1d(nph_SG,nph_G)==True]
    
    # for ng in range(Gen.shape[0]):
    #     N=Gen.iloc[ng]['bus']
    #     ph1=Gen.iloc[ng]['from_phase']
    #     ph2=Gen.iloc[ng]['to_phase']
    #     # Position in the overall system of equations of the terminals
    #     ph1_g=Y_index.loc[(N,ph1)]['yrow'].astype(int)
    #     ph2_g=Y_index.loc[(N,ph2)]['yrow'].astype(int)
    #     # Active and reactive power set-points
    #     p=Gen.iloc[ng]['p_mw']/sbase
        
    #     # Calculate active power injection due to const admitt
    #     Ey=Eg[nph_G==ph1_g]-Eg[nph_G==ph2_g]
        
    #     # Calculate the active power variation with respect to P set-point
    #     sx=Ey*np.conjugate(Ix[nph_G==ph1_g])
    #     px=np.real(sx)
    #     deltap=p-(px)
    #     # Calculate current injection for active power adjustment
    #     Ir=deltap/np.conjugate(Ey)
        
        
    #     Icorr[ph1_g]=Icorr[ph1_g]+Ix[nph_G==ph1_g]+Ir
    #     Icorr[ph2_g]=Icorr[ph2_g]-Ix[nph_G==ph1_g]-Ir
        
    return Icorr