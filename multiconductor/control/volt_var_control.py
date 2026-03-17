# -*- coding: utf-8 -*-

# Copyright (c) 2016-2025 by University of Kassel and Fraunhofer Institute for Energy Economics
# and Energy System Technology (IEE), Kassel. All rights reserved.

import numpy as np
import pandas as pd
from math import sqrt
from pandapower.auxiliary import ensure_iterability
from pandapower.control.controller.pq_control import PQController
from multiconductor.control.tools import convert_v_p2g_p2p

import logging

logger = logging.getLogger(__name__)

def vprint(s, verbose):
    if verbose:
        print(s)

def _add_volt_var_control_to_asym_sgen(mc_net, control_index, qv_curve, damping_coef=2):
    if (isinstance(control_index, int) and control_index not in set(
            mc_net.asymmetric_sgen.index.get_level_values(0))) or \
            (isinstance(control_index, tuple) and control_index not in mc_net.asymmetric_sgen.index):
        print("Error: Didn't find asymmetric_sgen with index " + str(control_index))
        return

    mc_net.asymmetric_sgen.at[control_index, 'control_mode'] = "Volt/Var"
    mc_net.asymmetric_sgen.at[control_index, 'control_curve'] = qv_curve

    VoltVarController(
        mc_net, element_index=[control_index], element='asymmetric_sgen',
        q_model=QModelQVCurve(qv_curve),
        saturate_sn_mva=mc_net.asymmetric_sgen.at[(control_index, 0), 'sn_mva'],
        max_p_error=1e-6, max_q_error=1e-6,
        damping_coef=damping_coef
    )


def add_volt_var_control(mc_net, element, qv_curve, et='asymmetric_sgen', mode='gang', damping_coef=2):
    """
    Add Volt-var controller to a pp-mc element 
    
    INPUT:
        **mc_net** (pandapower-mc net)

        **element** (int) - IDs of the element (first-level index). NOTE: the element MUST define sn_mva. 
    
        **qv_curve** (QVCurve) - Volt/Var control curve
    
    OPTIONAL:
    
        **et** (String, default 'asymmetric_sgen') - String indicating element type. 
                                                            For future extension, currently only 'asymmetric_sgen' supported. 
       
        **mode** (String, default 'gang') - String indicating operation mode. 
                                            For future extension, currently only 'gang' supported.
    """
    if (et not in ['asymmetric_sgen']):
        print("Volt/var control not supported for " + str(et))
        return

    if mode == "gang":
        control_index = element
    elif mode == "single_circuit":
        control_index = (element, 1)

    if et == 'asymmetric_sgen':
        _add_volt_var_control_to_asym_sgen(mc_net, control_index, qv_curve, damping_coef)
        
    # ensure compatibility with pp-os pip-installed version 3.1.2
    # if "svc" not in mc_net.keys():
    #     mc_net.svc = pd.DataFrame(columns=["in_service","vm_pu","control_mode_ac","control_value_ac"])
    #     mc_net.tcsc = mc_net.svc.copy()
    #     mc_net.ssc = mc_net.svc.copy()
    #     mc_net.vsc = mc_net.svc.copy()
    #     mc_net.gen = mc_net.svc.copy()
    #     mc_net.load = mc_net.svc.copy()
    

def check_ctr_sgen_stable_state(mc_net, sgen_first_lvl_idx, verbose=False):
    """
    Check asymmetric_sgen power outputs vs. voltage and control curve after controller action 
    
    INPUT:
        **mc_net** (pandapower-mc net)

        **sgen_first_lvl_idx** (int) - ID of the asymmetric_sgen (first-level index). NOTE: the element MUST define sn_mva.         
    
    OPTIONAL:
    
        **verbose** (boolean, default False) - If True, verbose output is printed to the console. If False, the function is executed silently.                                                              
       
    RETURNS:
    
        **passed** (boolean) - True if the check has passed successfully, False otherwise.                                              
    """
    sgen = mc_net.asymmetric_sgen.loc[sgen_first_lvl_idx]
    if len(sgen) != 3:
        vprint("check_ctr_sgen_stable_state: Only 3-phased sgen supported this time!", verbose)
        return False
    # check if all phases connected to same bus and control curves are the same 
    control_curves = []
    buses = []
    sn_mvas = []
    for i,row in sgen.iterrows():
        control_curves += [row['control_curve']]
        buses += [row['bus']]
        sn_mvas += [row['sn_mva']]
    if not all(x == control_curves[0] for x in control_curves):
        vprint("check_ctr_sgen_stable_state: sgen phases have different control_curves, please check!", verbose)
        return False
    if not all(x == buses[0] for x in buses):
        vprint("check_ctr_sgen_stable_state: sgen phases are connected to different buses, please check!", verbose)
        return False
    if not all(x == sn_mvas[0] for x in sn_mvas):
        vprint("check_ctr_sgen_stable_state: sgen phases have different sn_mva's, please check!", verbose)
        return False
    
    control_curve = control_curves[0] 
    bus = buses[0]
    sn_mva = sn_mvas[0]
    
    # get phase-to-phase voltages
    v_p2g = mc_net.res_bus.loc[bus][['vm_pu','va_degree']].drop(0)
    v_p2p = convert_v_p2g_p2p(v_p2g.values.tolist())
    vm_pu_p2p = np.array([x[0] for x in v_p2p])
    vm_worst_idx = np.argmax([abs(1-x) for x in vm_pu_p2p])
    vm_pu_worst = vm_pu_p2p[vm_worst_idx] # voltage relevant for controller setting 
    q_target = sn_mva * control_curve.step(vm_pu_worst)
    
    # check steady state of sgen
    ps = []
    qs = []    
    for i,row in sgen.iterrows():
        ps += [row['p_mw']]
        qs += [row['q_mvar']]
    
    # check if gang-operated    
    if not all(x == qs[0] for x in qs):
        vprint("check_ctr_sgen_stable_state: sgen phases have different reactive powers, please check!", verbose)
        return False          
    
    # check if reactive power matches target and all spparent power limits met 
    if abs((qs[0] - q_target)) > 1E-3:
        vprint("check_ctr_sgen_stable_state: sgen reactive power ("+str(qs[0])+") does not equal target ("+str(q_target)+")!", verbose)
        return False
    for i in range(0,3): 
        sn = sqrt(ps[i] ** 2 + qs[i] ** 2)
        if (sn-sn_mva) > 1E-3:
            vprint("check_ctr_sgen_stable_state: sgen apparent power for phase "+str(i)+" ("+str(sn)+") exceeds rated maximum ("+str(sn_mva)+")!", verbose)
            return False
    
    return True


# -------------------------------------------------------------------------------------------------
# Basics
# -------------------------------------------------------------------------------------------------
class BaseModel:
    @classmethod
    def name(cls):
        return str(cls).split(".")[-1][:-2]
    
class QModel(BaseModel):
    """
    Base class to model how q is determined in step().
    """

    def __init__(self, **kwargs):
        pass

    def step(self, vm_pu=None, p_pu=None):
        pass

    def __str__(self):
        return self.__class__.name()
    

class QVCurve:
    """ Characteristic curve for volt/var controller, defined by 'vm_points_pu' and
    'q_points_pu' (relative to sn_mva).

                                   - Q(Vm)/sn (underexcited)
                                   ^
                                   |
                                   |
                                   |               _______
                                   |              /
                                   |             /
                                   |            /
                                   |           /
                                   |          /
             v[0] v[1]   v[2]      |         /
       --------+----+-----+--------+--------+-----+------+------>
                         /         |      v[3]   v[4]   v[5]    Vm
                        /          |
                       /           |
                      /            |
                     /             |
                ____/              |
                                   + Q(Vm)/sn (overexcited)
    """
    def __init__(self, vm_points_pu, q_points_pu):
        self.vm_points_pu = vm_points_pu
        self.q_points_pu = q_points_pu

    def step(self, vm_pu):                
        return np.interp(vm_pu, self.vm_points_pu, self.q_points_pu)


class QModelQVCurve(QModel):
    """
    Base class to model that q is determined in dependency of the voltage.
    """

    def __init__(self, qv_curve):
        if isinstance(qv_curve, dict):
            self.qv_curve = QVCurve(**qv_curve)
        else:
            self.qv_curve = qv_curve

    def step(self, vm_pu, p_pu=None):
        q_pu = self.qv_curve.step(vm_pu)
        return q_pu

# -------------------------------------------------------------------------------------------------
# VoltVarController
# -------------------------------------------------------------------------------------------------
class VoltVarController(PQController):
    """Volt-var controller according to WP1.5 specification 
    
    INPUT:
        **net** (pandapower net)

        **element_index** (int[]) - IDs of the controlled elements
        
        **q_model** (object, None) - an q_model, such as provided in this file, should be passed to
        model how the q value should be determined.
       
        **saturate_sn_mva** (float) - Maximum apparent power of the inverter. If given, the
        p value is reduced to this maximum apparent power if neccessary.     


    OPTIONAL:
        **element** (str, default "asymmetric_sgen") - element type which is controlled
       
        **damping_coef** (float, 2) - damping coefficient to influence the power updating process
        of the control loop. A higher value mean slower changes of q towards the latest target
        value. Note: this might greatly influence the controller's performance. Also, in grids with multiple
        controllers, too low values for damping_coef might hinder convergence. Higher values (e.g. 4) will help this.  

        **max_p_error** (float, 1E-6) - Maximum absolute error of active power in MW

        **max_q_error** (float, 1E-6) - Maximum absolute error of reactive power in Mvar
                  
        **in_service** (bool, True) - Indicates if the controller is currently in_service

        **ts_absolute** (bool, True) - Whether the time step values are absolute power values or
        scaling factors


    """

    def __init__(self, net, element_index, element="asymmetric_sgen",
                 q_model=None, saturate_sn_mva=None, damping_coef=2,
                 max_p_error=1e-6, max_q_error=1e-6, in_service=True, 
                 order=0, level=0, drop_same_existing_ctrl=False, matching_params=None, **kwargs):
        
        element_index = list(ensure_iterability(element_index))
        #----new
        df = getattr(net, element)
        if isinstance(df.index, pd.MultiIndex):
            element_index = self._expand_multiindex(df.index, element_index)

        if (len(element_index) < 3):
            raise ValueError("The Volt/Var Controller only supports 3-phase elements, this one is "+str(len(element_index))+"-phased (element index "+str(element_index)+").")
        if matching_params is None:
            matching_params = {"element_index": element_index}
        super().__init__(net, element_index=element_index, element=element, max_p_error=max_p_error,
                         max_q_error=max_q_error, in_service=in_service,
                         initial_run=True,
                         drop_same_existing_ctrl=drop_same_existing_ctrl,
                         matching_params=matching_params, initial_powerflow=False,
                         order=order, level=level, **kwargs)

        # --- init DER Model params
        self.q_model = q_model        
                
        self.damping_coef = damping_coef
        self.saturate_sn_mva = np.array(ensure_iterability(saturate_sn_mva))
        self.saturate_sn_mva_active = isinstance(self.saturate_sn_mva, np.ndarray) 
        
        # --- log unexpected param values
        if n_nan_sn := sum(self.sn_mva.isnull()):
            logger.error(f"The Volt/Var Controller relates to sn_mva, but for {n_nan_sn} elements "
                         "sn_mva is NaN.")
        if self.saturate_sn_mva_active and (self.saturate_sn_mva <= 0).any():
            raise ValueError(f"saturate_sn_mva cannot be <= 0 but is {self.saturate_sn_mva}")
        
    def __str__(self):
        return "VoltVarControl for "+str(self.element)+" index "+str(self.element_index)
        
    def time_step(self, net, time):
        # get new values from profiles
        self.read_profiles(time)
        self.p_series_mw = self.p_mw
        self.q_series_mvar = self.q_mvar
    

    def is_converged(self, net):

        self._determine_target_powers(net)
        
        converged = np.allclose(self.target_q_mvar, self.q_mvar, atol=self.max_q_error) and np.allclose(self.target_p_mw, self.p_mw, atol=self.max_p_error)   
        return converged

    def control_step(self, net):

        if "target_p_mw" not in vars(self) or "target_q_mvar" not in vars(self):
            self._determine_target_powers(net)

        self.p_mw, self.q_mvar = self.target_p_mw, self.target_q_mvar

        self.write_to_net(net)

    def _determine_target_powers(self, net):
        full_vm = net.res_bus[["vm_pu","va_degree"]] # phase-to-ground voltages
        if isinstance(full_vm.index, pd.MultiIndex): #multiindex version
            # ToDo: This is not optimal yet. At the moment element index gives bus and from phase to get the bus vm_pu results.
            mi = pd.MultiIndex.from_tuples(self.element_index,
                                           names=full_vm.index.names)
            buses = net[self.element].loc[mi].bus.values
            from_phases = net[self.element].loc[mi].from_phase.values
            tuples = list(zip(buses.astype(int), from_phases.astype(int)))

            # now create the MultiIndex
            midx = pd.MultiIndex.from_tuples(
                tuples,
                names=["bus", "from_phase"]
            )
            vm_pu = full_vm.reindex(midx)
            
            # convert to phase-to-phase
            idx = vm_pu.index 
            # TODO: following is meant for gang control only
            vm_pp = convert_v_p2g_p2p(vm_pu.values.tolist())
            vm_pp = np.array([vm_pp[i][0] for i in range(0,3)]) # use only voltage absolutes
            amax  = np.argmax(abs(vm_pp-1)) # index of voltage with highest deviation from nominal (aka. "worst" voltage)             
            vm_pp_amax = [vm_pp[amax]]*3 # use worst voltage for all phases (gang control)
            vm_pu = pd.DataFrame(vm_pp_amax, index=idx, columns=['vm_pu']) # this should now contain phase-to-phase p.u. values                        
            
        else: #existing code
            raw = full_vm.loc[self.bus]
            vm_pu = pd.Series([raw] * len(self.element_index),
                              index=self.element_index)
        #vm_pu = net.res_bus.loc[self.bus, "vm_pu"].set_axis(self.element_index)
        p_series_mw = getattr(self, "p_series_mw", getattr(self, "p_mw", self.sn_mva))
        q_series_mvar = getattr(self, "q_series_mw", self.q_mvar)

        # --- calculate target p and q -------------------------------------------------------------

        if np.any(p_series_mw < 0):
            logger.info("p_series_mw is forced to be greater/equal zero")
            p_series_mw[p_series_mw < 0] = 0.

        # --- First Step: Calculate/Select P, Q
        p_pu = self._step_p(p_series_mw)
        q_pu = self._step_q(p_series_mw=p_series_mw, q_series_mvar=q_series_mvar, vm_pu=vm_pu)
        if (isinstance(p_pu, pd.Series) and not isinstance(q_pu, pd.Series)): 
            q_pu = pd.Series(q_pu[:,0], index=p_pu.index)
        # --- Second Step: Saturates P, Q according to SnMVA 
        if self.saturate_sn_mva_active:  
            p_pu, q_pu = self._saturate(p_pu, q_pu, vm_pu)

        # --- Third Step: Convert relative P, Q to p_mw, q_mvar
        target_p_mw, target_q_mvar = p_pu * self.sn_mva, q_pu * self.sn_mva

        # --- Apply target p and q considering the damping factor coefficient ----------------------
        self.target_p_mw = self.p_mw + (target_p_mw - self.p_mw) / self.damping_coef
        self.target_q_mvar = self.q_mvar + (target_q_mvar - self.q_mvar) / self.damping_coef
            
    
    def _step_p(self, p_series_mw=None):
        return p_series_mw / self.sn_mva

    def _step_q(self, p_series_mw=None, q_series_mvar=None, vm_pu=None):
        """Q priority: Q setpoint > Q model > Q series"""
        if self.q_model is not None:
            q_pu = self.q_model.step(vm_pu=vm_pu)
        else:
            if q_series_mvar is None:
                raise Exception("No Q_model and no q_profile available.")
            q_pu = q_series_mvar / self.sn_mva
        return q_pu

    def _saturate(self, p_pu, q_pu, vm_pu):
        assert p_pu is not None and q_pu is not None

        # Saturation on given pqv_area        
        if self.saturate_sn_mva_active:
            p_pu, q_pu = self._saturate_sn_mva_step(p_pu, q_pu, vm_pu)
        return p_pu, q_pu

    def _saturate_sn_mva_step(self, p_pu, q_pu, vm_pu):
        # Saturation on SnMVA according to priority mode
        sat_s_pu = self.saturate_sn_mva / self.sn_mva  # sat_s is relative to sn_mva
        to_saturate = (p_pu ** 2 + q_pu ** 2) > sat_s_pu ** 2
        if any(to_saturate):            
            q_pu[to_saturate] = np.clip(q_pu[to_saturate], -sat_s_pu[to_saturate],
                                        sat_s_pu[to_saturate])
            p_pu[to_saturate] = np.sqrt(sat_s_pu[to_saturate] ** 2 - q_pu[to_saturate] ** 2)
        return p_pu, q_pu

    @staticmethod
    def _expand_multiindex(df_index: pd.Index, keys):
        """
        Given a pandas.MultiIndex and a list of keys (some top‐level, some full tuples),
        return the flattened list of full tuples.
        """
        out = []
        for k in keys:
            if isinstance(k, tuple):
                out.append(k)
            else:
                # collect all tuples whose first level == k
                matches = df_index[df_index.get_level_values(0) == k].tolist()
                if not matches:
                    raise KeyError(f"No entries in index for top‐level key {k}")
                out += matches
        return out

    # def __str__(self):
    #     el_id_str = f"len(element_index)={len(self.element_index)}" if len(self.element_index) > 6 \
    #         else f"element_index={self.element_index}"
    #     return (f"Volt/Var Controller({el_id_str}, q_model={self.q_model}, "
    #             f"saturate_sn_mva={self.saturate_sn_mva}, "
    #             f"damping_coef={self.damping_coef})")


if __name__ == "__main__":
    pass