"""
Ductbank Cable Temperature Calculation

This module calculates underground cable temperatures in a user defined ductbank.
The methodology is based off the Neher-McGrath method.
See "The Calculation of the Temperature Rise and Load Capability of Cable Systems",
J.H.Neher & M.H.McGrath. AIEE Transactions, October 1957. pp. 752-765.

AND

Dynamic Transformer Rating (DTR) System for OFAF Transformers
Predictive thermal modeling for high EV charging penetration areas
Based on IEEE C57.91-2011 and IEC 60076-7 standards
"""

import math
import numpy as np
import pandas as pd
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional
from enum import Enum
from scipy.integrate import odeint

# Global Cable Variables (Default/Fallback)
DEFAULT_VARS = {
    "Rdc25": 23.6,  # dc resistance of conductor @ 25 C
    "Tj": 0.11,  # thickness of jacket
    "d": 0.115,  # Diameter of concentric wires
    "Lf": 1.048,  # Lay factor of concentric neutral
    "ks": 1.0,  # Skin effect factor of conductor
    "er": 2.8,  # SIC of insulation
    "cos0": 0.004,  # dissipation factor
    "Nprime": 1.0,  # # of cables in a stated diameter
    "Ts": 78.5,  # Shield temperature
    "a": 17,  # Constant for qs
    "S": 7,  # spacing between conductors
}


class ThermalParameters:
    def __init__(
        self,
        total_resistance,
        total_capacitance,
        time_constant,
        insulation_r,
        jacket_r,
        earth_r,
    ):
        self.total_resistance = total_resistance
        self.total_capacitance = total_capacitance
        self.time_constant = time_constant
        self.insulation_r = insulation_r
        self.jacket_r = jacket_r
        self.earth_r = earth_r


class TransientResult:
    def __init__(self, temperature_history, time_array):
        self.temperature_history = temperature_history
        self.time_array = time_array


def calculate_steady_state(CurrentArray, CableType, DuctBank, cols):
    """
    Calculate steady state temperatures for cables in a duct bank.

    Args:
        CurrentArray (list): List of currents in Amps for each cable position.
        CableType (list): List of cable parameter lists/tuples.
        DuctBank (list): Duct bank parameters.
        cols (int): Number of columns in the duct bank.
    """
    # Unpacking DuctBank params (matching index usage in original)
    # DuctBank indices used: 2 (rho?), 4, 5, 6, 7, 8
    # Looking at original code:
    # 2: earthRho/dbFillRho? Re uses DuctBank[2].
    # 8: Earth Rho?
    # 4,5,6: Geometry?
    # 7: ?

    # We should probably define a helper for DuctBank structure or assume the list format
    # from the tutorial is maintained by the caller.

    Gb = 0.76

    num_cables = len(CurrentArray)
    temperature = [0.0] * num_cables
    Wc = [0.0] * num_cables
    qs = [0.0] * num_cables

    dPkn = np.zeros((num_cables, num_cables))
    dkn = np.zeros((num_cables, num_cables))
    Re = np.zeros((num_cables, num_cables))
    Ti = np.zeros((num_cables, num_cables))
    Tn = [0.0] * num_cables

    for d_idx in range(num_cables):
        ROW1 = d_idx // cols
        COL1 = d_idx % cols

        # Calculate self-heating
        Vars = calculate_single_cable(
            CurrentArray[d_idx],
            CableType[d_idx] if isinstance(CableType[0], (list, tuple)) else CableType,
        )

        temperature[d_idx] = Vars[0]
        Wc[d_idx] = Vars[1]
        qs[d_idx] = Vars[2]

        for i in range(num_cables):
            ROW2 = i // cols
            COL2 = i % cols
            if d_idx != i:
                # Geometric calculations for mutual heating
                # DuctBank[4] -> Depth?
                # DuctBank[5] -> Vert Spacing?
                # DuctBank[6] -> Horiz Spacing?

                # Formula:
                # dPkn (distance to image): sqrt( (depth_terms)^2 + (horiz_dist)^2 )
                # dkn (distance to cable): sqrt( (horiz_dist)^2 + (vert_dist)^2 )

                # Assuming DuctBank format from tutorial context:
                # 0: ? 1: ? 2: Rho_effective? 3: ? 4: Depth 5: VSpace 6: HSpace 7: N_factor? 8: Rho_Earth

                # Original: math.sqrt(math.pow(36 * 2 + DuctBank[4] * 2 + ((ROW2 - ROW1 + 1) * DuctBank[5]), 2) + ...
                # The '36*2' looks like a hardcoded base depth or offset in original?

                # We will preserve the exact logic.

                # Note: The original code used Hardcoded "36" in global dbDepth but also DuctBank[4]?
                # Wait, global dbDepth = 36.
                # In calculate(), loop: math.pow(36 * 2 + DuctBank[4] * 2 ...
                # Maybe DuctBank[4] is NOT depth but an offset? Or depth?
                # Let's assume the caller provides the same list structure.

                term_y = 36 * 2 + DuctBank[4] * 2 + ((ROW2 - ROW1 + 1) * DuctBank[5])
                term_x = (COL2 - COL1) * DuctBank[6]

                dPkn[d_idx][i] = math.sqrt(term_y**2 + term_x**2)
                dkn[d_idx][i] = math.sqrt(
                    math.pow((COL2 - COL1) * 7.0, 2)
                    + math.pow(((ROW2 - ROW1) * 5.5), 2)
                )
                # Note: 7 and 5.5 hardcoded in dkn calculation in original!
                # dbVertSpacing=5.5, dbHorizSpacing=7 (globals).
                # But dPkn uses DuctBank[5] and [6].
                # This suggests inconsistency in original script (mixing globals and passed args).
                # We should use DuctBank params if possible, or fallback to constants matching original.

                rho_fill = DuctBank[2]
                rho_earth = DuctBank[8]
                n_factor = DuctBank[7]

                term_log = math.log(dPkn[d_idx][i] / dkn[d_idx][i]) / math.log(10)
                Re[d_idx][i] = (0.00522 * rho_fill) * term_log + 0.00522 * (
                    rho_earth - rho_fill
                ) * Gb

                Ti[d_idx][i] = (3 * Wc[d_idx] * qs[d_idx] * Re[d_idx][i] * n_factor) + (
                    3 * Re[d_idx][i] * 0.0221
                )
            else:
                Ti[d_idx][i] = 0

    for c in range(num_cables):
        Tn[c] = temperature[c]
        for j in range(num_cables):
            if CurrentArray[j] > 0:
                Tn[c] += Ti[j][c]

    return Tn


def calculate_single_cable(Current, CableType):
    """
    Calculate temperature rise for a single cable (self-heating).
    """
    # Unpack CableType
    # 0: Dc (Diameter Conductor)
    # 2: Tcs (Thick Cond Shield)
    # 3: Tin (Thick Insul)
    # 4: Tis (Thick Insul Shield)
    # 7: n (Concentric wires count)
    # 10: kp (Prox effect)
    # 11: Ta (Ambient Temp)
    # 13: Volts (LL kV)
    # 14: pi (Rho Insul)
    # 15: pj (Rho Jacket)
    # 19: pe (Rho Earth)
    # 31: L (Depth)

    Dc = CableType[0]
    Tcs = CableType[2]
    Tin = CableType[3]
    Tis = CableType[4]
    n = CableType[7]
    kp = CableType[10]
    Ta = CableType[11]
    Volts = CableType[13]
    pi = CableType[14]
    pj = CableType[15]
    pe = CableType[19]
    L = CableType[31]

    # Globals
    Rdc25 = DEFAULT_VARS["Rdc25"]
    Tj = DEFAULT_VARS["Tj"]
    d = DEFAULT_VARS["d"]
    Lf = DEFAULT_VARS["Lf"]
    ks = DEFAULT_VARS["ks"]
    er = DEFAULT_VARS["er"]
    cos0 = DEFAULT_VARS["cos0"]
    Nprime = DEFAULT_VARS["Nprime"]
    Ts = DEFAULT_VARS["Ts"]
    a = DEFAULT_VARS["a"]
    S = DEFAULT_VARS["S"]
    loadFactor = 0.69  # Default?

    # Diameters
    Dcs = Dc + 2 * Tcs
    Di = Dcs + 2 * Tin
    Dis = Di + 2 * Tis
    Dshld = Dis + 2 * d
    Dsm = (Dshld + Dis) / 2
    Dj = Dshld + 2 * Tj
    De = Dj

    E = Volts / math.sqrt(3)
    LF = 0.3 * loadFactor + 0.7 * loadFactor * loadFactor

    # Resistances (AC)
    Rdc = Rdc25 * ((75 + 228.1) / (25 + 228.1))
    Rs = ((10.575 * Lf) / (n * d * d)) * ((Ts + 234.5) / (25 + 234.5))
    xs = Rdc / ks
    Fxs = 11.0 / (math.pow((xs + (4 / xs) - (2.56 / (xs * xs))), 2))
    Ycs = Fxs
    xp = Rdc / kp
    Fxp = 11.0 / (math.pow((xp + (4 / xp) - (2.56 / (xp * xp))), 2))
    Ycp = (
        Fxp
        * (Dc / S)
        * (Dc / S)
        * ((1.18 / (Fxp + 0.27)) + (0.312) * (Dc / S) * (Dc / S))
    )
    Yc = Ycs + Ycp

    Xm = 52.92 * ((math.log((2 * S) / Dsm)) / (math.log(10)))
    Y = Xm + a
    Z = Xm - (a / 3)
    P = Rs / Y
    Q = Rs / Z

    qs1 = 1 + (
        ((P * P + 3 * Q * Q) + 2 * 1.732 * (P - Q) + 4)
        / (4 * (P * P + 1) * (Q * Q + 1))
    ) * Rs / (Rdc * (1 + Yc))
    qs2 = 1 + (1 / (Q * Q + 1)) * (Rs / (Rdc * (1 + Yc)))
    qs3 = 1 + (
        ((P * P + 3 * Q * Q) - 2 * 1.732 * (P - Q) + 4)
        / (4 * (P * P + 1) * (Q * Q + 1))
    ) * Rs / (Rdc * (1 + Yc))

    Rd = 0
    Rsd = 0

    Dx = 1.02 * math.sqrt((104 / math.pow(pe, 0.8)) * 24)
    Ri = 0.012 * pi * (math.log(Dis / Dc) / math.log(10))
    Rj = 0.012 * pj * (math.log(Dj / Dis) / math.log(10))

    term_F = math.sqrt((S * S + ((2 * L) * (2 * L)))) / S
    F = term_F * term_F
    F21 = term_F
    F23 = F21

    Re1 = 0.012 * pe * Nprime * (math.log(Dx / De) / math.log(10))
    Re2 = 0.012 * pe * Nprime * LF * (math.log((4 * L) / Dx) / math.log(10))
    R21 = 0.012 * pe * Nprime * LF * (math.log(F21) / math.log(10))
    R23 = 0.012 * pe * Nprime * LF * (math.log(F23) / math.log(10))

    calc1 = Dx / De
    calc2 = ((4 * L) / Dx) * F

    Red2 = (
        0.012
        * pe
        * Nprime
        * ((math.log(calc1) / math.log(10)) + (1 * (math.log(calc2) / math.log(10))))
    )
    Rda2 = 0.5 * Ri + Rj + Rd + Rsd + Red2
    Rse = Rj + Rsd + Rd
    Rca2 = Ri + (qs2 * (Rse + Re1 + Re2)) + qs1 * R21 + qs3 * R23

    Wd = (0.00276 * E * E * er * cos0) / ((math.log(Di / Dcs)) / (math.log(10)))
    deltaTd2 = Wd * Rda2

    curr_kA = Current / 1000.0
    Tc = curr_kA * curr_kA * Rdc * (1 + Yc) * Rca2 + Ta + deltaTd2

    Wc2 = curr_kA * curr_kA * Rdc * (1 + Yc)

    return [Tc, Wc2, qs1]


def calculate_thermal_parameters(cableType):
    Dc = cableType[0]
    Tin = cableType[3]
    Tis = cableType[4]
    pi = cableType[14]
    pj = cableType[15]
    pe = cableType[19]
    L = cableType[31]

    Tj = DEFAULT_VARS["Tj"]
    d = DEFAULT_VARS["d"]

    Dcs = Dc + 2 * 0.03
    Di = Dcs + 2 * Tin
    Dis = Di + 2 * Tis
    Dshld = Dis + 2 * d
    Dj = Dshld + 2 * Tj
    De = Dj

    Ri = 0.012 * pi * (math.log(Dis / Dc) / math.log(10))
    Rj = 0.012 * pj * (math.log(Dj / Dis) / math.log(10))
    Re = 0.012 * pe * (math.log(4 * L / De) / math.log(10))

    conductor_area = math.pi * math.pow(Dc / 2, 2)
    insulation_area = math.pi * (math.pow(Di / 2, 2) - math.pow(Dcs / 2, 2))
    jacket_area = math.pi * (math.pow(Dj / 2, 2) - math.pow(Dis / 2, 2))

    copper_capacity = 0.092
    insulation_capacity = 0.4
    jacket_capacity = 0.4

    copper_density = 0.324
    insulation_density = 0.032
    jacket_density = 0.040

    Cc = conductor_area * copper_density * copper_capacity * 12
    Ci = insulation_area * insulation_density * insulation_capacity * 12
    Cj = jacket_area * jacket_density * jacket_capacity * 12

    Rtotal = Ri + Rj + Re
    Ctotal = Cc + Ci + Cj

    time_constant = Rtotal * Ctotal

    return ThermalParameters(Rtotal, Ctotal, time_constant, Ri, Rj, Re)


def interpolate_load_profile(profile, profile_time_step, current_time):
    if current_time <= 0:
        return profile[0]

    index = int(current_time / profile_time_step)
    if index >= len(profile) - 1:
        return profile[-1]

    t1 = index * profile_time_step
    t2 = (index + 1) * profile_time_step
    factor = (current_time - t1) / (t2 - t1)

    return profile[index] + factor * (profile[index + 1] - profile[index])


def calculate_derivatives(
    temperatures,
    current_array,
    cable_type,
    duct_bank,
    duct,
    cols,
    thermal_params,
    load_factor,
):
    n = len(temperatures)
    derivatives = [0.0] * n

    for i in range(n):
        current = current_array[i] * load_factor
        temperature = temperatures[i]

        # Handle cable_type list structure flexibility
        ctype = (
            cable_type[i] if isinstance(cable_type[0], (list, tuple)) else cable_type
        )
        ambient_temp = ctype[11]

        vars_ = calculate_single_cable(current, ctype)
        heat_generated = vars_[1]

        heat_dissipated = (temperature - ambient_temp) / thermal_params[
            i
        ].total_resistance

        heat_balance = heat_generated - heat_dissipated

        derivatives[i] = heat_balance / thermal_params[i].total_capacitance

    return derivatives


def runge_kutta_step(
    current_temps,
    current_array,
    cable_type,
    duct_bank,
    duct,
    cols,
    thermal_params,
    load_factor,
    time_step,
):
    n = len(current_temps)
    h = time_step

    k1 = calculate_derivatives(
        current_temps,
        current_array,
        cable_type,
        duct_bank,
        duct,
        cols,
        thermal_params,
        load_factor,
    )

    temp1 = [current_temps[i] + 0.5 * h * k1[i] for i in range(n)]
    k2 = calculate_derivatives(
        temp1,
        current_array,
        cable_type,
        duct_bank,
        duct,
        cols,
        thermal_params,
        load_factor,
    )

    temp2 = [current_temps[i] + 0.5 * h * k2[i] for i in range(n)]
    k3 = calculate_derivatives(
        temp2,
        current_array,
        cable_type,
        duct_bank,
        duct,
        cols,
        thermal_params,
        load_factor,
    )

    temp3 = [current_temps[i] + h * k3[i] for i in range(n)]
    k4 = calculate_derivatives(
        temp3,
        current_array,
        cable_type,
        duct_bank,
        duct,
        cols,
        thermal_params,
        load_factor,
    )

    new_temps = [
        current_temps[i] + (h / 6.0) * (k1[i] + 2 * k2[i] + 2 * k3[i] + k4[i])
        for i in range(n)
    ]

    return new_temps


def calculate_transient_temperatures(
    CurrentArray,
    CableType,
    DuctBank,
    Duct,
    cols,
    timeStep,
    totalTime,
    loadProfile,
    initialTemp,
):
    """
    Simulate transient temperature rise.
    """
    num_cables = len(CurrentArray)
    num_time_steps = int(totalTime / timeStep) + 1
    temperature_history = [[0.0] * num_time_steps for _ in range(num_cables)]
    time_array = [0.0] * num_time_steps

    for i in range(num_cables):
        temperature_history[i][0] = initialTemp[i]

    # Handle single cable type shared or list
    if isinstance(CableType[0], (list, tuple)):
        thermal_params = [calculate_thermal_parameters(ct) for ct in CableType]
    else:
        thermal_params = [
            calculate_thermal_parameters(CableType) for _ in range(num_cables)
        ]

    profile_time_step = totalTime / (len(loadProfile) - 1)

    for t in range(1, num_time_steps):
        current_time = t * timeStep
        time_array[t] = current_time
        load_factor = interpolate_load_profile(
            loadProfile, profile_time_step, current_time
        )

        current_temps = [temperature_history[i][t - 1] for i in range(num_cables)]
        new_temps = runge_kutta_step(
            current_temps,
            CurrentArray,
            CableType,
            DuctBank,
            Duct,
            cols,
            thermal_params,
            load_factor,
            timeStep,
        )

        for i in range(num_cables):
            temperature_history[i][t] = new_temps[i]

    return TransientResult(temperature_history, time_array)


# ==============================================================================
# Transformer Thermal Model Classes
# ==============================================================================


class CoolingMode(Enum):
    """Transformer cooling modes per IEEE standards"""

    ONAN = "ONAN"  # Oil Natural Air Natural
    ONAF = "ONAF"  # Oil Natural Air Forced
    OFAF = "OFAF"  # Oil Forced Air Forced
    OFWF = "OFWF"  # Oil Forced Water Forced


@dataclass
class TransformerParameters:
    """OFAF Transformer thermal and electrical parameters"""

    # Nameplate ratings
    rated_power: float = 50.0  # MVA
    rated_voltage_hv: float = 138.0  # kV
    rated_voltage_lv: float = 13.8  # kV

    # Thermal parameters for OFAF
    top_oil_rise_rated: float = 55.0  # K at rated load
    hot_spot_rise_rated: float = 65.0  # K at rated load
    oil_time_constant: float = 90.0  # minutes (40-120 for OFAF)
    winding_time_constant: float = 7.0  # minutes (2-10 for OFAF)

    # Loss parameters
    no_load_loss: float = 35.0  # kW
    load_loss_rated: float = 235.0  # kW at rated load
    ratio_load_to_no_load: float = 6.71  # R ratio

    # Cooling system parameters
    oil_exponent: float = 0.8  # n for OFAF (0.8 typical)
    winding_exponent: float = 0.8  # m for OFAF
    num_cooling_stages: int = 2  # Number of fan stages
    fan_trigger_temps: List[float] = None  # Fan activation temperatures
    fan_capacities: List[float] = None  # Relative cooling capacity per stage

    # Emergency rating limits per IEEE C57.91
    normal_hot_spot_limit: float = 110.0  # °C
    emergency_hot_spot_limit: float = 140.0  # °C
    emergency_top_oil_limit: float = 110.0  # °C

    # Aging parameters
    reference_temp: float = 110.0  # °C for aging calculation
    aging_constant: float = 15000.0  # Arrhenius constant

    def __post_init__(self):
        if self.fan_trigger_temps is None:
            self.fan_trigger_temps = [65.0, 75.0]  # °C
        if self.fan_capacities is None:
            self.fan_capacities = [1.3, 1.6]  # Relative to ONAN


class TransformerThermalModel:
    """IEEE C57.91 thermal model for OFAF transformers"""

    def __init__(self, params: TransformerParameters):
        self.params = params

    def calculate_ultimate_top_oil_rise(
        self, load_pu: float, ambient: float, cooling_mode: str = "OFAF"
    ) -> float:
        """Calculate ultimate top oil temperature rise"""
        R = self.params.ratio_load_to_no_load
        K = load_pu  # Per unit load
        n = self.params.oil_exponent

        # Base temperature rise
        delta_theta_to_u = self.params.top_oil_rise_rated * (
            ((R * K**2 + 1) / (R + 1)) ** n
        )

        # Cooling mode adjustment
        cooling_factor = self._get_cooling_factor(
            ambient + delta_theta_to_u, cooling_mode
        )

        return delta_theta_to_u / cooling_factor

    def calculate_hot_spot_rise(self, load_pu: float, top_oil_rise: float) -> float:
        """Calculate hot spot temperature rise over top oil"""
        m = self.params.winding_exponent
        return self.params.hot_spot_rise_rated * (load_pu ** (2 * m))

    def thermal_dynamics(
        self, state: List[float], t: float, load_pu: float, ambient: float
    ) -> List[float]:
        """Differential equations for thermal dynamics
        state = [top_oil_temp, hot_spot_temp]
        """
        top_oil_temp, hot_spot_temp = state

        # Calculate ultimate temperatures
        top_oil_rise_u = self.calculate_ultimate_top_oil_rise(load_pu, ambient)
        top_oil_temp_u = ambient + top_oil_rise_u

        hot_spot_rise = self.calculate_hot_spot_rise(load_pu, top_oil_temp - ambient)
        hot_spot_temp_u = top_oil_temp + hot_spot_rise

        # Time derivatives
        d_top_oil = (top_oil_temp_u - top_oil_temp) / self.params.oil_time_constant
        d_hot_spot = (
            hot_spot_temp_u - hot_spot_temp
        ) / self.params.winding_time_constant

        return [d_top_oil * 60, d_hot_spot * 60]  # Convert to per hour

    def simulate_thermal_response(
        self,
        load_profile: np.ndarray,
        ambient_profile: np.ndarray,
        initial_temps: List[float] = None,
    ) -> Dict:
        """Simulate transformer thermal response over time"""
        if initial_temps is None:
            initial_temps = [ambient_profile[0] + 20, ambient_profile[0] + 40]

        hours = len(load_profile)
        time_points = np.arange(hours)

        # Store results
        top_oil_temps = np.zeros(hours)
        hot_spot_temps = np.zeros(hours)

        current_state = initial_temps

        for i in range(hours):
            # Solve for this hour
            t_span = [0, 1]  # One hour
            solution = odeint(
                self.thermal_dynamics,
                current_state,
                t_span,
                args=(load_profile[i], ambient_profile[i]),
            )

            current_state = solution[-1]
            top_oil_temps[i] = current_state[0]
            hot_spot_temps[i] = current_state[1]

        return {
            "time": time_points,
            "top_oil": top_oil_temps,
            "hot_spot": hot_spot_temps,
            "load_pu": load_profile,
            "ambient": ambient_profile,
        }

    def calculate_loss_of_life(
        self, hot_spot_temps: np.ndarray, time_step_hours: float = 1.0
    ) -> Dict:
        """Calculate transformer loss of life using Arrhenius equation"""
        # Aging acceleration factor for each time step
        faa = np.exp(
            self.params.aging_constant / (self.params.reference_temp + 273)
            - self.params.aging_constant / (hot_spot_temps + 273)
        )

        # Equivalent aging factor
        feqa = np.mean(faa)

        # Total loss of life (in hours of normal life)
        total_hours = len(hot_spot_temps) * time_step_hours
        lol_hours = feqa * total_hours
        lol_percent = (lol_hours / (180000)) * 100  # 180,000 hours normal life

        return {
            "faa": faa,
            "feqa": feqa,
            "lol_hours": lol_hours,
            "lol_percent": lol_percent,
            "peak_faa": np.max(faa),
        }

    def _get_cooling_factor(self, top_oil_temp: float, mode: str) -> float:
        """Get cooling enhancement factor based on temperature and mode"""
        if mode == "ONAN":
            return 1.0

        # Check fan stages for OFAF
        factor = 1.0
        for i, trigger_temp in enumerate(self.params.fan_trigger_temps):
            if top_oil_temp >= trigger_temp:
                factor = self.params.fan_capacities[i]

        return factor


class TransformerRatingCalculator:
    """Calculate dynamic transformer ratings based on thermal constraints"""

    def __init__(
        self,
        transformer_params: TransformerParameters,
        thermal_model: TransformerThermalModel,
    ):
        self.transformer = transformer_params
        self.thermal = thermal_model

    def calculate_hourly_ratings(
        self,
        load_forecast: np.ndarray,
        ambient_forecast: np.ndarray,
        initial_temps: List[float] = None,
    ) -> pd.DataFrame:
        """Calculate hourly dynamic ratings based on thermal constraints"""
        hours = len(load_forecast)

        # Initialize results
        ratings = {
            "hour": np.arange(hours),
            "normal_rating_mva": np.zeros(hours),
            "emergency_rating_mva": np.zeros(hours),
            "load_forecast_mva": load_forecast * self.transformer.rated_power,
            "ambient_c": ambient_forecast,
            "top_oil_c": np.zeros(hours),
            "hot_spot_c": np.zeros(hours),
            "cooling_stage": np.zeros(hours, dtype=int),
        }

        # Simulate base case thermal response
        base_simulation = self.thermal.simulate_thermal_response(
            load_forecast, ambient_forecast, initial_temps
        )

        ratings["top_oil_c"] = base_simulation["top_oil"]
        ratings["hot_spot_c"] = base_simulation["hot_spot"]

        # Calculate available ratings for each hour
        for hour in range(hours):
            # Current thermal state
            current_ambient = ambient_forecast[hour]
            current_top_oil = ratings["top_oil_c"][hour]

            # Determine cooling stage
            cooling_stage = self._determine_cooling_stage(current_top_oil)
            ratings["cooling_stage"][hour] = cooling_stage

            # Calculate maximum allowable load for normal operation
            normal_rating = self._calculate_rating_for_limit(
                current_ambient, self.transformer.normal_hot_spot_limit, "normal"
            )

            # Calculate emergency rating
            emergency_rating = self._calculate_rating_for_limit(
                current_ambient, self.transformer.emergency_hot_spot_limit, "emergency"
            )

            ratings["normal_rating_mva"][hour] = normal_rating
            ratings["emergency_rating_mva"][hour] = emergency_rating

        return pd.DataFrame(ratings)

    def calculate_emergency_duration(
        self, overload_factor: float, initial_hot_spot: float, ambient: float
    ) -> float:
        """Calculate allowable emergency overload duration"""
        # Based on IEEE C57.91 exponential equations
        if overload_factor <= 1.0:
            return float("inf")

        # Time to reach emergency limit
        tau = self.transformer.winding_time_constant / 60  # Convert to hours

        # Ultimate hot spot at overload
        ultimate_hot_spot = (
            ambient
            + self.thermal.calculate_ultimate_top_oil_rise(overload_factor, ambient)
            + self.thermal.calculate_hot_spot_rise(
                overload_factor,
                self.thermal.calculate_ultimate_top_oil_rise(overload_factor, ambient),
            )
        )

        if ultimate_hot_spot <= self.transformer.emergency_hot_spot_limit:
            return float("inf")

        # Time to reach limit
        time_to_limit = -tau * np.log(
            (self.transformer.emergency_hot_spot_limit - ultimate_hot_spot)
            / (initial_hot_spot - ultimate_hot_spot)
        )

        return max(0, time_to_limit)

    def _calculate_rating_for_limit(
        self, ambient: float, limit: float, rating_type: str
    ) -> float:
        """Calculate rating that keeps hot spot below limit"""
        # Binary search for maximum load
        low, high = 0.0, 3.0  # 0 to 300% rating
        tolerance = 0.001

        while high - low > tolerance:
            mid = (low + high) / 2

            # Calculate resulting hot spot (steady state)
            top_oil_rise = self.thermal.calculate_ultimate_top_oil_rise(mid, ambient)
            hot_spot_rise = self.thermal.calculate_hot_spot_rise(mid, top_oil_rise)
            hot_spot = ambient + top_oil_rise + hot_spot_rise

            if hot_spot < limit:
                low = mid
            else:
                high = mid

        return low * self.transformer.rated_power

    def _determine_cooling_stage(self, top_oil_temp: float) -> int:
        """Determine required cooling stage based on temperature"""
        stage = 0
        for i, trigger in enumerate(self.transformer.fan_trigger_temps):
            if top_oil_temp >= trigger:
                stage = i + 1
        return stage
