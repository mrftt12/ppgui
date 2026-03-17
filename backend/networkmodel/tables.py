from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .base import (
    IndexBusCircuitModel,
    IndexCircuitModel,
    IndexModel,
    IndexPhaseModel,
    IndexSequenceModel,
    RowModel,
)


@dataclass
class NetworkMeta(RowModel):
    table_name = "network_meta"
    primary_key = ("network_id", "key")

    key: Optional[str] = None
    value: Optional[str] = None


@dataclass
class Bus(IndexPhaseModel):
    table_name = "bus"
    primary_key = ("network_id", "index", "phase")

    name: Optional[str] = None
    vn_kv: Optional[float] = None
    grounded: Optional[bool] = None
    grounding_r_ohm: Optional[float] = None
    grounding_x_ohm: Optional[float] = None
    in_service: Optional[bool] = None
    type: Optional[str] = None
    zone: Optional[str] = None


@dataclass
class ExtGrid(IndexCircuitModel):
    table_name = "ext_grid"
    primary_key = ("network_id", "index", "circuit")

    name: Optional[str] = None
    bus: Optional[int] = None
    from_phase: Optional[int] = None
    to_phase: Optional[int] = None
    vm_pu: Optional[float] = None
    va_degree: Optional[float] = None
    r_ohm: Optional[float] = None
    x_ohm: Optional[float] = None
    in_service: Optional[bool] = None


@dataclass
class ExtGridSequence(IndexSequenceModel):
    table_name = "ext_grid_sequence"
    primary_key = ("network_id", "index", "sequence")

    name: Optional[str] = None
    bus: Optional[int] = None
    from_phase: Optional[int] = None
    to_phase: Optional[int] = None
    vm_pu: Optional[float] = None
    va_degree: Optional[float] = None
    r_ohm: Optional[float] = None
    x_ohm: Optional[float] = None
    in_service: Optional[bool] = None


@dataclass
class AsymmetricLoad(IndexCircuitModel):
    table_name = "asymmetric_load"
    primary_key = ("network_id", "index", "circuit")

    name: Optional[str] = None
    bus: Optional[int] = None
    from_phase: Optional[int] = None
    to_phase: Optional[int] = None
    p_mw: Optional[float] = None
    q_mvar: Optional[float] = None
    const_z_percent_p: Optional[float] = None
    const_i_percent_p: Optional[float] = None
    const_z_percent_q: Optional[float] = None
    const_i_percent_q: Optional[float] = None
    sn_mva: Optional[float] = None
    scaling: Optional[float] = None
    in_service: Optional[bool] = None
    type: Optional[str] = None


@dataclass
class AsymmetricSgen(IndexCircuitModel):
    table_name = "asymmetric_sgen"
    primary_key = ("network_id", "index", "circuit")

    name: Optional[str] = None
    bus: Optional[int] = None
    from_phase: Optional[int] = None
    to_phase: Optional[int] = None
    p_mw: Optional[float] = None
    q_mvar: Optional[float] = None
    vm_pu: Optional[float] = None
    const_z_percent_p: Optional[float] = None
    const_i_percent_p: Optional[float] = None
    const_z_percent_q: Optional[float] = None
    const_i_percent_q: Optional[float] = None
    sn_mva: Optional[float] = None
    scaling: Optional[float] = None
    in_service: Optional[bool] = None
    type: Optional[str] = None
    current_source: Optional[bool] = None
    slack: Optional[bool] = None
    control_mode: Optional[str] = None
    control_curve: Optional[str] = None


@dataclass
class AsymmetricGen(IndexCircuitModel):
    table_name = "asymmetric_gen"
    primary_key = ("network_id", "index", "circuit")

    bus: Optional[int] = None
    from_phase: Optional[int] = None
    to_phase: Optional[int] = None
    p_mw: Optional[float] = None
    vm_pu: Optional[float] = None
    sn_mva: Optional[float] = None
    scaling: Optional[float] = None
    in_service: Optional[bool] = None
    name: Optional[str] = None
    type: Optional[str] = None
    current_source: Optional[bool] = None
    slack: Optional[bool] = None


@dataclass
class AsymmetricShunt(IndexCircuitModel):
    table_name = "asymmetric_shunt"
    primary_key = ("network_id", "index", "circuit")

    name: Optional[str] = None
    bus: Optional[int] = None
    from_phase: Optional[int] = None
    to_phase: Optional[int] = None
    p_mw: Optional[float] = None
    q_mvar: Optional[float] = None
    control_mode: Optional[str] = None
    closed: Optional[bool] = None
    v_threshold_on: Optional[float] = None
    v_threshold_off: Optional[float] = None
    max_q_mvar: Optional[float] = None
    max_p_mw: Optional[float] = None
    vn_kv: Optional[float] = None
    in_service: Optional[bool] = None


@dataclass
class Line(IndexCircuitModel):
    table_name = "line"
    primary_key = ("network_id", "index", "circuit")

    name: Optional[str] = None
    std_type: Optional[str] = None
    model_type: Optional[str] = None
    from_bus: Optional[int] = None
    from_phase: Optional[int] = None
    to_bus: Optional[int] = None
    to_phase: Optional[int] = None
    length_km: Optional[float] = None
    type: Optional[str] = None
    in_service: Optional[bool] = None


@dataclass
class Switch(IndexCircuitModel):
    table_name = "switch"
    primary_key = ("network_id", "index", "circuit")

    bus: Optional[int] = None
    phase: Optional[int] = None
    element: Optional[int] = None
    et: Optional[str] = None
    type: Optional[str] = None
    closed: Optional[bool] = None
    name: Optional[str] = None
    r_ohm: Optional[float] = None


@dataclass
class Trafo1ph(IndexBusCircuitModel):
    table_name = "trafo1ph"
    primary_key = ("network_id", "index", "bus", "circuit")

    name: Optional[str] = None
    from_phase: Optional[int] = None
    to_phase: Optional[int] = None
    vn_kv: Optional[float] = None
    sn_mva: Optional[float] = None
    vk_percent: Optional[float] = None
    vkr_percent: Optional[float] = None
    pfe_kw: Optional[float] = None
    i0_percent: Optional[float] = None
    tap_neutral: Optional[int] = None
    tap_min: Optional[int] = None
    tap_max: Optional[int] = None
    tap_step_percent: Optional[float] = None
    tap_pos: Optional[int] = None
    in_service: Optional[bool] = None
    tap_side: Optional[str] = None
    tap_step_degree: Optional[float] = None


@dataclass
class Measurement(IndexModel):
    table_name = "measurement"
    primary_key = ("network_id", "index")

    name: Optional[str] = None
    measurement_type: Optional[str] = None
    element_type: Optional[str] = None
    element: Optional[int] = None
    value: Optional[float] = None
    std_dev: Optional[float] = None
    side: Optional[str] = None


@dataclass
class PwlCost(IndexModel):
    table_name = "pwl_cost"
    primary_key = ("network_id", "index")

    power_type: Optional[str] = None
    element: Optional[int] = None
    et: Optional[str] = None
    points: Optional[str] = None


@dataclass
class PolyCost(IndexModel):
    table_name = "poly_cost"
    primary_key = ("network_id", "index")

    element: Optional[int] = None
    et: Optional[str] = None
    cp0_eur: Optional[float] = None
    cp1_eur_per_mw: Optional[float] = None
    cp2_eur_per_mw2: Optional[float] = None
    cq0_eur: Optional[float] = None
    cq1_eur_per_mvar: Optional[float] = None
    cq2_eur_per_mvar2: Optional[float] = None


@dataclass
class Characteristic(IndexModel):
    table_name = "characteristic"
    primary_key = ("network_id", "index")

    object: Optional[str] = None


@dataclass
class Controller(IndexModel):
    table_name = "controller"
    primary_key = ("network_id", "index")

    object: Optional[str] = None
    in_service: Optional[bool] = None
    order: Optional[float] = None
    level: Optional[str] = None
    initial_run: Optional[bool] = None
    recycle: Optional[str] = None


@dataclass
class GroupModel(IndexModel):
    table_name = "group"
    primary_key = ("network_id", "index")

    name: Optional[str] = None
    element_type: Optional[str] = None
    element: Optional[str] = None
    reference_column: Optional[str] = None


@dataclass
class LineGeodata(IndexModel):
    table_name = "line_geodata"
    primary_key = ("network_id", "index")

    coords: Optional[str] = None


@dataclass
class BusGeodata(IndexModel):
    table_name = "bus_geodata"
    primary_key = ("network_id", "index")

    x: Optional[float] = None
    y: Optional[float] = None
    coords: Optional[str] = None


@dataclass
class ConfigurationStdType(IndexCircuitModel):
    table_name = "configuration_std_type"
    primary_key = ("network_id", "index", "circuit")

    conductor_outer_diameter_m: Optional[float] = None
    gmr_coefficient: Optional[float] = None
    r_dc_ohm_per_km: Optional[float] = None
    g_us_per_km: Optional[float] = None
    max_i_ka: Optional[float] = None
    x_m: Optional[str] = None
    y_m: Optional[str] = None


@dataclass
class SequenceStdType(IndexModel):
    table_name = "sequence_std_type"
    primary_key = ("network_id", "index")

    name: Optional[str] = None
    r_ohm_per_km: Optional[float] = None
    x_ohm_per_km: Optional[float] = None
    r0_ohm_per_km: Optional[float] = None
    x0_ohm_per_km: Optional[float] = None
    c_nf_per_km: Optional[float] = None
    c0_nf_per_km: Optional[float] = None
    max_i_ka: Optional[float] = None


@dataclass
class MatrixStdType(IndexCircuitModel):
    table_name = "matrix_std_type"
    primary_key = ("network_id", "index", "circuit")

    name: Optional[str] = None
    max_i_ka: Optional[float] = None
    r_1_ohm_per_km: Optional[float] = None
    r_2_ohm_per_km: Optional[float] = None
    r_3_ohm_per_km: Optional[float] = None
    r_4_ohm_per_km: Optional[float] = None
    x_1_ohm_per_km: Optional[float] = None
    x_2_ohm_per_km: Optional[float] = None
    x_3_ohm_per_km: Optional[float] = None
    x_4_ohm_per_km: Optional[float] = None
    g_1_us_per_km: Optional[float] = None
    g_2_us_per_km: Optional[float] = None
    g_3_us_per_km: Optional[float] = None
    g_4_us_per_km: Optional[float] = None
    b_1_us_per_km: Optional[float] = None
    b_2_us_per_km: Optional[float] = None
    b_3_us_per_km: Optional[float] = None
    b_4_us_per_km: Optional[float] = None


@dataclass
class ResBus(IndexPhaseModel):
    table_name = "res_bus"
    primary_key = ("network_id", "index", "phase")

    vm_pu: Optional[float] = None
    va_degree: Optional[float] = None
    p_mw: Optional[float] = None
    q_mvar: Optional[float] = None
    imbalance_percent: Optional[float] = None


@dataclass
class ResLine(IndexCircuitModel):
    table_name = "res_line"
    primary_key = ("network_id", "index", "circuit")

    i_from_ka: Optional[float] = None
    ia_from_degree: Optional[float] = None
    i_to_ka: Optional[float] = None
    ia_to_degree: Optional[float] = None
    i_ka: Optional[float] = None
    p_from_mw: Optional[float] = None
    p_to_mw: Optional[float] = None
    q_from_mvar: Optional[float] = None
    q_to_mvar: Optional[float] = None
    pl_mw: Optional[float] = None
    ql_mvar: Optional[float] = None
    vm_from_pu: Optional[float] = None
    vm_to_pu: Optional[float] = None
    va_from_degree: Optional[float] = None
    va_to_degree: Optional[float] = None
    loading_percent: Optional[float] = None


@dataclass
class ResTrafo(IndexBusCircuitModel):
    table_name = "res_trafo"
    primary_key = ("network_id", "index", "bus", "circuit")

    p_mw: Optional[float] = None
    q_mvar: Optional[float] = None
    i_ka: Optional[float] = None
    vm_pu: Optional[float] = None
    va_degree: Optional[float] = None
    pl_mw: Optional[float] = None
    ql_mvar: Optional[float] = None
    loading_percent: Optional[float] = None


@dataclass
class EmptyResBus(IndexModel):
    table_name = "_empty_res_bus"
    primary_key = ("network_id", "index")

    vm_pu: Optional[float] = None
    va_degree: Optional[float] = None
    p_mw: Optional[float] = None
    q_mvar: Optional[float] = None


@dataclass
class EmptyResLine(IndexModel):
    table_name = "_empty_res_line"
    primary_key = ("network_id", "index")

    p_from_mw: Optional[float] = None
    q_from_mvar: Optional[float] = None
    p_to_mw: Optional[float] = None
    q_to_mvar: Optional[float] = None
    pl_mw: Optional[float] = None
    ql_mvar: Optional[float] = None
    i_from_ka: Optional[float] = None
    i_to_ka: Optional[float] = None
    i_ka: Optional[float] = None
    vm_from_pu: Optional[float] = None
    va_from_degree: Optional[float] = None
    vm_to_pu: Optional[float] = None
    va_to_degree: Optional[float] = None
    loading_percent: Optional[float] = None


@dataclass
class EmptyResTrafo(IndexModel):
    table_name = "_empty_res_trafo"
    primary_key = ("network_id", "index")

    p_mw: Optional[float] = None
    q_mvar: Optional[float] = None
    pl_mw: Optional[float] = None
    ql_mvar: Optional[float] = None
    i_ka: Optional[float] = None
    vm_pu: Optional[float] = None
    va_degree: Optional[float] = None
    loading_percent: Optional[float] = None


@dataclass
class EmptyResAsymmetricLoad(IndexModel):
    table_name = "_empty_res_asymmetric_load"
    primary_key = ("network_id", "index")

    p_mw: Optional[float] = None
    q_mvar: Optional[float] = None


@dataclass
class EmptyResAsymmetricSgen(IndexModel):
    table_name = "_empty_res_asymmetric_sgen"
    primary_key = ("network_id", "index")

    p_mw: Optional[float] = None
    q_mvar: Optional[float] = None
    va_degree: Optional[float] = None
    vm_pu: Optional[float] = None


@dataclass
class EmptyResAsymmetricGen(IndexModel):
    table_name = "_empty_res_asymmetric_gen"
    primary_key = ("network_id", "index")

    p_mw: Optional[float] = None
    q_mvar: Optional[float] = None
    va_degree: Optional[float] = None
    vm_pu: Optional[float] = None


@dataclass
class EmptyResExtGrid(IndexModel):
    table_name = "_empty_res_ext_grid"
    primary_key = ("network_id", "index")

    p_mw: Optional[float] = None
    q_mvar: Optional[float] = None


@dataclass
class EmptyResExtGridSequence(IndexModel):
    table_name = "_empty_res_ext_grid_sequence"
    primary_key = ("network_id", "index")

    p_mw: Optional[float] = None
    q_mvar: Optional[float] = None
