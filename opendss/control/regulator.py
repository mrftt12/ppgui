"""
OpenDSS RegControl generation from multiconductor tap-changer controllers.

Translates ``LoadTapChangerControl`` and ``LineDropControlExtended``
(stored in ``net.controller``) to OpenDSS ``New RegControl`` DSS commands.

Usage::

    from opendss.control.regulator import write_regcontrols
    write_regcontrols(net, dss_lines)
"""
import math
from typing import List


def _safe_float(value, default: float = 0.0) -> float:
    try:
        v = float(value)
        return v if math.isfinite(v) else default
    except (TypeError, ValueError):
        return default


def _bus_name(net, bus_idx: int) -> str:
    try:
        row = net.bus.xs(bus_idx, level=0).iloc[0]
        raw = row.get("name") if hasattr(row, "get") else getattr(row, "name", None)
        if raw and isinstance(raw, str):
            return raw.replace(" ", "_").replace(".", "_")
    except Exception:
        pass
    return f"bus_{bus_idx}"


def _bus_vn_kv(net, bus_idx: int) -> float:
    try:
        row = net.bus.xs(bus_idx, level=0).iloc[0]
        return float(row["vn_kv"])
    except Exception:
        return 0.4


def _get_trafo_name(net, trafo_top_level_index, circ_idx=0):
    """Return the OpenDSS transformer element name matching ``_write_transformers`` output."""
    if not hasattr(net, "trafo1ph") or net.trafo1ph.empty:
        return None
    try:
        trafo_grp = net.trafo1ph.loc[trafo_top_level_index]
    except KeyError:
        return None

    bus_levels = trafo_grp.index.get_level_values("bus")
    unique_buses = list(dict.fromkeys(int(b) for b in bus_levels))
    if len(unique_buses) < 2:
        return None

    bus_vn = {b: _bus_vn_kv(net, b) for b in unique_buses}
    buses_sorted = sorted(unique_buses, key=lambda b: bus_vn[b], reverse=True)
    hv_bus = buses_sorted[0]
    hv_rows = trafo_grp.xs(hv_bus, level="bus")

    try:
        hv_row = hv_rows.iloc[circ_idx]
    except IndexError:
        hv_row = hv_rows.iloc[0]

    raw_name = None
    try:
        raw_name = hv_row.get("name") if hasattr(hv_row, "get") else getattr(hv_row, "name", None)
        raw_name = None if str(raw_name) == "None" else str(raw_name)
    except Exception:
        pass
    if raw_name:
        base = f"{raw_name}_{trafo_top_level_index}_{circ_idx}"
    else:
        base = f"T_{trafo_top_level_index}_{circ_idx}"
    return base.replace(" ", "_").replace(".", "_")


def write_regcontrols(net, lines: List[str]) -> None:
    """Append OpenDSS ``New RegControl`` commands to *lines*.

    Iterates over ``net.controller`` looking for multiconductor
    ``LoadTapChangerControl``, ``LineDropControl``, and
    ``LineDropControlExtended`` objects and emits the corresponding
    OpenDSS ``RegControl`` elements.

    Parameters
    ----------
    net : pandapowerNet
        Multiconductor network with a ``controller`` DataFrame.
    lines : list[str]
        Accumulator list of DSS script lines.
    """
    if not hasattr(net, "controller"):
        return
    try:
        if net.controller is None or net.controller.empty:
            return
    except Exception:
        return

    _LTC_TYPES = ("LoadTapChangerControl", "LineDropControl",
                   "LineDropControlExtended")
    has_content = False

    for _ctrl_idx, ctrl_row in net.controller.iterrows():
        ctrl = ctrl_row["object"]
        ctrl_type = type(ctrl).__name__

        if ctrl_type not in _LTC_TYPES:
            continue
        if not ctrl_row.get("in_service", True):
            continue

        tidx = ctrl.trafo_top_level_index
        try:
            trafo_grp = net.trafo1ph.loc[tidx]
        except (KeyError, AttributeError):
            continue

        bus_levels = trafo_grp.index.get_level_values("bus")
        unique_buses = list(dict.fromkeys(int(b) for b in bus_levels))
        if len(unique_buses) < 2:
            continue

        bus_vn = {b: _bus_vn_kv(net, b) for b in unique_buses}
        buses_sorted = sorted(unique_buses, key=lambda b: bus_vn[b], reverse=True)
        hv_bus = buses_sorted[0]
        lv_bus = buses_sorted[1]

        tap_side = getattr(ctrl, "side", "lv")
        controlled_bus = lv_bus if tap_side == "lv" else hv_bus
        vn_kv = bus_vn[controlled_bus]

        hv_rows = trafo_grp.xs(hv_bus, level="bus")
        lv_rows = trafo_grp.xs(lv_bus, level="bus")
        ncircuits = len(hv_rows)

        for circ_idx in range(ncircuits):
            trafo_name = _get_trafo_name(net, tidx, circ_idx)
            if trafo_name is None:
                continue

            reg_name = f"Reg_{trafo_name}"

            # Voltage setpoints ------------------------------------------------
            vm_lower = getattr(ctrl, "vm_lower_pu", 0.975)
            vm_upper = getattr(ctrl, "vm_upper_pu", 1.025)
            if hasattr(vm_lower, "__iter__"):
                vm_lower = float(min(vm_lower))
                vm_upper = float(max(vm_upper))
            else:
                vm_lower = float(vm_lower)
                vm_upper = float(vm_upper)

            # OpenDSS vreg / band are on a 120 V secondary base
            vreg_120 = (vm_lower + vm_upper) / 2.0 * 120.0
            band_120 = (vm_upper - vm_lower) * 120.0

            # PT ratio: Vln (volts) / 120
            vln_v = vn_kv / math.sqrt(3) * 1000.0
            ptratio = vln_v / 120.0

            # CT primary: kVA / Vln(kV) as a reasonable default
            try:
                side_rows = lv_rows if tap_side == "lv" else hv_rows
                sn_kva = _safe_float(side_rows.iloc[circ_idx]["sn_mva"], 0.1) * 1000.0
                ct_kv = vn_kv / math.sqrt(3)
                ctprim = sn_kva / ct_kv if ct_kv > 0 else 700.0
            except Exception:
                ctprim = 700.0

            if not has_content:
                lines.append("! Regulator Controls")
                has_content = True

            cmd = (
                f"New RegControl.{reg_name} transformer={trafo_name} winding=2 "
                f"vreg={vreg_120:.4g} band={band_120:.4g} "
                f"ptratio={ptratio:.6g} ctprim={ctprim:.6g}"
            )

            # Line-drop compensation R and X
            if ctrl_type in ("LineDropControl", "LineDropControlExtended"):
                r_ldc = _safe_float(getattr(ctrl, "r_ldc_v", getattr(ctrl, "R_comp", 0.0)))
                x_ldc = _safe_float(getattr(ctrl, "x_ldc_v", getattr(ctrl, "X_comp", 0.0)))
                cmd += f" R={r_ldc:.6g} X={x_ldc:.6g}"

            lines.append(cmd)

    if has_content:
        lines.append("")
