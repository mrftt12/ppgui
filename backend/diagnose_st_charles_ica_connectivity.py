import copy
import pickle

import numpy as np
import pandas as pd
import pandapower as pp

from multiconductor.create import create_asymmetric_sgen
from multiconductor.pycci.cci_ica import _flatten_net_for_topology


def normalize_int_value(value):
    if isinstance(value, tuple):
        if len(value) == 0:
            return np.nan
        return normalize_int_value(value[0])
    if isinstance(value, list):
        if len(value) == 0:
            return np.nan
        return normalize_int_value(value[0])
    if pd.isna(value):
        return np.nan
    return int(value)


def normalize_bool_value(value):
    if isinstance(value, (np.bool_, bool)):
        return bool(value)
    if isinstance(value, (int, np.integer)):
        return bool(value)
    if pd.isna(value):
        return False
    return str(value).strip().lower() in {"true", "1", "t", "yes", "y"}


def normalize_bus_id(value):
    if isinstance(value, tuple):
        return normalize_bus_id(value[0]) if len(value) else None
    if isinstance(value, list):
        return normalize_bus_id(value[0]) if len(value) else None
    if pd.isna(value):
        return None
    try:
        return int(value)
    except Exception:
        return value


def deactivate_by_bus(net_local, table_name, bus_cols, unsupplied):
    if table_name not in net_local:
        return 0
    df = net_local[table_name]
    if df is None or df.empty or "in_service" not in df.columns:
        return 0

    mask = pd.Series(False, index=df.index)
    for col in bus_cols:
        if col not in df.columns:
            continue
        col_mask = df[col].map(normalize_bus_id).isin(unsupplied)
        mask = mask | col_mask

    if mask.any():
        net_local[table_name].loc[mask, "in_service"] = False
        return int(mask.sum())
    return 0


def main():
    net = pickle.load(open("backend/ST_CHARLES.pkl", "rb"))
    net_iter = copy.deepcopy(net)

    if "switch" in net_iter and net_iter.switch is not None and not net_iter.switch.empty:
        if "in_service" not in net_iter.switch.columns:
            net_iter.switch["in_service"] = True
        for col in ("bus", "element", "phase"):
            if col in net_iter.switch.columns:
                net_iter.switch[col] = net_iter.switch[col].map(normalize_int_value)
                if net_iter.switch[col].notna().all():
                    net_iter.switch[col] = net_iter.switch[col].astype(np.int64)
        if "closed" in net_iter.switch.columns:
            net_iter.switch["closed"] = net_iter.switch["closed"].map(normalize_bool_value)
        if "in_service" in net_iter.switch.columns:
            net_iter.switch["in_service"] = net_iter.switch["in_service"].map(normalize_bool_value)

    for table_name, df in net_iter.items():
        if not isinstance(df, pd.DataFrame) or df.empty:
            continue
        if "in_service" in df.columns and df["in_service"].dtype == object:
            net_iter[table_name]["in_service"] = df["in_service"].map(normalize_bool_value)

    if isinstance(net_iter.bus.index, pd.MultiIndex):
        nodes = sorted(net_iter.bus.index.get_level_values("index").unique())
    else:
        nodes = net_iter.bus.index.tolist()

    for bus in nodes:
        if isinstance(net_iter.bus.index, pd.MultiIndex):
            avail_phases = net_iter.bus.xs(bus, level=0).index.tolist()
            active_phases = [p for p in avail_phases if p in (1, 2, 3)]
        else:
            active_phases = [1, 2, 3]
        if not active_phases:
            continue

        p_val = tuple([0.0] * len(active_phases))
        create_asymmetric_sgen(
            net_iter,
            bus=bus,
            from_phase=tuple(active_phases),
            to_phase=0,
            p_mw=p_val,
            q_mvar=p_val,
            name=f"ica_dummy_bus{bus}",
        )

    print("--- BASE ---")
    print("bus rows:", len(net_iter.bus))
    print("line rows:", len(net_iter.line))
    print("switch rows:", len(net_iter.switch))

    net_flat = _flatten_net_for_topology(net_iter)
    slack_buses = set()
    if "ext_grid" in net_iter and not net_iter.ext_grid.empty:
        slack_buses.update(
            normalize_bus_id(b)
            for b in net_iter.ext_grid.get("bus", pd.Series(dtype=object)).tolist()
        )
    if (
        "ext_grid_sequence" in net_iter
        and getattr(net_iter, "ext_grid_sequence", None) is not None
        and not net_iter.ext_grid_sequence.empty
    ):
        slack_buses.update(
            normalize_bus_id(b)
            for b in net_iter.ext_grid_sequence.get("bus", pd.Series(dtype=object)).tolist()
        )
    slack_buses = {b for b in slack_buses if b is not None}

    unsupplied = pp.topology.unsupplied_buses(
        net_flat, slacks=slack_buses, respect_switches=True
    )
    unsupplied = {
        normalize_bus_id(b) for b in unsupplied if normalize_bus_id(b) is not None
    }
    print("unsupplied bus count:", len(unsupplied))

    if isinstance(net_iter.bus.index, pd.MultiIndex):
        bus_idx = net_iter.bus.index.get_level_values(0)
        bus_mask = bus_idx.map(normalize_bus_id).isin(unsupplied)
        net_iter.bus.loc[bus_mask, "in_service"] = False

    deactivated = {}
    for table_name, cols in [
        ("line", ["from_bus", "to_bus"]),
        ("trafo", ["hv_bus", "lv_bus"]),
        ("trafo3w", ["hv_bus", "mv_bus", "lv_bus"]),
        ("impedance", ["from_bus", "to_bus"]),
        ("switch", ["bus"]),
        ("load", ["bus"]),
        ("asymmetric_load", ["bus"]),
        ("sgen", ["bus"]),
        ("asymmetric_sgen", ["bus"]),
        ("gen", ["bus"]),
        ("shunt", ["bus"]),
        ("ward", ["bus"]),
        ("xward", ["bus"]),
        ("storage", ["bus"]),
        ("motor", ["bus"]),
        ("ext_grid", ["bus"]),
        ("ext_grid_sequence", ["bus"]),
    ]:
        deactivated[table_name] = deactivate_by_bus(
            net_iter, table_name, cols, unsupplied
        )

    print("--- AFTER UNSUPPLIED DEACTIVATION ---")
    print("active bus rows:", int(net_iter.bus["in_service"].sum()))
    print("active line rows:", int(net_iter.line["in_service"].sum()))
    if "in_service" in net_iter.switch.columns:
        print("active switch rows:", int(net_iter.switch["in_service"].sum()))
    else:
        print("active switch rows:", len(net_iter.switch))
    print("deactivated rows by table:", {k: v for k, v in deactivated.items() if v > 0})

    bus_is = net_iter.bus[net_iter.bus["in_service"] == True].copy()
    line_all = net_iter.line.copy()
    line_is = net_iter.line[net_iter.line["in_service"] == True].copy()
    sw_all = net_iter.switch.copy()
    sw_is = net_iter.switch[net_iter.switch["in_service"] == True].copy()

    bus_ids = set(bus_is.index.get_level_values(0).unique())
    bus_phase_active = set(bus_is.index.tolist())

    bad_lines_missing_bus_refs = line_is[
        (~line_is["from_bus"].isin(bus_ids)) | (~line_is["to_bus"].isin(bus_ids))
    ]
    print("bad_lines_missing_bus_refs:", len(bad_lines_missing_bus_refs))

    bad_switch_bus_refs = sw_is[~sw_is["bus"].isin(bus_ids)]
    print("bad_switch_bus_refs:", len(bad_switch_bus_refs))

    sw_b = sw_is[sw_is["et"] == "b"]
    bad_switch_element_refs = sw_b[~sw_b["element"].isin(bus_ids)]
    print("bad_switch_element_refs:", len(bad_switch_element_refs))

    sw_b_closed = sw_b[sw_b["closed"] == True] if "closed" in sw_b.columns else sw_b
    bad_switch_element_refs_closed = sw_b_closed[
        ~sw_b_closed["element"].isin(bus_ids)
    ]
    print(
        "bad_switch_element_refs_closed_only:",
        len(bad_switch_element_refs_closed),
    )

    bad_line_phase_rows = line_is[
        (~line_is["from_phase"].isin([0, 1, 2, 3]))
        | (~line_is["to_phase"].isin([0, 1, 2, 3]))
    ]
    print("bad_line_phase_rows:", len(bad_line_phase_rows))

    bad_switch_phase_rows = sw_is[~sw_is["phase"].isin([0, 1, 2, 3])]
    print("bad_switch_phase_rows:", len(bad_switch_phase_rows))

    line_bad_terminal = []
    for idx, fb, fp, tb, tp in zip(
        line_is.index.tolist(),
        line_is["from_bus"].tolist(),
        line_is["from_phase"].tolist(),
        line_is["to_bus"].tolist(),
        line_is["to_phase"].tolist(),
    ):
        if (fb, fp) not in bus_phase_active or (tb, tp) not in bus_phase_active:
            line_bad_terminal.append((idx, fb, fp, tb, tp))

    print("line_rows_with_missing_bus_phase_terminal:", len(line_bad_terminal))
    if line_bad_terminal:
        print("sample line bad terminals:")
        for row in line_bad_terminal[:30]:
            print(" ", row)

    line_bad_terminal_all = []
    for idx, fb, fp, tb, tp, _is in zip(
        line_all.index.tolist(),
        line_all["from_bus"].tolist(),
        line_all["from_phase"].tolist(),
        line_all["to_bus"].tolist(),
        line_all["to_phase"].tolist(),
        line_all["in_service"].tolist(),
    ):
        if (fb, fp) not in bus_phase_active or (tb, tp) not in bus_phase_active:
            line_bad_terminal_all.append((idx, fb, fp, tb, tp, bool(_is)))

    print(
        "line_rows_with_missing_bus_phase_terminal_all_rows:",
        len(line_bad_terminal_all),
    )
    if line_bad_terminal_all:
        print("sample line bad terminals (all rows):")
        for row in line_bad_terminal_all[:30]:
            print(" ", row)

    sw_bad_terminal = []
    for idx, b, p, e, et in zip(
        sw_is.index.tolist(),
        sw_is["bus"].tolist(),
        sw_is["phase"].tolist(),
        sw_is["element"].tolist(),
        sw_is["et"].tolist(),
    ):
        if (b, p) not in bus_phase_active:
            sw_bad_terminal.append((idx, b, p, e, et, "bus_phase_missing"))
        if et == "b" and (e, p) not in bus_phase_active:
            sw_bad_terminal.append((idx, b, p, e, et, "element_phase_missing"))

    print("switch_rows_with_missing_bus_phase_terminal:", len(sw_bad_terminal))
    if sw_bad_terminal:
        print("sample switch bad terminals:")
        for row in sw_bad_terminal[:30]:
            print(" ", row)

    sw_bad_terminal_closed = []
    sw_closed = sw_is[sw_is["closed"] == True] if "closed" in sw_is.columns else sw_is
    for idx, b, p, e, et in zip(
        sw_closed.index.tolist(),
        sw_closed["bus"].tolist(),
        sw_closed["phase"].tolist(),
        sw_closed["element"].tolist(),
        sw_closed["et"].tolist(),
    ):
        if (b, p) not in bus_phase_active:
            sw_bad_terminal_closed.append((idx, b, p, e, et, "bus_phase_missing"))
        if et == "b" and (e, p) not in bus_phase_active:
            sw_bad_terminal_closed.append(
                (idx, b, p, e, et, "element_phase_missing")
            )
    print(
        "switch_rows_with_missing_bus_phase_terminal_closed_only:",
        len(sw_bad_terminal_closed),
    )

    sw_bad_terminal_closed_all_rows = []
    sw_closed_all = (
        sw_all[sw_all["closed"] == True] if "closed" in sw_all.columns else sw_all
    )
    for idx, b, p, e, et in zip(
        sw_closed_all.index.tolist(),
        sw_closed_all["bus"].tolist(),
        sw_closed_all["phase"].tolist(),
        sw_closed_all["element"].tolist(),
        sw_closed_all["et"].tolist(),
    ):
        if (b, p) not in bus_phase_active:
            sw_bad_terminal_closed_all_rows.append(
                (idx, b, p, e, et, "bus_phase_missing")
            )
        if et == "b" and (e, p) not in bus_phase_active:
            sw_bad_terminal_closed_all_rows.append(
                (idx, b, p, e, et, "element_phase_missing")
            )
    print(
        "switch_rows_with_missing_bus_phase_terminal_closed_all_rows:",
        len(sw_bad_terminal_closed_all_rows),
    )
    if sw_bad_terminal_closed_all_rows:
        print("sample closed-switch bad terminals (all rows):")
        for row in sw_bad_terminal_closed_all_rows[:30]:
            print(" ", row)

    def _check_element_terminal_table(table_name):
        if table_name not in net_iter:
            print(f"{table_name}_rows_with_missing_bus_phase_terminal: 0")
            return
        df = net_iter[table_name]
        if df is None or df.empty:
            print(f"{table_name}_rows_with_missing_bus_phase_terminal: 0")
            return
        if "in_service" not in df.columns:
            df = df.copy()
            df["in_service"] = True

        bad_rows = []
        for idx, bus, fp, tp, _is in zip(
            df.index.tolist(),
            df["bus"].tolist() if "bus" in df.columns else [None] * len(df),
            df["from_phase"].tolist()
            if "from_phase" in df.columns
            else [None] * len(df),
            df["to_phase"].tolist() if "to_phase" in df.columns else [None] * len(df),
            df["in_service"].tolist(),
        ):
            if not bool(_is):
                continue
            if (bus, fp) not in bus_phase_active or (bus, tp) not in bus_phase_active:
                bad_rows.append((idx, bus, fp, tp))

        print(f"{table_name}_rows_with_missing_bus_phase_terminal:", len(bad_rows))
        if bad_rows:
            print(f"sample {table_name} bad terminals:")
            for row in bad_rows[:30]:
                print(" ", row)

    _check_element_terminal_table("asymmetric_load")
    _check_element_terminal_table("asymmetric_sgen")
    _check_element_terminal_table("asymmetric_shunt")

    ext_tab = net_iter["ext_grid"] if len(net_iter["ext_grid"]) else net_iter["ext_grid_sequence"]
    ext_with_phase = ext_tab.copy()
    ext_with_phase["_phase"] = ext_with_phase["from_phase"]
    ext_with_phase["_bus_phase_active"] = ext_with_phase.apply(
        lambda row: (int(row["bus"]), int(row["_phase"])) in bus_phase_active, axis=1
    )
    if "in_service" in ext_with_phase.columns:
        ext_with_phase["_active_ext"] = ext_with_phase["in_service"].astype(bool)
    else:
        ext_with_phase["_active_ext"] = True
    bad_ext = ext_with_phase[
        (ext_with_phase["_active_ext"] == True)
        & (ext_with_phase["_bus_phase_active"] == False)
    ]
    print("active_ext_grid_rows_with_missing_bus_phase_terminal:", len(bad_ext))
    if len(bad_ext):
        print(
            bad_ext[["bus", "from_phase", "in_service"]].head(20).to_string()
            if "in_service" in bad_ext.columns
            else bad_ext[["bus", "from_phase"]].head(20).to_string()
        )

    if line_bad_terminal:
        bad_index = [r[0] for r in line_bad_terminal[:20]]
        print("\nline sample details:")
        print(
            line_is.loc[
                bad_index,
                [
                    "from_bus",
                    "from_phase",
                    "to_bus",
                    "to_phase",
                    "in_service",
                    "name",
                ],
            ].to_string()
        )

    if sw_bad_terminal:
        bad_sw_idx = [r[0] for r in sw_bad_terminal[:20]]
        print("\nswitch sample details:")
        print(
            sw_is.loc[
                bad_sw_idx,
                ["bus", "element", "phase", "et", "closed", "in_service", "name"],
            ].to_string()
        )


if __name__ == "__main__":
    main()
