import pandas as pd
import numpy as np

PACKAGE_DEPENDENCIES = (
    "snowflake-snowpark-python",
    "geojson",
    "deepdiff",
    "lxml",
    "numba",
    "typing_extensions==4.15.0",
    "numpy==1.26.4",
    "packaging==25.0",
    "tqdm==4.67.1",
    "pandas==2.3.2",
    "networkx==3.4.2",
    "scipy==1.15.3",
    "Jinja2",
)

PHASE_MAP = {1: "A", 2: "B", 3: "C"}


def _phase_series(df):
    if "from_phase" in df.columns:
        return df["from_phase"]
    if isinstance(df.index, pd.MultiIndex):
        if "from_phase" in df.index.names:
            return df.index.get_level_values("from_phase")
        return df.index.get_level_values(-1)
    return pd.Series(dtype=int)


def _sum_by_phase(df, value_col):
    if value_col not in df.columns:
        return {"A": np.nan, "B": np.nan, "C": np.nan}
    phases = _phase_series(df)
    if phases is None or len(phases) == 0:
        return {"A": np.nan, "B": np.nan, "C": np.nan}
    phase_vals = pd.to_numeric(phases, errors="coerce")
    if phase_vals.notna().any() and phase_vals.min() == 0 and phase_vals.max() <= 2:
        phase_vals = phase_vals + 1
    temp = pd.DataFrame(
        {
            "phase": phase_vals,
            "value": pd.to_numeric(df[value_col], errors="coerce"),
        }
    )
    grouped = temp.groupby("phase")["value"].sum()
    return {
        "A": float(grouped.get(1, np.nan)),
        "B": float(grouped.get(2, np.nan)),
        "C": float(grouped.get(3, np.nan)),
    }


def _imbalance_metrics(sums):
    vals = [v for v in sums.values() if pd.notna(v)]
    if len(vals) == 0:
        return {"avg": np.nan, "max_dev": np.nan, "imbalance_pct": np.nan}
    avg = float(np.mean(vals))
    max_dev = float(np.max([abs(v - avg) for v in vals]))
    imbalance_pct = float(max_dev / avg) if avg != 0 else np.nan
    return {"avg": avg, "max_dev": max_dev, "imbalance_pct": imbalance_pct}


def get_loading_by_phase(new_net):
    if (
        hasattr(new_net, "asymmetric_load")
        and new_net.asymmetric_load is not None
        and len(new_net.asymmetric_load) > 0
    ):
        load_df = new_net.asymmetric_load
        p_sums = _sum_by_phase(load_df, "p_mw")
        q_sums = _sum_by_phase(load_df, "q_mvar")

        p_imb = _imbalance_metrics(p_sums)
        q_imb = _imbalance_metrics(q_sums)

        p_total = float(np.nansum([p_sums["A"], p_sums["B"], p_sums["C"]]))
        q_total = float(np.nansum([q_sums["A"], q_sums["B"], q_sums["C"]]))
        summary = {
            "P_MW_TOTAL": p_total,
            "Q_MW_TOTAL": q_total,
            "P_A_MW": p_sums["A"],
            "P_B_MW": p_sums["B"],
            "P_C_MW": p_sums["C"],
            "Q_A_MVAR": q_sums["A"],
            "Q_B_MVAR": q_sums["B"],
            "Q_C_MVAR": q_sums["C"],
            "P_avg_MW": p_imb["avg"],
            "P_max_dev_MW": p_imb["max_dev"],
            "P_imbalance_pct": p_imb["imbalance_pct"],
            "Q_avg_MVAR": q_imb["avg"],
            "Q_max_dev_MVAR": q_imb["max_dev"],
            "Q_imbalance_pct": q_imb["imbalance_pct"],
        }
        imbalance_df = pd.DataFrame([summary])
        return imbalance_df
    else:
        print("No asymmetric_load data found for phase sums/imbalance.")


def get_asymmetric_load_by_phase(
    new_net, group_cols=None, value_cols=("p_mw", "q_mvar")
):
    """
    Return asymmetric_load in wide format with one column per phase.
    Example output columns: p_mw_A, p_mw_B, p_mw_C, q_mvar_A, q_mvar_B, q_mvar_C
    """
    if (
        not hasattr(new_net, "asymmetric_load")
        or new_net.asymmetric_load is None
        or len(new_net.asymmetric_load) == 0
    ):
        print("No asymmetric_load data found.")
        return pd.DataFrame()

    load_df = new_net.asymmetric_load.copy()

    phase_vals = pd.to_numeric(_phase_series(load_df), errors="coerce")
    if phase_vals.notna().any() and phase_vals.min() == 0 and phase_vals.max() <= 2:
        phase_vals = phase_vals + 1

    phase_labels = phase_vals.map(PHASE_MAP)
    load_df["phase"] = phase_labels

    if group_cols is None:
        preferred = ["bus", "name", "type", "in_service"]
        group_cols = [col for col in preferred if col in load_df.columns]
        if not group_cols:
            load_df = load_df.reset_index().rename(columns={"index": "load_idx"})
            group_cols = ["load_idx"]
    else:
        missing = [col for col in group_cols if col not in load_df.columns]
        if missing:
            raise ValueError(f"Missing grouping columns in asymmetric_load: {missing}")

    valid_value_cols = [col for col in value_cols if col in load_df.columns]
    if not valid_value_cols:
        raise ValueError(f"None of value_cols found in asymmetric_load: {value_cols}")

    base = load_df[group_cols + ["phase"] + valid_value_cols].copy()
    base = base.dropna(subset=["phase"])

    pivot = base.pivot_table(
        index=group_cols, columns="phase", values=valid_value_cols, aggfunc="sum"
    )

    pivot.columns = [f"{val}_{phase}" for val, phase in pivot.columns]

    for val_col in valid_value_cols:
        for phase in ("A", "B", "C"):
            col_name = f"{val_col}_{phase}"
            if col_name not in pivot.columns:
                pivot[col_name] = np.nan

    ordered_cols = []
    for val_col in valid_value_cols:
        ordered_cols.extend([f"{val_col}_A", f"{val_col}_B", f"{val_col}_C"])

    return pivot[ordered_cols].reset_index()
