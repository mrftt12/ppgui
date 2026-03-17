"""Multiconductor Network Diagnostics Suite.

Usage::

    from multiconductor.tools.diagnostics import run_diagnostics, run_auto_fix

    result = run_diagnostics(net)
    result.issues          # DataFrame of flagged issues
    result.recommendations # prioritized corrective actions
    result.summary         # count by severity

    # Auto-fix with rollback for any change that worsens the network:
    report = run_auto_fix(net)
    report.summary         # DataFrame of all fix attempts
    report.applied         # fixes that improved or maintained the network
    report.rejected        # fixes that were rolled back
"""
from __future__ import annotations

from typing import Any, Sequence

import pandas as pd

from multiconductor.tools.network_validators import ValidationResult

from ._diag_voltage_base import check_voltage_base
from ._diag_transformer import check_transformers
from ._diag_grounding import check_grounding
from ._diag_phase import check_phase_connectivity
from ._diag_impedance import check_impedance
from ._diag_open_conductor import check_open_conductor
from ._diag_load_model import check_load_model
from ._diag_controls import check_controls
from ._diag_duplicates import check_duplicates
from ._diag_topology import check_topology
from ._auto_fix import run_auto_fix, AutoFixReport, FixAttempt


# ---------------------------------------------------------------------------
# Category registry
# ---------------------------------------------------------------------------

_ALL_CATEGORIES: dict[str, Any] = {
    "voltage_base": check_voltage_base,
    "transformer": check_transformers,
    "grounding": check_grounding,
    "phase": check_phase_connectivity,
    "impedance": check_impedance,
    "open_conductor": check_open_conductor,
    "load_model": check_load_model,
    "controls": check_controls,
    "duplicates": check_duplicates,
    "topology": check_topology,
}

_FAST_CATEGORIES = {"voltage_base", "duplicates", "impedance", "load_model"}

_SEVERITY_ORDER = {"critical": 1, "high": 2, "medium": 3, "low": 4, "info": 5}

_ISSUE_COLUMNS = [
    "severity", "check", "element_type", "element_index",
    "field", "message", "suggestion",
]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_diagnostics(
    net: Any,
    *,
    categories: str | Sequence[str] = "all",
    severity_threshold: str = "info",
    fast_only: bool = False,
) -> ValidationResult:
    """Run diagnostic checks on a multiconductor network.

    Parameters
    ----------
    net : pandapowerNet
        The multiconductor network object.
    categories : str or list of str
        ``"all"`` to run every category, or a list of category names
        (e.g. ``["voltage_base", "impedance", "topology"]``).
    severity_threshold : str
        Lowest severity to include: ``"critical"``, ``"high"``,
        ``"medium"``, ``"low"``, or ``"info"``.
    fast_only : bool
        If True, only run fast Phase-1 checks (duplicates, voltage_base
        scalars, impedance ranges, load model basics).

    Returns
    -------
    ValidationResult
        Same dataclass as ``network_validators.py`` with ``.issues``,
        ``.recommendations``, and ``.summary`` DataFrames.
    """
    # Determine which categories to run
    if categories == "all":
        selected = set(_ALL_CATEGORIES)
    else:
        selected = set(categories)

    if fast_only:
        selected = selected.intersection(_FAST_CATEGORIES)

    # Run checks
    all_issues: list[dict] = []
    for name in _ALL_CATEGORIES:
        if name not in selected:
            continue
        check_fn = _ALL_CATEGORIES[name]
        # Pass fast_only-related kwargs where supported
        kwargs: dict[str, Any] = {}
        if name == "voltage_base" and fast_only:
            kwargs["include_bfs"] = False
        elif name == "impedance" and fast_only:
            kwargs["include_matrix_checks"] = False
        elif name == "load_model" and fast_only:
            kwargs["include_capacity_checks"] = False

        try:
            issues = check_fn(net, **kwargs)
            all_issues.extend(issues)
        except Exception as exc:
            all_issues.append({
                "severity": "high",
                "check": f"{name}_error",
                "element_type": "diagnostics",
                "element_index": name,
                "field": "exception",
                "message": f"Diagnostic category '{name}' raised: {exc}",
                "suggestion": "Investigate the error; the network data may be malformed.",
            })

    # Build result
    issues_df = pd.DataFrame(all_issues, columns=_ISSUE_COLUMNS)

    # Filter by severity threshold
    threshold_rank = _SEVERITY_ORDER.get(severity_threshold, 5)
    if not issues_df.empty:
        issues_df["_rank"] = issues_df["severity"].map(_SEVERITY_ORDER).fillna(99)
        issues_df = issues_df[issues_df["_rank"] <= threshold_rank].drop(columns=["_rank"])

    if issues_df.empty:
        return ValidationResult(
            issues=issues_df,
            recommendations=pd.DataFrame(columns=["priority", "issue_type", "recommendation"]),
            summary=pd.DataFrame([{"severity": "info", "count": 0}]),
        )

    # Recommendations
    rec_df = _build_recommendations(issues_df)
    # Summary
    summary_df = issues_df.groupby("severity", dropna=False).size().reset_index(name="count")
    summary_df["_rank"] = summary_df["severity"].map(_SEVERITY_ORDER).fillna(99)
    summary_df = summary_df.sort_values("_rank").drop(columns=["_rank"]).reset_index(drop=True)

    return ValidationResult(issues=issues_df, recommendations=rec_df, summary=summary_df)


# ---------------------------------------------------------------------------
# Recommendation builder
# ---------------------------------------------------------------------------

_ACTION_MAP = {
    "voltage_base": "Verify and correct bus nominal voltages (vn_kv) and source voltage settings.",
    "transformer_model": "Fix transformer impedance, turns ratio, or tap settings per nameplate data.",
    "grounding": "Ensure neutral grounding paths are continuous and substation ground is defined.",
    "phase_connectivity": "Correct phase assignments and verify per-phase reachability from sources.",
    "impedance_data": "Verify conductor impedance data against standard libraries and check units.",
    "open_conductor": "Investigate broken conductor conditions and restore phase continuity.",
    "load_model": "Correct load power factor, voltage level, and balance across phases.",
    "control_error": "Fix control element references, setpoints, and switching thresholds.",
    "duplicate_equipment": "Remove or merge duplicate elements and resolve contradictory data.",
    "topology": "Repair network connectivity, remove unintended loops, and verify source paths.",
}


def _build_recommendations(issues_df: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        issues_df.groupby(["check", "severity"], dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values(
            by=["severity", "count"],
            key=lambda s: s.map(_SEVERITY_ORDER).fillna(99) if s.name == "severity" else s,
            ascending=[True, False],
        )
    )
    rows: list[dict] = []
    for _, row in grouped.iterrows():
        issue_type = str(row["check"])
        rows.append({
            "priority": str(row["severity"]),
            "issue_type": issue_type,
            "recommendation": _ACTION_MAP.get(issue_type, "Investigate and resolve reported issues."),
        })
    return pd.DataFrame(rows)
