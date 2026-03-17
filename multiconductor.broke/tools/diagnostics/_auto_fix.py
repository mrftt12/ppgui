"""Auto-fix framework for multiconductor network diagnostics.

Core pattern: deep-copy → apply fix → re-diagnose → keep or rollback.

Usage::

    from multiconductor.tools.diagnostics import run_auto_fix

    report = run_auto_fix(net)
    # report.applied   → list of fixes that improved the network
    # report.rejected  → list of fixes that were rolled back
    # report.net       → the improved network (original if nothing helped)
"""
from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any, Callable

import pandas as pd


def _run_diagnostics(net, **kwargs):
    """Lazy import to avoid circular dependency with __init__.py."""
    from multiconductor.tools.diagnostics import run_diagnostics
    return run_diagnostics(net, **kwargs)


def _check_pf_stable(net) -> tuple[bool, float]:
    """Run power flow and return (stable, mean_vm_pu).

    Instead of relying on net.converged (which may not be set), we check
    whether res_bus.vm_pu values are finite and within a sane range
    [0.5, 1.5] pu — i.e. the solver produced a physically reasonable result.
    """
    try:
        import numpy as np
        from multiconductor.pycci.cci_powerflow import run_pf
        run_pf(net, run_control=False, MaxIter=100)
        res_bus = getattr(net, "res_bus", None)
        if res_bus is None or res_bus.empty or "vm_pu" not in res_bus.columns:
            return False, float("nan")
        vm = res_bus["vm_pu"].astype(float)
        vm_finite = vm[np.isfinite(vm)]
        if vm_finite.empty:
            return False, float("nan")
        mean_vm = float(vm_finite.mean())
        # Stable if mean voltage is in a reasonable band and no NaN/inf dominate
        stable = (0.5 <= mean_vm <= 1.5) and (len(vm_finite) > 0.9 * len(vm))
        return stable, mean_vm
    except Exception:
        return False, float("nan")


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class FixAttempt:
    """Record of a single fix attempt."""
    fix_id: str
    description: str
    category: str
    issues_before: int
    issues_after: int
    applied: bool
    detail: str = ""


@dataclass
class AutoFixReport:
    """Summary of all fix attempts."""
    net: Any
    applied: list[FixAttempt] = field(default_factory=list)
    rejected: list[FixAttempt] = field(default_factory=list)
    issues_before_total: int = 0
    issues_after_total: int = 0

    @property
    def summary(self) -> pd.DataFrame:
        rows = []
        for fa in self.applied + self.rejected:
            rows.append({
                "fix_id": fa.fix_id,
                "category": fa.category,
                "description": fa.description,
                "issues_before": fa.issues_before,
                "issues_after": fa.issues_after,
                "delta": fa.issues_after - fa.issues_before,
                "applied": fa.applied,
                "detail": fa.detail,
            })
        return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Core try-fix engine
# ---------------------------------------------------------------------------

def try_fix(
    net: Any,
    fix_fn: Callable[[Any], str],
    fix_id: str,
    description: str,
    category: str,
    *,
    scope_categories: str | list[str] = "all",
    severity_threshold: str = "info",
    check_pf: bool = False,
) -> tuple[Any, FixAttempt]:
    """Apply a fix function and verify it doesn't worsen the network.

    Parameters
    ----------
    net : pandapowerNet
        Current network state (will NOT be mutated on failure).
    fix_fn : callable(net) -> str
        Mutates *net* in place and returns a detail string.
        If it raises, the fix is rejected.
    fix_id : str
        Short identifier for this fix.
    description : str
        Human-readable description of what the fix does.
    category : str
        Diagnostic category name for scoped re-check.
    scope_categories : str or list
        Which diagnostic categories to evaluate for before/after comparison.
        Defaults to "all" for a global check.
    severity_threshold : str
        Minimum severity to count when comparing before/after.
    check_pf : bool
        If True, run power flow after the fix and rollback if it doesn't converge.

    Returns
    -------
    (net, FixAttempt)
        If the fix improves or maintains the network, returns the modified
        net.  Otherwise returns the original (deep-copied-back) net.
    """
    # Snapshot before
    before = _run_diagnostics(
        net, categories=scope_categories, severity_threshold=severity_threshold,
    )
    n_before = len(before.issues)

    # Deep-copy so we can rollback
    snapshot = copy.deepcopy(net)

    # Apply fix
    try:
        detail = fix_fn(net)
    except Exception as exc:
        # Rollback
        _restore(net, snapshot)
        return net, FixAttempt(
            fix_id=fix_id, description=description, category=category,
            issues_before=n_before, issues_after=n_before,
            applied=False, detail=f"Exception: {exc}",
        )

    # Measure after
    after = _run_diagnostics(
        net, categories=scope_categories, severity_threshold=severity_threshold,
    )
    n_after = len(after.issues)

    if n_after > n_before:
        # Worsened — rollback
        _restore(net, snapshot)
        return net, FixAttempt(
            fix_id=fix_id, description=description, category=category,
            issues_before=n_before, issues_after=n_after,
            applied=False, detail=f"Rolled back: issues went from {n_before} → {n_after}",
        )

    # Power-flow gate: run PF before and after, compare mean vm_pu
    if check_pf:
        # Run PF on the snapshot (pre-fix state) to get baseline
        baseline_snapshot = copy.deepcopy(snapshot)
        stable_before, vm_before = _check_pf_stable(baseline_snapshot)

        # Run PF on the fixed network
        stable_after, vm_after = _check_pf_stable(net)

        if not stable_after:
            _restore(net, snapshot)
            return net, FixAttempt(
                fix_id=fix_id, description=description, category=category,
                issues_before=n_before, issues_after=n_after,
                applied=False,
                detail=f"Rolled back: PF unstable after fix (mean vm_pu={vm_after:.4f})",
            )

        # If baseline was stable, reject if mean vm_pu shifted by more than 5%
        if stable_before and vm_before > 0:
            drift = abs(vm_after - vm_before) / vm_before
            if drift > 0.05:
                _restore(net, snapshot)
                return net, FixAttempt(
                    fix_id=fix_id, description=description, category=category,
                    issues_before=n_before, issues_after=n_after,
                    applied=False,
                    detail=f"Rolled back: mean vm_pu drifted {drift*100:.1f}% "
                           f"({vm_before:.4f} → {vm_after:.4f})",
                )

    # Improved or neutral — keep
    return net, FixAttempt(
        fix_id=fix_id, description=description, category=category,
        issues_before=n_before, issues_after=n_after,
        applied=True, detail=detail or f"Issues: {n_before} → {n_after}",
    )


def _restore(net: Any, snapshot: Any) -> None:
    """Restore net in-place from a snapshot (deep copy all DataFrame attrs)."""
    for attr in dir(snapshot):
        if attr.startswith("_"):
            continue
        val = getattr(snapshot, attr, None)
        if isinstance(val, pd.DataFrame):
            setattr(net, attr, val)
        elif isinstance(val, dict) and attr not in ("dtype",):
            try:
                setattr(net, attr, copy.deepcopy(val))
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Master entry point
# ---------------------------------------------------------------------------

def run_auto_fix(
    net: Any,
    *,
    severity_threshold: str = "info",
    dry_run: bool = False,
    fix_ids: list[str] | None = None,
    check_pf: bool = False,
) -> AutoFixReport:
    """Apply every registered fix strategy with automatic rollback.

    Parameters
    ----------
    net : pandapowerNet
        The network to repair.  Modified **in-place** for accepted fixes.
    severity_threshold : str
        Minimum severity when counting issues for before/after comparison.
    dry_run : bool
        If True, don't actually keep any fix — just report what would happen.
    fix_ids : list[str] or None
        If given, only attempt fixes whose ``fix_id`` is in this list.
        Otherwise all registered fixes are attempted in order.
    check_pf : bool
        If True, run power flow after each fix and rollback any fix that
        causes the power flow to fail to converge.

    Returns
    -------
    AutoFixReport
        Summary of applied and rejected fixes, plus the resulting network.
    """
    from ._fix_strategies import FIX_REGISTRY

    # Baseline issue count
    baseline = _run_diagnostics(net, severity_threshold=severity_threshold)
    report = AutoFixReport(net=net, issues_before_total=len(baseline.issues))

    for fix_id, description, fix_fn, scope in FIX_REGISTRY:
        if fix_ids is not None and fix_id not in fix_ids:
            continue

        if dry_run:
            # For dry-run, work on a copy so the original net is never mutated
            trial = copy.deepcopy(net)
            trial, attempt = try_fix(
                trial,
                fix_fn,
                fix_id=fix_id,
                description=description,
                category=fix_id.split("_")[0],
                scope_categories=scope,
                severity_threshold=severity_threshold,
                check_pf=check_pf,
            )
        else:
            net, attempt = try_fix(
                net,
                fix_fn,
                fix_id=fix_id,
                description=description,
                category=fix_id.split("_")[0],
                scope_categories=scope,
                severity_threshold=severity_threshold,
                check_pf=check_pf,
            )

        if attempt.applied:
            report.applied.append(attempt)
        else:
            report.rejected.append(attempt)

    # Final issue count
    final = _run_diagnostics(net, severity_threshold=severity_threshold)
    report.issues_after_total = len(final.issues)
    report.net = net

    return report
