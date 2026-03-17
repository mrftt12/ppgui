"""Pointers to the existing CYME runtime control scripts.

The actual CYME Python-script controls in this repository already live under
``multiconductor/cyme`` because they are authored as CYME-executed scripts,
not normal importable Python modules. Re-exporting them here avoids duplicate
copies while giving the new ``cyme`` package the same top-level structure as
``opendss``.
"""

from __future__ import annotations

from pathlib import Path


_SCRIPT_DIR = Path(__file__).resolve().parents[2] / "multiconductor" / "cyme"

AVAILABLE_CONTROL_SCRIPTS = {
    "bess_control": _SCRIPT_DIR / "bess_control.py",
    "cap_control": _SCRIPT_DIR / "cap_control.py",
    "cap_controller": _SCRIPT_DIR / "cap_controller.py",
    "line_drop_control": _SCRIPT_DIR / "line_drop_control.py",
    "load_tap_changer": _SCRIPT_DIR / "load_tap_changer.py",
    "regulator": _SCRIPT_DIR / "regulator.py",
    "shunt_controller": _SCRIPT_DIR / "shunt_controller.py",
    "single_phase_regulator": _SCRIPT_DIR / "single_phase_regulator.py",
    "volt_var": _SCRIPT_DIR / "volt_var.py",
    "volt_var_control": _SCRIPT_DIR / "volt_var_control.py",
}


def get_control_script_path(name: str) -> Path:
    try:
        return AVAILABLE_CONTROL_SCRIPTS[name]
    except KeyError as exc:
        raise KeyError(
            f"Unknown CYME control script '{name}'. Available: {sorted(AVAILABLE_CONTROL_SCRIPTS)}"
        ) from exc
