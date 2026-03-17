"""
SCE Power Flow Analysis Package

This package provides data quality tools for multiconductor power flow analysis:

- newton_rules_engine: Field mapping and validation rules
- nr_iteration_scrubber: Data cleanup functions for convergence
- wrapper: Unified power flow interface with auto-fix

Usage:
    from sce.wrapper import run_pf
    from sce.newton_rules_engine import get_pf_rule_mismatches, new_pf_field_mapping
    from sce import nr_iteration_scrubber as scrubber

Copyright 2026, iTron.
Authors: Frank M Gonzales, Ajith Joseph
"""

"""SCE package exports."""

from importlib import import_module
from pkgutil import iter_modules
from pathlib import Path

_pkg_dir = Path(__file__).resolve().parent

# Discover all modules/packages under sce, excluding private names.
_module_names = sorted(
    m.name for m in iter_modules([str(_pkg_dir)])
    if not m.name.startswith("_")
)

# Import each discovered module into package namespace:
#   import sce; sce.load_allocation, sce.some_other_module, ...
for _name in _module_names:
    globals()[_name] = import_module(f".{_name}", __name__)

# Export all discovered modules.
__all__ = _module_names

# Cleanup internals
del import_module, iter_modules, Path, _pkg_dir, _module_names, _name