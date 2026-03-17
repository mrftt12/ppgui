# Multiconductor Network Diagnostics — Implementation Plan

## 1. Overview

Build a comprehensive diagnostics suite that catches the 10 most common error categories in multiconductor power system networks **before** running power flow or short-circuit studies. The suite extends the existing `network_validators.py` framework and adds deeper, physics-aware checks.

### Design Principles

| Principle | Description |
|---|---|
| **Same contract** | Every check returns `list[dict]` with keys `severity`, `check`, `element_type`, `element_index`, `field`, `message`, `suggestion` — identical to existing validators. |
| **Composable** | Each category lives in its own module (`_diag_*.py`) but is callable individually or via a master `run_diagnostics(net)` entry point. |
| **Non-destructive** | Read-only analysis — never modifies `net`. |
| **Progressive** | Fast structural checks first; expensive numerical / cross-simulation checks opt-in. |

### Integration Point

```
multiconductor/tools/
    network_validators.py          ← existing (6 checks, ~830 lines)
    diagnostics/
        __init__.py                ← run_diagnostics() entry point
        _diag_voltage_base.py      ← Category 1
        _diag_transformer.py       ← Category 2
        _diag_grounding.py         ← Category 3
        _diag_phase.py             ← Category 4
        _diag_impedance.py         ← Category 5
        _diag_open_conductor.py    ← Category 6
        _diag_load_model.py        ← Category 7
        _diag_controls.py          ← Category 8
        _diag_duplicates.py        ← Category 9
        _diag_topology.py          ← Category 10
        _common.py                 ← shared helpers (reuse from network_validators)
```

`run_diagnostics()` returns the same `ValidationResult(issues, recommendations, summary)` dataclass already defined in `network_validators.py`.

---

## 2. Gap Analysis — Existing vs. Needed Coverage

| # | Category | Existing Coverage in `network_validators.py` | Gaps / New Checks Needed |
|---|---|---|---|
| 1 | Wrong voltage base | `_check_bus_and_element_voltage_consistency` — flags line endpoints with >15 % vn_kv mismatch | Per-bus base-kV vs. upstream transformer secondary; ext_grid vm_pu / va_degree vs. source bus; detect kV-level unit confusion (e.g., 12.47 stored as 0.01247) |
| 2 | Transformer modeling errors | `_check_transformers_and_shunts` — vector group, tap bounds | Missing/zero impedance (vk_percent, vkr_percent); turns ratio vs. bus vn_kv mismatch; sn_mva sanity; single-phase trafo1ph winding sense (polarity); center-tap grounding for split-phase |
| 3 | Floating / weakly grounded nodes | `_check_topology_and_radiality` — islands detection | No grounding-path check; no Thevenin-impedance-to-ground estimation; no neutral conductor continuity check |
| 4 | Bad phase connectivity | `_check_phase_connectivity` — element phases ⊆ bus phases | No cross-element phase path tracing (load on phase B but upstream line only carries A,C); no neutral-phase mismatch detection |
| 5 | Incorrect impedance data | None | r/x per-unit range checks; zero-sequence consistency; mutual impedance matrix symmetry & positive definiteness; length-normalized impedance outlier detection |
| 6 | Open conductor issues | Topology check detects open switches | No per-phase open-conductor detection; no single-phasing detection; no series impedance discontinuity check |
| 7 | Load model problems | `_check_load_and_generation_values` — sign, zero, missing p/q | No power-factor range validation; no kVA vs. transformer capacity check; no load-allocation residual check; no load-to-bus voltage-level mismatch |
| 8 | Regulator / capacitor / inverter control errors | Shunt v_threshold ordering | No regulator bandwidth/setpoint sanity; no inverter reactive capability check; no control element–to–controlled-bus mapping validation |
| 9 | Duplicate / contradictory equipment | None | Duplicate element indices; overlapping line segments; parallel branches with contradictory impedances; contradictory bus definitions |
| 10 | Bad topology / connectivity | `_check_topology_and_radiality` — islands, loops; `_check_line_connectivity` — endpoint validity, self-loops | No source-reachability per phase; no downstream dead-end detection; no switch–element type mismatch |

---

## 3. Detailed Check Specifications by Category

### Category 1 — Wrong Voltage Base (`_diag_voltage_base.py`)

| Check ID | Severity | What it detects | Tables used | Logic |
|---|---|---|---|---|
| `vb_01` | critical | Bus vn_kv = 0 or NaN | `bus` | `bus.vn_kv.isna() \| bus.vn_kv <= 0` |
| `vb_02` | high | Bus vn_kv doesn't match transformer secondary | `bus`, `trafo1ph` | For each trafo1ph, get LV-side bus vn_kv and compare to trafo winding vn_kv |
| `vb_03` | high | ext_grid source voltage mismatch | `bus`, `ext_grid`, `ext_grid_sequence` | ext_grid bus vn_kv should match system nominal; vm_pu should be ≈ 1.0 ± 0.1 |
| `vb_04` | medium | Likely unit confusion (kV vs V vs MVA) | `bus` | Flag vn_kv < 0.1 (probably V stored as kV) or vn_kv > 500 (unusual for distribution) |
| `vb_05` | medium | Voltage base inconsistency along feeder path | `bus`, `line`, `trafo1ph` | BFS from source; every line should connect buses of equal vn_kv; flag transitions that don't go through a transformer |

**Existing overlap**: `_check_bus_and_element_voltage_consistency` covers line-endpoint mismatch. Keep it; these are deeper / complementary.

---

### Category 2 — Transformer Modeling Errors (`_diag_transformer.py`)

| Check ID | Severity | What it detects | Tables used | Logic |
|---|---|---|---|---|
| `tx_01` | critical | Zero or missing impedance | `trafo`, `trafo1ph` | `vk_percent == 0`, `vkr_percent is NaN` |
| `tx_02` | high | Turns ratio vs bus kV mismatch | `trafo1ph`, `bus` | Compare `vn_kv` at each winding side to the bus it's connected to; flag >10 % difference |
| `tx_03` | high | sn_mva out of range | `trafo`, `trafo1ph` | `sn_mva <= 0` or `sn_mva > 500` (distribution limit) |
| `tx_04` | medium | Tap position at extreme | `trafo`, `trafo1ph` | `tap_pos == tap_min` or `tap_pos == tap_max` |
| `tx_05` | medium | Center-tap / split-phase grounding | `trafo1ph` | For 120/240 V center-tap trafos, verify neutral bus exists and grounding conductor is present |
| `tx_06` | low | X/R ratio out of typical range | `trafo`, `trafo1ph` | `vk_percent / vkr_percent` outside [2, 50] for distribution transformers |

**Existing overlap**: `_check_transformers_and_shunts` covers vector group validation and tap bounds. Keep it.

---

### Category 3 — Floating / Weakly Grounded Nodes (`_diag_grounding.py`)

| Check ID | Severity | What it detects | Tables used | Logic |
|---|---|---|---|---|
| `gnd_01` | critical | Bus with no connected elements | `bus`, all element tables | For each bus, count connected lines + trafos + loads + gens. Flag bus with 0 connections. |
| `gnd_02` | high | No ground path from neutral | `bus`, `line`, `trafo1ph` | Trace neutral conductor (phase=0) connectivity from each bus back to a grounded source. Flag unreachable neutrals. |
| `gnd_03` | high | Substation ground reference missing | `ext_grid_sequence`, `bus` | Check that zero-sequence source bus has a grounding impedance defined |
| `gnd_04` | medium | Degree-1 buses (dead-ends) that aren't loads/gens | `bus`, `line`, `trafo1ph`, `asymmetric_load`, `asymmetric_sgen` | Bus with exactly 1 branch connection and no load/gen/source attached |

---

### Category 4 — Bad Phase Connectivity (`_diag_phase.py`)

| Check ID | Severity | What it detects | Tables used | Logic |
|---|---|---|---|---|
| `ph_01` | critical | Load on phase not served by upstream line | `asymmetric_load`, `line`, `bus` | BFS from source per-phase; mark which phases reach each bus; flag loads whose phase isn't reachable |
| `ph_02` | high | Phase renumbering across transformer | `trafo1ph`, `line` | Verify that phase labels exiting a transformer are compatible with phases entering it |
| `ph_03` | high | Neutral conductor present without ground return | `line` (phase=0 circuits) | Lines carrying neutral (phase 0) that don't terminate at a grounded bus |
| `ph_04` | medium | Single-phase load on 3-phase bus but wrong phase label | `asymmetric_load`, `bus` | Load says phase=2 but bus only has phases {0,1,3} |
| `ph_05` | low | Phase ordering anomalies | `line` | from_phase / to_phase swapped compared to standard (CBA instead of ABC) |

**Existing overlap**: `_check_phase_connectivity` covers element phases ⊆ bus phases. `ph_01` goes deeper with path-based reachability.

---

### Category 5 — Incorrect Impedance Data (`_diag_impedance.py`)

| Check ID | Severity | What it detects | Tables used | Logic |
|---|---|---|---|---|
| `imp_01` | critical | Zero or negative self-impedance | `line` | `r_ohm_per_km <= 0` or `x_ohm_per_km == 0` |
| `imp_02` | high | Impedance matrix not positive definite | `line` (mutual impedance columns) | Build Z matrix per line from r/x columns, check eigenvalues > 0 |
| `imp_03` | high | Impedance outlier (per-length) | `line` | `r_ohm_per_km / length_km` outside [0.01, 10] Ω/km for typical distribution conductors |
| `imp_04` | medium | Zero-sequence impedance inconsistent | `line` | z0/z1 ratio outside [1, 10] for typical overhead lines |
| `imp_05` | medium | Asymmetric mutual impedance matrix | `line` | `Z[i,j] != Z[j,i]` within tolerance |
| `imp_06` | low | X/R ratio unusually high or low | `line` | `x_ohm_per_km / r_ohm_per_km` outside [0.2, 5] for distribution |
| `imp_07` | low | Very short or very long lines | `line` | `length_km < 0.001` or `length_km > 100` |

---

### Category 6 — Open Conductor Issues (`_diag_open_conductor.py`)

| Check ID | Severity | What it detects | Tables used | Logic |
|---|---|---|---|---|
| `oc_01` | critical | Phase present at from_bus but missing at to_bus | `line` | For each line, compare from_phase set to to_phase set; unequal sets indicate broken conductor |
| `oc_02` | high | Single-phasing: 3-phase bus with only 2 phases served | `bus`, `line` | Bus expects 3 phases but only 2 incoming line circuits |
| `oc_03` | high | Series element with one end out-of-service | `line`, `switch` | Line in-service but connected to an out-of-service bus |
| `oc_04` | medium | Switch open on a phase that isolates downstream loads | `switch`, `line`, `asymmetric_load` | Trace downstream from open switch; if loads exist with no alternate path, flag |

**Existing overlap**: `_check_line_connectivity` catches from_phase ≠ to_phase mismatch. `oc_01` refines this specifically for open-conductor semantics.

---

### Category 7 — Load Model Problems (`_diag_load_model.py`)

| Check ID | Severity | What it detects | Tables used | Logic |
|---|---|---|---|---|
| `ld_01` | high | Load kVA exceeds transformer capacity | `asymmetric_load`, `trafo1ph` | Sum load p_mw downstream of each transformer; flag if > sn_mva × 1.5 |
| `ld_02` | high | Power factor out of range | `asymmetric_load` | `pf = p / sqrt(p² + q²)` outside [0.7, 1.0] |
| `ld_03` | medium | Load on wrong voltage level | `asymmetric_load`, `bus` | Load p_mw suggests secondary (< 1 kW) but bus vn_kv is primary (> 4 kV) |
| `ld_04` | medium | Load allocation residual too large | `asymmetric_load` | After allocation: `|Σp_allocated - p_measured| / p_measured > 5%` |
| `ld_05` | low | Unbalanced loading exceeds threshold | `asymmetric_load` | Phase imbalance `(max - min) / avg > 20%` across A, B, C |

**Existing overlap**: `_check_load_and_generation_values` covers sign convention, zero/missing values.

---

### Category 8 — Regulator / Capacitor / Inverter Control Errors (`_diag_controls.py`)

| Check ID | Severity | What it detects | Tables used | Logic |
|---|---|---|---|---|
| `ctrl_01` | high | Control element references non-existent bus | `shunt`, `asymmetric_shunt`, controller tables | Controlled bus not in `net.bus` |
| `ctrl_02` | high | Regulator setpoint outside normal range | Controller tables (if present) | Voltage setpoint outside [0.90, 1.10] pu |
| `ctrl_03` | medium | Capacitor switching thresholds inverted | `asymmetric_shunt` | `v_threshold_on >= v_threshold_off` (already partially covered) |
| `ctrl_04` | medium | Inverter reactive power exceeds capability | `asymmetric_sgen` | `|q_mvar| > sqrt(sn_mva² - p_mw²)` |
| `ctrl_05` | low | Multiple controllers on same bus | Controller tables | Two regulators or capacitors controlling the same bus |

**Existing overlap**: Shunt threshold ordering is in `_check_transformers_and_shunts`.

---

### Category 9 — Duplicate / Contradictory Equipment (`_diag_duplicates.py`)

| Check ID | Severity | What it detects | Tables used | Logic |
|---|---|---|---|---|
| `dup_01` | critical | Duplicate element index | All element tables | `table.index.duplicated()` — same element appears twice |
| `dup_02` | high | Parallel lines with contradictory impedance | `line` | Two lines with same from_bus/to_bus but impedance differs > 50 % |
| `dup_03` | high | Bus defined with conflicting vn_kv | `bus` | Same bus ID appears with different vn_kv across phases (shouldn't happen) |
| `dup_04` | medium | Overlapping transformers | `trafo1ph` | Two trafos connecting same bus pair |
| `dup_05` | medium | Duplicate loads on same bus/phase | `asymmetric_load` | Two loads at same bus + phase combination |
| `dup_06` | low | Name collisions | All tables with `name` column | Different elements with identical name strings |

---

### Category 10 — Bad Topology / Connectivity (`_diag_topology.py`)

| Check ID | Severity | What it detects | Tables used | Logic |
|---|---|---|---|---|
| `top_01` | critical | Bus not reachable from any source | `bus`, `ext_grid`, graph | BFS from all ext_grid buses; flag unreachable buses that are in-service |
| `top_02` | high | Source-to-source path (unintended parallel sources) | `ext_grid`, graph | Two ext_grid buses connected without an open switch between them |
| `top_03` | high | Switch references wrong element type | `switch` | `switch.et` says "b" (bus-bus) but `switch.element` is not a valid bus |
| `top_04` | medium | Radial violation — mesh detected | Graph | Cycle detection (already partially covered), but now also reports which element to open |
| `top_05` | medium | Long radial path (voltage drop risk) | Graph + `line` | Path from source to leaf bus has > N segments or > total km threshold |
| `top_06` | low | Degree-1 buses that aren't leaf loads | Graph, `asymmetric_load` | Structural dead-ends without loads attached |

**Existing overlap**: `_check_topology_and_radiality` covers islands and loops. These checks are more targeted.

---

## 4. Implementation Priority

### Phase 1 — Quick Wins (builds on existing framework, highest impact)

| Priority | Module | Checks | Estimated effort |
|---|---|---|---|
| P1 | `_diag_duplicates.py` | dup_01 – dup_06 | Small — pure DataFrame operations |
| P1 | `_diag_voltage_base.py` | vb_01 – vb_04 | Small — scalar comparisons |
| P1 | `_diag_impedance.py` | imp_01, imp_06, imp_07 | Small — range checks |
| P1 | `_diag_load_model.py` | ld_02, ld_05 | Small — arithmetic on existing columns |

### Phase 2 — Core Structural (requires graph traversal)

| Priority | Module | Checks | Estimated effort |
|---|---|---|---|
| P2 | `_diag_topology.py` | top_01 – top_06 | Medium — extends existing graph code |
| P2 | `_diag_voltage_base.py` | vb_05 | Medium — BFS with transformer boundary |
| P2 | `_diag_grounding.py` | gnd_01 – gnd_04 | Medium — neutral conductor path tracing |
| P2 | `_diag_open_conductor.py` | oc_01 – oc_04 | Medium — per-phase reachability |

### Phase 3 — Physics-Aware (requires domain logic)

| Priority | Module | Checks | Estimated effort |
|---|---|---|---|
| P3 | `_diag_transformer.py` | tx_01 – tx_06 | Medium — impedance & turns ratio math |
| P3 | `_diag_phase.py` | ph_01 – ph_05 | Medium-Large — per-phase BFS |
| P3 | `_diag_load_model.py` | ld_01, ld_03, ld_04 | Medium — cross-table aggregation |
| P3 | `_diag_impedance.py` | imp_02 – imp_05 | Medium — matrix algebra (numpy) |
| P3 | `_diag_controls.py` | ctrl_01 – ctrl_05 | Medium — depends on controller table schema |

---

## 5. Entry Point API

```python
from multiconductor.tools.diagnostics import run_diagnostics

result = run_diagnostics(
    net,
    categories="all",          # or list: ["voltage_base", "impedance", "topology"]
    severity_threshold="low",  # suppress "info" level
    fast_only=False,           # True = skip Phase 3 expensive checks
)

# result.issues       → DataFrame of all flagged issues
# result.recommendations → prioritized corrective actions
# result.summary      → count by severity
```

The existing `run_multiconductor_validations()` in `network_validators.py` remains as-is. `run_diagnostics()` calls the new category modules and can optionally include the existing validators too via `include_base_validators=True`.

---

## 6. Shared Utilities (`_common.py`)

Reuse / import from `network_validators.py`:
- `_issue()` — dict builder
- `_get_table()`, `_unique_bus_ids()`, `_get_bus_vn_kv()`, `_is_in_service()`, `_safe_float()`
- `_get_element_circuits()`, `_get_element_phase_set()`
- `_candidate_bus_values()`, `_parse_phases()`

New shared utilities:
- `build_network_graph(net, per_phase=False)` — returns `nx.Graph` or `nx.DiGraph` with optional per-phase nodes
- `bfs_from_sources(net, graph)` — BFS reachability from ext_grid buses
- `get_downstream_loads(net, graph, bus)` — loads on subtree rooted at bus
- `trafo1ph_bus_pair(trafo1ph_table, trafo_id)` — extract (hv_bus, lv_bus) from MultiIndex trafo1ph

---

## 7. Testing Strategy

Each `_diag_*.py` module gets a corresponding `test_diag_*.py`:

1. **Fixture networks**: Build small pandapowerNet objects with known defects (e.g., a 5-bus network with a wrong voltage base on bus 3).
2. **Expected issues**: Assert specific check IDs appear in the returned issues list.
3. **Clean network**: Assert zero issues for a correctly built network.
4. **Regression**: Run against existing pickle files in `networks/mc_xfmr/` and verify no crashes; count-check for known circuits.

---

## 8. Notebook Integration

Add a cell to `mc_dss_pf_results.ipynb` (or a new `network_diagnostics.ipynb`) that:

```python
from multiconductor.tools.diagnostics import run_diagnostics

for pkl in Path("networks/new/mc").glob("*.pkl"):
    net = pp.from_pickle(pkl)
    result = run_diagnostics(net, fast_only=True)
    if not result.issues.empty:
        display(HTML(f"<h4>{pkl.stem}: {len(result.issues)} issues</h4>"))
        display(result.issues.style.applymap(
            lambda v: "background: #ff6b6b" if v == "critical" else "", subset=["severity"]
        ))
```

---

## 9. Summary

| Metric | Value |
|---|---|
| Total new checks | 47 |
| New modules | 10 + 1 common + 1 init |
| Categories fully new (no existing coverage) | 3 (impedance, open conductor, duplicates) |
| Categories partially covered (need deepening) | 7 |
| Phase 1 checks (quick wins) | 14 |
| Phase 2 checks (structural) | 14 |
| Phase 3 checks (physics-aware) | 19 |
