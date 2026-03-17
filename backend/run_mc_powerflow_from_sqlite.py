import json
import sqlite3
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import numpy as np
import pandas as pd


def _ensure_repo_on_path(db_path: Path) -> None:
    repo_root = db_path.resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))


def _load_meta(conn: sqlite3.Connection, network_id: Optional[str]) -> Dict:
    if network_id is None:
        return {}
    df = pd.read_sql_query(
        "SELECT key, value FROM network_meta WHERE network_id = ?",
        conn,
        params=(network_id,),
    )
    meta: Dict = {}
    for _, row in df.iterrows():
        key = row["key"]
        value = row["value"]
        try:
            meta[key] = json.loads(value)
        except Exception:
            meta[key] = value
    return meta


def _load_network_row(conn: sqlite3.Connection, network_id: Optional[str]) -> Dict:
    if network_id is None:
        return {}
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='networks'")
    if cur.fetchone() is None:
        return {}
    cur.execute("SELECT * FROM networks WHERE network_id = ?", (network_id,))
    row = cur.fetchone()
    if row is None:
        return {}
    columns = [desc[0] for desc in cur.description]
    return dict(zip(columns, row))


def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = ["__".join(str(part) for part in col) for col in df.columns.values]
    return df


def _reset_index_with_names(df: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(df.index, pd.MultiIndex):
        if df.index.name is None:
            df = df.copy()
            df.index.name = "index"
        return df.reset_index()

    idx_names = list(df.index.names)
    if all(name is None or name == "" for name in idx_names):
        idx_names = [f"level_{i}" for i in range(len(idx_names))]
    else:
        idx_names = [name if name not in (None, "") else f"level_{i}" for i, name in enumerate(idx_names)]

    cols = set(df.columns)
    deduped = []
    for i, name in enumerate(idx_names):
        candidate = name
        if candidate in cols or candidate in deduped:
            candidate = f"{candidate}_idx"
        if candidate in cols or candidate in deduped:
            candidate = f"idx_{i}"
        deduped.append(candidate)
    df = df.copy()
    df.index.set_names(deduped, inplace=True)
    return df.reset_index()


def _restore_index(df: pd.DataFrame, table: str) -> pd.DataFrame:
    multi_index_map = {
        "bus": ["index", "phase"],
        "line": ["index", "circuit"],
        "switch": ["index", "circuit"],
        "trafo1ph": ["index", "bus", "circuit"],
        "asymmetric_load": ["index", "circuit"],
        "asymmetric_sgen": ["index", "circuit"],
        "asymmetric_gen": ["index", "circuit"],
        "asymmetric_shunt": ["index", "circuit"],
        "ext_grid": ["index", "circuit"],
        "ext_grid_sequence": ["index", "sequence"],
        "configuration_std_type": ["index", "circuit"],
        "matrix_std_type": ["index", "circuit"],
    }

    if table in multi_index_map:
        cols = multi_index_map[table]
        if all(col in df.columns for col in cols):
            return df.set_index(cols)

    if "index" in df.columns:
        return df.set_index("index")

    return df


def _coerce_bool_columns(df: pd.DataFrame, table: str) -> pd.DataFrame:
    bool_map = {
        "bus": ["in_service", "grounded"],
        "line": ["in_service"],
        "switch": ["closed"],
        "trafo1ph": ["in_service"],
        "asymmetric_load": ["in_service"],
        "asymmetric_sgen": ["in_service", "current_source", "slack"],
        "asymmetric_gen": ["in_service", "current_source", "slack"],
        "asymmetric_shunt": ["in_service", "closed"],
        "ext_grid": ["in_service"],
        "ext_grid_sequence": ["in_service"],
        "controller": ["in_service", "initial_run"],
    }

    cols = bool_map.get(table, [])
    if not cols:
        return df

    df = df.copy()
    for col in cols:
        if col in df.columns:
            df[col] = df[col].map(lambda v: bool(v) if not pd.isna(v) else False)
    return df


def _load_tables(conn: sqlite3.Connection, network_id: Optional[str]) -> Dict[str, pd.DataFrame]:
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    table_names = [row[0] for row in cur.fetchall()]

    tables: Dict[str, pd.DataFrame] = {}
    for name in table_names:
        if name in {"network_meta", "networks", "meta"}:
            continue
        if network_id is not None:
            try:
                df = pd.read_sql_query(
                    f'SELECT * FROM "{name}" WHERE network_id = ?',
                    conn,
                    params=(network_id,),
                )
            except Exception:
                df = pd.read_sql_query(f'SELECT * FROM "{name}"', conn)
        else:
            df = pd.read_sql_query(f'SELECT * FROM "{name}"', conn)
        df = _restore_index(df, name)
        df = _coerce_bool_columns(df, name)
        tables[name] = df
    return tables


def _write_results(
    conn: sqlite3.Connection,
    net,
    result_tables: Iterable[str],
    network_id: Optional[str],
) -> None:
    for name in result_tables:
        if not hasattr(net, name):
            continue
        df = net[name]
        if not isinstance(df, pd.DataFrame):
            continue
        df = _flatten_columns(df)
        df = _reset_index_with_names(df)
        if network_id is not None:
            if "network_id" in df.columns:
                df["network_id"] = network_id
            else:
                df.insert(0, "network_id", network_id)
            conn.execute(f'DELETE FROM "{name}" WHERE network_id = ?', (network_id,))
            df.to_sql(name, conn, if_exists="append", index=False)
        else:
            df.to_sql(name, conn, if_exists="replace", index=False)


def _upsert_meta(conn: sqlite3.Connection, network_id: Optional[str], key: str, value) -> None:
    value_json = json.dumps(value)
    if network_id is None:
        conn.execute("DELETE FROM network_meta WHERE key = ?", (key,))
        conn.execute("INSERT INTO network_meta (key, value) VALUES (?, ?)", (key, value_json))
    else:
        conn.execute(
            "DELETE FROM network_meta WHERE network_id = ? AND key = ?",
            (network_id, key),
        )
        conn.execute(
            "INSERT INTO network_meta (network_id, key, value) VALUES (?, ?, ?)",
            (network_id, key, value_json),
        )


def build_network_from_db(db_path: Path, network_id: Optional[str] = None):
    _ensure_repo_on_path(db_path)
    import multiconductor as mc

    conn = sqlite3.connect(db_path)
    try:
        meta = _load_network_row(conn, network_id)
        meta.update(_load_meta(conn, network_id))
        tables = _load_tables(conn, network_id)
    finally:
        conn.close()

    net = mc.create_empty_network(
        name=meta.get("name", ""),
        sn_mva=float(meta.get("sn_mva", 1.0)),
        rho_ohmm=float(meta.get("rho_ohmm", 100.0)),
        f_hz=float(meta.get("f_hz", 50.0)),
        add_stdtypes=False,
    )
    net.version = meta.get("version", net.version)
    net.format_version = meta.get("format_version", net.format_version)
    net.std_types = meta.get("std_types", net.std_types)
    net.user_pf_options = meta.get("user_pf_options", {})

    for name, df in tables.items():
        net[name] = df

    return net


def run_powerflow(db_path: Path, network_id: Optional[str] = None) -> None:
    _ensure_repo_on_path(db_path)
    from multiconductor.pycci import cci_powerflow

    net = build_network_from_db(db_path, network_id)

    converged = False
    try:
        cci_powerflow.run_pf(net)
        converged = True
    except Exception as exc:
        raise RuntimeError("Powerflow did not converge") from exc

    if not converged:
        raise RuntimeError("Powerflow did not converge")

    conn = sqlite3.connect(db_path)
    try:
        result_tables: List[str] = [
            "res_bus",
            "res_ext_grid",
            "res_ext_grid_sequence",
            "res_line",
            "res_trafo",
            "res_asymmetric_load",
            "res_asymmetric_sgen",
            "res_asymmetric_gen",
            "res_asymmetric_shunt",
        ]
        _write_results(conn, net, result_tables, network_id)
        _upsert_meta(conn, network_id, "converged", True)
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run multiconductor powerflow from SQLite.")
    parser.add_argument(
        "--db",
        type=Path,
        default=Path(__file__).resolve().parent / "multiconductor.db",
        help="Path to multi-network SQLite DB.",
    )
    parser.add_argument(
        "--network-id",
        type=str,
        default="CKT_114_16955",
        help="Network ID to load and solve.",
    )
    args = parser.parse_args()

    run_powerflow(args.db, network_id=args.network_id)
    print(f"Powerflow complete and results written to {args.db} (network_id={args.network_id})")
