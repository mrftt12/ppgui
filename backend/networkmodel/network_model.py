from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Type

from .base import RowModel
from .tables import (
    AsymmetricGen,
    AsymmetricLoad,
    AsymmetricSgen,
    AsymmetricShunt,
    Bus,
    BusGeodata,
    Characteristic,
    ConfigurationStdType,
    Controller,
    EmptyResAsymmetricGen,
    EmptyResAsymmetricLoad,
    EmptyResAsymmetricSgen,
    EmptyResBus,
    EmptyResExtGrid,
    EmptyResExtGridSequence,
    EmptyResLine,
    EmptyResTrafo,
    ExtGrid,
    ExtGridSequence,
    GroupModel,
    Line,
    LineGeodata,
    MatrixStdType,
    Measurement,
    NetworkMeta,
    PolyCost,
    PwlCost,
    ResBus,
    ResLine,
    ResTrafo,
    SequenceStdType,
    Switch,
    Trafo1ph,
)


TABLE_MODEL_MAP: Dict[str, Type[RowModel]] = {
    "network_meta": NetworkMeta,
    "bus": Bus,
    "ext_grid": ExtGrid,
    "ext_grid_sequence": ExtGridSequence,
    "asymmetric_load": AsymmetricLoad,
    "asymmetric_sgen": AsymmetricSgen,
    "asymmetric_gen": AsymmetricGen,
    "asymmetric_shunt": AsymmetricShunt,
    "line": Line,
    "switch": Switch,
    "trafo1ph": Trafo1ph,
    "measurement": Measurement,
    "pwl_cost": PwlCost,
    "poly_cost": PolyCost,
    "characteristic": Characteristic,
    "controller": Controller,
    "group": GroupModel,
    "line_geodata": LineGeodata,
    "bus_geodata": BusGeodata,
    "configuration_std_type": ConfigurationStdType,
    "sequence_std_type": SequenceStdType,
    "matrix_std_type": MatrixStdType,
    "res_bus": ResBus,
    "res_line": ResLine,
    "res_trafo": ResTrafo,
    "_empty_res_bus": EmptyResBus,
    "_empty_res_line": EmptyResLine,
    "_empty_res_trafo": EmptyResTrafo,
    "_empty_res_asymmetric_load": EmptyResAsymmetricLoad,
    "_empty_res_asymmetric_sgen": EmptyResAsymmetricSgen,
    "_empty_res_asymmetric_gen": EmptyResAsymmetricGen,
    "_empty_res_ext_grid": EmptyResExtGrid,
    "_empty_res_ext_grid_sequence": EmptyResExtGridSequence,
}


@dataclass
class NetworkModel:
    db_path: Path
    meta: Dict[str, Any] = field(default_factory=dict)
    tables: Dict[str, List[RowModel]] = field(default_factory=dict)

    def get_table(self, name: str) -> List[RowModel]:
        return self.tables.get(name, [])

    def __getattr__(self, name: str) -> Any:
        if name in self.tables:
            return self.tables[name]
        raise AttributeError(name)


def _default_db_path() -> Path:
    return Path(__file__).resolve().parents[1] / "multiconductor.db"


def _fetch_table_names(conn: sqlite3.Connection) -> List[str]:
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    return [row[0] for row in cur.fetchall()]


def _table_has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info('{table}')")
    return any(row[1] == column for row in cur.fetchall())


def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def _load_meta(conn: sqlite3.Connection, network_id: Optional[str]) -> Dict[str, Any]:
    if "network_meta" not in _fetch_table_names(conn):
        return {}
    if network_id is None:
        return {}
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT key, value FROM network_meta WHERE network_id = ?", (network_id,))
    meta: Dict[str, Any] = {}
    for row in cur.fetchall():
        key = row["key"]
        value = row["value"]
        try:
            meta[key] = json.loads(value)
        except Exception:
            meta[key] = value
    return meta


def _load_network_row(conn: sqlite3.Connection, network_id: str) -> Dict[str, Any]:
    if "networks" not in _fetch_table_names(conn):
        return {}
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM networks WHERE network_id = ?", (network_id,))
    row = cur.fetchone()
    if row is None:
        return {}
    return _row_to_dict(row)


def _load_table_rows(
    conn: sqlite3.Connection, table: str, network_id: Optional[str]
) -> Iterable[Dict[str, Any]]:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    if network_id and _table_has_column(conn, table, "network_id"):
        cur.execute(f'SELECT * FROM "{table}" WHERE network_id = ?', (network_id,))
    else:
        cur.execute(f'SELECT * FROM "{table}"')
    for row in cur.fetchall():
        yield _row_to_dict(row)


def _load_network_from_db(db_path: Path, network_id: Optional[str]) -> NetworkModel:
    conn = sqlite3.connect(db_path)
    try:
        table_names = _fetch_table_names(conn)
        meta: Dict[str, Any] = {}
        if network_id is not None:
            meta.update(_load_network_row(conn, network_id))
            meta.update(_load_meta(conn, network_id))
        tables: Dict[str, List[RowModel]] = {}
        for table in table_names:
            model_cls = TABLE_MODEL_MAP.get(table)
            if model_cls is None:
                continue
            records = list(_load_table_rows(conn, table, network_id))
            tables[table] = model_cls.from_records(records)
        return NetworkModel(db_path=db_path, meta=meta, tables=tables)
    finally:
        conn.close()


def load_network(id: str) -> NetworkModel:
    db_path = Path(id)
    if db_path.exists() and db_path.suffix == ".db":
        return _load_network_from_db(db_path, None)
    return _load_network_from_db(_default_db_path(), id)


def _load_network_ids(conn: sqlite3.Connection) -> List[str]:
    if "networks" not in _fetch_table_names(conn):
        return []
    cur = conn.cursor()
    cur.execute("SELECT network_id FROM networks ORDER BY network_id")
    return [row[0] for row in cur.fetchall()]


def load_networks() -> List[NetworkModel]:
    db_path = _default_db_path()
    if not db_path.exists():
        return []

    conn = sqlite3.connect(db_path)
    try:
        network_ids = _load_network_ids(conn)
    finally:
        conn.close()

    if not network_ids:
        return [_load_network_from_db(db_path, None)]

    return [_load_network_from_db(db_path, network_id) for network_id in network_ids]
