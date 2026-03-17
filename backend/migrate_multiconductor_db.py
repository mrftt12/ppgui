import argparse
import json
import sqlite3
from pathlib import Path
from typing import Dict, List

import pandas as pd


def _init_schema(conn: sqlite3.Connection, schema_path: Path) -> None:
    conn.executescript(schema_path.read_text(encoding="utf-8"))


def _get_tables(conn: sqlite3.Connection) -> List[str]:
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    return [row[0] for row in cur.fetchall()]


def _table_has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info('{table}')")
    return any(row[1] == column for row in cur.fetchall())


def _dtype_to_sqlite(dtype) -> str:
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    if pd.api.types.is_float_dtype(dtype):
        return "REAL"
    if pd.api.types.is_bool_dtype(dtype):
        return "INTEGER"
    return "TEXT"


def _ensure_columns(conn: sqlite3.Connection, table: str, df: pd.DataFrame) -> None:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info('{table}')")
    existing = {row[1] for row in cur.fetchall()}
    missing = [col for col in df.columns if col not in existing]
    for col in missing:
        col_type = _dtype_to_sqlite(df[col].dtype)
        cur.execute(f'ALTER TABLE "{table}" ADD COLUMN "{col}" {col_type}')


def _load_old_meta(conn: sqlite3.Connection) -> Dict[str, str]:
    if "network_meta" not in _get_tables(conn):
        return {}
    df = pd.read_sql_query("SELECT key, value FROM network_meta", conn)
    return {row["key"]: row["value"] for _, row in df.iterrows()}


def _parse_meta_value(value: str):
    try:
        return json.loads(value)
    except Exception:
        return value


def _upsert_network(conn: sqlite3.Connection, network_id: str, meta: Dict[str, str]) -> None:
    parsed = {key: _parse_meta_value(value) for key, value in meta.items()}
    conn.execute(
        """
        INSERT OR REPLACE INTO networks
        (network_id, name, description, sn_mva, f_hz, rho_ohmm, version, format_version, user_pf_options)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            network_id,
            parsed.get("name"),
            parsed.get("description"),
            parsed.get("sn_mva"),
            parsed.get("f_hz"),
            parsed.get("rho_ohmm"),
            parsed.get("version"),
            parsed.get("format_version"),
            json.dumps(parsed.get("user_pf_options", {})),
        ),
    )


def _write_network_meta(conn: sqlite3.Connection, network_id: str, meta: Dict[str, str]) -> None:
    if not meta:
        return
    conn.execute("DELETE FROM network_meta WHERE network_id = ?", (network_id,))
    rows = [
        {"network_id": network_id, "key": key, "value": value}
        for key, value in meta.items()
    ]
    pd.DataFrame(rows).to_sql("network_meta", conn, if_exists="append", index=False)


def migrate_network(
    source_db: Path, target_db: Path, network_id: str, schema_path: Path
) -> None:
    if not source_db.exists():
        raise FileNotFoundError(f"Source DB not found: {source_db}")
    target_db.parent.mkdir(parents=True, exist_ok=True)

    src_conn = sqlite3.connect(source_db)
    tgt_conn = sqlite3.connect(target_db)
    try:
        _init_schema(tgt_conn, schema_path)

        old_tables = set(_get_tables(src_conn))
        new_tables = set(_get_tables(tgt_conn))

        meta = _load_old_meta(src_conn)
        _upsert_network(tgt_conn, network_id, meta)
        _write_network_meta(tgt_conn, network_id, meta)

        skip_tables = {"meta", "network_meta", "networks"}
        for table in sorted(old_tables & new_tables):
            if table in skip_tables:
                continue

            df = pd.read_sql_query(f'SELECT * FROM "{table}"', src_conn)
            if df.empty:
                continue
            if not _table_has_column(tgt_conn, table, "network_id"):
                continue

            df.insert(0, "network_id", network_id)
            _ensure_columns(tgt_conn, table, df)
            tgt_conn.execute(f'DELETE FROM "{table}" WHERE network_id = ?', (network_id,))
            df.to_sql(table, tgt_conn, if_exists="append", index=False)

        tgt_conn.commit()
    finally:
        src_conn.close()
        tgt_conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate single-network DB into multi-network DB.")
    parser.add_argument(
        "--source",
        type=Path,
        default=Path(__file__).resolve().parent / "multiconductor_st_charles.db",
        help="Path to source single-network SQLite DB.",
    )
    parser.add_argument(
        "--target",
        type=Path,
        default=Path(__file__).resolve().parent / "multiconductor.db",
        help="Path to target multi-network SQLite DB.",
    )
    parser.add_argument(
        "--network-id",
        type=str,
        default="CKT_114_16955",
        help="Network ID to assign in the target DB.",
    )
    parser.add_argument(
        "--schema",
        type=Path,
        default=Path(__file__).resolve().parent / "multiconductor_schema.sql",
        help="Path to schema SQL file.",
    )
    args = parser.parse_args()

    migrate_network(args.source, args.target, args.network_id, args.schema)
    print(f"Migrated {args.source} -> {args.target} with network_id={args.network_id}")


if __name__ == "__main__":
    main()
