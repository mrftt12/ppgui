import sqlite3
import json
import uuid
from datetime import datetime, timezone
from typing import List, Dict, Optional, Any

DB_PATH = "loadflow.db"


class Database:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._init_db()

    def _get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        conn = self._get_connection()
        cursor = conn.cursor()

        # Networks table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS networks (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)

        # Network Versions table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS network_versions (
                id TEXT PRIMARY KEY,
                network_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                data TEXT NOT NULL,
                created_at TEXT NOT NULL,
                description TEXT,
                FOREIGN KEY (network_id) REFERENCES networks (id)
            )
        """)

        conn.commit()
        conn.close()

    def create_network(self, network_id: str, name: str) -> str:
        conn = self._get_connection()
        cursor = conn.cursor()
        now = datetime.now(timezone.utc).isoformat()

        cursor.execute(
            "INSERT INTO networks (id, name, created_at, updated_at) VALUES (?, ?, ?, ?)",
            (network_id, name, now, now),
        )
        conn.commit()
        conn.close()
        return network_id

    def list_networks(self) -> List[Dict]:
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM networks ORDER BY updated_at DESC")
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def get_network(self, network_id: str) -> Optional[Dict]:
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM networks WHERE id = ?", (network_id,))
        row = cursor.fetchone()
        conn.close()
        if row:
            return dict(row)
        return None

    def save_version(
        self, network_id: str, data: Dict[str, Any], description: Optional[str] = None
    ) -> int:
        conn = self._get_connection()
        cursor = conn.cursor()

        # Check if network exists, if not create it (auto-registration for existing in-memory nets)
        cursor.execute("SELECT * FROM networks WHERE id = ?", (network_id,))
        if not cursor.fetchone():
            # Try to find a name from the data or default
            name = data.get("name", "Untitled Network")
            self.create_network(network_id, name)

        # Get next version number
        cursor.execute(
            "SELECT MAX(version) as max_ver FROM network_versions WHERE network_id = ?",
            (network_id,),
        )
        row = cursor.fetchone()
        next_version = (row["max_ver"] or 0) + 1

        version_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()
        json_data = json.dumps(data)

        cursor.execute(
            """INSERT INTO network_versions (id, network_id, version, data, created_at, description)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (version_id, network_id, next_version, json_data, now, description),
        )

        # Update network updated_at
        cursor.execute(
            "UPDATE networks SET updated_at = ? WHERE id = ?", (now, network_id)
        )

        conn.commit()
        conn.close()
        return next_version

    def get_history(self, network_id: str) -> List[Dict]:
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, version, created_at, description FROM network_versions WHERE network_id = ? ORDER BY version DESC",
            (network_id,),
        )
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def load_version(
        self,
        network_id: str,
        version_id: Optional[str] = None,
        version_num: Optional[int] = None,
    ) -> Optional[Dict]:
        conn = self._get_connection()
        cursor = conn.cursor()

        if version_id:
            cursor.execute(
                "SELECT data FROM network_versions WHERE id = ?", (version_id,)
            )
        elif version_num is not None:
            cursor.execute(
                "SELECT data FROM network_versions WHERE network_id = ? AND version = ?",
                (network_id, version_num),
            )
        else:
            # Latest
            cursor.execute(
                "SELECT data FROM network_versions WHERE network_id = ? ORDER BY version DESC LIMIT 1",
                (network_id,),
            )

        row = cursor.fetchone()
        conn.close()

        if row:
            return json.loads(row["data"])
        return None
