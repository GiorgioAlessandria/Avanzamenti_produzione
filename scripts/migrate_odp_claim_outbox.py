from __future__ import annotations

import sqlite3
from pathlib import Path
import tomllib

CONFIG_PATH = Path("app_odp/static/config.toml")

DDL_ODP_CLAIM = """
                CREATE TABLE IF NOT EXISTS odp_claim (
                                                         id_documento TEXT NOT NULL,
                                                         id_riga      TEXT NOT NULL,
                                                         user_id      INTEGER NOT NULL,
                                                         claimed_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                                                         expires_at   TEXT NULL,
                                                         PRIMARY KEY (id_documento, id_riga)
                ); \
                """

INDEXES = [
    "CREATE INDEX IF NOT EXISTS ix_odp_claim_user ON odp_claim(user_id);",
    "CREATE INDEX IF NOT EXISTS ix_odp_claim_claimed_at ON odp_claim(claimed_at);",
    "CREATE INDEX IF NOT EXISTS ix_change_event_topic_id ON change_event(topic, id);",
    "CREATE INDEX IF NOT EXISTS ix_change_event_pk_id ON change_event(id_documento, id_riga, id);",
]

OUTBOX_COLUMNS = {
    "id_documento": "TEXT",
    "id_riga": "TEXT",
    "user_id": "INTEGER",
}


def load_db_path() -> Path:
    with CONFIG_PATH.open("rb") as f:
        cfg = tomllib.load(f)
    return Path(cfg["Percorsi"]["percorso_db"])


def column_exists(con: sqlite3.Connection, table: str, col: str) -> bool:
    rows = con.execute(f"PRAGMA table_info({table})").fetchall()
    return any(r[1] == col for r in rows)  # r[1] = name


def main() -> None:
    db_path = load_db_path()
    print(f"[migrate] DB = {db_path}")

    con = sqlite3.connect(str(db_path))
    try:
        # PRAGMA (per-connessione) + WAL (persistente)
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA busy_timeout=5000;")

        # Migrazione atomica
        con.execute("BEGIN;")
        con.execute(DDL_ODP_CLAIM)

        for col, typ in OUTBOX_COLUMNS.items():
            if not column_exists(con, "change_event", col):
                con.execute(f"ALTER TABLE change_event ADD COLUMN {col} {typ};")
                print(f"[migrate] added change_event.{col}")

        for idx in INDEXES:
            con.execute(idx)

        con.execute("COMMIT;")
        print("[migrate] OK")
    except Exception:
        con.execute("ROLLBACK;")
        raise
    finally:
        con.close()


if __name__ == "__main__":
    main()
