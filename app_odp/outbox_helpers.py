from __future__ import annotations
import json
import sqlite3


def emit_event_sqlite3(
    con: sqlite3.Connection,
    *,
    topic: str,
    scope: str | None = None,
    payload: dict | list | str | None = None,
    id_documento: str | None = None,
    id_riga: str | None = None,
    user_id: int | None = None,
) -> int:
    payload_json = None
    if payload is not None and not isinstance(payload, str):
        payload_json = json.dumps(payload, ensure_ascii=False, default=str)
    elif isinstance(payload, str):
        payload_json = payload

    con.execute(
        """
            INSERT INTO change_event(topic, scope, payload_json, id_documento, id_riga, user_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            """,
        (topic, scope, payload_json, id_documento, id_riga, user_id),
    )
    # sqlite3_changes() è esposto anche via SELECT changes() (per-connessione) citeturn2search9
    return int(con.execute("SELECT changes()").fetchone()[0] or 0)
