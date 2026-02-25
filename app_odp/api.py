from __future__ import annotations

import json
import time
from flask import Blueprint, request, abort, Response, stream_with_context
from flask_login import login_required, current_user
from sqlalchemy import text
import sqlite3
from app_odp.models import db, InputOdp, ChangeEvent
from app_odp.RBAC.policy import RbacPolicy
from app_odp.RBAC.decorator import require_perm

api_bp = Blueprint("api", __name__)


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


def _must_be_visible_odp(id_documento: str, id_riga: str) -> None:
    policy = RbacPolicy(current_user)
    q = InputOdp.query.filter_by(IdDocumento=id_documento, IdRiga=id_riga)
    q = policy.filter_input_odp(q)
    if q.first() is None:
        abort(404)


@api_bp.post("/api/odp/claim")
@login_required
@require_perm("home")
def odp_claim():
    body = request.get_json(silent=True) or {}
    id_documento = str(body.get("id_documento", "")).strip()
    id_riga = str(body.get("id_riga", "")).strip()
    if not id_documento or not id_riga:
        abort(400)

    _must_be_visible_odp(id_documento, id_riga)

    with db.session.begin():
        # Prova claim atomico (PK impedisce doppi claim)
        db.session.execute(
            text("""
                     INSERT OR IGNORE INTO odp_claim(id_documento, id_riga, user_id)
                     VALUES (:d, :r, :u)
                     """),
            {"d": id_documento, "r": id_riga, "u": int(current_user.id)},
        )

        inserted = int(db.session.execute(text("SELECT changes()")).scalar() or 0)

        if inserted == 1:
            # Evento outbox: propagazione agli altri operatori
            ev_payload = {
                "id_documento": id_documento,
                "id_riga": id_riga,
                "user_id": int(current_user.id),
            }
            db.session.add(
                ChangeEvent(
                    topic="odp_claimed",
                    payload_json=json.dumps(ev_payload, ensure_ascii=False),
                    id_documento=id_documento,
                    id_riga=id_riga,
                    user_id=int(current_user.id),
                )
            )
            return {
                "status": "claimed",
                "id_documento": id_documento,
                "id_riga": id_riga,
            }

        # Claim fallito: recupera owner
        row = db.session.execute(
            text(
                "SELECT user_id, claimed_at FROM odp_claim WHERE id_documento=:d AND id_riga=:r"
            ),
            {"d": id_documento, "r": id_riga},
        ).first()

        if row and int(row.user_id) == int(current_user.id):
            return {
                "status": "already_mine",
                "id_documento": id_documento,
                "id_riga": id_riga,
            }

        return {
            "status": "already_claimed",
            "id_documento": id_documento,
            "id_riga": id_riga,
        }, 409


@api_bp.post("/api/odp/release")
@login_required
@require_perm("home")
def odp_release():
    body = request.get_json(silent=True) or {}
    id_documento = str(body.get("id_documento", "")).strip()
    id_riga = str(body.get("id_riga", "")).strip()
    if not id_documento or not id_riga:
        abort(400)

    _must_be_visible_odp(id_documento, id_riga)

    with db.session.begin():
        # Solo il proprietario può rilasciare (estendibile con perm extra)
        db.session.execute(
            text("""
                     DELETE FROM odp_claim
                     WHERE id_documento=:d AND id_riga=:r AND user_id=:u
                     """),
            {"d": id_documento, "r": id_riga, "u": int(current_user.id)},
        )
        deleted = int(db.session.execute(text("SELECT changes()")).scalar() or 0)

        if deleted == 1:
            ev_payload = {
                "id_documento": id_documento,
                "id_riga": id_riga,
                "user_id": int(current_user.id),
            }
            db.session.add(
                ChangeEvent(
                    topic="odp_released",
                    payload_json=json.dumps(ev_payload, ensure_ascii=False),
                    id_documento=id_documento,
                    id_riga=id_riga,
                    user_id=int(current_user.id),
                )
            )
            return {
                "status": "released",
                "id_documento": id_documento,
                "id_riga": id_riga,
            }

    # idempotente: se non era claimato da me, non errore
    return {"status": "not_released", "id_documento": id_documento, "id_riga": id_riga}


@api_bp.get("/api/updates")
@login_required
@require_perm("home")
def updates():
    policy = RbacPolicy(current_user)
    after_id = request.args.get("after_id", default=0, type=int)
    limit = request.args.get("limit", default=50, type=int)
    limit = max(1, min(limit, 200))

    topic_raw = (request.args.get("topic") or "").strip()
    topics = (
        [t.strip() for t in topic_raw.split(",") if t.strip()] if topic_raw else None
    )

    q = (
        db.session.query(ChangeEvent)
        .filter(ChangeEvent.id > after_id)
        .order_by(ChangeEvent.id.asc())
        .limit(limit)
    )
    if topics:
        q = q.filter(ChangeEvent.topic.in_(topics))

    events = q.all()

    out = []
    last_id = after_id

    def can_see_odp(d: str, r: str) -> bool:
        q2 = InputOdp.query.filter_by(IdDocumento=d, IdRiga=r)
        q2 = policy.filter_input_odp(q2)
        return q2.first() is not None

    for ev in events:
        last_id = ev.id

        # Caso 1: claim/release (per ODP)
        if (
            ev.topic in ("odp_claimed", "odp_released")
            and ev.id_documento
            and ev.id_riga
        ):
            if not can_see_odp(ev.id_documento, ev.id_riga):
                continue
            payload = json.loads(ev.payload_json) if ev.payload_json else None
            out.append(
                {
                    "event_id": ev.id,
                    "topic": ev.topic,
                    "id_documento": ev.id_documento,
                    "id_riga": ev.id_riga,
                    "user_id": ev.user_id,
                    "payload": payload,
                    "created_at": ev.created_at,
                }
            )
            continue

        # Caso 2: nuovo_ordine emesso dal sync (aggregato)
        if ev.topic == "nuovo_ordine" and ev.payload_json:
            try:
                pk_list = json.loads(ev.payload_json)  # es: ["2,1","1,2",...]
            except Exception:
                pk_list = []

            for pk in pk_list:
                try:
                    d, r = [x.strip() for x in str(pk).split(",", 1)]
                except Exception:
                    continue
                if not d or not r:
                    continue
                if not can_see_odp(d, r):
                    continue
                out.append(
                    {
                        "event_id": ev.id,  # stesso id “padre”, ok per piccoli volumi
                        "topic": "nuovo_ordine",
                        "id_documento": d,
                        "id_riga": r,
                        "payload": {"pk": pk},
                        "created_at": ev.created_at,
                    }
                )
            continue

        # Default: passa-through (conservativo)
        out.append(
            {
                "event_id": ev.id,
                "topic": ev.topic,
                "payload": json.loads(ev.payload_json) if ev.payload_json else None,
                "created_at": ev.created_at,
            }
        )

    return {"last_id": last_id, "events": out}
