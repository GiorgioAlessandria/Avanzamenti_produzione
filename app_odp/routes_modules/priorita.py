# app_odp/routes_modules/priorita.py

from flask import (
    current_app,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
)
from sqlalchemy import func
from app_odp.services.priorita_service import (
    _cleanup_priorita_operatore,
    _priorita_visible_operator_ids_for_current_user,
    _get_priorita_visible_operatore_or_403,
    _compact_priorita_operatore,
    _make_ordine_fase_key,
    _priorita_2_max,
    _priority_now_iso,
    _ordine_fase_key,
    _ordine_priorita_payload,
    _ordini_pianificata_visibili_per_operatore,
    _priorita_map_for_operatore,
    _priorita_valid_keys_for_operatore,
)
from app_odp.models import (
    db,
    OdpPriorita,
    User,
)
from app_odp.operator_session import active_policy, active_token, active_user
from app_odp.policy.decorator import require_active_perm
from app_odp.routes_blueprint import main_bp
from app_odp.services.session_helpers import _current_username


@main_bp.get("/priorita")
@require_active_perm("priorita_view")
def priorita():
    policy = active_policy()
    token = active_token()

    redirect_kwargs = {}
    if token:
        redirect_kwargs["tab_session"] = token

    if policy.can("priorita_edit"):
        return redirect(url_for("main.priorita_edit", **redirect_kwargs))

    return redirect(url_for("main.priorita_view", **redirect_kwargs))


@main_bp.get("/priorita/view")
@require_active_perm("priorita_view")
def priorita_view():
    policy = active_policy()
    user = active_user()

    return render_template(
        "priorita_view.j2",
        policy=policy,
        priorita_2_max=_priorita_2_max(),
        current_operator_id=user.id,
        current_operator_username=user.username or "",
    )


@main_bp.get("/priorita/edit")
@require_active_perm("priorita_edit")
def priorita_edit():
    policy = active_policy()
    user = active_user()

    return render_template(
        "priorita_edit.j2",
        policy=policy,
        priorita_2_max=_priorita_2_max(),
        current_operator_id=user.id,
        current_operator_username=user.username or "",
    )


@main_bp.get("/api/priorita/operatori")
@require_active_perm("priorita_view")
def api_priorita_operatori():
    visible_ids = _priorita_visible_operator_ids_for_current_user()

    operatori = (
        User.query.filter(User.active.is_(True))
        .filter(User.id.in_(visible_ids))
        .order_by(func.lower(User.username))
        .all()
    )

    return jsonify(
        {
            "operatori": [
                {
                    "id": operatore.id,
                    "username": operatore.username,
                }
                for operatore in operatori
            ]
        }
    )


@main_bp.get("/api/priorita/operatori/<int:operatore_id>/ordini")
@require_active_perm("priorita_view")
def api_priorita_ordini_operatore(operatore_id: int):
    operatore = _get_priorita_visible_operatore_or_403(operatore_id)

    _cleanup_priorita_operatore(operatore)
    _compact_priorita_operatore(operatore.id)
    db.session.commit()

    ordini = _ordini_pianificata_visibili_per_operatore(operatore)
    priorita_map = _priorita_map_for_operatore(operatore.id)

    payload = {
        "available": [],
        "p1": [],
        "p2": [],
        "p3": [],
        "max_p2": _priorita_2_max(),
        "can_edit": active_policy().can("priorita_edit"),
    }

    for ordine in ordini:
        key = _ordine_fase_key(ordine)
        priorita_row = priorita_map.get(key)
        item = _ordine_priorita_payload(ordine, priorita_row)

        if priorita_row is None:
            payload["available"].append(item)
        elif priorita_row.Priorita == 1:
            payload["p1"].append(item)
        elif priorita_row.Priorita == 2:
            payload["p2"].append(item)
        elif priorita_row.Priorita == 3:
            payload["p3"].append(item)

    payload["available"].sort(key=lambda x: (x["ordine"], x["fase"]))
    payload["p1"].sort(key=lambda x: x["posizione"] or 0)
    payload["p2"].sort(key=lambda x: x["posizione"] or 0)
    payload["p3"].sort(key=lambda x: x["posizione"] or 0)

    return jsonify(payload)


@main_bp.post("/api/priorita/operatori/<int:operatore_id>/salva")
@require_active_perm("priorita_edit")
def api_priorita_salva_operatore(operatore_id: int):
    operatore = _get_priorita_visible_operatore_or_403(operatore_id)
    operatore_id = operatore.id
    operatore = User.query.get_or_404(operatore_id)
    payload = request.get_json(silent=True) or {}

    items = payload.get("items", [])

    if not isinstance(items, list):
        return jsonify({"ok": False, "error": "Payload items non valido."}), 400

    valid_orders = _priorita_valid_keys_for_operatore(operatore)
    seen = set()
    staged = []

    for item in items:
        try:
            priorita = int(item.get("priorita"))
            posizione = int(item.get("posizione"))
        except (TypeError, ValueError):
            return jsonify(
                {"ok": False, "error": "Priorità o posizione non valida."}
            ), 400

        if priorita not in (1, 2, 3):
            return jsonify({"ok": False, "error": "Priorità ammessa: 1, 2, 3."}), 400

        key = _make_ordine_fase_key(
            item.get("id_documento"),
            item.get("id_riga"),
            item.get("fase"),
        )

        if key in seen:
            return jsonify({"ok": False, "error": "Ordine duplicato nella coda."}), 400

        if key not in valid_orders:
            return jsonify(
                {
                    "ok": False,
                    "error": (
                        "Uno degli ordini non è più Pianificata oppure "
                        "non è più visibile per l'operatore selezionato."
                    ),
                }
            ), 409

        seen.add(key)
        staged.append(
            {
                "key": key,
                "priorita": priorita,
                "posizione": posizione,
            }
        )

    now_iso = _priority_now_iso()
    username = _current_username("sync_priorita")

    OdpPriorita.query.filter_by(operatore_id=operatore.id).delete(
        synchronize_session=False
    )

    for row in staged:
        id_documento, id_riga, fase = row["key"]

        db.session.add(
            OdpPriorita(
                operatore_id=operatore.id,
                IdDocumento=id_documento,
                IdRiga=id_riga,
                Fase=fase,
                Priorita=row["priorita"],
                Posizione=row["posizione"],
                created_at=now_iso,
                updated_at=now_iso,
                updated_by=username,
            )
        )

    db.session.flush()
    _compact_priorita_operatore(operatore.id)
    db.session.commit()

    return jsonify({"ok": True})


@main_bp.post("/api/priorita/operatori/<int:operatore_id>/reset")
@require_active_perm("priorita_edit")
def api_priorita_reset_operatore(operatore_id):
    operatore = _get_priorita_visible_operatore_or_403(operatore_id)

    try:
        deleted_count = OdpPriorita.query.filter(
            OdpPriorita.operatore_id == operatore.id
        ).delete(synchronize_session=False)

        db.session.commit()

        return jsonify(
            {
                "ok": True,
                "deleted": deleted_count,
            }
        )

    except Exception:
        db.session.rollback()
        current_app.logger.exception(
            "Errore reset priorità operatore_id=%s",
            operatore_id,
        )
        return jsonify(
            {
                "ok": False,
                "error": "Errore durante l'azzeramento delle priorità.",
            }
        ), 500
