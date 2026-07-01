from __future__ import annotations

from flask import jsonify, render_template, request

from app_odp.operator_session import active_token
from app_odp.policy.decorator import require_active_perm
from app_odp.routes_blueprint import main_bp
from app_odp.services.storico_ordini_service import (
    build_storico_ordini_detail,
    build_storico_ordini_list,
    default_period,
)


@main_bp.get("/storico-ordini")
@require_active_perm("storico_ordini")
def storico_ordini_page():
    date_from, date_to = default_period()
    return render_template(
        "storico_ordini.j2",
        default_date_from=date_from,
        default_date_to=date_to,
        tab_session=active_token(),
    )


@main_bp.get("/api/storico-ordini")
@require_active_perm("storico_ordini")
def api_storico_ordini():
    return jsonify(build_storico_ordini_list(request.args)), 200


@main_bp.get("/api/storico-ordini/detail")
@require_active_perm("storico_ordini")
def api_storico_ordini_detail():
    payload = build_storico_ordini_detail(request.args)
    status = 200 if payload.get("ok") else 400
    return jsonify(payload), status
