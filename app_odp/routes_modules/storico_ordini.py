from __future__ import annotations

from flask import jsonify, render_template, request, send_file

from app_odp.operator_session import active_token
from app_odp.policy.decorator import require_active_perm
from app_odp.routes_blueprint import main_bp
from app_odp.services.storico_matricole_export import build_matricole_excel
from app_odp.services.storico_ordini_service import (
    EVENT_FILTERS,
    build_storico_ordini_detail,
    build_storico_ordini_list,
    build_storico_lotti_search,
    build_storico_operatori_timeline,
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
        event_filters=EVENT_FILTERS,
    )


@main_bp.get("/api/storico-ordini")
@require_active_perm("storico_ordini")
def api_storico_ordini():
    return jsonify(build_storico_ordini_list(request.args)), 200


@main_bp.post("/api/storico-ordini/matricole/export")
@require_active_perm("storico_ordini")
def api_storico_matricole_export():
    try:
        output = build_matricole_excel(request.form.get("matricole", ""))
    except ValueError as exc:
        return jsonify(ok=False, error=str(exc)), 400
    return send_file(output, as_attachment=True, download_name="storico_matricole.xlsx",
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


@main_bp.get("/api/storico-ordini/detail")
@require_active_perm("storico_ordini")
def api_storico_ordini_detail():
    payload = build_storico_ordini_detail(request.args)
    status = 200 if payload.get("ok") else 400
    return jsonify(payload), status


@main_bp.get("/api/storico-ordini/lotti")
@require_active_perm("storico_ordini")
def api_storico_ordini_lotti():
    payload = build_storico_lotti_search(request.args)
    return jsonify(payload), 200 if payload.get("ok") else 400

@main_bp.get("/api/storico-ordini/operatori")
@require_active_perm("storico_ordini")
def api_storico_operatori():
    payload = build_storico_operatori_timeline(request.args)
    return jsonify(payload), 200 if payload.get("ok") else 400
