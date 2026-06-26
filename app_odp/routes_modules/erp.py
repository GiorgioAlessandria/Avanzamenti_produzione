# app_odp/routes_modules/erp.py

from app_odp.models import db
from app_odp.odp_output import txt_generator
from app_odp.operator_session import operator_perm_required
from flask import jsonify, request
from app_odp.routes_blueprint import main_bp
from app_odp.services.erp_export_service import (
    _get_pending_avp_export_rows,
    _write_txt_content,
)
from app_odp.services.order_helpers import (
    _norm_text,
    _now_rome_dt,
    _parse_bool_flag,
)


@main_bp.post("/api/erp/export/avp")
@operator_perm_required("home")
def api_export_avp_txt():
    data = request.get_json(silent=True) or {}

    suffix = _norm_text(data.get("suffix")) or "manuale"
    outbox_id_raw = data.get("outbox_id")

    try:
        outbox_id = int(outbox_id_raw)
    except (TypeError, ValueError):
        return jsonify(
            {
                "ok": False,
                "error": "Parametro outbox_id obbligatorio e non valido.",
            }
        ), 400

    export_rows = _get_pending_avp_export_rows(outbox_id=outbox_id)
    if not export_rows:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Nessun record ERP pending trovato per l'outbox richiesto.",
                }
            ),
            404,
        )

    outbox_rows = [row["outbox"] for row in export_rows]

    try:
        payload = export_rows[0]["payload"]
        include_time_line = _parse_bool_flag(payload.get("include_time_line", True))

        list_line = txt_generator(
            export_rows,
            include_time_line=include_time_line,
        )
        path_txt = _write_txt_content(
            list_line,
            prefix="AVPB",
            suffix=suffix,
            encoding="utf-8",
        )

        now_iso = _now_rome_dt().isoformat(timespec="seconds")
        for row in outbox_rows:
            row.status = "exported"
            row.exported_at = now_iso
            row.last_error = None
            row.attempts = int(row.attempts or 0) + 1

        db.session.commit()

        return jsonify(
            {
                "ok": True,
                "message": "File AVP generato correttamente",
                "file_name": path_txt.name,
                "file_path": str(path_txt),
                "records": len(outbox_rows),
            }
        )
    except Exception as exc:
        err = str(exc)

    try:
        for row in outbox_rows:
            row.status = "error"
            row.last_error = err
            row.attempts = int(row.attempts or 0) + 1
        db.session.commit()
    except Exception as exc:
        db.session.rollback()
        err = str(exc)

    return jsonify({"ok": False, "error": f"Errore generazione file AVP: {err}"}), 500
