# app_odp/routes_modules/erp.py

from sqlalchemy import or_

from app_odp.models import ErpOutbox, db
from app_odp.odp_output import txt_generator
from app_odp.operator_session import operator_perm_required
from app_odp.policy.decorator import require_active_perm
from flask import jsonify, render_template, request
from app_odp.routes_blueprint import main_bp
from app_odp.services.erp_export_service import (
    _get_outbox_payload,
    _get_erp_export_dir,
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
        export_suffix = f"{suffix}_{outbox_id}"
        path_txt = _write_txt_content(
            list_line,
            prefix="AVPB",
            suffix=export_suffix,
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


@main_bp.get("/admin/utility/ricrea-avp")
@require_active_perm("utility_ricrea_avp")
def admin_ricrea_avp_page():
    return render_template(
        "admin_ricrea_avp.j2",
        default_output_dir=str(_get_erp_export_dir()),
    )


@main_bp.get("/api/admin/utility/avanzamenti")
@require_active_perm("utility_ricrea_avp")
def api_admin_avanzamenti():
    q = _norm_text(request.args.get("q"))
    query = ErpOutbox.query.filter(ErpOutbox.kind == "consuntivo_fase")

    if q:
        like = f"%{q}%"
        query = query.filter(
            or_(
                ErpOutbox.RifRegistraz.ilike(like),
                ErpOutbox.IdDocumento.ilike(like),
                ErpOutbox.IdRiga.ilike(like),
                ErpOutbox.CodArt.ilike(like),
            )
        )

    rows = query.order_by(ErpOutbox.outbox_id.desc()).limit(100).all()
    return jsonify(
        {
            "ok": True,
            "rows": [
                {
                    "outbox_id": row.outbox_id,
                    "created_at": _norm_text(row.created_at),
                    "ordine": _norm_text(row.RifRegistraz)
                    or f"{row.IdDocumento}/{row.IdRiga}",
                    "id_documento": _norm_text(row.IdDocumento),
                    "id_riga": _norm_text(row.IdRiga),
                    "articolo": _norm_text(row.CodArt),
                    "fase": _norm_text(row.Fase),
                    "status": _norm_text(row.status),
                    "exported_at": _norm_text(row.exported_at),
                }
                for row in rows
            ],
        }
    )


@main_bp.post("/api/admin/utility/ricrea-avp")
@require_active_perm("utility_ricrea_avp")
def api_admin_ricrea_avp():
    data = request.get_json(silent=True) or {}
    output_dir = _norm_text(data.get("output_dir"))
    try:
        outbox_id = int(data.get("outbox_id"))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Avanzamento non valido."}), 400

    outbox = ErpOutbox.query.filter_by(
        outbox_id=outbox_id,
        kind="consuntivo_fase",
    ).first()
    if outbox is None:
        return jsonify({"ok": False, "error": "Avanzamento non trovato."}), 404

    try:
        payload = _get_outbox_payload(outbox)
        lines = txt_generator(
            [{"outbox": outbox, "payload": payload}],
            include_time_line=_parse_bool_flag(
                payload.get("include_time_line", True)
            ),
        )
        path_txt = _write_txt_content(
            lines,
            prefix="AVPB",
            suffix=f"ricreato_{outbox_id}",
            encoding="utf-8",
            output_dir=output_dir or None,
        )
    except Exception as exc:
        return jsonify(
            {"ok": False, "error": f"Errore ricreazione file AVP: {exc}"}
        ), 500

    return jsonify(
        {
            "ok": True,
            "message": "File AVP ricreato correttamente.",
            "file_name": path_txt.name,
            "file_path": str(path_txt),
        }
    )
