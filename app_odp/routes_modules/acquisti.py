# app_odp/routes_modules/acquisti.py

from io import BytesIO

from flask import abort, jsonify, render_template, request, send_file

from app_odp.routes import main_bp, _norm_text, _parse_bool_flag, _now_rome_dt

from app_odp.services.acquisti_service import (
    _build_acquisti_giacenze_rows,
    _build_acquisti_materiale_rows,
    _build_acquisti_ordini_rows,
    _build_acquisti_scorte_rows,
    _filter_acquisti_giacenze_rows,
    _filter_acquisti_materiale_rows,
    _filter_acquisti_scorte_rows,
    _build_acquisti_excel_workbook,
    _create_scorta_from_qrcode,
    _delete_scorte_chiuse_oltre_7_giorni,
)
from app_odp.models import db, AcqScortaSegnalata
from app_odp.operator_session import active_user
from app_odp.policy.decorator import require_active_perm


@main_bp.get("/acquisti")
@require_active_perm("home_acquisti")
def home_acquisti():
    deleted = _delete_scorte_chiuse_oltre_7_giorni()
    if deleted:
        db.session.commit()

    giacenze_rows = _build_acquisti_giacenze_rows()
    materiali_rows = _build_acquisti_materiale_rows()
    ordini_rows = _build_acquisti_ordini_rows()
    scorte_rows = _build_acquisti_scorte_rows()
    return render_template(
        "home_acquisti.j2",
        giacenze_rows=giacenze_rows,
        materiali_rows=materiali_rows,
        ordini_rows=ordini_rows,
        scorte_rows=scorte_rows,
    )


@main_bp.get("/api/acquisti/export/<section>")
@require_active_perm("home_acquisti")
def api_export_acquisti_excel(section):
    section = _norm_text(section).lower()

    if section == "giacenza":
        rows = _build_acquisti_giacenze_rows()
        rows = _filter_acquisti_giacenze_rows(
            rows,
            codart=request.args.get("codart", ""),
            variante=request.args.get("variante", ""),
            desart=request.args.get("desart", ""),
            only_negative=_parse_bool_flag(request.args.get("negative")),
            only_understock=_parse_bool_flag(request.args.get("understock")),
        )
        file_name = f"acquisti_giacenza_{_now_rome_dt().strftime('%Y%m%d_%H%M%S')}.xlsx"

    elif section == "materiale":
        rows = _build_acquisti_materiale_rows()
        rows = _filter_acquisti_materiale_rows(
            rows,
            codart=request.args.get("codart", ""),
            variante=request.args.get("variante", ""),
            desart=request.args.get("desart", ""),
            only_critical=_parse_bool_flag(request.args.get("critical")),
            only_understock=_parse_bool_flag(request.args.get("understock")),
        )
        file_name = (
            f"acquisti_materiale_{_now_rome_dt().strftime('%Y%m%d_%H%M%S')}.xlsx"
        )

    elif section == "scorte":
        rows = _build_acquisti_scorte_rows()
        rows = _filter_acquisti_scorte_rows(
            rows,
            codart=request.args.get("codart", ""),
            variante=request.args.get("variante", ""),
            desart=request.args.get("desart", ""),
            stato=request.args.get("stato", ""),
            segnalato_da=request.args.get("segnalato_da", ""),
            include_annullate=_parse_bool_flag(request.args.get("include_annullate")),
        )
        file_name = f"acquisti_scorte_{_now_rome_dt().strftime('%Y%m%d_%H%M%S')}.xlsx"

    else:
        abort(404)

    wb = _build_acquisti_excel_workbook(section, rows)

    output = BytesIO()
    wb.save(output)
    output.seek(0)

    return send_file(
        output,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name=file_name,
    )


@main_bp.get("/api/acquisti/bridge")
@require_active_perm("home_acquisti")
def api_acquisti_bridge():
    deleted = _delete_scorte_chiuse_oltre_7_giorni()
    if deleted:
        db.session.commit()

    giacenze_rows = _build_acquisti_giacenze_rows()
    materiali_rows = _build_acquisti_materiale_rows()
    ordini_rows = _build_acquisti_ordini_rows()
    scorte_rows = _build_acquisti_scorte_rows()

    fragments = {
        "tbody_acquisti_giacenza": render_template(
            "partials/_acquisti_giacenza_rows.j2",
            giacenze_rows=giacenze_rows,
        ),
        "tbody_acquisti_materiale": render_template(
            "partials/_acquisti_materiale_rows.j2",
            materiali_rows=materiali_rows,
        ),
        "acquisti_ordini_section": render_template(
            "partials/_acquisti_ordini_produzione.j2",
            ordini_rows=ordini_rows,
        ),
        "tbody_acquisti_scorte": render_template(
            "partials/_acquisti_scorte_rows.j2",
            scorte_rows=scorte_rows,
        ),
    }

    return jsonify(
        {
            "ok": True,
            "refreshed_at": _now_rome_dt().isoformat(timespec="seconds"),
            "fragments": fragments,
        }
    )


@main_bp.post("/api/scorte/segnala")
@require_active_perm("home")
def api_scorte_segnala():
    payload = request.get_json(silent=True) or {}
    raw_qrcode = payload.get("qrcode", "")

    try:
        row, created = _create_scorta_from_qrcode(raw_qrcode, active_user())
        db.session.commit()

    except ValueError as exc:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(exc)}), 400

    except Exception:
        db.session.rollback()
        return jsonify(
            {"ok": False, "error": "Errore durante il salvataggio della scorta."}
        ), 500

    return jsonify(
        {
            "ok": True,
            "created": created,
            "duplicate": not created,
            "message": (
                "Scorta segnalata correttamente."
                if created
                else "Segnalazione già aperta per questo operatore."
            ),
            "item": {
                "id": row.id,
                "cod_art": row.CodArt,
                "variante": row.VarianteArt,
                "revisione": row.IndiceModifica,
                "descrizione": row.DesArt,
                "stato": row.Stato,
                "segnalato_da": row.SegnalatoDa,
                "reparto": row.RepartoSegnalatore,
                "lookup_trovato": bool(row.LookupTrovato),
            },
        }
    )


@main_bp.patch("/api/acquisti/scorte/<int:scorta_id>")
@require_active_perm("home_acquisti")
def api_acquisti_scorta_update(scorta_id):
    payload = request.get_json(silent=True) or {}
    action = _norm_text(payload.get("action")).lower()
    note = _norm_text(payload.get("note"))

    row = AcqScortaSegnalata.query.get_or_404(scorta_id)
    now_iso = _now_rome_dt().isoformat(timespec="seconds")

    if action == "ordinata":
        row.Stato = "Ordinata"
        row.Annullata = False
        row.StatoChangedAt = now_iso

    elif action == "aperta":
        row.Stato = "Aperta"
        row.Annullata = False
        row.StatoChangedAt = now_iso

    elif action == "annulla":
        row.Annullata = True
        row.StatoChangedAt = now_iso

    elif action == "note":
        pass

    else:
        return jsonify({"ok": False, "error": "Azione non valida."}), 400

    row.Note = note

    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify(
            {"ok": False, "error": "Errore durante l'aggiornamento della scorta."}
        ), 500

    return jsonify({"ok": True})
