# app_odp/routes_modules/acquisti.py

from io import BytesIO

from flask import (
    abort,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    url_for,
)

from app_odp.routes_blueprint import main_bp
from app_odp.services.order_helpers import _norm_text, _now_rome_dt, _parse_bool_flag

from app_odp.services.acquisti_service import (
    _build_acquisti_giacenze_rows,
    _build_acquisti_materiale_rows,
    _build_acquisti_ordini_fornitore_rows,
    _build_acquisti_ordini_rows,
    _build_acquisti_scorte_rows,
    _filter_acquisti_giacenze_rows,
    _filter_acquisti_materiale_rows,
    _filter_acquisti_scorte_rows,
    _build_acquisti_excel_workbook,
    _create_scorta_from_qrcode,
    _scorta_to_row,
    _delete_scorte_chiuse_oltre_7_giorni,
    _group_acquisti_ordini_fornitore_rows,
)
from app_odp.models import (
    AcqOrdineFornitoreAperto,
    AcqOrdineFornitoreMeta,
    AcqScortaSegnalata,
    db,
)
from app_odp.operator_session import active_policy, active_user
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


@main_bp.get("/acquisti/ordini-fornitore")
@require_active_perm("home_acquisti")
def acquisti_ordini_fornitore():
    rows = _build_acquisti_ordini_fornitore_rows()
    groups = _group_acquisti_ordini_fornitore_rows(rows)
    return render_template(
        "acquisti_ordini_fornitore.j2",
        rows=rows,
        groups=groups,
        calendar_events=[
            {
                "date": group["DataConsegnaIso"],
                "title": " · ".join(
                    filter(
                        None,
                        (
                            group["NumRegistraz"],
                            group["Fornitore"],
                        ),
                    )
                ),
                "id_documento": group["IdDocumento"],
                "sollecitato": group["Sollecitato"],
                "gruppo_doc": group["GruppoDoc"],
                "critico": group["ConsegnaCritica"],
                "rows": [
                    {
                        "numero_registrazione": row["NumRegistraz"],
                        "codice_articolo": row["CodArt"],
                        "descrizione": row["DesArt"],
                        "data_consegna": row["DataConsegnaText"],
                        "udm": row["UmDoc"],
                        "ordinato": row["QtaOrd"],
                        "consegnata": row["QtaCons"],
                        "saldo_documento": row["QtaSaldo"],
                        "commento": row["CommentoRigaSaldata"],
                    }
                    for row in group["Righe"]
                ],
            }
            for group in groups
            if group["DataConsegnaIso"]
        ],
    )


@main_bp.post("/acquisti/ordini-fornitore")
@require_active_perm("home_acquisti")
def acquisti_ordine_fornitore_update():
    id_documento = _norm_text(request.form.get("id_documento"))
    id_riga = _norm_text(request.form.get("id_riga"))
    action = _norm_text(request.form.get("action")).lower()
    note = _norm_text(request.form.get("note"))

    if not id_documento or not id_riga or len(note) > 2000:
        abort(400)
    if db.session.get(AcqOrdineFornitoreAperto, (id_documento, id_riga)) is None:
        abort(404)

    row = db.session.get(AcqOrdineFornitoreMeta, (id_documento, id_riga))
    if row is None:
        row = AcqOrdineFornitoreMeta(
            IdDocumento=id_documento,
            IdRigaDoc=id_riga,
        )
        db.session.add(row)
    row.Note = note

    if action == "sollecita":
        row.Sollecitato = True
        row.SollecitatoAt = _now_rome_dt().isoformat(timespec="seconds")
        message = "Ordine segnato come sollecitato."
    elif action == "note":
        message = "Note salvate."
    else:
        abort(400)

    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        main_bp.logger.exception("Errore aggiornamento ordine fornitore")
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "error": "Errore durante il salvataggio."}), 500
        flash("Errore durante il salvataggio.", "danger")
    else:
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify(
                {
                    "ok": True,
                    "message": message,
                    "stato": "Sollecitato" if row.Sollecitato else "Aperto",
                    "sollecitato": bool(row.Sollecitato),
                    "id_documento": id_documento,
                }
            )
        flash(message, "success")
    return redirect(url_for("main.acquisti_ordini_fornitore"))


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
        "acquisti_giacenza_section": render_template(
            "partials/_acquisti_giacenza.j2",
            giacenze_rows=giacenze_rows,
        ),
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
        row, created = _create_scorta_from_qrcode(
            raw_qrcode,
            active_user(),
            allow_free_text=active_policy().can("scorte_segnalazione_libera"),
        )
        db.session.commit()

    except PermissionError as exc:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(exc)}), 403

    except ValueError as exc:
        db.session.rollback()
        main_bp.logger.warning("Errore di validazione in api_scorte_segnala")
        return jsonify({"ok": False, "error": str(exc)}), 400

    except Exception:
        db.session.rollback()
        return jsonify(
            {"ok": False, "error": "Errore durante il salvataggio della scorta."}
        ), 500

    item = _scorta_to_row(row)

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
                "id": item["Id"],
                "cod_art": item["CodArt"],
                "variante": item["VarianteArt"],
                "revisione": item["IndiceModifica"],
                "descrizione": item["DesArt"],
                "stato": item["Stato"],
                "segnalato_da": item["SegnalatoDa"],
                "reparto": item["RepartoSegnalatore"],
                "lookup_trovato": item["LookupTrovato"],
            },
        }
    )

@main_bp.patch("/api/acquisti/scorte/<int:scorta_id>")
@require_active_perm("home_acquisti")
def api_acquisti_scorta_update(scorta_id):
    payload = request.get_json(silent=True) or {}
    action = _norm_text(payload.get("action")).lower()
    has_note = "note" in payload
    note = _norm_text(payload.get("note")) if has_note else None

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
        if row.Stato not in {"Aperta", "Ordinata"}:
            row.Stato = "Aperta"
        row.Annullata = True
        row.StatoChangedAt = now_iso

    elif action == "note":
        if not has_note:
            return jsonify({"ok": False, "error": "Nota mancante."}), 400

    else:
        return jsonify({"ok": False, "error": "Azione non valida."}), 400

    if has_note:
        row.Note = note

    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify(
            {"ok": False, "error": "Errore durante l'aggiornamento della scorta."}
        ), 500

    return jsonify({"ok": True})
