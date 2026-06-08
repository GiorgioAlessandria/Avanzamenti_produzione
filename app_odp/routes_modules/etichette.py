# app_odp/routes_modules/etichette.py

from flask import abort, current_app, jsonify, request, send_file, url_for
from sqlalchemy import func

from app_odp.models import InputOdpLog, LottiGeneratiLog
from app_odp.operator_session import (
    active_token,
    operator_or_login_required,
    operator_perm_required,
)

from app_odp.routes import (
    main_bp,
    _norm_text,
    _print_label_png_to_windows_printer,
    _resolve_label_file_path,
)


@main_bp.get("/etichette/<path:filename>")
@operator_or_login_required
def etichetta_png(filename):
    file_path = _resolve_label_file_path(filename)

    if file_path is None or not file_path.is_file():
        abort(404)

    return send_file(
        file_path,
        mimetype="image/png",
        as_attachment=False,
        download_name=file_path.name,
    )


@main_bp.post("/api/etichette/stampa")
@operator_perm_required("home")
def api_stampa_etichetta():
    data = request.get_json(silent=True) or {}
    filename = _norm_text(data.get("filename"))

    if not filename:
        return jsonify({"ok": False, "error": "Nome file etichetta mancante."}), 400

    file_path = _resolve_label_file_path(filename)
    if file_path is None or not file_path.is_file():
        return jsonify({"ok": False, "error": "Etichetta non trovata."}), 404

    try:
        _print_label_png_to_windows_printer(file_path)
    except Exception as exc:
        current_app.logger.exception("Errore stampa etichetta %s", filename)
        return (
            jsonify(
                {
                    "ok": False,
                    "error": f"Errore durante la stampa dell'etichetta: {exc}",
                }
            ),
            500,
        )

    return jsonify(
        {
            "ok": True,
            "message": "Etichetta inviata in stampa.",
            "filename": filename,
        }
    )


@main_bp.get("/api/etichette/ricerca")
@operator_perm_required("home")
def api_ricerca_etichette():
    cod_art = _norm_text(request.args.get("cod_art"))
    lotto = _norm_text(request.args.get("lotto"))

    if not cod_art and not lotto:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Inserire almeno il codice articolo oppure il lotto.",
                }
            ),
            400,
        )

    q = LottiGeneratiLog.query

    if cod_art:
        q = q.filter(func.lower(LottiGeneratiLog.CodArt).like(f"%{cod_art.lower()}%"))

    if lotto:
        q = q.filter(
            func.lower(LottiGeneratiLog.RifLottoAlfa).like(f"%{lotto.lower()}%")
        )

    rows = q.order_by(LottiGeneratiLog.log_id.desc()).limit(100).all()

    operation_group_ids = {
        row.OperationGroupId for row in rows if _norm_text(row.OperationGroupId)
    }

    descrizioni_by_operation = {}

    if operation_group_ids:
        log_rows = (
            InputOdpLog.query.filter(
                InputOdpLog.OperationGroupId.in_(sorted(operation_group_ids))
            )
            .order_by(InputOdpLog.log_id.desc())
            .all()
        )

        for log_row in log_rows:
            op_id = _norm_text(log_row.OperationGroupId)
            if op_id and op_id not in descrizioni_by_operation:
                descrizioni_by_operation[op_id] = _norm_text(log_row.DesArt)

    items = []

    for row in rows:
        filename = _norm_text(getattr(row, "LabelFilename", ""))
        file_path = _resolve_label_file_path(filename) if filename else None
        file_exists = bool(file_path and file_path.is_file())

        items.append(
            {
                "log_id": row.log_id,
                "closed_at": _norm_text(row.ClosedAt or row.logged_at),
                "cod_art": _norm_text(row.CodArt),
                "descrizione": descrizioni_by_operation.get(
                    _norm_text(row.OperationGroupId),
                    "",
                ),
                "lotto": _norm_text(row.RifLottoAlfa),
                "quantita": _norm_text(row.Quantita),
                "filename": filename if file_exists else "",
                "label_url": (
                    url_for(
                        "main.etichetta_png",
                        filename=filename,
                        tab_session=active_token(),
                    )
                    if file_exists
                    else ""
                ),
                "file_exists": file_exists,
            }
        )

    return jsonify(
        {
            "ok": True,
            "items": items,
        }
    )
