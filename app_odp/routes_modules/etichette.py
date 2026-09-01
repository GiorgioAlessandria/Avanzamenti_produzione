# app_odp/routes_modules/etichette.py

from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory

from flask import abort, current_app, jsonify, request, send_file, url_for
from sqlalchemy import func

from app_odp.models import GiacenzaMateriale, InputOdpLog, LottiGeneratiLog, db
from app_odp.operator_session import (
    active_token,
    operator_or_login_required,
    operator_perm_required,
)

from app_odp.routes_blueprint import main_bp
from app_odp.services.etichette_service import (
    _genera_etichetta_lotto,
    _print_label_png_to_windows_printer,
    _resolve_label_file_path,
)
from app_odp.services.order_helpers import _norm_text


def _descrizione_articolo(lotto_row) -> str:
    codice = _norm_text(lotto_row.CodArt)
    anagrafica_row = GiacenzaMateriale.query.filter_by(CodArt=codice).first()
    if anagrafica_row is not None:
        return _norm_text(anagrafica_row.DesArt)

    operation_group_id = _norm_text(lotto_row.OperationGroupId)
    if not operation_group_id:
        return ""

    input_row = (
        InputOdpLog.query.filter_by(OperationGroupId=operation_group_id)
        .order_by(InputOdpLog.log_id.desc())
        .first()
    )
    return _norm_text(input_row.DesArt) if input_row is not None else ""


def _genera_etichetta_da_log(lotto_row):
    return _genera_etichetta_lotto(
        codice=_norm_text(lotto_row.CodArt),
        descrizione=_descrizione_articolo(lotto_row),
        lotto=_norm_text(lotto_row.RifLottoAlfa),
        quantita=_norm_text(lotto_row.Quantita),
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


@main_bp.get("/etichette/log/<int:log_id>.png")
@operator_or_login_required
def etichetta_lotto_png(log_id):
    lotto_row = db.session.get(LottiGeneratiLog, log_id)
    if lotto_row is None:
        abort(404)

    img = _genera_etichetta_da_log(lotto_row)
    output = BytesIO()
    img.save(output, format="PNG")
    output.seek(0)
    return send_file(
        output,
        mimetype="image/png",
        as_attachment=False,
        download_name=f"etichetta_{log_id}.png",
        max_age=0,
    )


@main_bp.post("/api/etichette/stampa")
@operator_perm_required("home")
def api_stampa_etichetta():
    data = request.get_json(silent=True) or {}
    log_id = data.get("log_id")

    if log_id not in (None, ""):
        try:
            log_id = int(log_id)
        except (TypeError, ValueError):
            return jsonify({"ok": False, "error": "Etichetta non valida."}), 400

        lotto_row = db.session.get(LottiGeneratiLog, log_id)
        if lotto_row is None:
            return jsonify({"ok": False, "error": "Etichetta non trovata."}), 404

        try:
            img = _genera_etichetta_da_log(lotto_row)
            with TemporaryDirectory() as temp_dir:
                file_path = Path(temp_dir) / f"etichetta_{log_id}.png"
                img.save(file_path, format="PNG")
                _print_label_png_to_windows_printer(file_path)
        except Exception as exc:
            current_app.logger.exception("Errore stampa etichetta %s", log_id)
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
                "message": "Etichetta rigenerata e inviata in stampa.",
                "log_id": log_id,
            }
        )
    else:
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

    codici = {_norm_text(row.CodArt) for row in rows if _norm_text(row.CodArt)}
    descrizioni_by_codart = {}
    if codici:
        for articolo in GiacenzaMateriale.query.filter(
            GiacenzaMateriale.CodArt.in_(sorted(codici))
        ).all():
            codice = _norm_text(articolo.CodArt)
            if codice and codice not in descrizioni_by_codart:
                descrizioni_by_codart[codice] = _norm_text(articolo.DesArt)

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
        codice = _norm_text(row.CodArt)

        items.append(
            {
                "log_id": row.log_id,
                "closed_at": _norm_text(row.ClosedAt or row.logged_at),
                "cod_art": codice,
                "descrizione": descrizioni_by_codart.get(
                    codice,
                    descrizioni_by_operation.get(
                        _norm_text(row.OperationGroupId),
                        "",
                    ),
                ),
                "lotto": _norm_text(row.RifLottoAlfa),
                "quantita": _norm_text(row.Quantita),
                "filename": "",
                "label_url": url_for(
                    "main.etichetta_lotto_png",
                    log_id=row.log_id,
                    tab_session=active_token(),
                ),
                "file_exists": True,
            }
        )

    return jsonify(
        {
            "ok": True,
            "items": items,
        }
    )
