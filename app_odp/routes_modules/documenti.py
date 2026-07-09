# app_odp/routes_modules/documenti.py

from decimal import Decimal
from pathlib import Path

from flask import abort, current_app, jsonify, request, send_file, url_for
from sqlalchemy import func, select

from app_odp.models import db, AcqGiacenze

from app_odp.operator_session import (
    active_token,
    operator_or_login_required,
    operator_perm_required,
)

from app_odp.routes_blueprint import main_bp

from app_odp.services.documenti_service import (
    _build_articolo_ordini_attivi_rows,
    _find_articolo_lookup,
    _find_materiale_image_path,
    _find_metodo_pdf_path,
    _find_montaggio_pdf_path,
    _get_materiale_image_dir,
    _get_montaggio_pdf_dir,
    _normalize_article_search_token,
    _normalize_indice_articolo_search,
    _normalize_indice_modifica_for_pdf,
    _normalize_variante_articolo_search,
)
from app_odp.services.order_helpers import (
    _norm_text,
    _decimal_to_text,
)


@main_bp.get("/documenti/metodo-utilizzo")
@operator_or_login_required
def metodo_utilizzo_pdf():
    base_dir_raw = _norm_text(current_app.config.get("METODO_UTILIZZO_DIR"))

    if not base_dir_raw:
        current_app.logger.warning("METODO_UTILIZZO_DIR non configurata")
        abort(404)

    base_dir = Path(base_dir_raw).expanduser()

    try:
        base_dir = base_dir.resolve()
    except Exception:
        current_app.logger.exception("Percorso METODO_UTILIZZO_DIR non valido")
        abort(404)

    if not base_dir.exists() or not base_dir.is_dir():
        current_app.logger.warning("METODO_UTILIZZO_DIR non valida: %s", base_dir)
        abort(404)

    pdf_path = (base_dir / "metodo_utilizzo.pdf").resolve()

    try:
        pdf_path.relative_to(base_dir)
    except ValueError:
        abort(403)

    if not pdf_path.is_file():
        abort(404)

    return send_file(
        pdf_path,
        mimetype="application/pdf",
        as_attachment=False,
        download_name=pdf_path.name,
    )


@main_bp.get("/api/documenti/metodo")
@operator_perm_required("home")
def api_metodo_pdf():
    cod_art = _norm_text(request.args.get("cod_art"))
    indice_modifica = _normalize_indice_modifica_for_pdf(
        request.args.get("indice_modifica")
    )
    path_key = _norm_text(request.args.get("path_key")) or "MONTAGGIO_PDF_DIR"
    prefisso = _norm_text(request.args.get("prefisso"))

    allowed_path_keys = {"MONTAGGIO_PDF_DIR", "COLLAUDO_PDF_DIR"}
    if path_key not in allowed_path_keys:
        abort(404)

    pdf_path = _find_metodo_pdf_path(
        cod_art=cod_art,
        indice_modifica=indice_modifica,
        path_key=path_key,
        prefisso=prefisso,
        force_refresh=True,
    )

    if pdf_path is None:
        current_app.logger.warning(
            "PDF metodo non trovato cod_art=%s indice_modifica=%s path_key=%s prefisso=%s",
            cod_art,
            indice_modifica,
            path_key,
            prefisso,
        )
        abort(404)

    return send_file(
        pdf_path,
        mimetype="application/pdf",
        as_attachment=False,
        download_name=pdf_path.name,
    )


@main_bp.get("/api/documenti/metodo-montaggio")
@operator_perm_required("home")
def api_metodo_montaggio_pdf():
    cod_art = _norm_text(request.args.get("cod_art"))
    indice_modifica = _normalize_indice_modifica_for_pdf(
        request.args.get("indice_modifica")
    )

    pdf_dir = _get_montaggio_pdf_dir()
    if pdf_dir is None:
        current_app.logger.error("MONTAGGIO_PDF_DIR non configurata o non valida")
        abort(404)

    pdf_path = _find_montaggio_pdf_path(
        cod_art=cod_art,
        indice_modifica=indice_modifica,
        force_refresh=True,
    )

    if pdf_path is None:
        current_app.logger.warning(
            "PDF metodo montaggio non trovato per cod_art=%s indice_modifica=%s",
            cod_art,
            indice_modifica,
        )
        abort(404)

    try:
        pdf_path = pdf_path.resolve()
        pdf_dir = pdf_dir.resolve()
        pdf_path.relative_to(pdf_dir)
    except Exception:
        current_app.logger.exception("Percorso PDF non valido")
        abort(403)

    if not pdf_path.exists() or not pdf_path.is_file():
        current_app.logger.warning("PDF non accessibile: %s", pdf_path)
        abort(404)

    current_app.logger.info("Invio PDF metodo montaggio: %s", pdf_path)

    response = send_file(
        pdf_path,
        mimetype="application/pdf",
        as_attachment=False,
        download_name=pdf_path.name,
        conditional=False,
    )
    response.headers["Content-Disposition"] = f'inline; filename="{pdf_path.name}"'
    return response


@main_bp.get("/api/materiali/foto")
@operator_or_login_required
def api_materiale_foto():
    cod_art = _normalize_article_search_token(request.args.get("cod_art"))
    variante_art = _normalize_variante_articolo_search(request.args.get("variante_art"))
    indice_modifica = _normalize_indice_articolo_search(
        request.args.get("indice_modifica")
    )

    img_dir = _get_materiale_image_dir()
    if img_dir is None:
        abort(404)

    img_path = _find_materiale_image_path(
        cod_art=cod_art,
        variante_art=variante_art,
        indice_modifica=indice_modifica,
        force_refresh=True,
    )
    if img_path is None:
        abort(404)

    try:
        img_path = img_path.resolve()
        img_dir = img_dir.resolve()
        img_path.relative_to(img_dir)
    except Exception:
        abort(403)

    if not img_path.exists() or not img_path.is_file():
        abort(404)

    return send_file(
        img_path,
        mimetype="image/png",
        as_attachment=False,
        download_name=img_path.name,
    )


@main_bp.post("/api/materiali/ricerca-articolo")
@operator_or_login_required
def api_ricerca_articolo():
    data = request.get_json(silent=True) or {}

    cod_art = _normalize_article_search_token(data.get("cod_art"))
    variante_art = _normalize_variante_articolo_search(data.get("variante_art"))

    if not cod_art:
        return jsonify({"ok": False, "error": "CodArt obbligatorio."}), 400

    articolo = _find_articolo_lookup(
        cod_art=cod_art,
        variante_art=variante_art,
    )

    if articolo is None:
        return jsonify(
            {
                "ok": True,
                "found_component": False,
                "message": "Il codice inserito è errato oppure non è presente a gestionale.",
                "component": None,
                "image": {"found": False, "url": "", "file_name": ""},
                "orders": [],
                "orders_message": "",
            }
        )
    cod_art = _normalize_article_search_token(getattr(articolo, "CodArt", ""))
    variante_art = _normalize_variante_articolo_search(
        getattr(articolo, "VarianteArt", "")
    )
    indice_modifica = _normalize_indice_articolo_search(
        getattr(articolo, "IndiceModifica", "")
    )
    giacenza_totale = (
        db.session.execute(
            select(func.coalesce(func.sum(AcqGiacenze.Giacenza), 0.0)).where(
                AcqGiacenze.CodArt == cod_art,
                AcqGiacenze.VarianteArt == variante_art,
            )
        ).scalar()
        or 0.0
    )

    image_path = _find_materiale_image_path(
        cod_art=cod_art,
        variante_art=variante_art,
        indice_modifica=indice_modifica,
    )
    image_url = (
        url_for(
            "main.api_materiale_foto",
            cod_art=cod_art,
            variante_art=variante_art,
            indice_modifica=indice_modifica,
            tab_session=active_token(),
        )
        if image_path is not None
        else ""
    )

    orders = _build_articolo_ordini_attivi_rows(
        cod_art=cod_art,
        variante_art=variante_art,
        indice_modifica=indice_modifica,
    )

    return jsonify(
        {
            "ok": True,
            "found_component": True,
            "message": "",
            "component": {
                "CodArt": cod_art,
                "VarianteArt": variante_art,
                "IndiceModifica": indice_modifica,
                "DesArt": _norm_text(getattr(articolo, "DesArt", "")),
                "MagUM": _norm_text(getattr(articolo, "MagUM", "")),
                "TecniciUm": _norm_text(getattr(articolo, "TecniciUm", "")),
                "GiacenzaTotale": float(giacenza_totale or 0.0),
                "GiacenzaTotaleText": _decimal_to_text(
                    Decimal(str(giacenza_totale or 0))
                ),
            },
            "image": {
                "found": image_path is not None,
                "url": image_url,
                "file_name": image_path.name if image_path is not None else "",
            },
            "orders": orders,
            "orders_message": (
                ""
                if orders
                else "Non sono presenti ordini attivi per questo componente."
            ),
        }
    )
