# app_odp/routes_modules/documenti.py

from decimal import Decimal
from pathlib import Path
import os
from tempfile import NamedTemporaryFile
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
import re
import secrets
import warnings
from io import BytesIO
from pathlib import PureWindowsPath

from PIL import Image, UnidentifiedImageError
from flask import render_template, session

from app_odp.models import AcqArticoliLookup
from app_odp.operator_session import get_operator_token
from app_odp.policy.decorator import require_active_perm
from app_odp.services.documenti_service import (
    _build_materiale_image_key,
    _build_metodo_pdf_key,
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


MAX_DOCUMENTO_BYTES = 20 * 1024 * 1024

DOCUMENT_UPLOAD_TYPES = {
    "immagini": ("Fotografie componenti PNG", "FOTOGRAFIE_MATERIALE"),
    "metodi": ("Metodi di produzione PDF", "MONTAGGIO_PDF_DIR"),
}


def _validate_upload_article(code, variant, revision):
    code = _normalize_article_search_token(code).upper()
    variant = _normalize_variante_articolo_search(variant).upper()
    revision = _normalize_indice_articolo_search(revision).upper()

    rows = AcqArticoliLookup.query.filter(
        func.upper(func.trim(AcqArticoliLookup.CodArt)) == code
    ).all()

    if not rows:
        raise ValueError(f"Codice articolo {code} inesistente.")

    for row in rows:
        row_variant = _normalize_variante_articolo_search(row.VarianteArt).upper()
        row_revision = _normalize_indice_articolo_search(row.IndiceModifica).upper()
        if row_variant == variant and row_revision == revision:
            return

    raise ValueError(
        f"Combinazione non presente: codice {code}, "
        f"variante {variant or '(vuota)'}, "
        f"revisione {revision or '(vuota)'}."
    )


def _parse_upload_name(name, kind, method_variant=""):
    if kind == "immagini":
        match = re.fullmatch(
            r"([A-Za-z0-9_-]+)\.([A-Za-z0-9_-]*)\.([A-Za-z0-9_-]*)\.png",
            name,
            flags=re.IGNORECASE,
        )
        if not match:
            raise ValueError("Nome richiesto: codice.variante.revisione.png.")

        code, variant, revision = match.groups()
        filename = _build_materiale_image_key(code, variant, revision) + ".png"

    elif kind == "metodi":
        match = re.fullmatch(
            r"([A-Za-z0-9_-]+)(?:\.([A-Za-z0-9_-]+))?\.pdf",
            name,
            flags=re.IGNORECASE,
        )
        if not match:
            raise ValueError("Nome richiesto: codice.revisione.pdf oppure codice.pdf.")

        code, revision = match.groups()
        revision = revision or ""
        variant = (method_variant or "").strip()
        if variant and not re.fullmatch(r"[A-Za-z0-9_-]+", variant):
            raise ValueError("Variante non valida.")

        filename = _build_metodo_pdf_key(code, revision) + ".pdf"

    else:
        raise ValueError("Tipo di documento non valido.")

    if PureWindowsPath(filename).is_reserved():
        raise ValueError("Nome file riservato da Windows.")
    if name.lower() != filename.lower():
        raise ValueError(f"Nome richiesto: {filename}")

    return code, variant, revision, filename


def _save_uploaded_document(upload, kind, method_variant, directory):
    code, variant, revision, filename = _parse_upload_name(
        upload.filename or "", kind, method_variant
    )
    _validate_upload_article(code, variant, revision)

    payload = upload.read(MAX_DOCUMENTO_BYTES + 1)
    if len(payload) > MAX_DOCUMENTO_BYTES:
        raise ValueError("Il file supera 20 MB.")

    if kind == "immagini":
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("error", Image.DecompressionBombWarning)
                with Image.open(BytesIO(payload)) as image:
                    if image.format != "PNG":
                        raise ValueError("Il contenuto del file non è PNG.")
                    if image.width * image.height > 20_000_000:
                        raise ValueError("L'immagine supera 20 milioni di pixel.")
                    image.verify()
        except (
            UnidentifiedImageError,
            OSError,
            SyntaxError,
            Image.DecompressionBombError,
            Image.DecompressionBombWarning,
        ) as exc:
            raise ValueError("PNG non valido o danneggiato.") from exc
    elif not payload.startswith(b"%PDF-"):
        raise ValueError("Il contenuto del file non è PDF.")

    target = directory / filename
    temporary = None
    try:
        with NamedTemporaryFile(
            dir=directory, prefix="upload_", suffix=".tmp", delete=False
        ) as output:
            temporary = Path(output.name)
            output.write(payload)

        os.replace(temporary, target)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)

    return filename


@main_bp.route("/documenti/carica", methods=["GET", "POST"])
@require_active_perm("carica_documenti")
def carica_documenti():
    # Un token operatore scaduto non deve usare il login condiviso.
    if get_operator_token() and get_operator_token() != active_token():
        abort(403)

    csrf = session.setdefault("document_upload_csrf", secrets.token_urlsafe(32))
    kind = "immagini"
    method_variant = ""
    results = []
    error = ""
    status = 200

    if request.method == "POST":
        if request.content_length is None:
            abort(411)
        if request.content_length > 100 * 1024 * 1024:
            abort(413)

        if not secrets.compare_digest(
            csrf.encode(), request.form.get("csrf_token", "").encode()
        ):
            abort(400)

        kind = request.form.get("tipo", "")
        if kind not in DOCUMENT_UPLOAD_TYPES:
            abort(400)

        method_variant = request.form.get("variante_metodi", "").strip()
        uploads = request.files.getlist("documenti")

        if not uploads or not any(upload.filename for upload in uploads):
            error, status = "Seleziona almeno un documento.", 400
        elif len(uploads) > 100:
            error, status = "Massimo 100 file per caricamento.", 400
        else:
            config_key = DOCUMENT_UPLOAD_TYPES[kind][1]
            raw_directory = _norm_text(current_app.config.get(config_key))
            directory = None

            if raw_directory:
                try:
                    candidate = Path(raw_directory).expanduser().resolve()
                    if candidate.is_dir():
                        directory = candidate
                except OSError:
                    pass

            if directory is None:
                error, status = "Cartella di destinazione non disponibile.", 503
            else:
                for upload in uploads:
                    result = {
                        "name": upload.filename or "(senza nome)",
                        "ok": False,
                    }
                    try:
                        result["name"] = _save_uploaded_document(
                            upload, kind, method_variant, directory
                        )
                    except FileExistsError:
                        result["message"] = "Già presente: non sovrascritto."
                    except ValueError as exc:
                        result["message"] = str(exc)
                    except OSError as exc:
                        current_app.logger.exception(
                            "Salvataggio documento non riuscito: cartella=%s file=%s",
                            directory,
                            upload.filename,
                        )

                        error_code = getattr(exc, "winerror", None) or exc.errno
                        if isinstance(exc, PermissionError):
                            message = "Il server non ha i permessi per scrivere nella cartella."
                        elif isinstance(exc, FileNotFoundError):
                            message = (
                                "La cartella di destinazione non è più disponibile."
                            )
                        else:
                            message = "Errore del filesystem durante il salvataggio."

                        result["message"] = (
                            f"{message} Tipo: {type(exc).__name__}, "
                            f"codice: {error_code}."
                        )
                    else:
                        result.update(ok=True, message="Caricato.")
                    results.append(result)

    response = current_app.make_response(
        (
            render_template(
                "carica_documenti.j2",
                upload_csrf=csrf,
                upload_types=DOCUMENT_UPLOAD_TYPES,
                selected_type=kind,
                method_variant=method_variant,
                upload_results=results,
                upload_error=error,
            ),
            status,
        )
    )
    response.headers["Cache-Control"] = "no-store"
    return response
