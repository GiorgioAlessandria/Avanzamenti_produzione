from __future__ import annotations

from pathlib import Path

from flask import (
    abort,
    current_app,
    flash,
    redirect,
    render_template,
    request,
    send_file,
    url_for,
)

from app_odp.models import db
from app_odp.operator_session import active_token, active_user
from app_odp.policy.decorator import require_active_any_perm
from app_odp.routes_blueprint import main_bp
from app_odp.tarature_models import EventoTaratura
from app_odp.services.tarature_service import (
    build_page_context,
    create_spedizione,
    create_strumento,
    create_tipologia,
    record_external_calibration,
    record_internal_check,
    set_stato,
    update_strumento,
    update_tipologia,
)


def _back():
    token = active_token()
    return redirect(url_for("main.tarature_home", **({"tab_session": token} if token else {})))


def _execute(callback, success: str):
    try:
        callback()
        flash(success, "success")
    except (TypeError, ValueError) as exc:
        db.session.rollback()
        flash(str(exc), "danger")
    return _back()


@main_bp.get("/tarature")
@require_active_any_perm("tarature")
def tarature_home():
    return render_template("tarature/index.j2", **build_page_context())


@main_bp.post("/tarature/tipologie")
@require_active_any_perm("tarature")
def tarature_tipologia_create():
    return _execute(
        lambda: create_tipologia(request.form, active_user()),
        "Tipologia creata.",
    )


@main_bp.post("/tarature/tipologie/<int:tipologia_id>")
@require_active_any_perm("tarature")
def tarature_tipologia_update(tipologia_id: int):
    return _execute(
        lambda: update_tipologia(tipologia_id, request.form, active_user()),
        "Tipologia aggiornata e scadenze ricalcolate.",
    )


@main_bp.post("/tarature/strumenti")
@require_active_any_perm("tarature")
def tarature_strumento_create():
    return _execute(
        lambda: create_strumento(request.form, active_user()),
        "Strumento inserito e scadenze calcolate.",
    )


@main_bp.post("/tarature/strumenti/<int:strumento_id>")
@require_active_any_perm("tarature")
def tarature_strumento_update(strumento_id: int):
    return _execute(
        lambda: update_strumento(strumento_id, request.form, active_user()),
        "Anagrafica aggiornata.",
    )


@main_bp.post("/tarature/strumenti/<int:strumento_id>/stato")
@require_active_any_perm("tarature")
def tarature_strumento_stato(strumento_id: int):
    return _execute(
        lambda: set_stato(
            strumento_id,
            request.form.get("stato"),
            active_user(),
        ),
        "Stato aggiornato.",
    )


@main_bp.post("/tarature/strumenti/<int:strumento_id>/verifica-interna")
@require_active_any_perm("tarature")
def tarature_verifica_interna(strumento_id: int):
    return _execute(
        lambda: record_internal_check(strumento_id, request.form, active_user()),
        "Verifica interna registrata.",
    )


@main_bp.post("/tarature/strumenti/<int:strumento_id>/taratura-esterna")
@require_active_any_perm("tarature")
def tarature_esito_esterno(strumento_id: int):
    return _execute(
        lambda: record_external_calibration(
            strumento_id,
            request.form,
            active_user(),
            request.files.get("certificato"),
        ),
        "Rapporto e certificato di taratura registrati.",
    )


@main_bp.get("/tarature/certificati/<int:evento_id>")
@require_active_any_perm("tarature")
def tarature_certificato(evento_id: int):
    evento = db.session.get(EventoTaratura, evento_id)
    if (
        evento is None
        or not evento.certificato_nome
        or not evento.certificato_file
    ):
        abort(404)

    try:
        base_dir = Path(current_app.config["TARATURE_CERTIFICATI_DIR"]).resolve()
        pdf_path = (base_dir / evento.certificato_file).resolve()
        pdf_path.relative_to(base_dir)
        if not pdf_path.is_file():
            abort(404)
    except (OSError, ValueError):
        abort(404)

    response = send_file(
        pdf_path,
        mimetype="application/pdf",
        as_attachment=False,
        download_name=evento.certificato_nome,
        max_age=0,
    )
    response.headers["X-Content-Type-Options"] = "nosniff"
    return response


@main_bp.post("/tarature/spedizioni")
@require_active_any_perm("tarature")
def tarature_spedizione_create():
    return _execute(
        lambda: create_spedizione(
            request.form,
            request.form.getlist("strumento_id"),
            active_user(),
        ),
        "Spedizione creata: gli strumenti selezionati sono In taratura.",
    )
