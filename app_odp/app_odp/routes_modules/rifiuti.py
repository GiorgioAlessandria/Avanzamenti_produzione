from __future__ import annotations

from datetime import datetime

from flask import (
    current_app,
    flash,
    redirect,
    render_template,
    request,
    send_file,
    url_for,
)

from app_odp.models import db
from app_odp.operator_session import (
    active_policy,
    active_token,
    active_user,
)
from app_odp.policy.decorator import (
    require_active_any_perm,
    require_active_perm,
)
from app_odp.routes_blueprint import main_bp
from app_odp.services.rifiuti_service import (
    CodiceCerNonValidoError,
    RifiutiServiceError,
    build_carichi_smaltiti_rows,
    build_carichi_presenti_rows,
    build_rifiuti_export,
    build_rifiuti_stock_export,
    calculate_totale_presente,
    create_carico_rifiuto,
    create_codice_cer,
    deactivate_codice_cer,
    delete_carico_rifiuto,
    format_peso_kg,
    list_carichi_presenti,
    list_carichi_tutti,
    list_codici_cer_attivi,
    smaltisci_carichi,
    update_codice_cer,
)


def _redirect_rifiuti():
    token = active_token()
    kwargs = {}

    if token:
        kwargs["tab_session"] = token

    return redirect(
        url_for(
            "main.rifiuti_page",
            **kwargs,
        )
    )


def _redirect_codici_cer():
    token = active_token()
    kwargs = {"tab_session": token} if token else {}
    return redirect(url_for("main.rifiuti_codici_cer", **kwargs))


@main_bp.get("/rifiuti/codici-cer")
@require_active_perm("rifiuti_elimina")
def rifiuti_codici_cer():
    return render_template("rifiuti_codici_cer.j2", codici_cer=list_codici_cer_attivi())


@main_bp.post("/rifiuti/codici-cer")
@require_active_perm("rifiuti_elimina")
def rifiuti_codici_cer_save():
    action = str(request.form.get("action") or "").strip().lower()

    try:
        if action == "create":
            create_codice_cer(
                codice=request.form.get("codice"),
                descrizione=request.form.get("descrizione"),
                commit=False,
            )
            message = "Codice CER aggiunto correttamente."
        elif action == "update":
            update_codice_cer(
                codice_cer_id=request.form.get("codice_cer_id"),
                codice=request.form.get("codice"),
                descrizione=request.form.get("descrizione"),
                commit=False,
            )
            message = "Codice CER aggiornato correttamente."
        elif action == "delete":
            deactivate_codice_cer(
                request.form.get("codice_cer_id"),
                commit=False,
            )
            message = "Codice CER rimosso correttamente."
        else:
            raise CodiceCerNonValidoError("Operazione CER non valida.")

        db.session.commit()
    except RifiutiServiceError as exc:
        db.session.rollback()
        flash(str(exc), "danger")
    except Exception:
        db.session.rollback()
        current_app.logger.exception("Errore durante la gestione dei codici CER.")
        flash("Errore durante il salvataggio del codice CER.", "danger")
    else:
        flash(message, "success")

    return _redirect_codici_cer()


@main_bp.get("/rifiuti")
@require_active_any_perm(
    "rifiuti_carica",
    "rifiuti_elimina",
)
def rifiuti_page():
    policy = active_policy()

    return render_template(
        "rifiuti.j2",
        codici_cer=list_codici_cer_attivi(),
        carichi=build_carichi_presenti_rows(),
        smaltimenti=build_carichi_smaltiti_rows(),
        totale_peso_kg=format_peso_kg(calculate_totale_presente()),
        can_carica=policy.can("rifiuti_carica"),
        can_elimina=policy.can("rifiuti_elimina"),
    )


@main_bp.post("/rifiuti/carica")
@require_active_perm("rifiuti_carica")
def rifiuti_carica():
    try:
        create_carico_rifiuto(
            codice_cer_id=request.form.get("codice_cer_id"),
            peso_kg=request.form.get("peso_kg"),
            note=request.form.get("note"),
            user=active_user(),
            commit=False,
        )

        db.session.commit()

    except RifiutiServiceError as exc:
        db.session.rollback()
        flash(
            str(exc),
            "danger",
        )

    except Exception:
        db.session.rollback()
        current_app.logger.exception(
            "Errore durante il caricamento del materiale da smaltire."
        )
        flash(
            "Errore durante il salvataggio del materiale.",
            "danger",
        )

    else:
        flash(
            "Materiale inserito correttamente nello stock rifiuti.",
            "success",
        )

    return _redirect_rifiuti()


@main_bp.post("/rifiuti/elimina")
@require_active_perm("rifiuti_elimina")
def rifiuti_elimina():
    try:
        delete_carico_rifiuto(
            request.form.get("carico_id"),
            commit=False,
        )
        db.session.commit()
    except RifiutiServiceError as exc:
        db.session.rollback()
        flash(str(exc), "danger")
    except Exception:
        db.session.rollback()
        current_app.logger.exception(
            "Errore durante la cancellazione del carico rifiuti."
        )
        flash("Errore durante la cancellazione del carico.", "danger")
    else:
        flash("Riga cancellata correttamente.", "success")

    return _redirect_rifiuti()


@main_bp.post("/rifiuti/smaltisci")
@require_active_perm("rifiuti_elimina")
def rifiuti_smaltisci():
    try:
        carichi = smaltisci_carichi(
            carico_ids=request.form.getlist("carico_id"),
            user=active_user(),
            commit=False,
        )
        db.session.commit()
    except RifiutiServiceError as exc:
        db.session.rollback()
        flash(str(exc), "danger")
    except Exception:
        db.session.rollback()
        current_app.logger.exception(
            "Errore durante la registrazione dello smaltimento rifiuti."
        )
        flash("Errore durante la registrazione dello smaltimento.", "danger")
    else:
        flash(
            f"Registrato lo smaltimento di {len(carichi)} carichi.",
            "success",
        )

    return _redirect_rifiuti()


def _send_rifiuti_export(carichi, prefix: str, builder=build_rifiuti_export):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return send_file(
        builder(carichi),
        as_attachment=True,
        download_name=f"{prefix}_{timestamp}.xlsx",
        mimetype=(
            "application/vnd.openxmlformats-officedocument."
            "spreadsheetml.sheet"
        ),
    )


@main_bp.get("/rifiuti/export")
@require_active_any_perm("rifiuti_carica", "rifiuti_elimina")
def rifiuti_export():
    return _send_rifiuti_export(
        list_carichi_presenti(),
        "rifiuti_stock",
        build_rifiuti_stock_export,
    )


@main_bp.get("/rifiuti/export-storico")
@require_active_any_perm("rifiuti_carica", "rifiuti_elimina")
def rifiuti_export_storico():
    return _send_rifiuti_export(
        list_carichi_tutti(),
        "rifiuti_storico",
    )
