from __future__ import annotations

from datetime import date, datetime

from flask import current_app, flash, redirect, render_template, request, url_for

from app_odp.logistica_models import MovimentoLogistico, VettoreTrasporto
from app_odp.models import db
from app_odp.operator_session import active_policy, active_token, active_user
from app_odp.policy.decorator import require_active_any_perm, require_active_perm
from app_odp.routes_blueprint import main_bp


def _redirect_logistica():
    token = active_token()
    kwargs = {"tab_session": token} if token else {}
    return redirect(url_for("main.logistica_page", **kwargs))


def _required_text(name: str, label: str, max_length: int) -> str:
    value = str(request.form.get(name) or "").strip()
    if not value:
        raise ValueError(f"{label}: valore obbligatorio.")
    if len(value) > max_length:
        raise ValueError(f"{label}: massimo {max_length} caratteri.")
    return value


def _optional_text(name: str, max_length: int) -> str | None:
    value = str(request.form.get(name) or "").strip()
    if len(value) > max_length:
        raise ValueError(f"Note: massimo {max_length} caratteri.")
    return value or None


def _parse_date(value) -> date:
    try:
        return date.fromisoformat(str(value or "").strip())
    except ValueError as exc:
        raise ValueError("Inserire una data valida.") from exc


def _later_date(current: date, value) -> date:
    new_date = _parse_date(value)
    if new_date <= current:
        raise ValueError("La nuova data deve essere successiva a quella attuale.")
    return new_date


def _row_class(movimento: MovimentoLogistico, oggi: date) -> str:
    if movimento.data < oggi:
        return "movimento-scaduto"
    if movimento.tipologia == "CLIENTE":
        return "movimento-cliente"
    return "movimento-fornitore"


def _actor() -> tuple[int | None, str]:
    user = active_user()
    return (
        getattr(user, "id", None),
        str(getattr(user, "username", None) or "utente"),
    )


def _movimento_atteso(movimento_id: int) -> MovimentoLogistico:
    movimento = db.session.get(MovimentoLogistico, movimento_id)
    if movimento is None:
        raise ValueError("Movimentazione non trovata.")
    if movimento.completato_il is not None:
        raise ValueError("La movimentazione risulta già completata.")
    return movimento


def _save(action, success_message: str):
    try:
        message = action() or success_message
        db.session.commit()
    except ValueError as exc:
        db.session.rollback()
        flash(str(exc), "danger")
    except Exception:
        db.session.rollback()
        current_app.logger.exception("Errore durante la gestione della logistica.")
        flash("Errore durante il salvataggio.", "danger")
    else:
        flash(message, "success")
    return _redirect_logistica()


@main_bp.get("/carichi-scarichi")
@require_active_any_perm("carica", "ricezione")
def logistica_page():
    policy = active_policy()
    attesi = (
        MovimentoLogistico.query.filter(MovimentoLogistico.completato_il.is_(None))
        .order_by(MovimentoLogistico.data.asc(), MovimentoLogistico.id.asc())
        .all()
    )
    completati = (
        MovimentoLogistico.query.filter(MovimentoLogistico.completato_il.is_not(None))
        .order_by(MovimentoLogistico.completato_il.desc())
        .limit(50)
        .all()
    )
    return render_template(
        "logistica.j2",
        vettori=VettoreTrasporto.query.order_by(VettoreTrasporto.nome.asc()).all(),
        attesi=attesi,
        completati=completati,
        oggi=date.today(),
        row_class=_row_class,
        can_carica=policy.can("carica"),
    )


@main_bp.post("/carichi-scarichi/vettori")
@require_active_perm("carica")
def logistica_vettore_create():
    def action():
        nome = _required_text("nome", "Vettore", 120)
        if VettoreTrasporto.query.filter_by(nome=nome).first() is not None:
            raise ValueError("Il vettore è già presente.")
        db.session.add(VettoreTrasporto(nome=nome))

    return _save(action, "Vettore aggiunto.")


@main_bp.post("/carichi-scarichi/movimenti")
@require_active_perm("carica")
def logistica_movimento_create():
    def action():
        try:
            vettore_id = int(request.form.get("vettore_id") or "")
        except ValueError as exc:
            raise ValueError("Selezionare un vettore valido.") from exc

        vettore = db.session.get(VettoreTrasporto, vettore_id)
        if vettore is None:
            raise ValueError("Selezionare un vettore valido.")

        movimento = str(request.form.get("movimento") or "").strip().upper()
        if movimento not in {"CARICO", "SCARICO"}:
            raise ValueError("Selezionare carico o scarico.")

        tipologia = str(request.form.get("tipologia") or "").strip().upper()
        if tipologia not in {"CLIENTE", "FORNITORE"}:
            raise ValueError("Selezionare cliente o fornitore.")

        user_id, username = _actor()
        db.session.add(
            MovimentoLogistico(
                vettore_id=vettore.id,
                movimento=movimento,
                tipologia=tipologia,
                controparte=_required_text("controparte", "Controparte", 160),
                data=_parse_date(request.form.get("data")),
                materiale=_required_text("materiale", "Materiale", 300),
                note=_optional_text("note", 1000),
                creato_da_id=user_id,
                creato_da_nome=username,
            )
        )

    return _save(action, "Spedizione aggiunta.")


@main_bp.post("/carichi-scarichi/movimenti/<int:movimento_id>/sollecito")
@require_active_perm("carica")
def logistica_movimento_sollecito(movimento_id: int):
    def action():
        movimento = _movimento_atteso(movimento_id)
        if movimento.sollecitato_il is None:
            movimento.sollecitato_il = datetime.now()
            return "Sollecito registrato."
        movimento.sollecitato_il = None
        return "Sollecito rimosso."

    return _save(action, "")


@main_bp.post("/carichi-scarichi/movimenti/<int:movimento_id>/data")
@require_active_perm("carica")
def logistica_movimento_data(movimento_id: int):
    def action():
        movimento = _movimento_atteso(movimento_id)
        movimento.data = _later_date(
            movimento.data,
            request.form.get("data"),
        )

    return _save(action, "Data aggiornata.")


@main_bp.post("/carichi-scarichi/movimenti/<int:movimento_id>/conferma")
@require_active_any_perm("carica", "ricezione")
def logistica_movimento_conferma(movimento_id: int):
    def action():
        movimento = _movimento_atteso(movimento_id)
        user_id, username = _actor()
        movimento.completato_il = datetime.now()
        movimento.completato_da_id = user_id
        movimento.completato_da_nome = username

    return _save(action, "Movimentazione confermata.")
