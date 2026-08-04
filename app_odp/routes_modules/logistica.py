from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from types import SimpleNamespace

from flask import (
    current_app,
    flash,
    redirect,
    render_template,
    request,
    send_file,
    url_for,
)

from app_odp.logistica_models import (
    ClientePackingList,
    MovimentoLogistico,
    VettoreTrasporto,
)
from app_odp.models import db
from app_odp.operator_session import active_policy, active_token, active_user
from app_odp.policy.decorator import require_active_any_perm, require_active_perm
from app_odp.routes_blueprint import main_bp
from app_odp.services.packing_list_pdf_service import build_packing_list_pdf


def _redirect_logistica():
    token = active_token()
    kwargs = {"tab_session": token} if token else {}
    return redirect(url_for("main.logistica_page", **kwargs))


def _redirect_packing_list():
    token = active_token()
    kwargs = {"tab_session": token} if token else {}
    return redirect(url_for("main.packing_list_page", **kwargs))


def _required_text(name: str, label: str, max_length: int) -> str:
    value = str(request.form.get(name) or "").strip()
    if not value:
        raise ValueError(f"{label}: valore obbligatorio.")
    if len(value) > max_length:
        raise ValueError(f"{label}: massimo {max_length} caratteri.")
    return value


def _optional_text(
    name: str,
    max_length: int,
    label: str = "Note",
) -> str | None:
    value = str(request.form.get(name) or "").strip()
    if len(value) > max_length:
        raise ValueError(f"{label}: massimo {max_length} caratteri.")
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


def _non_negative_int(name: str, label: str) -> int:
    value = str(request.form.get(name) or "").strip()
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(f"{label}: inserire un numero intero valido.") from exc
    if parsed < 0:
        raise ValueError(f"{label}: il valore non può essere negativo.")
    return parsed


def _decimal_value(value, label: str, *, positive: bool = False) -> Decimal:
    normalized = str(value or "").strip().replace(",", ".")
    try:
        parsed = Decimal(normalized)
    except InvalidOperation as exc:
        raise ValueError(f"{label}: inserire un numero valido.") from exc

    if not parsed.is_finite() or parsed < 0 or (positive and parsed <= 0):
        qualifier = "maggiore di zero" if positive else "non negativo"
        raise ValueError(f"{label}: inserire un valore {qualifier}.")
    if parsed > Decimal("999999999.999"):
        raise ValueError(f"{label}: valore troppo grande.")
    if parsed.as_tuple().exponent < -3:
        raise ValueError(f"{label}: usare al massimo 3 decimali.")
    return parsed


def _packing_rows() -> list[tuple[str, str, str, Decimal]]:
    codes = request.form.getlist("item_code")
    descriptions = request.form.getlist("item_description")
    serial_numbers = request.form.getlist("item_serial_number")
    quantities = request.form.getlist("item_quantity")
    row_count = max(
        len(codes),
        len(descriptions),
        len(serial_numbers),
        len(quantities),
        0,
    )
    rows = []

    for index in range(row_count):
        code = str(codes[index] if index < len(codes) else "").strip()
        description = str(
            descriptions[index] if index < len(descriptions) else ""
        ).strip()
        serial_number = str(
            serial_numbers[index] if index < len(serial_numbers) else ""
        ).strip()
        raw_quantity = str(
            quantities[index] if index < len(quantities) else ""
        ).strip()

        if not any((code, description, serial_number, raw_quantity)):
            continue
        if not all((code, description, serial_number, raw_quantity)):
            raise ValueError(
                f"Riga {index + 1}: compilare Code, Description, "
                "Serial number e Quantity."
            )
        if len(code) > 120:
            raise ValueError(f"Riga {index + 1}: Code massimo 120 caratteri.")
        if len(description) > 500:
            raise ValueError(
                f"Riga {index + 1}: Description massimo 500 caratteri."
            )
        if len(serial_number) > 200:
            raise ValueError(
                f"Riga {index + 1}: Serial number massimo 200 caratteri."
            )

        rows.append(
            (
                code,
                description,
                serial_number,
                _decimal_value(
                    raw_quantity,
                    f"Riga {index + 1} - Quantity",
                    positive=True,
                ),
            )
        )

    if not rows:
        raise ValueError("Inserire almeno una riga nella packing list.")
    return rows


def _cliente_values(prefix: str = "") -> dict[str, str]:
    return {
        "nome": _required_text(f"{prefix}nome", "Nome cliente", 160),
        "indirizzo": _required_text(f"{prefix}indirizzo", "Indirizzo", 300),
        "provincia": _required_text(f"{prefix}provincia", "Provincia", 100),
        "paese": _required_text(f"{prefix}paese", "Paese", 100),
    }


def _cliente_from_form(prefix: str = "") -> ClientePackingList:
    return ClientePackingList(**_cliente_values(prefix))


def _cliente(cliente_id: int) -> ClientePackingList:
    cliente = db.session.get(ClientePackingList, cliente_id)
    if cliente is None:
        raise ValueError("Cliente non trovato.")
    return cliente


def _selected_cliente() -> ClientePackingList:
    cliente_id = str(request.form.get("cliente_id") or "").strip()
    if cliente_id == "new":
        cliente = _cliente_from_form("nuovo_cliente_")
        db.session.add(cliente)
        db.session.flush()
        return cliente

    try:
        parsed_id = int(cliente_id)
    except ValueError as exc:
        raise ValueError("Selezionare un cliente valido.") from exc

    return _cliente(parsed_id)


def _packing_list_from_form():
    cliente = _selected_cliente()
    net_weight = _decimal_value(
        request.form.get("total_net_weight"),
        "Total net weight (Kg.)",
    )
    gross_weight = _decimal_value(
        request.form.get("total_gross_weight"),
        "Total gross weight (Kg.)",
    )
    if gross_weight < net_weight:
        raise ValueError(
            "Total gross weight (Kg.) non può essere inferiore al peso netto."
        )

    return SimpleNamespace(
        cliente=cliente,
        transport_document=_required_text(
            "transport_document",
            "Transport document",
            120,
        ),
        invoice_number=_required_text("invoice_number", "Invoice number", 120),
        invoice_date=_parse_date(request.form.get("invoice_date")),
        total_pallets=_non_negative_int(
            "total_pallets",
            "Total Nr. of pallets",
        ),
        total_net_weight=net_weight,
        total_gross_weight=gross_weight,
        comments=_optional_text("comments", 2000, "Comments"),
        delivery=SimpleNamespace(
            nome=_required_text(
                "delivery_nome",
                "Delivery address - Customer",
                160,
            ),
            indirizzo=_required_text(
                "delivery_indirizzo",
                "Delivery address - Address",
                300,
            ),
            provincia=_required_text(
                "delivery_provincia",
                "Delivery address - Province",
                100,
            ),
            paese=_required_text(
                "delivery_paese",
                "Delivery address - Country",
                100,
            ),
        ),
        delivery_terms=_required_text("delivery_terms", "Delivery terms", 200),
        forwarder=_required_text("forwarder", "Forwarder", 200),
        righe=[
            SimpleNamespace(
                codice=code,
                descrizione=description,
                numero_seriale=serial_number,
                quantita=quantity,
            )
            for code, description, serial_number, quantity in _packing_rows()
        ],
    )


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


def _movimento(movimento_id: int) -> MovimentoLogistico:
    movimento = db.session.get(MovimentoLogistico, movimento_id)
    if movimento is None:
        raise ValueError("Movimentazione non trovata.")
    return movimento


def _movimento_atteso(movimento_id: int) -> MovimentoLogistico:
    movimento = _movimento(movimento_id)
    if movimento.completato_il is not None:
        raise ValueError("La movimentazione risulta già completata.")
    return movimento


def _save(action, success_message: str, redirector=None):
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
    return (redirector or _redirect_logistica)()


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


@main_bp.get("/carichi-scarichi/packing-list")
@require_active_perm("carica")
def packing_list_page():
    return render_template(
        "packing_list.j2",
        clienti=ClientePackingList.query.order_by(
            ClientePackingList.nome.asc(),
            ClientePackingList.id.asc(),
        ).all(),
        oggi=date.today(),
    )


@main_bp.post("/carichi-scarichi/packing-list/clienti")
@require_active_perm("carica")
def packing_list_cliente_create():
    def action():
        db.session.add(_cliente_from_form())

    return _save(
        action,
        "Cliente aggiunto.",
        redirector=_redirect_packing_list,
    )


@main_bp.post("/carichi-scarichi/packing-list/clienti/<int:cliente_id>")
@require_active_perm("carica")
def packing_list_cliente_update(cliente_id: int):
    def action():
        cliente = _cliente(cliente_id)
        for field, value in _cliente_values().items():
            setattr(cliente, field, value)

    return _save(
        action,
        "Cliente aggiornato.",
        redirector=_redirect_packing_list,
    )


@main_bp.post(
    "/carichi-scarichi/packing-list/clienti/<int:cliente_id>/elimina"
)
@require_active_perm("carica")
def packing_list_cliente_delete(cliente_id: int):
    def action():
        db.session.delete(_cliente(cliente_id))

    return _save(
        action,
        "Cliente eliminato.",
        redirector=_redirect_packing_list,
    )


@main_bp.post("/carichi-scarichi/packing-list")
@require_active_perm("carica")
def packing_list_print():
    try:
        pdf = build_packing_list_pdf(
            _packing_list_from_form(),
            logo_path=(
                Path(current_app.static_folder)
                / "assets"
                / "img"
                / "logo_completo.jpg"
            ),
            font_path=current_app.config.get("FONT_PATH"),
        )
        db.session.commit()
    except ValueError as exc:
        db.session.rollback()
        flash(str(exc), "danger")
        return _redirect_packing_list()
    except Exception:
        db.session.rollback()
        current_app.logger.exception(
            "Errore durante la generazione della packing list."
        )
        flash("Errore durante la generazione del PDF.", "danger")
        return _redirect_packing_list()

    return send_file(
        pdf,
        mimetype="application/pdf",
        as_attachment=False,
        download_name="packing-list.pdf",
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


@main_bp.post("/carichi-scarichi/movimenti/<int:movimento_id>/note")
@require_active_perm("carica")
def logistica_movimento_note(movimento_id: int):
    def action():
        movimento = _movimento(movimento_id)
        movimento.note = _optional_text("note", 1000)

    return _save(action, "Note aggiornate.")


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
