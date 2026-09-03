import json
import unicodedata
from datetime import date
from types import SimpleNamespace

from sqlalchemy import func, or_, tuple_
from sqlalchemy.orm import selectinload

from app_odp.models import AcqArticoliLookup, InputOdp, InputOdpLog, db
from app_odp.ordine_ref import format_ordine_ref_display
from app_odp.services.order_helpers import (
    _fase_to_int,
    _norm_text,
    _now_rome_dt,
    _ordine_ref_label,
)
from app_odp.services.ordini_query_service import _base_odp_query
from app_odp.services.vendite_service import (
    _canonical_state,
    _phase_label,
    is_open_machine_order,
    load_machine_orders,
)
from app_odp.vendite_models import (
    VENDITE_DEFAULT_PACKAGING_NOTES,
    VENDITE_INTERNAL_REFERENCES,
    VenditeMacchinaStock,
    VenditeNotaImballaggio,
    VenditeOrdineCliente,
    VenditeOrdineClienteRiga,
    VenditeSpedizioneConfermata,
)


MAX_CUSTOMER_NAME = 160
MAX_CUSTOMER_ORDER = 120
MAX_NOTE = 1000
MAX_EXPANDED_ROWS = 500
LOG_QUERY_BATCH_SIZE = 400
STOCK_LABEL = "STOCK"


class VenditeAssegnazioniError(ValueError):
    pass


class VenditeAssegnazioniConflictError(VenditeAssegnazioniError):
    pass


def _required_text(value, label: str, max_length: int) -> str:
    text = _norm_text(value)
    if not text:
        raise VenditeAssegnazioniError(f"{label} è obbligatorio.")
    if len(text) > max_length:
        raise VenditeAssegnazioniError(
            f"{label} non può superare {max_length} caratteri."
        )
    return text


def _optional_text(value, label: str, max_length: int) -> str:
    text = _norm_text(value)
    if len(text) > max_length:
        raise VenditeAssegnazioniError(
            f"{label} non può superare {max_length} caratteri."
        )
    return text


def _positive_integer(value, label: str) -> int:
    if isinstance(value, bool):
        raise VenditeAssegnazioniError(f"{label} deve essere un numero intero positivo.")

    if isinstance(value, int):
        number = value
    elif isinstance(value, str) and value.strip().isdigit():
        try:
            number = int(value.strip())
        except ValueError as exc:
            raise VenditeAssegnazioniError(
                f"{label} deve essere un numero intero positivo."
            ) from exc
    else:
        raise VenditeAssegnazioniError(f"{label} deve essere un numero intero positivo.")

    if number <= 0:
        raise VenditeAssegnazioniError(f"{label} deve essere maggiore di zero.")
    return number


def _delivery_date(value, label: str) -> date:
    try:
        parsed = date.fromisoformat(_norm_text(value))
    except ValueError as exc:
        raise VenditeAssegnazioniError(
            f"{label} deve essere una data valida."
        ) from exc
    return parsed


def _optional_date(value, label: str) -> date | None:
    return _delivery_date(value, label) if _norm_text(value) else None


def _model_key(model_code, variant) -> str:
    return json.dumps(
        [
            _norm_text(model_code).casefold(),
            _norm_text(variant).casefold(),
        ],
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _normalized_key(value) -> str:
    return unicodedata.normalize("NFKC", _norm_text(value)).casefold()


def _machine_key(order) -> tuple[str, str]:
    return (
        _norm_text(getattr(order, "IdDocumento", "")),
        _norm_text(getattr(order, "IdRiga", "")),
    )


def _actor(user) -> tuple[int | None, str]:
    return (
        getattr(user, "id", None),
        _norm_text(getattr(user, "username", "")) or "utente",
    )


def _is_stock_machine(machine) -> bool:
    return bool(getattr(machine, "IsStock", False))


def _machine_order_label(machine) -> str:
    return STOCK_LABEL if _is_stock_machine(machine) else _ordine_ref_label(machine)


def _machine_serial(machine) -> str:
    return _norm_text(getattr(machine, "CodMatricola", ""))


def _unique_machines_by_serial(machines) -> list:
    unique = []
    seen_keys = set()
    seen_serials = set()
    for machine in machines:
        key = _machine_key(machine)
        serial_key = _normalized_key(_machine_serial(machine))
        if key in seen_keys or (serial_key and serial_key in seen_serials):
            continue
        unique.append(machine)
        seen_keys.add(key)
        if serial_key:
            seen_serials.add(serial_key)
    return unique


def _customer_rows_for_machine(
    id_documento: str,
    id_riga: str,
    serial_number: str,
    *,
    exclude_row_id: int | None = None,
):
    conditions = [
        (
            (VenditeOrdineClienteRiga.odp_id_documento == id_documento)
            & (VenditeOrdineClienteRiga.odp_id_riga == id_riga)
        )
    ]
    if serial_number:
        conditions.append(VenditeOrdineClienteRiga.odp_matricola == serial_number)
    query = VenditeOrdineClienteRiga.query.filter(or_(*conditions))
    if exclude_row_id is not None:
        query = query.filter(VenditeOrdineClienteRiga.id != exclude_row_id)
    return query.all()


def _matching_stock_record(
    id_documento: str,
    id_riga: str,
    serial_number: str,
) -> VenditeMacchinaStock | None:
    by_key = VenditeMacchinaStock.query.filter_by(
        odp_id_documento=id_documento,
        odp_id_riga=id_riga,
    ).one_or_none()
    by_serial = VenditeMacchinaStock.query.filter_by(
        matricola=serial_number,
    ).one_or_none()
    if by_key is not None and by_key.matricola != serial_number:
        raise VenditeAssegnazioniConflictError(
            "L'ordine macchina risulta già memorizzato nello STOCK con una matricola diversa."
        )
    if by_serial is not None and (
        by_serial.odp_id_documento,
        by_serial.odp_id_riga,
    ) != (id_documento, id_riga):
        raise VenditeAssegnazioniConflictError(
            "La matricola risulta già memorizzata nello STOCK per un altro ordine macchina."
        )
    return by_key or by_serial


def validate_closed_machine_stock(machine) -> bool:
    """Valida i dati necessari al registro locale di una chiusura macchina."""
    id_documento, id_riga = _machine_key(machine)
    if not id_documento or not id_riga:
        raise VenditeAssegnazioniError(
            "Impossibile chiudere la macchina: riferimento dell'ordine incompleto."
        )

    model_code = _norm_text(getattr(machine, "CodArt", ""))
    if not model_code:
        raise VenditeAssegnazioniError(
            "Impossibile chiudere la macchina: il modello non è valorizzato."
        )
    serial_number = _machine_serial(machine)
    if (
        len(serial_number) != 6
        or not serial_number.isascii()
        or not serial_number.isdigit()
    ):
        raise VenditeAssegnazioniError(
            "Impossibile chiudere la macchina: la matricola deve contenere esattamente 6 cifre."
        )

    assigned_by_key = VenditeOrdineClienteRiga.query.filter_by(
        odp_id_documento=id_documento,
        odp_id_riga=id_riga,
    ).first()
    if assigned_by_key is not None and _normalized_key(
        assigned_by_key.odp_matricola
    ) != _normalized_key(serial_number):
        raise VenditeAssegnazioniConflictError(
            "La matricola dell'ordine macchina non coincide con quella già assegnata "
            "all'ordine cliente."
        )

    assigned_serial_query = VenditeOrdineClienteRiga.query.filter_by(
        odp_matricola=serial_number,
    )
    if assigned_by_key is not None:
        assigned_serial_query = assigned_serial_query.filter(
            VenditeOrdineClienteRiga.id != assigned_by_key.id,
        )
    assigned_by_serial = assigned_serial_query.first()
    if assigned_by_serial is not None:
        raise VenditeAssegnazioniConflictError(
            "La matricola risulta già assegnata a un altro ordine cliente."
        )

    _matching_stock_record(id_documento, id_riga, serial_number)
    return True


def register_closed_machine_stock(
    machine,
    *,
    closed_at: str | None = None,
    closed_by: str = "",
) -> VenditeMacchinaStock:
    validate_closed_machine_stock(machine)

    id_documento, id_riga = _machine_key(machine)
    serial_number = _machine_serial(machine)
    model_code = _norm_text(getattr(machine, "CodArt", ""))
    existing = _matching_stock_record(id_documento, id_riga, serial_number)
    if existing is not None:
        return existing

    stock = VenditeMacchinaStock(
        odp_id_documento=id_documento,
        odp_id_riga=id_riga,
        odp_rif_registraz=(
            _norm_text(getattr(machine, "RifRegistraz", "")) or None
        ),
        odp_num_progr_riga=(
            _norm_text(getattr(machine, "NumProgrRiga", "")) or None
        ),
        modello_codice=model_code,
        modello_variante=_norm_text(getattr(machine, "VarianteArt", "")),
        modello_descrizione=(
            _norm_text(getattr(machine, "DesArt", "")) or None
        ),
        matricola=serial_number,
        inserita_il=closed_at or _now_rome_dt().isoformat(timespec="seconds"),
        inserita_da_nome=_norm_text(closed_by) or "operatore",
    )
    db.session.add(stock)
    db.session.flush()
    return stock


def _stock_machine(stock: VenditeMacchinaStock):
    return SimpleNamespace(
        IdDocumento=stock.odp_id_documento,
        IdRiga=stock.odp_id_riga,
        RifRegistraz=STOCK_LABEL,
        NumProgrRiga="",
        CodArt=stock.modello_codice,
        VarianteArt=stock.modello_variante or "",
        DesArt=stock.modello_descrizione or "",
        CodMatricola=stock.matricola,
        GestioneMatricola="si",
        FaseAttiva="2",
        StatoOrdine="Chiusa",
        IsStock=True,
        StockRecord=stock,
    )


def load_stock_machine_orders() -> list:
    return [
        _stock_machine(stock)
        for stock in VenditeMacchinaStock.query.order_by(
            VenditeMacchinaStock.matricola,
            VenditeMacchinaStock.id,
        ).all()
    ]


def load_assignable_machine_orders() -> list:
    return _unique_machines_by_serial(
        load_stock_machine_orders() + load_machine_orders()
    )


def _stock_record(id_documento: str, id_riga: str):
    return VenditeMacchinaStock.query.filter_by(
        odp_id_documento=id_documento,
        odp_id_riga=id_riga,
    ).one_or_none()


def _ensure_no_production_serial_conflict(stock: VenditeMacchinaStock) -> None:
    duplicate = (
        _base_odp_query()
        .filter(
            InputOdp.CodMatricola == stock.matricola,
            or_(
                InputOdp.IdDocumento != stock.odp_id_documento,
                InputOdp.IdRiga != stock.odp_id_riga,
            ),
        )
        .first()
    )
    if duplicate is not None:
        raise VenditeAssegnazioniConflictError(
            "La matricola STOCK risulta presente anche su un altro ordine macchina in produzione. "
            "Correggere il conflitto prima di confermare la spedizione."
        )


def _find_current_machine(id_documento: str, id_riga: str):
    stock = _stock_record(id_documento, id_riga)
    if stock is not None:
        return _stock_machine(stock)
    return (
        _base_odp_query()
        .filter(
            InputOdp.IdDocumento == id_documento,
            InputOdp.IdRiga == id_riga,
        )
        .one_or_none()
    )


def _find_assignable_machine(id_documento: str, id_riga: str):
    machine = _find_current_machine(id_documento, id_riga)
    if machine is None or (
        not _is_stock_machine(machine) and not is_open_machine_order(machine)
    ):
        return None
    return machine


def _remove_shipped_stock(row: VenditeOrdineClienteRiga) -> None:
    stock = _stock_record(row.odp_id_documento, row.odp_id_riga)
    if stock is not None:
        _ensure_no_production_serial_conflict(stock)
        db.session.delete(stock)


def ship_stock_machine(
    id_documento,
    id_riga,
    *,
    commit: bool = False,
) -> VenditeMacchinaStock:
    id_documento = _required_text(id_documento, "IdDocumento", 500)
    id_riga = _required_text(id_riga, "IdRiga", 500)
    stock = _stock_record(id_documento, id_riga)
    if stock is None:
        raise VenditeAssegnazioniConflictError(
            "La matricola STOCK non è più disponibile. Aggiornare la pagina e riprovare."
        )
    if _customer_rows_for_machine(
        id_documento,
        id_riga,
        stock.matricola,
    ):
        raise VenditeAssegnazioniConflictError(
            "La matricola è già assegnata a un ordine cliente e deve essere spedita da quell'ordine."
        )

    _ensure_no_production_serial_conflict(stock)
    db.session.delete(stock)
    db.session.flush()
    if commit:
        db.session.commit()
    return stock


def _internal_reference(value) -> str:
    reference = _norm_text(value).upper() or "ITALIA"
    if reference not in VENDITE_INTERNAL_REFERENCES:
        raise VenditeAssegnazioniError(
            "Il riferimento interno deve essere ITALIA, ESTERO oppure EXTRACEE."
        )
    return reference


def _packaging_notes_by_reference() -> dict[str, str]:
    notes = dict(VENDITE_DEFAULT_PACKAGING_NOTES)
    for item in VenditeNotaImballaggio.query.all():
        if item.riferimento_interno in notes:
            notes[item.riferimento_interno] = item.note or ""
    return notes


def _model_catalog(orders) -> dict[str, dict]:
    catalog = {}
    for order in orders:
        code = _norm_text(getattr(order, "CodArt", ""))
        if not code:
            continue
        variant = _norm_text(getattr(order, "VarianteArt", ""))
        key = _model_key(code, variant)
        catalog.setdefault(
            key,
            {
                "key": key,
                "model_code": code,
                "variant": variant,
                "description": _norm_text(getattr(order, "DesArt", "")),
            },
        )
    return catalog


def _available_model_catalog(open_machines) -> dict[str, dict]:
    catalog = _model_catalog(open_machines)
    known_machine_models = AcqArticoliLookup.query.filter(
        func.lower(
            func.trim(func.coalesce(AcqArticoliLookup.GestioneMatricola, ""))
        )
        == "si"
    ).all()
    for key, model in _model_catalog(known_machine_models).items():
        catalog.setdefault(key, model)
    return catalog


def create_customer_order(payload, user, *, commit: bool = False):
    if not isinstance(payload, dict):
        raise VenditeAssegnazioniError("Dati dell'ordine cliente non validi.")

    customer_name = _required_text(
        payload.get("customer_name"),
        "Il nome cliente",
        MAX_CUSTOMER_NAME,
    )
    customer_order = _required_text(
        payload.get("customer_order"),
        "L'ordine cliente",
        MAX_CUSTOMER_ORDER,
    )
    internal_reference = _internal_reference(payload.get("internal_reference"))
    lines = payload.get("lines")
    if not isinstance(lines, list) or not lines:
        raise VenditeAssegnazioniError("Inserire almeno una riga macchina.")

    customer_key = _normalized_key(customer_name)
    order_key = _normalized_key(customer_order)
    duplicate = VenditeOrdineCliente.query.filter_by(
        cliente_chiave=customer_key,
        numero_ordine_chiave=order_key,
    ).first()
    if duplicate is not None:
        raise VenditeAssegnazioniConflictError(
            "Questo ordine cliente è già presente."
        )

    open_machines = load_assignable_machine_orders()
    catalog = _available_model_catalog(open_machines)
    machines_by_key = {_machine_key(machine): machine for machine in open_machines}
    assigned_snapshots = (
        VenditeOrdineClienteRiga.query.with_entities(
            VenditeOrdineClienteRiga.odp_id_documento,
            VenditeOrdineClienteRiga.odp_id_riga,
            VenditeOrdineClienteRiga.odp_matricola,
        )
        .filter(
            or_(
                VenditeOrdineClienteRiga.odp_id_documento.is_not(None),
                VenditeOrdineClienteRiga.odp_matricola.is_not(None),
            )
        )
        .all()
    )
    already_assigned_keys = {
        (id_documento, id_riga)
        for id_documento, id_riga, _serial_number in assigned_snapshots
        if id_documento and id_riga
    }
    already_assigned_serials = {
        _normalized_key(serial_number)
        for _id_documento, _id_riga, serial_number in assigned_snapshots
        if _norm_text(serial_number)
    }
    selected_machine_keys = set()
    selected_machine_serials = set()
    expanded_rows = []
    total_quantity = 0

    for index, raw_line in enumerate(lines, start=1):
        if not isinstance(raw_line, dict):
            raise VenditeAssegnazioniError(f"Riga {index}: dati non validi.")

        model_key = _required_text(
            raw_line.get("model_key"),
            f"Riga {index}: il modello",
            500,
        )
        model = catalog.get(model_key)
        if model is None:
            raise VenditeAssegnazioniError(
                f"Riga {index}: il modello selezionato non è disponibile in produzione."
            )

        quantity = _positive_integer(
            raw_line.get("quantity"),
            f"Riga {index}: la quantità",
        )
        total_quantity += quantity
        if total_quantity > MAX_EXPANDED_ROWS:
            raise VenditeAssegnazioniError(
                f"Un ordine cliente può contenere al massimo {MAX_EXPANDED_ROWS} macchine."
            )

        note = _optional_text(
            raw_line.get("sales_note", raw_line.get("note")),
            f"Riga {index}: le note di vendita",
            MAX_NOTE,
        )
        delivery = _delivery_date(
            raw_line.get("delivery_date")
            or payload.get("shipping_date")
            or payload.get("delivery_date"),
            f"Riga {index}: la data di consegna",
        )
        id_documento = _norm_text(raw_line.get("id_documento"))
        id_riga = _norm_text(raw_line.get("id_riga"))
        if bool(id_documento) != bool(id_riga):
            raise VenditeAssegnazioniError(
                f"Riga {index}: il riferimento della matricola non è completo."
            )

        selected_machine = None
        if id_documento:
            machine_key = (id_documento, id_riga)
            selected_machine = machines_by_key.get(machine_key)
            selected_serial = _normalized_key(_machine_serial(selected_machine))
            if (
                selected_machine is None
                or machine_key in already_assigned_keys
                or selected_serial in already_assigned_serials
            ):
                raise VenditeAssegnazioniConflictError(
                    f"Riga {index}: la matricola selezionata non è più disponibile."
                )
            if not selected_serial:
                raise VenditeAssegnazioniConflictError(
                    f"Riga {index}: la matricola selezionata non è disponibile."
                )
            if (
                machine_key in selected_machine_keys
                or selected_serial in selected_machine_serials
            ):
                raise VenditeAssegnazioniConflictError(
                    f"Riga {index}: la matricola è già selezionata in questo ordine."
                )
            if _model_key(
                selected_machine.CodArt,
                selected_machine.VarianteArt,
            ) != model_key:
                raise VenditeAssegnazioniConflictError(
                    f"Riga {index}: la matricola non appartiene al modello selezionato."
                )
            if quantity != 1:
                raise VenditeAssegnazioniError(
                    f"Riga {index}: con una matricola selezionata la quantità deve essere 1."
                )
            selected_machine_keys.add(machine_key)
            selected_machine_serials.add(selected_serial)

        expanded_rows.append((model, note, delivery, selected_machine))
        expanded_rows.extend(
            (model, note, delivery, None) for _ in range(quantity - 1)
        )

    shipping_date = min(item[2] for item in expanded_rows)
    packaging_note = _packaging_notes_by_reference()[internal_reference]

    actor_id, actor_name = _actor(user)
    customer = VenditeOrdineCliente(
        cliente_nome=customer_name,
        cliente_chiave=customer_key,
        numero_ordine=customer_order,
        numero_ordine_chiave=order_key,
        riferimento_interno=internal_reference,
        data_spedizione=shipping_date,
        creato_da_id=actor_id,
        creato_da_nome=actor_name,
    )
    customer.righe = [
        VenditeOrdineClienteRiga(
            posizione=position,
            modello_codice=model["model_code"],
            modello_variante=model["variant"],
            modello_descrizione=model["description"] or None,
            note=note or None,
            note_spedizione=packaging_note or None,
            data_disponibile=None,
            data_consegna=delivery,
        )
        for position, (model, note, delivery, _machine) in enumerate(
            expanded_rows,
            start=1,
        )
    ]
    db.session.add(customer)
    db.session.flush()
    for row, (_model, _note, _delivery, machine) in zip(
        customer.righe,
        expanded_rows,
    ):
        if machine is not None:
            set_machine_assignment(
                row.id,
                {
                    "version": row.versione,
                    "id_documento": machine.IdDocumento,
                    "id_riga": machine.IdRiga,
                },
                user,
            )
    db.session.flush()
    if commit:
        db.session.commit()
    return customer


def _customer_row(row_id: int) -> VenditeOrdineClienteRiga:
    row = db.session.get(VenditeOrdineClienteRiga, row_id)
    if row is None:
        raise VenditeAssegnazioniError("Riga dell'ordine cliente non trovata.")
    return row


def _check_row_version(row: VenditeOrdineClienteRiga, payload) -> None:
    expected_version = _positive_integer(
        payload.get("version"),
        "La versione della riga",
    )
    if expected_version != row.versione:
        raise VenditeAssegnazioniConflictError(
            "La riga è stata modificata da un altro operatore. Aggiornare la pagina."
        )


def _clear_assignment(row: VenditeOrdineClienteRiga) -> None:
    row.odp_id_documento = None
    row.odp_id_riga = None
    row.odp_rif_registraz = None
    row.odp_num_progr_riga = None
    row.odp_matricola = None
    row.assegnata_il = None
    row.assegnata_da_id = None
    row.assegnata_da_nome = None
    row.assegnazione_automatica = False


def _assign_machine_snapshot(
    row: VenditeOrdineClienteRiga,
    machine,
    *,
    actor_id: int | None,
    actor_name: str,
    automatic: bool,
) -> None:
    row.odp_id_documento = _norm_text(machine.IdDocumento)
    row.odp_id_riga = _norm_text(machine.IdRiga)
    row.odp_rif_registraz = _norm_text(machine.RifRegistraz) or None
    row.odp_num_progr_riga = _norm_text(machine.NumProgrRiga) or None
    row.odp_matricola = _norm_text(machine.CodMatricola) or None
    row.assegnata_il = _now_rome_dt().isoformat(timespec="seconds")
    row.assegnata_da_id = actor_id
    row.assegnata_da_nome = actor_name
    row.assegnazione_automatica = automatic


def auto_assign_activated_machine(machine, *, phase) -> VenditeOrdineClienteRiga | None:
    if (
        _fase_to_int(phase) not in {1, 2}
        or _norm_text(getattr(machine, "GestioneMatricola", "")).casefold()
        != "si"
        or not _norm_text(getattr(machine, "CodMatricola", ""))
    ):
        return None

    stock = VenditeMacchinaStock.query.filter_by(
        matricola=_machine_serial(machine),
    ).first()
    if stock is not None:
        machine = _stock_machine(stock)

    machine_key = _machine_key(machine)
    if not all(machine_key):
        return None
    serial_number = _machine_serial(machine)
    if _customer_rows_for_machine(
        machine_key[0],
        machine_key[1],
        serial_number,
    ):
        return None

    model_key = _model_key(machine.CodArt, machine.VarianteArt)
    today = _now_rome_dt().date()
    candidates = [
        row
        for row in VenditeOrdineClienteRiga.query.filter(
            VenditeOrdineClienteRiga.odp_id_documento.is_(None),
            VenditeOrdineClienteRiga.odp_id_riga.is_(None),
        ).all()
        if _model_key(row.modello_codice, row.modello_variante) == model_key
    ]
    if not candidates:
        return None

    row = min(
        candidates,
        key=lambda item: (
            abs((item.data_consegna - today).days),
            item.data_consegna,
            item.id,
        ),
    )
    _assign_machine_snapshot(
        row,
        machine,
        actor_id=None,
        actor_name="Assegnazione automatica",
        automatic=True,
    )
    db.session.flush()
    return row


def set_machine_assignment(
    row_id: int,
    payload,
    user,
    *,
    commit: bool = False,
):
    if not isinstance(payload, dict):
        raise VenditeAssegnazioniError("Dati dell'assegnazione non validi.")

    row = _customer_row(row_id)
    _check_row_version(row, payload)
    id_documento = _norm_text(payload.get("id_documento"))
    id_riga = _norm_text(payload.get("id_riga"))

    if not id_documento and not id_riga:
        _clear_assignment(row)
        db.session.flush()
        if commit:
            db.session.commit()
        return row

    if not id_documento or not id_riga:
        raise VenditeAssegnazioniError(
            "Il riferimento della matricola non è completo."
        )

    machine = _find_assignable_machine(id_documento, id_riga)
    if machine is None:
        raise VenditeAssegnazioniConflictError(
            "La matricola non è più disponibile. Aggiornare la pagina e riprovare."
        )
    serial_number = _machine_serial(machine)
    if not serial_number:
        raise VenditeAssegnazioniConflictError(
            "La matricola selezionata non è disponibile."
        )

    if _model_key(machine.CodArt, machine.VarianteArt) != _model_key(
        row.modello_codice,
        row.modello_variante,
    ):
        raise VenditeAssegnazioniConflictError(
            "Il modello della matricola non corrisponde alla richiesta cliente."
        )

    already_assigned_rows = _customer_rows_for_machine(
        id_documento,
        id_riga,
        serial_number,
        exclude_row_id=row.id,
    )
    for already_assigned in already_assigned_rows:
        _clear_assignment(already_assigned)
    if already_assigned_rows:
        db.session.flush()

    actor_id, actor_name = _actor(user)
    _assign_machine_snapshot(
        row,
        machine,
        actor_id=actor_id,
        actor_name=actor_name,
        automatic=False,
    )
    db.session.flush()
    if commit:
        db.session.commit()
    return row


def _snapshot_order_label(row: VenditeOrdineClienteRiga) -> str:
    if _norm_text(row.odp_rif_registraz) == STOCK_LABEL:
        return STOCK_LABEL
    return format_ordine_ref_display(
        row.odp_rif_registraz,
        row.odp_num_progr_riga,
        row.odp_id_riga,
    ) or " ".join(
        value
        for value in (row.odp_id_documento, row.odp_id_riga)
        if value
    )


def _phase_two_closed_keys(order_keys) -> set[tuple[str, str]]:
    keys = sorted(
        {
            (_norm_text(id_documento), _norm_text(id_riga))
            for id_documento, id_riga in order_keys
            if _norm_text(id_documento) and _norm_text(id_riga)
        }
    )
    latest = {}
    for start in range(0, len(keys), LOG_QUERY_BATCH_SIZE):
        batch = keys[start : start + LOG_QUERY_BATCH_SIZE]
        logs = (
            InputOdpLog.query.with_entities(
                InputOdpLog.IdDocumento,
                InputOdpLog.IdRiga,
                InputOdpLog.FaseConsuntivata,
                InputOdpLog.FaseAttiva,
                InputOdpLog.StatoOrdinePost,
            )
            .filter(tuple_(InputOdpLog.IdDocumento, InputOdpLog.IdRiga).in_(batch))
            .order_by(InputOdpLog.log_id.desc())
            .all()
        )
        for log in logs:
            key = (_norm_text(log.IdDocumento), _norm_text(log.IdRiga))
            latest.setdefault(key, log)

    return {
        key
        for key, log in latest.items()
        if _fase_to_int(log.FaseConsuntivata or log.FaseAttiva) == 2
        and _canonical_state(log.StatoOrdinePost).casefold() == "chiusa"
    }


def _assignment_payload(
    row,
    current_machine,
    *,
    completed_from_log: bool = False,
) -> dict | None:
    if not row.odp_id_documento or not row.odp_id_riga:
        return None

    is_stock = current_machine is not None and _is_stock_machine(current_machine)
    assigned_from_stock = (
        is_stock and _norm_text(row.odp_rif_registraz) == STOCK_LABEL
    )
    machine_is_open = current_machine is not None and (
        is_stock or is_open_machine_order(current_machine)
    )
    machine_is_completed = is_stock or completed_from_log or bool(
        current_machine is not None
        and _fase_to_int(getattr(current_machine, "FaseAttiva", "")) == 2
        and _canonical_state(getattr(current_machine, "StatoOrdine", "")).casefold()
        == "chiusa"
    )
    return {
        "id_documento": row.odp_id_documento,
        "id_riga": row.odp_id_riga,
        "order": (
            (
                _machine_order_label(current_machine)
                if not is_stock or assigned_from_stock
                else _snapshot_order_label(row)
            )
            if current_machine is not None
            else _snapshot_order_label(row)
        ),
        "serial_number": (
            _norm_text(getattr(current_machine, "CodMatricola", ""))
            if current_machine is not None
            else _norm_text(row.odp_matricola)
        )
        or "Non assegnata",
        "phase": (
            _phase_label(getattr(current_machine, "FaseAttiva", ""))
            if current_machine is not None
            else "2" if completed_from_log else ""
        ),
        "state": (
            _canonical_state(getattr(current_machine, "StatoOrdine", ""))
            if current_machine is not None
            else "Chiusa" if completed_from_log else "Non presente"
        ),
        "present": current_machine is not None,
        "open": machine_is_open,
        "completed": machine_is_completed,
        "automatic": bool(row.assegnazione_automatica),
    }


def _apply_note_updates(
    row: VenditeOrdineClienteRiga,
    payload,
    *,
    can_edit_sales: bool,
    can_edit_production: bool,
) -> None:
    updated = False
    if can_edit_sales:
        if "sales_note" in payload:
            row.note = _optional_text(
                payload.get("sales_note"),
                "Le note di vendita",
                MAX_NOTE,
            ) or None
            updated = True
        if "shipping_note" in payload:
            row.note_spedizione = _optional_text(
            payload.get("shipping_note"),
            "Le note di imballaggio",
                MAX_NOTE,
            ) or None
            updated = True
    if can_edit_production and "production_note" in payload:
        row.note_produzione = _optional_text(
            payload.get("production_note"),
            "Le note di produzione",
            MAX_NOTE,
        ) or None
        updated = True
    if not updated:
        raise VenditeAssegnazioniError(
            "Nessuna nota modificabile ricevuta."
        )


def update_customer_row_notes(
    row_id: int,
    payload,
    *,
    can_edit_sales: bool,
    can_edit_production: bool,
    commit: bool = False,
) -> VenditeOrdineClienteRiga:
    if not isinstance(payload, dict):
        raise VenditeAssegnazioniError("Dati delle note non validi.")

    row = _customer_row(row_id)
    _check_row_version(row, payload)
    _apply_note_updates(
        row,
        payload,
        can_edit_sales=can_edit_sales,
        can_edit_production=can_edit_production,
    )
    db.session.flush()
    if commit:
        db.session.commit()
    return row


def confirm_customer_row_shipment(
    row_id: int,
    payload,
    user,
    *,
    can_edit_production: bool = False,
    check_version: bool = True,
    commit: bool = False,
) -> VenditeSpedizioneConfermata:
    if not isinstance(payload, dict):
        raise VenditeAssegnazioniError("Dati della spedizione non validi.")

    row = _customer_row(row_id)
    if check_version:
        _check_row_version(row, payload)
    if row.spedizione is not None:
        raise VenditeAssegnazioniConflictError(
            "La spedizione di questa macchina è già stata confermata."
        )

    machine = None
    if row.odp_id_documento and row.odp_id_riga:
        machine = _find_current_machine(
            row.odp_id_documento,
            row.odp_id_riga,
        )
    key = (row.odp_id_documento, row.odp_id_riga)
    assignment = _assignment_payload(
        row,
        machine,
        completed_from_log=(
            machine is None and key in _phase_two_closed_keys([key])
        ),
    )
    if not assignment or not assignment["completed"]:
        raise VenditeAssegnazioniConflictError(
            "La spedizione può essere confermata solo per una macchina con Fase 2 chiusa."
        )

    _apply_note_updates(
        row,
        payload,
        can_edit_sales=True,
        can_edit_production=can_edit_production,
    )
    if "delivery_date" in payload:
        row.data_consegna = _delivery_date(
            payload.get("delivery_date"),
            "La data di consegna",
        )
    if can_edit_production and "available_date" in payload:
        row.data_disponibile = _optional_date(
            payload.get("available_date"),
            "La data disponibile",
        )
    row.ordine_cliente.data_spedizione = min(
        item.data_consegna for item in row.ordine_cliente.righe
    )
    actor_id, actor_name = _actor(user)
    shipment = VenditeSpedizioneConfermata(
        riga_ordine_cliente=row,
        cliente_nome=row.ordine_cliente.cliente_nome,
        numero_ordine=row.ordine_cliente.numero_ordine,
        riferimento_interno=row.ordine_cliente.riferimento_interno,
        data_spedizione=row.ordine_cliente.data_spedizione,
        data_disponibile=row.data_disponibile,
        ordine_cliente_creato_il=row.ordine_cliente.creato_il,
        ordine_cliente_creato_da_nome=row.ordine_cliente.creato_da_nome,
        posizione=row.posizione,
        modello_codice=row.modello_codice,
        modello_variante=row.modello_variante or "",
        modello_descrizione=row.modello_descrizione,
        data_consegna=row.data_consegna,
        note_vendita=row.note,
        note_produzione=row.note_produzione,
        note_spedizione=row.note_spedizione,
        odp_id_documento=row.odp_id_documento,
        odp_id_riga=row.odp_id_riga,
        odp_rif_registraz=row.odp_rif_registraz,
        odp_num_progr_riga=row.odp_num_progr_riga,
        odp_matricola=row.odp_matricola or "",
        assegnata_il=row.assegnata_il,
        assegnata_da_nome=row.assegnata_da_nome,
        assegnazione_automatica=bool(row.assegnazione_automatica),
        confermata_il=_now_rome_dt().isoformat(timespec="seconds"),
        confermata_da_id=actor_id,
        confermata_da_nome=actor_name,
    )
    db.session.add(shipment)
    _remove_shipped_stock(row)
    db.session.flush()
    if commit:
        db.session.commit()
    return shipment


def confirm_customer_order_shipment(
    order_id: int,
    payload,
    user,
    *,
    can_edit_production: bool = False,
    commit: bool = False,
) -> VenditeOrdineCliente:
    if not isinstance(payload, dict) or not isinstance(payload.get("rows"), list):
        raise VenditeAssegnazioniError("Dati della spedizione non validi.")

    customer = db.session.get(VenditeOrdineCliente, order_id)
    if customer is None:
        raise VenditeAssegnazioniError("Ordine cliente non trovato.")

    rows_payload = {
        int(item["id"]): item
        for item in payload["rows"]
        if isinstance(item, dict) and str(item.get("id", "")).isdigit()
    }
    pending_rows = [row for row in customer.righe if row.spedizione is None]
    if not pending_rows:
        raise VenditeAssegnazioniConflictError(
            "La spedizione di questo ordine è già stata confermata."
        )
    if any(row.id not in rows_payload for row in pending_rows):
        raise VenditeAssegnazioniError(
            "Mancano i dati di una o più macchine dell'ordine."
        )

    # Applica prima tutte le date: ogni snapshot della spedizione deve riportare
    # la stessa data finale dell'ordine, anche se più righe vengono aggiornate insieme.
    for row in pending_rows:
        row_payload = rows_payload[row.id]
        _check_row_version(row, row_payload)
        if "delivery_date" in row_payload:
            row.data_consegna = _delivery_date(
                row_payload.get("delivery_date"),
                "La data di consegna",
            )
        if can_edit_production and "available_date" in row_payload:
            row.data_disponibile = _optional_date(
                row_payload.get("available_date"),
                "La data disponibile",
            )
    customer.data_spedizione = min(row.data_consegna for row in customer.righe)

    for row in pending_rows:
        confirm_customer_row_shipment(
            row.id,
            rows_payload[row.id],
            user,
            can_edit_production=can_edit_production,
            check_version=False,
        )
    if commit:
        db.session.commit()
    return customer


def confirm_customer_order_read(
    order_id: int,
    user,
    *,
    commit: bool = False,
) -> VenditeOrdineCliente:
    customer = db.session.get(VenditeOrdineCliente, order_id)
    if customer is None:
        raise VenditeAssegnazioniError("Ordine cliente non trovato.")

    if not customer.confermato_il:
        _actor_id, actor_name = _actor(user)
        customer.confermato_il = _now_rome_dt().isoformat(timespec="seconds")
        customer.confermato_da_nome = actor_name
        db.session.flush()
    if commit:
        db.session.commit()
    return customer


def delete_customer_order(
    order_id: int,
    *,
    commit: bool = False,
) -> VenditeOrdineCliente:
    customer = db.session.get(VenditeOrdineCliente, order_id)
    if customer is None:
        raise VenditeAssegnazioniError("Ordine cliente non trovato.")

    db.session.delete(customer)
    db.session.flush()
    if commit:
        db.session.commit()
    return customer


def update_customer_order_details(
    order_id: int,
    payload,
    *,
    commit: bool = False,
) -> VenditeOrdineCliente:
    if not isinstance(payload, dict):
        raise VenditeAssegnazioniError("Dati dell'ordine cliente non validi.")

    customer = db.session.get(VenditeOrdineCliente, order_id)
    if customer is None:
        raise VenditeAssegnazioniError("Ordine cliente non trovato.")

    new_reference = _internal_reference(payload.get("internal_reference"))
    old_reference = customer.riferimento_interno or "ITALIA"
    if new_reference != old_reference:
        packaging_notes = _packaging_notes_by_reference()
        old_default = _norm_text(packaging_notes.get(old_reference))
        new_default = packaging_notes[new_reference]
        for row in customer.righe:
            current_note = _norm_text(row.note_spedizione)
            if not current_note or current_note == old_default:
                row.note_spedizione = new_default or None

    customer.riferimento_interno = new_reference
    db.session.flush()
    if commit:
        db.session.commit()
    return customer


def update_customer_row_dates(
    row_id: int,
    payload,
    *,
    can_edit_delivery: bool,
    can_edit_available: bool,
    commit: bool = False,
) -> VenditeOrdineClienteRiga:
    if not isinstance(payload, dict):
        raise VenditeAssegnazioniError("Dati delle date non validi.")

    row = _customer_row(row_id)
    _check_row_version(row, payload)

    updated = False
    if can_edit_delivery and "delivery_date" in payload:
        row.data_consegna = _delivery_date(
            payload.get("delivery_date"),
            "La data di consegna",
        )
        updated = True
    if can_edit_available and "available_date" in payload:
        row.data_disponibile = _optional_date(
            payload.get("available_date"),
            "La data disponibile",
        )
        updated = True
    if not updated:
        raise VenditeAssegnazioniError("Nessuna data modificabile ricevuta.")

    row.ordine_cliente.data_spedizione = min(
        item.data_consegna for item in row.ordine_cliente.righe
    )
    db.session.flush()
    if commit:
        db.session.commit()
    return row


def update_customer_row(
    row_id: int,
    payload,
    user,
    *,
    can_edit_sales: bool,
    can_edit_production: bool,
    can_assign: bool,
    commit: bool = False,
) -> VenditeOrdineClienteRiga:
    if not isinstance(payload, dict):
        raise VenditeAssegnazioniError("Dati della riga non validi.")

    work_payload = dict(payload)
    row = _customer_row(row_id)
    _check_row_version(row, work_payload)
    updated = False

    if (can_edit_sales and "delivery_date" in work_payload) or (
        can_edit_production and "available_date" in work_payload
    ):
        row = update_customer_row_dates(
            row_id,
            work_payload,
            can_edit_delivery=can_edit_sales,
            can_edit_available=can_edit_production,
        )
        work_payload["version"] = row.versione
        updated = True

    if (
        can_edit_sales
        and ({"sales_note", "shipping_note"} & work_payload.keys())
    ) or (can_edit_production and "production_note" in work_payload):
        row = update_customer_row_notes(
            row_id,
            work_payload,
            can_edit_sales=can_edit_sales,
            can_edit_production=can_edit_production,
        )
        work_payload["version"] = row.versione
        updated = True

    if can_assign and work_payload.get("assignment_changed"):
        row = set_machine_assignment(row_id, work_payload, user)
        updated = True

    if not updated:
        raise VenditeAssegnazioniError("Nessun dato modificabile ricevuto.")
    if commit:
        db.session.commit()
    return row


def update_packaging_notes(
    payload,
    user,
    *,
    commit: bool = False,
) -> list[VenditeNotaImballaggio]:
    if not isinstance(payload, dict) or not isinstance(
        payload.get("notes"),
        dict,
    ):
        raise VenditeAssegnazioniError("Note di imballaggio non valide.")

    raw_notes = payload["notes"]
    if set(raw_notes) - set(VENDITE_INTERNAL_REFERENCES):
        raise VenditeAssegnazioniError("Riferimento interno non valido.")

    actor_id, actor_name = _actor(user)
    updated_at = _now_rome_dt().isoformat(timespec="seconds")
    items = []
    for reference in VENDITE_INTERNAL_REFERENCES:
        note = _optional_text(
            raw_notes.get(reference),
            f"Le note di imballaggio {reference}",
            MAX_NOTE,
        )
        item = db.session.get(VenditeNotaImballaggio, reference)
        old_default = _norm_text(
            item.note
            if item is not None
            else VENDITE_DEFAULT_PACKAGING_NOTES[reference]
        )
        for row in (
            VenditeOrdineClienteRiga.query.join(VenditeOrdineCliente)
            .filter(VenditeOrdineCliente.riferimento_interno == reference)
            .all()
        ):
            current_note = _norm_text(row.note_spedizione)
            if not current_note or current_note == old_default:
                row.note_spedizione = note or None
        if item is None:
            item = VenditeNotaImballaggio(riferimento_interno=reference)
            db.session.add(item)
        item.note = note or None
        item.aggiornato_il = updated_at
        item.aggiornato_da_id = actor_id
        item.aggiornato_da_nome = actor_name
        items.append(item)

    db.session.flush()
    if commit:
        db.session.commit()
    return items


def build_assignment_dashboard() -> dict:
    production_machines = load_machine_orders(include_closed=True)
    stock_machines = load_stock_machine_orders()
    all_machines = _unique_machines_by_serial(stock_machines + production_machines)
    all_machine_map = {_machine_key(machine): machine for machine in all_machines}
    all_machine_by_serial = {
        _normalized_key(_machine_serial(machine)): machine
        for machine in all_machines
        if _machine_serial(machine)
    }
    open_machines = _unique_machines_by_serial(
        stock_machines
        + [
            machine
            for machine in production_machines
            if is_open_machine_order(machine)
        ]
    )
    customer_orders = (
        VenditeOrdineCliente.query.options(
            selectinload(VenditeOrdineCliente.righe).selectinload(
                VenditeOrdineClienteRiga.spedizione
            ),
        )
        .order_by(
            VenditeOrdineCliente.creato_il.desc(),
            VenditeOrdineCliente.id.desc(),
        )
        .all()
    )
    missing_assigned_keys = {
        key
        for customer in customer_orders
        for row in customer.righe
        for key in [(row.odp_id_documento, row.odp_id_riga)]
        if row.odp_id_documento
        and row.odp_id_riga
        and key not in all_machine_map
        and _normalized_key(row.odp_matricola) not in all_machine_by_serial
    }
    completed_missing_keys = _phase_two_closed_keys(missing_assigned_keys)

    assigned_by_machine = {}
    assigned_by_serial = {}
    total_demand = 0
    assigned_demand = 0
    customer_payload = []

    for customer in customer_orders:
        rows_payload = []
        customer_assigned = 0
        customer_completed = 0
        for row in customer.righe:
            total_demand += 1
            current_machine = all_machine_map.get(
                (row.odp_id_documento, row.odp_id_riga)
            )
            if current_machine is None and row.odp_matricola:
                current_machine = all_machine_by_serial.get(
                    _normalized_key(row.odp_matricola)
                )
            assignment = _assignment_payload(
                row,
                current_machine,
                completed_from_log=(
                    (row.odp_id_documento, row.odp_id_riga)
                    in completed_missing_keys
                ),
            )
            if assignment is not None:
                assigned_demand += 1
                customer_assigned += 1
                assigned_by_machine[(row.odp_id_documento, row.odp_id_riga)] = (
                    customer,
                    row,
                )
                if row.odp_matricola:
                    assigned_by_serial[_normalized_key(row.odp_matricola)] = (
                        customer,
                        row,
                    )
            shipment = row.spedizione
            completed = bool(
                shipment is not None or assignment and assignment["completed"]
            )
            if completed:
                customer_completed += 1

            rows_payload.append(
                {
                    "id": row.id,
                    "position": row.posizione,
                    "version": row.versione,
                    "model_key": _model_key(
                        row.modello_codice,
                        row.modello_variante,
                    ),
                    "model_code": row.modello_codice,
                    "variant": row.modello_variante or "",
                    "description": row.modello_descrizione or "",
                    "sales_note": row.note or "",
                    "production_note": row.note_produzione or "",
                    "shipping_note": row.note_spedizione or "",
                    "available_date": (
                        row.data_disponibile.isoformat()
                        if row.data_disponibile
                        else ""
                    ),
                    "delivery_date": row.data_consegna.isoformat(),
                    "completed": completed,
                    "shipment": (
                        {
                            "confirmed_at": shipment.confermata_il,
                            "confirmed_by_name": shipment.confermata_da_nome,
                        }
                        if shipment is not None
                        else None
                    ),
                    "assignment": assignment,
                }
            )

        customer_payload.append(
            {
                "id": customer.id,
                "customer_name": customer.cliente_nome,
                "customer_order": customer.numero_ordine,
                "internal_reference": customer.riferimento_interno or "ITALIA",
                "created_at": customer.creato_il,
                "created_by_name": customer.creato_da_nome,
                "read_confirmed": bool(customer.confermato_il),
                "confirmed_at": customer.confermato_il or "",
                "confirmed_by_name": customer.confermato_da_nome or "",
                "total_rows": len(customer.righe),
                "assigned_rows": customer_assigned,
                "completed_rows": customer_completed,
                "completed": bool(
                    customer.righe
                    and customer_completed == len(customer.righe)
                ),
                "shipment_ready": bool(
                    any(row["shipment"] is None for row in rows_payload)
                    and all(
                        row["completed"]
                        for row in rows_payload
                        if row["shipment"] is None
                    )
                ),
                "rows": rows_payload,
            }
        )

    assignment_machines_payload = []
    for machine in open_machines:
        key = _machine_key(machine)
        assigned = assigned_by_machine.get(key) or assigned_by_serial.get(
            _normalized_key(_machine_serial(machine))
        )
        assigned_customer, assigned_row = assigned or (None, None)
        assignment_machines_payload.append(
            {
                "id_documento": key[0],
                "id_riga": key[1],
                "order": _machine_order_label(machine),
                "is_stock": _is_stock_machine(machine),
                "model_key": _model_key(machine.CodArt, machine.VarianteArt),
                "model_code": _norm_text(machine.CodArt),
                "variant": _norm_text(machine.VarianteArt),
                "description": _norm_text(machine.DesArt),
                "serial_number": _norm_text(machine.CodMatricola)
                or "Non assegnata",
                "has_serial": bool(_norm_text(machine.CodMatricola)),
                "phase": _phase_label(machine.FaseAttiva),
                "state": _canonical_state(machine.StatoOrdine),
                "assigned": assigned is not None,
                "assigned_row_id": assigned_row.id if assigned_row else None,
                "assigned_customer_name": (
                    assigned_customer.cliente_nome if assigned_customer else ""
                ),
                "assigned_customer_order": (
                    assigned_customer.numero_ordine if assigned_customer else ""
                ),
            }
        )

    assignment_machines_payload.sort(
        key=lambda item: (
            item["model_code"].casefold(),
            item["variant"].casefold(),
            item["serial_number"].casefold(),
            item["order"].casefold(),
        )
    )
    machines_payload = [
        machine
        for machine in assignment_machines_payload
        if not machine["assigned"]
    ]
    models = sorted(
        _available_model_catalog(open_machines).values(),
        key=lambda item: (
            item["model_code"].casefold(),
            item["variant"].casefold(),
            item["description"].casefold(),
        ),
    )
    configured_notes = {
        item.riferimento_interno: item
        for item in VenditeNotaImballaggio.query.all()
    }

    return {
        "generated_at": _now_rome_dt().isoformat(timespec="seconds"),
        "summary": {
            "open_machines": len(machines_payload),
            "total_demand": total_demand,
            "assigned_demand": assigned_demand,
            "unassigned_demand": total_demand - assigned_demand,
        },
        "models": models,
        "machines": machines_payload,
        "assignment_machines": assignment_machines_payload,
        "customer_orders": customer_payload,
        "packaging_notes": [
            {
                "reference": reference,
                "note": (
                    configured_notes[reference].note or ""
                    if reference in configured_notes
                    else VENDITE_DEFAULT_PACKAGING_NOTES[reference]
                ),
                "updated_at": (
                    configured_notes[reference].aggiornato_il or ""
                    if reference in configured_notes
                    else ""
                ),
                "updated_by_name": (
                    configured_notes[reference].aggiornato_da_nome or ""
                    if reference in configured_notes
                    else ""
                ),
            }
            for reference in VENDITE_INTERNAL_REFERENCES
        ],
    }
