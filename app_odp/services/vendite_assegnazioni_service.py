import json
import unicodedata
from datetime import date

from sqlalchemy.orm import selectinload

from app_odp.models import InputOdp, db
from app_odp.ordine_ref import format_ordine_ref_display
from app_odp.services.order_helpers import (
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
    VenditeOrdineCliente,
    VenditeOrdineClienteRiga,
)


MAX_CUSTOMER_NAME = 160
MAX_CUSTOMER_ORDER = 120
MAX_NOTE = 1000
MAX_EXPANDED_ROWS = 500


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

    catalog = _model_catalog(load_machine_orders())
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
            raw_line.get("note"),
            f"Riga {index}: le note",
            MAX_NOTE,
        )
        delivery = _delivery_date(
            raw_line.get("delivery_date"),
            f"Riga {index}: la data di consegna",
        )
        expanded_rows.extend((model, note, delivery) for _ in range(quantity))

    actor_id, actor_name = _actor(user)
    customer = VenditeOrdineCliente(
        cliente_nome=customer_name,
        cliente_chiave=customer_key,
        numero_ordine=customer_order,
        numero_ordine_chiave=order_key,
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
            data_consegna=delivery,
        )
        for position, (model, note, delivery) in enumerate(expanded_rows, start=1)
    ]
    db.session.add(customer)
    db.session.flush()
    if commit:
        db.session.commit()
    return customer


def _customer_row(row_id: int) -> VenditeOrdineClienteRiga:
    row = db.session.get(VenditeOrdineClienteRiga, row_id)
    if row is None:
        raise VenditeAssegnazioniError("Riga dell'ordine cliente non trovata.")
    return row


def _clear_assignment(row: VenditeOrdineClienteRiga) -> None:
    row.odp_id_documento = None
    row.odp_id_riga = None
    row.odp_rif_registraz = None
    row.odp_num_progr_riga = None
    row.odp_matricola = None
    row.assegnata_il = None
    row.assegnata_da_id = None
    row.assegnata_da_nome = None


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
    expected_version = _positive_integer(
        payload.get("version"),
        "La versione della riga",
    )
    if expected_version != row.versione:
        raise VenditeAssegnazioniConflictError(
            "La riga è stata modificata da un altro operatore. Aggiornare la pagina."
        )
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
            "Il riferimento dell'ordine macchina non è completo."
        )

    machine = (
        _base_odp_query()
        .filter(
            InputOdp.IdDocumento == id_documento,
            InputOdp.IdRiga == id_riga,
        )
        .one_or_none()
    )
    if machine is None or not is_open_machine_order(machine):
        raise VenditeAssegnazioniConflictError(
            "L'ordine macchina non è più disponibile. Aggiornare la pagina e riprovare."
        )

    if _model_key(machine.CodArt, machine.VarianteArt) != _model_key(
        row.modello_codice,
        row.modello_variante,
    ):
        raise VenditeAssegnazioniConflictError(
            "Il modello dell'ordine macchina non corrisponde alla richiesta cliente."
        )

    already_assigned = VenditeOrdineClienteRiga.query.filter(
        VenditeOrdineClienteRiga.odp_id_documento == id_documento,
        VenditeOrdineClienteRiga.odp_id_riga == id_riga,
        VenditeOrdineClienteRiga.id != row.id,
    ).first()
    if already_assigned is not None:
        raise VenditeAssegnazioniConflictError(
            "L'ordine macchina è già assegnato a un altro ordine cliente."
        )

    actor_id, actor_name = _actor(user)
    row.odp_id_documento = id_documento
    row.odp_id_riga = id_riga
    row.odp_rif_registraz = _norm_text(machine.RifRegistraz) or None
    row.odp_num_progr_riga = _norm_text(machine.NumProgrRiga) or None
    row.odp_matricola = _norm_text(machine.CodMatricola) or None
    row.assegnata_il = _now_rome_dt().isoformat(timespec="seconds")
    row.assegnata_da_id = actor_id
    row.assegnata_da_nome = actor_name
    db.session.flush()
    if commit:
        db.session.commit()
    return row


def _snapshot_order_label(row: VenditeOrdineClienteRiga) -> str:
    return format_ordine_ref_display(
        row.odp_rif_registraz,
        row.odp_num_progr_riga,
        row.odp_id_riga,
    ) or " ".join(
        value
        for value in (row.odp_id_documento, row.odp_id_riga)
        if value
    )


def _assignment_payload(row, current_machine) -> dict | None:
    if not row.odp_id_documento or not row.odp_id_riga:
        return None

    machine_is_open = (
        current_machine is not None and is_open_machine_order(current_machine)
    )
    return {
        "id_documento": row.odp_id_documento,
        "id_riga": row.odp_id_riga,
        "order": (
            _ordine_ref_label(current_machine)
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
            else ""
        ),
        "state": (
            _canonical_state(getattr(current_machine, "StatoOrdine", ""))
            if current_machine is not None
            else "Non presente"
        ),
        "present": current_machine is not None,
        "open": machine_is_open,
    }


def build_assignment_dashboard() -> dict:
    all_machines = load_machine_orders(include_closed=True)
    all_machine_map = {_machine_key(machine): machine for machine in all_machines}
    open_machines = [
        machine for machine in all_machines if is_open_machine_order(machine)
    ]
    customer_orders = (
        VenditeOrdineCliente.query.options(
            selectinload(VenditeOrdineCliente.righe),
        )
        .order_by(
            VenditeOrdineCliente.creato_il.desc(),
            VenditeOrdineCliente.id.desc(),
        )
        .all()
    )

    assigned_by_machine = {}
    total_demand = 0
    assigned_demand = 0
    customer_payload = []

    for customer in customer_orders:
        rows_payload = []
        customer_assigned = 0
        for row in customer.righe:
            total_demand += 1
            current_machine = all_machine_map.get(
                (row.odp_id_documento, row.odp_id_riga)
            )
            assignment = _assignment_payload(row, current_machine)
            if assignment is not None:
                assigned_demand += 1
                customer_assigned += 1
                assigned_by_machine[(row.odp_id_documento, row.odp_id_riga)] = (
                    customer,
                    row,
                )

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
                    "note": row.note or "",
                    "delivery_date": row.data_consegna.isoformat(),
                    "assignment": assignment,
                }
            )

        customer_payload.append(
            {
                "id": customer.id,
                "customer_name": customer.cliente_nome,
                "customer_order": customer.numero_ordine,
                "created_at": customer.creato_il,
                "created_by_name": customer.creato_da_nome,
                "total_rows": len(customer.righe),
                "assigned_rows": customer_assigned,
                "rows": rows_payload,
            }
        )

    machines_payload = []
    for machine in open_machines:
        key = _machine_key(machine)
        assigned = assigned_by_machine.get(key)
        machines_payload.append(
            {
                "id_documento": key[0],
                "id_riga": key[1],
                "order": _ordine_ref_label(machine),
                "model_key": _model_key(machine.CodArt, machine.VarianteArt),
                "model_code": _norm_text(machine.CodArt),
                "variant": _norm_text(machine.VarianteArt),
                "description": _norm_text(machine.DesArt),
                "serial_number": _norm_text(machine.CodMatricola)
                or "Non assegnata",
                "phase": _phase_label(machine.FaseAttiva),
                "state": _canonical_state(machine.StatoOrdine),
                "assignment": (
                    {
                        "row_id": assigned[1].id,
                        "customer_name": assigned[0].cliente_nome,
                        "customer_order": assigned[0].numero_ordine,
                    }
                    if assigned is not None
                    else None
                ),
            }
        )

    machines_payload.sort(
        key=lambda item: (
            item["model_code"].casefold(),
            item["variant"].casefold(),
            item["serial_number"].casefold(),
            item["order"].casefold(),
        )
    )
    models = sorted(
        _model_catalog(open_machines).values(),
        key=lambda item: (
            item["model_code"].casefold(),
            item["variant"].casefold(),
            item["description"].casefold(),
        ),
    )

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
        "customer_orders": customer_payload,
    }
