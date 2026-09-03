from collections import defaultdict

from sqlalchemy import func, or_, tuple_

from app_odp.models import InputOdp, InputOdpLog
from app_odp.vendite_models import (
    VenditeOrdineCliente,
    VenditeOrdineClienteRiga,
)
from app_odp.services.order_helpers import (
    _fase_to_int,
    _norm_text,
    _now_rome_dt,
    _ordine_ref_label,
)
from app_odp.services.ordini_query_service import _base_odp_query


_STATE_LABELS = {
    "attiva": "Attivo",
    "attivo": "Attivo",
    "chiusa": "Chiusa",
    "chiuso": "Chiusa",
    "in sospeso": "In Sospeso",
    "in corso": "Attivo",
    "pianificata": "Pianificata",
    "pianificato": "Pianificata",
    "sospeso": "In Sospeso",
    "sospesa": "In Sospeso",
}
_STATE_ORDER = {
    "Pianificata": 0,
    "Attivo": 1,
    "In Sospeso": 2,
}
_STANDARD_STATES = tuple(_STATE_ORDER)
_TERMINAL_STATES = {"chiusa"}
_LOG_QUERY_BATCH_SIZE = 400


def _canonical_state(value) -> str:
    raw = _norm_text(value) or "Pianificata"
    normalized = " ".join(raw.casefold().split())
    return _STATE_LABELS.get(normalized, raw)


def _phase_label(value) -> str:
    phase_number = _fase_to_int(value)
    if phase_number is not None and phase_number > 0:
        return str(phase_number)
    return _norm_text(value) or "1"


def _phase_sort_key(value) -> tuple:
    phase_number = _fase_to_int(value)
    if phase_number is not None:
        return 0, phase_number
    return 1, _norm_text(value).casefold()


def _state_sort_key(value) -> tuple:
    state = _canonical_state(value)
    return _STATE_ORDER.get(state, 99), state.casefold()


def _parse_suspension_cause(note) -> str:
    for part in _norm_text(note).split("|"):
        label, separator, value = part.strip().partition(":")
        if separator and label.strip().casefold() == "causale":
            return value.strip()
    return ""


def _latest_suspension_causes(order_keys) -> dict[tuple[str, str], str]:
    keys = sorted(
        {
            (_norm_text(id_documento), _norm_text(id_riga))
            for id_documento, id_riga in order_keys
            if _norm_text(id_documento) and _norm_text(id_riga)
        }
    )
    latest = {}

    for start in range(0, len(keys), _LOG_QUERY_BATCH_SIZE):
        batch = keys[start : start + _LOG_QUERY_BATCH_SIZE]
        rows = (
            InputOdpLog.query.with_entities(
                InputOdpLog.IdDocumento,
                InputOdpLog.IdRiga,
                InputOdpLog.NoteChiusura,
            )
            .filter(
                tuple_(InputOdpLog.IdDocumento, InputOdpLog.IdRiga).in_(batch),
                func.lower(func.coalesce(InputOdpLog.StatoOrdinePost, "")).like(
                    "%sospes%"
                ),
                func.lower(func.coalesce(InputOdpLog.NoteChiusura, "")).like(
                    "sospensione%"
                ),
            )
            .order_by(InputOdpLog.log_id.desc())
            .all()
        )

        for row in rows:
            key = (_norm_text(row.IdDocumento), _norm_text(row.IdRiga))
            if key not in latest:
                latest[key] = _parse_suspension_cause(row.NoteChiusura)

    return latest


def is_open_machine_order(order) -> bool:
    return (
        _norm_text(getattr(order, "GestioneMatricola", "")).casefold() == "si"
        and _canonical_state(getattr(order, "StatoOrdine", "")).casefold()
        not in _TERMINAL_STATES
    )


def load_machine_orders(*, include_closed: bool = False) -> list:
    orders = (
        _base_odp_query()
        .filter(
            func.lower(func.trim(func.coalesce(InputOdp.GestioneMatricola, "")))
            == "si"
        )
        .all()
    )
    if include_closed:
        return orders
    return [order for order in orders if is_open_machine_order(order)]


def _customer_assignments(orders) -> dict[tuple, dict[str, str]]:
    order_keys = {
        (
            _norm_text(getattr(order, "IdDocumento", "")),
            _norm_text(getattr(order, "IdRiga", "")),
        )
        for order in orders
        if _norm_text(getattr(order, "IdDocumento", ""))
        and _norm_text(getattr(order, "IdRiga", ""))
    }
    serial_numbers = {
        _norm_text(getattr(order, "CodMatricola", ""))
        for order in orders
        if _norm_text(getattr(order, "CodMatricola", ""))
    }
    conditions = []
    if order_keys:
        conditions.append(
            tuple_(
                VenditeOrdineClienteRiga.odp_id_documento,
                VenditeOrdineClienteRiga.odp_id_riga,
            ).in_(order_keys)
        )
    if serial_numbers:
        conditions.append(
            VenditeOrdineClienteRiga.odp_matricola.in_(serial_numbers)
        )
    if not conditions:
        return {}

    rows = (
        VenditeOrdineClienteRiga.query.join(VenditeOrdineCliente)
        .with_entities(
            VenditeOrdineClienteRiga.odp_id_documento,
            VenditeOrdineClienteRiga.odp_id_riga,
            VenditeOrdineClienteRiga.odp_matricola,
            VenditeOrdineCliente.numero_ordine.label("customer_order"),
            VenditeOrdineClienteRiga.note_produzione.label("production_note"),
        )
        .filter(or_(*conditions))
        .all()
    )
    assignments = {}
    for row in rows:
        data = {
            "customer_order": row.customer_order or "",
            "production_note": row.production_note or "",
        }
        if row.odp_id_documento and row.odp_id_riga:
            assignments[("order", row.odp_id_documento, row.odp_id_riga)] = data
        if row.odp_matricola:
            assignments[("serial", row.odp_matricola.casefold())] = data
    return assignments


def _build_vendite_payload(
    orders,
    latest_causes=None,
    *,
    customer_assignments=None,
    generated_at: str | None = None,
) -> dict:
    latest_causes = latest_causes or {}
    customer_assignments = customer_assignments or {}
    machine_rows = []
    model_groups = {}
    phases = set()
    states = set(_STANDARD_STATES)

    for order in orders:
        state = _canonical_state(getattr(order, "StatoOrdine", ""))
        if state.casefold() in _TERMINAL_STATES:
            continue

        phase = _phase_label(getattr(order, "FaseAttiva", ""))
        model_code = _norm_text(getattr(order, "CodArt", "")) or "Senza modello"
        variant = _norm_text(getattr(order, "VarianteArt", ""))
        description = _norm_text(getattr(order, "DesArt", ""))
        order_key = (
            _norm_text(getattr(order, "IdDocumento", "")),
            _norm_text(getattr(order, "IdRiga", "")),
        )
        serial_number = _norm_text(getattr(order, "CodMatricola", ""))
        customer_assignment = customer_assignments.get(
            ("order", *order_key)
        ) or customer_assignments.get(
            ("serial", serial_number.casefold())
        )
        model_key = (model_code.casefold(), variant.casefold())
        combination = (phase, state)

        phases.add(phase)
        states.add(state)
        group = model_groups.setdefault(
            model_key,
            {
                "model_code": model_code,
                "variant": variant,
                "description": description,
                "counts": defaultdict(int),
                "total": 0,
            },
        )
        group["counts"][combination] += 1
        group["total"] += 1

        machine_rows.append(
            {
                "order": _ordine_ref_label(order),
                "customer_order": (
                    customer_assignment["customer_order"]
                    if customer_assignment else ""
                ),
                "model_code": model_code,
                "variant": variant,
                "description": description,
                "serial_number": serial_number or "Non assegnata",
                "phase": phase,
                "state": state,
                "last_suspension_cause": latest_causes.get(order_key, ""),
                "production_note": (
                    customer_assignment["production_note"]
                    if customer_assignment else ""
                ),
            }
        )

    ordered_combinations = [
        (phase, state)
        for phase in sorted(phases, key=_phase_sort_key)
        for state in sorted(states, key=_state_sort_key)
    ]
    columns = [
        {"phase": phase, "state": state}
        for phase, state in ordered_combinations
    ]

    models = []
    for group in sorted(
        model_groups.values(),
        key=lambda item: (
            item["model_code"].casefold(),
            item["variant"].casefold(),
            item["description"].casefold(),
        ),
    ):
        counts = group.pop("counts")
        models.append(
            {
                **group,
                "counts": [counts[combination] for combination in ordered_combinations],
            }
        )

    machine_rows.sort(
        key=lambda item: (
            item["model_code"].casefold(),
            item["variant"].casefold(),
            item["serial_number"].casefold(),
            item["order"].casefold(),
        )
    )

    return {
        "generated_at": generated_at
        or _now_rome_dt().isoformat(timespec="seconds"),
        "total_machines": len(machine_rows),
        "columns": columns,
        "models": models,
        "machines": machine_rows,
    }


def build_vendite_payload() -> dict:
    orders = load_machine_orders()
    order_keys = [
        (order.IdDocumento, order.IdRiga)
        for order in orders
    ]
    return _build_vendite_payload(
        orders,
        _latest_suspension_causes(order_keys),
        customer_assignments=_customer_assignments(orders),
    )
