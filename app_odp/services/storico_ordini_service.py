from __future__ import annotations

from datetime import datetime, date, timedelta, time
import json

from sqlalchemy import and_, desc, or_

from app_odp.models import (
    AcqArticoli,
    AcqArticoliLookup,
    GiacenzaLotti,
    GiacenzaMateriale,
    InputOdpLog,
    LottiGeneratiLog,
    LottiUsatiLog,
    OdpRuntimeLog,
    OdpWorkGroup,
    OdpWorkGroupMember,
)
from app_odp.services.order_helpers import (
    _decimal_to_text,
    _json_safe,
    _norm_text,
    _parse_qty_decimal,
)


PAGE_SIZE_DEFAULT = 50
SCAN_LIMIT = 5000
LOT_SEARCH_LIMIT = 200


ACTION_LABELS = {
    "presa_in_carico": "Presa in carico",
    "presa_in_carico_gruppo": "Presa in carico gruppo",
    "sospensione": "Lavorazione sospesa",
    "sospensione_gruppo": "Lavorazione del gruppo sospesa",
    "riattivazione": "Lavorazione ripresa",
    "riattivazione_macchina": "Lavorazione ripresa",
    "riattivazione_gruppo": "Lavorazione del gruppo ripresa",
    "chiusura_finale": "Fase completata",
    "chiusura_macchina": "Fase completata",
    "chiusura_parziale": "Avanzamento parziale",
    "chiusura_parziale_macchina": "Avanzamento parziale",
    "chiusura_membro_scioglie_gruppo": "Chiusura singola: gruppo sciolto",
    "runtime_pre_gruppo": "Tempo maturato prima dell'ingresso nel gruppo",
    "runtime_gruppo": "Tempo di lavorazione attribuito dal gruppo",
    "scioglimento_gruppo_attiva_membro": "Gruppo sciolto: ordine prosegue singolarmente",
    "scioglimento_gruppo_sospende_membro": "Gruppo sciolto: ordine sospeso",
    "blocco_distinta_materiale_assente": "Presa in carico bloccata: distinta materiali assente",
}

EVENT_FILTERS = {
    "presa_in_carico": "Presa in carico",
    "riattivazione": "Lavorazione ripresa",
    "sospensione": "Lavorazione sospesa",
    "chiusura_parziale": "Avanzamento parziale",
    "fase_completata": "Fase completata",
    "ordine_completato": "Ordine completato",
    "gruppo": "Scioglimento gruppo",
    "blocco": "Presa in carico bloccata",
    "tempi": "Registrazioni dei tempi",
    "altro": "Altri eventi",
}

GROUP_TYPE_LABELS = {
    "MULTIPLO": "Multiplo",
    "MASCHERATO": "Mascherato",
    "MISTO": "Misto",
}


def default_period() -> tuple[str, str]:
    end = date.today()
    start = end - timedelta(days=30)
    return start.isoformat(), end.isoformat()


def _parse_date(value: str | None, *, end_of_day: bool = False) -> datetime | None:
    raw = _norm_text(value)
    if not raw:
        return None
    try:
        day = date.fromisoformat(raw[:10])
    except ValueError:
        return None
    return datetime.combine(day, time.max if end_of_day else time.min)


def _parse_dt(value) -> datetime | None:
    raw = _norm_text(value)
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def _format_dt(value) -> str:
    parsed = _parse_dt(value)
    if parsed is None:
        return _norm_text(value) or "-"
    return parsed.strftime("%d/%m/%Y %H:%M:%S")


def _payload(row) -> dict:
    raw = _norm_text(getattr(row, "PayloadJson", ""))
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except Exception:
        return (
            {} if "tempo_non_funzionamento_secondi" in raw.lower() else {"_raw": raw}
        )
    if isinstance(parsed, dict):
        parsed.pop("tempo_non_funzionamento_secondi", None)
        return parsed
    return {"value": parsed}


def _visible_note(value) -> str:
    return " | ".join(
        part.strip()
        for part in _norm_text(value).split("|")
        if not part.strip().lower().startswith("tempo non funzionamento secondi:")
    )


def _action_label(action: str) -> str:
    action = _norm_text(action)
    return ACTION_LABELS.get(action, action.replace("_", " ").strip().capitalize())


def _positive_number_text(value) -> str:
    raw = _norm_text(value)
    if not raw:
        return ""
    try:
        number = float(raw.replace(",", "."))
    except ValueError:
        return ""
    return f"{number:.2f}".rstrip("0").rstrip(".") if number > 0 else ""


def _non_working_minutes(row) -> str:
    minutes = _positive_number_text(getattr(row, "TempoNonFunzionamentoMinuti", ""))
    if minutes:
        return minutes

    seconds = _positive_number_text(getattr(row, "TempoNonFunzionamentoSecondi", ""))
    if not seconds:
        return ""
    return f"{float(seconds) / 60:.2f}".rstrip("0").rstrip(".")


def _bounded_int(value, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return min(max(parsed, minimum), maximum)


def _order_key(row) -> tuple[str, str]:
    return _norm_text(row.IdDocumento), _norm_text(row.IdRiga)


def _member_maps(order_keys: set[tuple[str, str]]):
    if not order_keys:
        return {}, {}

    docs = {doc for doc, _riga in order_keys if doc}
    if not docs:
        return {}, {}

    members = OdpWorkGroupMember.query.filter(
        OdpWorkGroupMember.IdDocumento.in_(list(docs))
    ).all()
    members = [
        m
        for m in members
        if (_norm_text(m.IdDocumento), _norm_text(m.IdRiga)) in order_keys
    ]

    group_uids = {_norm_text(m.GroupUid) for m in members if _norm_text(m.GroupUid)}
    groups = (
        OdpWorkGroup.query.filter(OdpWorkGroup.GroupUid.in_(list(group_uids))).all()
        if group_uids
        else []
    )
    groups_by_uid = {_norm_text(g.GroupUid): g for g in groups}

    by_order: dict[tuple[str, str], list[OdpWorkGroupMember]] = {}
    for member in members:
        by_order.setdefault(
            (_norm_text(member.IdDocumento), _norm_text(member.IdRiga)), []
        ).append(member)

    return by_order, groups_by_uid


def _row_event_at(row) -> str:
    return _norm_text(getattr(row, "EventAt", "")) or _norm_text(
        getattr(row, "ClosedAt", "")
    )


def _event_in_group_window(event_at: str, group: OdpWorkGroup | None) -> bool:
    if group is None:
        return False
    event_dt = _parse_dt(event_at)
    start_dt = _parse_dt(group.CreatedAt)
    end_dt = _parse_dt(group.ClosedAt) or _parse_dt(group.DissolvedAt)
    if event_dt is None:
        return False
    if start_dt is not None and event_dt < start_dt:
        return False
    if end_dt is not None and event_dt > end_dt + timedelta(minutes=5):
        return False
    return True


def _group_for_event(row, payload: dict, member_by_order, groups_by_uid) -> str:
    group_uid = _norm_text(payload.get("group_uid"))
    if group_uid:
        return group_uid

    matches = {}
    for member in member_by_order.get(_order_key(row), []):
        uid = _norm_text(member.GroupUid)
        group = groups_by_uid.get(uid)
        if uid and _event_in_group_window(_row_event_at(row), group):
            matches[uid] = group

    return max(
        matches,
        key=lambda uid: (
            _norm_text(getattr(matches[uid], "CreatedAt", "")),
            uid,
        ),
        default="",
    )


def _effective_group_type(group, payload: dict | None = None) -> str:
    payload = payload or {}
    group_type = _norm_text(payload.get("group_type")) or _norm_text(
        getattr(group, "GroupType", "")
    )
    is_mixed = group_type.upper() == "MULTIPLO" and (
        _norm_text(getattr(group, "Note", "")).upper() == "MISTO"
        or any(
            _norm_text(getattr(member, "TimeShareMode", "")).upper() == "ZERO"
            for member in getattr(group, "members", [])
        )
    )
    return "MISTO" if is_mixed else group_type


def _group_label(group_type: str) -> str:
    return GROUP_TYPE_LABELS.get(_norm_text(group_type).upper(), _norm_text(group_type))


def _operation_ids(rows) -> list[str]:
    return list(
        {
            _norm_text(getattr(row, "OperationGroupId", ""))
            for row in rows
            if _norm_text(getattr(row, "OperationGroupId", ""))
        }
    )


def _input_rows_without_runtime_duplicates(input_rows, operation_ids):
    operation_ids = set(operation_ids)
    return [
        row
        for row in input_rows
        if not _norm_text(getattr(row, "OperationGroupId", ""))
        or _norm_text(getattr(row, "OperationGroupId", "")) not in operation_ids
    ]


def _input_logs_by_operation(operation_ids: list[str]) -> dict[str, list[InputOdpLog]]:
    if not operation_ids:
        return {}
    out: dict[str, list[InputOdpLog]] = {}
    for row in InputOdpLog.query.filter(
        InputOdpLog.OperationGroupId.in_(operation_ids)
    ):
        out.setdefault(_norm_text(row.OperationGroupId), []).append(row)
    return out


def _row_matches_python_filters(entry, params) -> bool:
    risorsa = _norm_text(params.get("risorsa")).lower()
    if risorsa and not any(risorsa in value.lower() for value in entry["risorse"]):
        return False

    group_type = _norm_text(params.get("tipo_gruppo")).upper()
    if group_type and _norm_text(entry.get("group_type")).upper() != group_type:
        return False

    return True


def _input_event_action(row: InputOdpLog) -> str:
    partial = _norm_text(row.ChiusuraParziale).lower() in {"true", "1", "si", "yes"}
    if partial:
        return "chiusura_parziale"
    if _norm_text(row.QuantitaConforme) or _norm_text(row.QuantitaNonConforme):
        return "chiusura_finale"

    note = _visible_note(row.NoteChiusura).lower()
    if "distinta" in note and ("assente" in note or "mancante" in note):
        return "blocco_distinta_materiale_assente"
    if "scioglimento gruppo" in note:
        return "scioglimento_gruppo_sospende_membro" if "sospes" in note else "scioglimento_gruppo_attiva_membro"
    if "riattivazione" in note or "ripresa" in note:
        return "riattivazione"
    for action in sorted(ACTION_LABELS, key=len, reverse=True):
        if action.replace("_", " ") in note or action in note:
            return action

    post = _norm_text(row.StatoOrdinePost).lower()
    if post in {"chiusa", "chiuso"}:
        return "chiusura_finale"
    if "attiv" in post:
        pre = _norm_text(row.StatoOrdinePre).lower()
        return "riattivazione" if "sospes" in pre else "presa_in_carico"
    if "sospes" in post:
        return "sospensione"
    return "input_odp_log"


def _event_category(action: str, state_post: str = "", payload=None) -> str:
    payload = payload or {}
    if action in {"runtime_gruppo", "runtime_pre_gruppo"}:
        return "tempi"
    if action.startswith("blocco_"):
        return "blocco"
    if "scioglie" in action or "scioglimento" in action:
        return "gruppo"
    for prefix in ("presa_in_carico", "riattivazione", "sospensione", "chiusura_parziale"):
        if action.startswith(prefix):
            return prefix
    if action in {"chiusura_finale", "chiusura_macchina"}:
        terminal = _norm_text(state_post).lower() in {"chiusa", "chiuso"}
        return "ordine_completato" if terminal or payload.get("is_last_phase") is True else "fase_completata"
    return "altro"


def _event_label(action: str, state_post: str = "", payload=None) -> str:
    category = _event_category(action, state_post, payload)
    if category in {"fase_completata", "ordine_completato"}:
        return EVENT_FILTERS[category]
    return _action_label(action) if action != "input_odp_log" else "Evento storico"


def _input_event_label(row: InputOdpLog) -> str:
    return _event_label(_input_event_action(row), row.StatoOrdinePost)


def _matches_event_filter(action, state_post, payload, selected):
    selected = _norm_text(selected)
    if not selected:
        return True
    if selected in EVENT_FILTERS:
        return _event_category(action, state_post, payload) == selected
    return selected.lower() in f"{action} {_event_label(action, state_post, payload)}".lower()


def _event_entry(entries: dict, row, group_uid: str, payload: dict, groups_by_uid):
    if group_uid:
        key = f"group:{group_uid}"
        group = groups_by_uid.get(group_uid)
        group_type = _effective_group_type(group, payload)
        return entries.setdefault(
            key,
            {
                "kind": "group",
                "key": key,
                "group_uid": group_uid,
                "group_type": group_type,
                "group_type_label": _group_label(group_type),
                "label": f"Gruppo {group_uid}",
                "orders": set(),
                "num_progr_righe": {
                    _norm_text(getattr(member, "NumProgrRiga", ""))
                    for member in getattr(group, "members", [])
                    if _norm_text(getattr(member, "NumProgrRiga", ""))
                },
                "articles": set(),
                "risorse": set(),
                "operatori": set(),
                "eventi": set(),
                "count": 0,
                "last_event_at": "",
                "last_event": "",
                "status": _norm_text(getattr(group, "Status", "")),
            },
        )

    key = f"order:{_norm_text(row.IdDocumento)}|{_norm_text(row.IdRiga)}"
    label = _norm_text(row.RifRegistraz) or f"{row.IdDocumento}/{row.IdRiga}"
    return entries.setdefault(
        key,
        {
            "kind": "order",
            "key": key,
            "group_uid": "",
            "group_type": "",
            "group_type_label": "Singolo",
            "label": label,
            "orders": set(),
            "num_progr_righe": set(),
            "articles": set(),
            "risorse": set(),
            "operatori": set(),
            "eventi": set(),
            "count": 0,
            "last_event_at": "",
            "last_event": "",
            "status": "",
        },
    )


def _add_entry_event(entry: dict, row, event_label: str, user: str = "") -> None:
    order_ref = _norm_text(row.RifRegistraz) or f"{row.IdDocumento}/{row.IdRiga}"
    entry["orders"].add(order_ref)
    entry["num_progr_righe"].add(_norm_text(getattr(row, "NumProgrRiga", "")))
    entry["articles"].add(_norm_text(getattr(row, "CodArt", "")))
    entry["operatori"].add(_norm_text(user))
    entry["eventi"].add(event_label)
    entry["risorse"].add(
        _norm_text(getattr(row, "RisorsaAttiva", ""))
        or _norm_text(getattr(row, "CodRisorsaProd", ""))
    )
    entry["count"] += 1

    event_at = _row_event_at(row)
    technical = _norm_text(getattr(row, "Azione", "")) in {"runtime_gruppo", "runtime_pre_gruppo"}
    previous_technical = entry.get("last_event_technical", technical)
    if (not entry["last_event_at"] or (previous_technical and not technical)
            or (previous_technical == technical and event_at > entry["last_event_at"])):
        entry["last_event_at"] = event_at
        entry["last_event"] = event_label
        entry["last_operator"] = _norm_text(user)
        entry["last_event_technical"] = technical
        payload = _payload(row)
        entry["last_phase"] = _norm_text(payload.get("fase")) or _norm_text(
            getattr(row, "FaseConsuntivata", "") or getattr(row, "FasePre", "")
            or getattr(row, "FaseAttiva", "") or getattr(row, "FasePost", "")
        )


def _filtered_runtime_query(params, start_dt, end_dt):
    query = OdpRuntimeLog.query
    if start_dt is not None:
        query = query.filter(
            OdpRuntimeLog.EventAt >= start_dt.isoformat(timespec="seconds")
        )
    if end_dt is not None:
        query = query.filter(
            OdpRuntimeLog.EventAt <= end_dt.isoformat(timespec="seconds")
        )

    text = _norm_text(params.get("q"))
    if text:
        like = f"%{text}%"
        query = query.filter(
            or_(
                OdpRuntimeLog.IdDocumento.ilike(like),
                OdpRuntimeLog.IdRiga.ilike(like),
                OdpRuntimeLog.RifRegistraz.ilike(like),
                OdpRuntimeLog.CodArt.ilike(like),
                OdpRuntimeLog.Azione.ilike(like),
                OdpRuntimeLog.UtenteOperazione.ilike(like),
            )
        )

    for field, column in (
        ("articolo", OdpRuntimeLog.CodArt),
        ("reparto", OdpRuntimeLog.CodReparto),
        ("operatore", OdpRuntimeLog.UtenteOperazione),
    ):
        value = _norm_text(params.get(field))
        if value:
            query = query.filter(column.ilike(f"%{value}%"))

    return query


def _filtered_input_query(params, start_dt, end_dt):
    query = InputOdpLog.query
    if start_dt is not None:
        query = query.filter(
            InputOdpLog.ClosedAt >= start_dt.isoformat(timespec="seconds")
        )
    if end_dt is not None:
        query = query.filter(
            InputOdpLog.ClosedAt <= end_dt.isoformat(timespec="seconds")
        )

    text = _norm_text(params.get("q"))
    if text:
        like = f"%{text}%"
        query = query.filter(
            or_(
                InputOdpLog.IdDocumento.ilike(like),
                InputOdpLog.IdRiga.ilike(like),
                InputOdpLog.RifRegistraz.ilike(like),
                InputOdpLog.CodArt.ilike(like),
                InputOdpLog.ClosedBy.ilike(like),
                InputOdpLog.NoteChiusura.ilike(like),
            )
        )

    for field, column in (
        ("articolo", InputOdpLog.CodArt),
        ("reparto", InputOdpLog.CodReparto),
        ("operatore", InputOdpLog.ClosedBy),
    ):
        value = _norm_text(params.get(field))
        if value:
            query = query.filter(column.ilike(f"%{value}%"))

    return query


def build_storico_ordini_list(params) -> dict:
    page = _bounded_int(params.get("page"), 1, 1, 1_000_000)
    page_size = _bounded_int(
        params.get("page_size"),
        PAGE_SIZE_DEFAULT,
        10,
        100,
    )

    start_dt = _parse_date(params.get("date_from"))
    end_dt = _parse_date(params.get("date_to"), end_of_day=True)

    runtime_rows = (
        _filtered_runtime_query(params, start_dt, end_dt)
        .order_by(desc(OdpRuntimeLog.EventAt), desc(OdpRuntimeLog.log_id))
        .limit(SCAN_LIMIT)
        .all()
    )
    input_rows = (
        _filtered_input_query(params, start_dt, end_dt)
        .order_by(desc(InputOdpLog.ClosedAt), desc(InputOdpLog.log_id))
        .limit(SCAN_LIMIT)
        .all()
    )

    runtime_operation_ids = _operation_ids(runtime_rows)
    input_event_rows = _input_rows_without_runtime_duplicates(
        input_rows, runtime_operation_ids
    )
    order_keys = {_order_key(row) for row in runtime_rows} | {
        _order_key(row) for row in input_event_rows
    }
    member_by_order, groups_by_uid = _member_maps(order_keys)
    input_by_operation = _input_logs_by_operation(runtime_operation_ids)

    entries = {}

    for row in runtime_rows:
        payload = _payload(row)
        state_post = row.StatoOrdinePost or row.StatoOdpPost
        if not _matches_event_filter(row.Azione, state_post, payload, params.get("evento")):
            continue
        group_uid = _group_for_event(row, payload, member_by_order, groups_by_uid)
        entry = _event_entry(entries, row, group_uid, payload, groups_by_uid)
        _add_entry_event(entry, row, _event_label(row.Azione, state_post, payload), row.UtenteOperazione)

        for input_log in input_by_operation.get(_norm_text(row.OperationGroupId), []):
            entry["risorse"].add(
                _norm_text(input_log.RisorsaAttiva)
                or _norm_text(input_log.CodRisorsaProd)
            )
            entry["articles"].add(_norm_text(input_log.CodArt))

    for row in input_event_rows:
        payload = {}
        if not _matches_event_filter(_input_event_action(row), row.StatoOrdinePost, payload, params.get("evento")):
            continue
        group_uid = _group_for_event(row, payload, member_by_order, groups_by_uid)
        entry = _event_entry(entries, row, group_uid, payload, groups_by_uid)
        _add_entry_event(entry, row, _input_event_label(row), row.ClosedBy)

    filtered = [
        entry
        for entry in entries.values()
        if _row_matches_python_filters(entry, params)
    ]
    filtered.sort(key=lambda item: item["last_event_at"], reverse=True)

    total = len(filtered)
    start_idx = (page - 1) * page_size
    page_rows = filtered[start_idx : start_idx + page_size]

    for entry in page_rows:
        for field in (
            "orders",
            "num_progr_righe",
            "articles",
            "risorse",
            "operatori",
            "eventi",
        ):
            entry[field] = [value for value in sorted(entry[field]) if value]
        entry["last_event_at_display"] = _format_dt(entry["last_event_at"])

    scan_count = max(len(runtime_rows), len(input_rows))
    return _json_safe(
        {
            "ok": True,
            "rows": page_rows,
            "page": page,
            "page_size": page_size,
            "total": total,
            "total_pages": max((total + page_size - 1) // page_size, 1),
            "scan_limit_reached": scan_count >= SCAN_LIMIT,
        }
    )


def _runtime_rows_for_order(id_documento: str, id_riga: str):
    return (
        OdpRuntimeLog.query.filter(
            OdpRuntimeLog.IdDocumento == id_documento,
            OdpRuntimeLog.IdRiga == id_riga,
        )
        .order_by(OdpRuntimeLog.EventAt.asc(), OdpRuntimeLog.log_id.asc())
        .all()
    )


def _runtime_rows_for_group(group_uid: str):
    group = OdpWorkGroup.query.filter_by(GroupUid=group_uid).first()
    members = OdpWorkGroupMember.query.filter_by(GroupUid=group_uid).all()
    rows = []

    for member in members:
        for row in _runtime_rows_for_order(
            _norm_text(member.IdDocumento), _norm_text(member.IdRiga)
        ):
            payload = _payload(row)
            recorded_group = _norm_text(payload.get("group_uid"))
            if (recorded_group == group_uid if recorded_group
                    else _event_in_group_window(row.EventAt, group)):
                rows.append(row)

    payload_rows = OdpRuntimeLog.query.filter(
        OdpRuntimeLog.PayloadJson.ilike(f"%{group_uid}%")
    ).all()
    by_id = {row.log_id: row for row in rows}
    for row in payload_rows:
        if _norm_text(_payload(row).get("group_uid")) == group_uid:
            by_id[row.log_id] = row

    return sorted(
        by_id.values(), key=lambda row: (_norm_text(row.EventAt), row.log_id or 0)
    )


def _historical_rows_for_orders(candidates, operation_ids, order_keys, group):
    known_operations = set()
    if group is not None:
        candidate_ops = _operation_ids(candidates)
        if candidate_ops:
            known_operations = set(_operation_ids(OdpRuntimeLog.query.filter(
                OdpRuntimeLog.OperationGroupId.in_(candidate_ops)
            ).all()))
    allowed_operations = set(operation_ids)
    return [row for row in candidates if _order_key(row) in order_keys and (
        group is None or (
            _event_in_group_window(row.ClosedAt, group)
            and (_norm_text(row.OperationGroupId) not in known_operations
                 or _norm_text(row.OperationGroupId) in allowed_operations)
        )
    )]


def _input_logs_for_runtime_or_orders(runtime_rows, order_keys=None, group=None):
    operation_ids = _operation_ids(runtime_rows)
    rows = []
    if operation_ids:
        rows.extend(
            InputOdpLog.query.filter(
                InputOdpLog.OperationGroupId.in_(operation_ids)
            ).all()
        )

    order_keys = order_keys or set()
    if order_keys:
        docs = {doc for doc, _riga in order_keys if doc}
        if docs:
            candidates = InputOdpLog.query.filter(
                InputOdpLog.IdDocumento.in_(list(docs))
            ).all()
            rows.extend(_historical_rows_for_orders(candidates, operation_ids, order_keys, group))

    by_id = {row.log_id: row for row in rows}
    return sorted(
        by_id.values(), key=lambda row: (_norm_text(row.ClosedAt), row.log_id or 0)
    )


def _lotti_for_runtime_or_inputs(
    model, runtime_rows, input_rows, order_keys=None, group=None
):
    operation_ids = set(_operation_ids(runtime_rows)) | set(_operation_ids(input_rows))
    rows = []
    if operation_ids:
        rows.extend(
            model.query.filter(model.OperationGroupId.in_(list(operation_ids))).all()
        )

    order_keys = order_keys or set()
    if order_keys:
        docs = {doc for doc, _riga in order_keys if doc}
        if docs:
            candidates = model.query.filter(model.IdDocumento.in_(list(docs))).all()
            rows.extend(_historical_rows_for_orders(candidates, operation_ids, order_keys, group))

    by_id = {row.log_id: row for row in rows}
    return sorted(
        by_id.values(), key=lambda row: (_norm_text(row.ClosedAt), row.log_id or 0)
    )


def _event_description(row, payload: dict) -> str:
    action = _event_label(row.Azione, row.StatoOrdinePost or row.StatoOdpPost, payload)
    user = _norm_text(row.UtenteOperazione) or _norm_text(payload.get("utente")) or "-"
    parts = [f"{user} - {action}"]

    stato_pre = _norm_text(row.StatoOrdinePre or row.StatoOdpPre)
    stato_post = _norm_text(row.StatoOrdinePost or row.StatoOdpPost)
    if stato_pre or stato_post:
        parts.append(f"stato {stato_pre or '-'} -> {stato_post or '-'}")

    q_ok = _norm_text(row.QuantitaConforme)
    q_ko = _norm_text(row.QuantitaNonConforme)
    if q_ok or q_ko:
        parts.append(f"OK {q_ok or '0'} / KO {q_ko or '0'}")

    if payload.get("tempo_avanzamento_forzato") is True:
        minuti = _norm_text(payload.get("tempo_avanzamento_minuti")) or "-"
        ore = _norm_text(payload.get("tempo_avanzamento_ore")) or "-"
        calcolato = (
            _norm_text(payload.get("tempo_funzionamento_calcolato")) or "-"
        )
        parts.append(
            f"Tempo avanzamento {minuti} min ({ore} h), tempo calcolato {calcolato} h"
        )

    note = _norm_text(row.Note or row.Motivo)
    if note:
        parts.append(note)

    return " | ".join(parts)


def _num_progr_by_order(*row_groups) -> dict[tuple[str, str], str]:
    out = {}
    for rows in row_groups:
        for row in rows:
            value = _norm_text(getattr(row, "NumProgrRiga", ""))
            if value:
                out[_order_key(row)] = value
    return out


def _runtime_to_dict(row):
    payload = _payload(row)
    return {
        "log_id": row.log_id,
        "source": "runtime",
        "event_at": _norm_text(row.EventAt),
        "event_at_display": _format_dt(row.EventAt),
        "azione": _norm_text(row.Azione),
        "azione_label": _event_label(row.Azione, row.StatoOrdinePost or row.StatoOdpPost, payload),
        "descrizione": _event_description(row, payload),
        "utente": _norm_text(row.UtenteOperazione) or _norm_text(payload.get("utente")),
        "operation_group_id": _norm_text(row.OperationGroupId),
        "fase": _norm_text(payload.get("fase")) or _norm_text(row.FasePre or row.FasePost),
        "causale": _norm_text(row.Causale) or _norm_text(payload.get("causale")),
        "note": _visible_note(row.Note or row.Motivo),
        "quantita_residua": _norm_text(row.QtyDaLavorarePost),
        "fase_successiva": _norm_text(payload.get("fase_successiva")),
        "ordine": _norm_text(row.RifRegistraz) or f"{row.IdDocumento}/{row.IdRiga}",
        "id_documento": _norm_text(row.IdDocumento),
        "id_riga": _norm_text(row.IdRiga),
        "num_progr_riga": _norm_text(row.NumProgrRiga),
        "articolo": _norm_text(row.CodArt),
        "fase_pre": _norm_text(row.FasePre),
        "fase_post": _norm_text(row.FasePost),
        "stato_pre": _norm_text(row.StatoOrdinePre or row.StatoOdpPre),
        "stato_post": _norm_text(row.StatoOrdinePost or row.StatoOdpPost),
        "q_ok": _norm_text(row.QuantitaConforme),
        "q_ko": _norm_text(row.QuantitaNonConforme),
        "elapsed_seconds": _norm_text(row.ElapsedSeconds),
        "tempo_lavorazione_ore": _positive_number_text(row.TempoFunzionamentoPost),
        "tempo_non_funzionamento_minuti": _non_working_minutes(row)
        or _positive_number_text(payload.get("tempo_non_funzionamento_minuti")),
        "payload": payload,
        "payload_pretty": json.dumps(payload, ensure_ascii=False, indent=2)
        if payload
        else "",
    }


def _input_to_dict(row, num_progr_by_order=None):
    num_progr_by_order = num_progr_by_order or {}
    return {
        "log_id": row.log_id,
        "operation_group_id": _norm_text(row.OperationGroupId),
        "closed_at": _norm_text(row.ClosedAt),
        "closed_at_display": _format_dt(row.ClosedAt),
        "ordine": _norm_text(row.RifRegistraz) or f"{row.IdDocumento}/{row.IdRiga}",
        "id_documento": _norm_text(row.IdDocumento),
        "id_riga": _norm_text(row.IdRiga),
        "num_progr_riga": num_progr_by_order.get(_order_key(row), ""),
        "articolo": _norm_text(row.CodArt),
        "descrizione": _norm_text(row.DesArt),
        "fase": _norm_text(row.FaseConsuntivata or row.FaseAttiva),
        "risorsa": _norm_text(row.RisorsaAttiva or row.CodRisorsaProd),
        "q_ok": _norm_text(row.QuantitaConforme),
        "q_ko": _norm_text(row.QuantitaNonConforme),
        "tempo_finale": _norm_text(row.TempoFunzionamentoFinale),
        "tempo_non_funzionamento_minuti": _non_working_minutes(row),
        "chiusura_parziale": _norm_text(row.ChiusuraParziale),
        "note": _visible_note(row.NoteChiusura),
        "utente": _norm_text(row.ClosedBy),
        "matricola": _norm_text(row.CodMatricola),
    }


def _input_timeline_to_dict(row, num_progr_by_order=None):
    num_progr_by_order = num_progr_by_order or {}
    label = _input_event_label(row)
    user = _norm_text(row.ClosedBy) or "-"
    description = f"{user} - {label}"
    state_pre = _norm_text(row.StatoOrdinePre)
    state_post = _norm_text(row.StatoOrdinePost)
    if state_pre or state_post:
        description += f" | stato {state_pre or '-'} -> {state_post or '-'}"
    if _norm_text(row.QuantitaConforme) or _norm_text(row.QuantitaNonConforme):
        description += f" | OK {_norm_text(row.QuantitaConforme) or '0'} / KO {_norm_text(row.QuantitaNonConforme) or '0'}"
    note = _visible_note(row.NoteChiusura)
    cause = next((part.partition(":")[2].strip() for part in note.split("|")
                  if part.strip().lower().startswith("causale:")), "")
    if note:
        description += f" | {note}"

    payload = {
        "source": "input_odp_log",
        "operation_group_id": _norm_text(row.OperationGroupId),
        "fase": _norm_text(row.FaseConsuntivata or row.FaseAttiva),
        "risorsa": _norm_text(row.RisorsaAttiva or row.CodRisorsaProd),
        "tempo_funzionamento_finale": _norm_text(row.TempoFunzionamentoFinale),
        "chiusura_parziale": _norm_text(row.ChiusuraParziale),
    }

    return {
        "log_id": row.log_id,
        "source": "input",
        "event_at": _norm_text(row.ClosedAt),
        "event_at_display": _format_dt(row.ClosedAt),
        "azione": _input_event_action(row),
        "azione_label": label,
        "operation_group_id": _norm_text(row.OperationGroupId),
        "fase": payload["fase"],
        "causale": cause,
        "note": note,
        "quantita_residua": _norm_text(row.QtyDaLavorarePost),
        "fase_successiva": "",
        "descrizione": description,
        "utente": _norm_text(row.ClosedBy),
        "ordine": _norm_text(row.RifRegistraz) or f"{row.IdDocumento}/{row.IdRiga}",
        "id_documento": _norm_text(row.IdDocumento),
        "id_riga": _norm_text(row.IdRiga),
        "num_progr_riga": num_progr_by_order.get(_order_key(row), ""),
        "articolo": _norm_text(row.CodArt),
        "fase_pre": "",
        "fase_post": _norm_text(row.FaseConsuntivata or row.FaseAttiva),
        "stato_pre": state_pre,
        "stato_post": state_post,
        "q_ok": _norm_text(row.QuantitaConforme),
        "q_ko": _norm_text(row.QuantitaNonConforme),
        "elapsed_seconds": "",
        "tempo_lavorazione_ore": _positive_number_text(row.TempoFunzionamentoFinale),
        "tempo_non_funzionamento_minuti": _non_working_minutes(row),
        "payload": payload,
        "payload_pretty": json.dumps(payload, ensure_ascii=False, indent=2),
    }


def _prepare_timeline(timeline, input_logs, used_lots, generated_lots):
    related = []
    for rows, convert in (
        (input_logs, _input_to_dict),
        (used_lots, _lotto_usato_to_dict),
        (generated_lots, _lotto_generato_to_dict),
    ):
        by_operation, by_time = {}, {}
        for row in rows:
            order = _order_key(row)
            operation = _norm_text(row.OperationGroupId)
            converted = convert(row)
            if operation:
                by_operation.setdefault((operation, *order), []).append(converted)
            by_time.setdefault((_row_event_at(row), *order), []).append(converted)
        related.append((by_operation, by_time))

    owners = {}
    for event in timeline:
        action = event["azione"]
        payload = event["payload"]
        category = _event_category(action, event["stato_post"], payload)
        event["categoria"] = category
        event["tecnico"] = category == "tempi"
        order = (event["id_documento"], event["id_riga"])
        for field, (by_operation, by_time) in zip(
            ("documenti", "lotti_usati", "lotti_generati"), related
        ):
            operation = event["operation_group_id"]
            event[field] = (
                by_operation.get((operation, *order), []) if operation
                else by_time.get((event["event_at"], *order), [])
            )
        # La chiusura può azzerare il runtime per la fase successiva.
        if category in {"fase_completata", "ordine_completato", "chiusura_parziale"} and event["documenti"]:
            document = event["documenti"][0]
            event["tempo_lavorazione_ore"] = _positive_number_text(document["tempo_finale"])
            event["fase"] = document["fase"] or event["fase"]
        owner_key = (*order, event["fase"])
        if category in {"presa_in_carico", "riattivazione"}:
            owners[owner_key] = event["utente"]
        recorded_owner = _norm_text(payload.get("operatore_in_carico"))
        event["operatore_in_carico"] = recorded_owner or owners.get(owner_key, "")
        event["presa_in_carico_ricostruita"] = bool(
            category not in {"presa_in_carico", "riattivazione"}
            and not recorded_owner and event["operatore_in_carico"]
        )

    return timeline


def _timeline_steps(timeline, group_uid=""):
    steps, grouped = [], {}
    for event in timeline:
        if event["tecnico"]:
            continue
        uid = _norm_text(event["payload"].get("group_uid")) or group_uid
        key = (uid, event["event_at"], event["azione"], event["fase"], event["utente"])
        step = grouped.get(key) if uid else None
        order = (event["id_documento"], event["id_riga"])
        if step is None or any((e["id_documento"], e["id_riga"]) == order for e in step["eventi"]):
            step = {"fase": event["fase"], "event_at_display": event["event_at_display"],
                    "azione_label": event["azione_label"], "utente": event["utente"], "eventi": []}
            steps.append(step)
            grouped[key] = step
        step["eventi"].append(event)
        labels = {e["azione_label"] for e in step["eventi"]}
        if len(labels) > 1:
            step["azione_label"] = "Completamento lavorazioni del gruppo"
    return steps


def _lotto_usato_to_dict(row):
    return {
        "log_id": row.log_id,
        "closed_at_display": _format_dt(row.ClosedAt),
        "ordine": _norm_text(row.RifRegistraz) or f"{row.IdDocumento}/{row.IdRiga}",
        "articolo": _norm_text(row.CodArt),
        "lotto": _norm_text(row.RifLottoAlfa),
        "quantita": _norm_text(row.Quantita),
        "esito": _norm_text(row.Esito),
        "fase": _norm_text(row.Fase),
        "utente": _norm_text(row.ClosedBy),
    }


def _lotto_generato_to_dict(row):
    return {
        "log_id": row.log_id,
        "closed_at_display": _format_dt(row.ClosedAt),
        "ordine": _norm_text(row.RifRegistraz) or f"{row.IdDocumento}/{row.IdRiga}",
        "articolo": _norm_text(row.CodArt),
        "lotto": _norm_text(row.RifLottoAlfa),
        "quantita": _norm_text(row.Quantita),
        "fase": _norm_text(row.Fase),
        "utente": _norm_text(row.ClosedBy),
        "label": _norm_text(row.LabelFilename),
    }


def _lotto_search_row(row, kind: str) -> dict:
    return {
        "log_id": row.log_id,
        "kind": kind,
        "kind_label": "Usato" if kind == "used" else "Generato",
        "event_at": _norm_text(row.ClosedAt) or _norm_text(row.logged_at),
        "event_at_display": _format_dt(row.ClosedAt or row.logged_at),
        "ordine": _norm_text(row.RifRegistraz)
        or f"{row.IdDocumento}/{row.IdRiga}",
        "id_documento": _norm_text(row.IdDocumento),
        "id_riga": _norm_text(row.IdRiga),
        "articolo": _norm_text(row.CodArt),
        "lotto": _norm_text(row.RifLottoAlfa),
        "quantita": _norm_text(row.Quantita),
        "fase": _norm_text(row.Fase),
        "utente": _norm_text(row.ClosedBy),
    }


def _lot_materials(lotto: str) -> list[dict]:
    codes = set()
    for model in (GiacenzaLotti, LottiUsatiLog, LottiGeneratiLog):
        codes.update(_norm_text(code) for (code,) in model.query.with_entities(
            model.CodArt
        ).filter_by(RifLottoAlfa=lotto).distinct().all() if _norm_text(code))
    if not codes:
        return []

    descriptions = {}
    for model in (AcqArticoli, AcqArticoliLookup, GiacenzaMateriale):
        missing = codes - descriptions.keys()
        if not missing:
            break
        query = model.query.with_entities(model.CodArt, model.DesArt).filter(model.CodArt.in_(missing))
        if model is AcqArticoliLookup:
            query = query.order_by(model.synced_at.desc(), model.IndiceModifica.desc(), model.VarianteArt)
        else:
            query = query.order_by(model.CodArt, model.DesArt)
        for row in query:
            if _norm_text(row.DesArt):
                descriptions.setdefault(_norm_text(row.CodArt), _norm_text(row.DesArt))
    missing = codes - descriptions.keys()
    if missing:
        for row in InputOdpLog.query.with_entities(InputOdpLog.CodArt, InputOdpLog.DesArt).filter(
            InputOdpLog.CodArt.in_(missing)
        ).order_by(
            InputOdpLog.log_id.desc()
        ):
            if _norm_text(row.DesArt):
                descriptions.setdefault(_norm_text(row.CodArt), _norm_text(row.DesArt))
    return [{"codice": code, "descrizione": descriptions.get(code, "")} for code in sorted(codes)]


def _lot_quantity_summary(lotto: str, articolo: str) -> dict:

    stock_rows = GiacenzaLotti.query.filter_by(RifLottoAlfa=lotto, CodArt=articolo).all()
    used_rows = LottiUsatiLog.query.filter_by(RifLottoAlfa=lotto, CodArt=articolo).all()
    generated_rows = LottiGeneratiLog.query.filter_by(RifLottoAlfa=lotto, CodArt=articolo).all()

    def total(rows, field):
        value = 0
        for row in rows:
            try:
                value += _parse_qty_decimal(getattr(row, field, 0))
            except ValueError:
                continue
        return value

    available = total(stock_rows, "Giacenza")
    used = total(used_rows, "Quantita")
    generated = total(generated_rows, "Quantita")
    original = generated if generated_rows else available + used
    return {
        "original_quantity": _decimal_to_text(original),
        "available_quantity": _decimal_to_text(available),
        "used_quantity": _decimal_to_text(used),
        "original_source": "generated" if generated_rows else "reconstructed",
    }


def build_storico_lotti_search(params) -> dict:
    lotto = _norm_text(params.get("lotto"))
    articolo = _norm_text(params.get("articolo"))
    if not lotto:
        return {"ok": False, "error": "Inserire il lotto completo."}

    materials = _lot_materials(lotto)
    if not articolo:
        return {"ok": True, "lotto": lotto, "materiali": materials,
                "rows": [], "total": 0, "limit_reached": False, "quantity_summary": None}
    selected = next((item for item in materials if item["codice"] == articolo), None)
    if selected is None:
        return {"ok": False, "error": "Il codice materiale non risulta associato al lotto indicato."}

    rows = []
    for model, kind in (
        (LottiUsatiLog, "used"),
        (LottiGeneratiLog, "generated"),
    ):
        matches = (
            model.query.filter_by(RifLottoAlfa=lotto, CodArt=articolo)
            .order_by(desc(model.ClosedAt), desc(model.logged_at), desc(model.log_id))
            .limit(LOT_SEARCH_LIMIT + 1)
            .all()
        )
        rows.extend(_lotto_search_row(row, kind) for row in matches)

    rows.sort(key=lambda row: (row["event_at"], row["log_id"]), reverse=True)
    limited = len(rows) > LOT_SEARCH_LIMIT
    return _json_safe(
        {
            "ok": True,
            "lotto": lotto,
            "articolo": articolo,
            "descrizione": selected["descrizione"],
            "materiali": materials,
            "rows": rows[:LOT_SEARCH_LIMIT],
            "total": min(len(rows), LOT_SEARCH_LIMIT),
            "limit_reached": limited,
            "quantity_summary": _lot_quantity_summary(lotto, articolo),
        }
    )


def build_storico_ordini_detail(params) -> dict:
    kind = _norm_text(params.get("kind"))
    members = []

    if kind == "group":
        group_uid = _norm_text(params.get("group_uid"))
        if not group_uid:
            return {"ok": False, "error": "Gruppo non valido."}
        runtime_rows = _runtime_rows_for_group(group_uid)
        group = OdpWorkGroup.query.filter_by(GroupUid=group_uid).first()
        members = OdpWorkGroupMember.query.filter_by(GroupUid=group_uid).all()
        order_keys = {
            (_norm_text(m.IdDocumento), _norm_text(m.IdRiga)) for m in members
        }
        title = f"Gruppo {group_uid}"
        header = {
            "kind": "group",
            "title": title,
            "group_uid": group_uid,
            "group_type": _group_label(_effective_group_type(group)),
            "status": _norm_text(getattr(group, "Status", "")),
            "members": [
                {
                    "ordine": _norm_text(m.RifRegistraz)
                    or f"{m.IdDocumento}/{m.IdRiga}",
                    "id_documento": _norm_text(m.IdDocumento),
                    "id_riga": _norm_text(m.IdRiga),
                    "num_progr_riga": _norm_text(m.NumProgrRiga),
                    "articolo": _norm_text(m.CodArt),
                    "descrizione": _norm_text(m.DesArt),
                    "ruolo": _norm_text(m.Role),
                    "time_share": _norm_text(m.TimeShareMode),
                    "status": _norm_text(m.Status),
                }
                for m in members
            ],
        }
    else:
        id_documento = _norm_text(params.get("id_documento"))
        id_riga = _norm_text(params.get("id_riga"))
        if not id_documento or not id_riga:
            return {"ok": False, "error": "Ordine non valido."}
        runtime_rows = _runtime_rows_for_order(id_documento, id_riga)
        group = None
        order_keys = {(id_documento, id_riga)}
        title = f"{id_documento}/{id_riga}"
        header = {
            "kind": "order",
            "title": title,
            "id_documento": id_documento,
            "id_riga": id_riga,
            "members": [],
        }

    input_logs = _input_logs_for_runtime_or_orders(
        runtime_rows,
        order_keys=order_keys,
        group=group,
    )
    num_progr_map = _num_progr_by_order(runtime_rows, members)
    used_lots = _lotti_for_runtime_or_inputs(
        LottiUsatiLog,
        runtime_rows,
        input_logs,
        order_keys=order_keys,
        group=group,
    )
    generated_lots = _lotti_for_runtime_or_inputs(
        LottiGeneratiLog,
        runtime_rows,
        input_logs,
        order_keys=order_keys,
        group=group,
    )

    timeline = [_runtime_to_dict(row) for row in runtime_rows]
    runtime_ops = set(_operation_ids(runtime_rows))
    timeline.extend(
        _input_timeline_to_dict(row, num_progr_map)
        for row in input_logs
        if _norm_text(row.OperationGroupId) not in runtime_ops
    )
    timeline.sort(
        key=lambda row: (_norm_text(row["event_at"]), row["source"], row["log_id"])
    )
    timeline = _prepare_timeline(timeline, input_logs, used_lots, generated_lots)
    if header["kind"] == "order":
        first = next(iter(timeline), None)
        if first:
            header["title"] = first["ordine"]

    return _json_safe(
        {
            "ok": True,
            "header": header,
            "timeline": timeline,
            "passaggi": _timeline_steps(timeline, header.get("group_uid", "")),
            "registrazioni_tempi": [event for event in timeline if event["tecnico"]],
            "input_logs": [_input_to_dict(row, num_progr_map) for row in input_logs],
            "lotti_usati": [_lotto_usato_to_dict(row) for row in used_lots],
            "lotti_generati": [_lotto_generato_to_dict(row) for row in generated_lots],
        }
    )
