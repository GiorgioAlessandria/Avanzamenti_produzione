from __future__ import annotations

from datetime import datetime, date, timedelta, time
import json

from sqlalchemy import and_, desc, or_

from app_odp.models import (
    InputOdpLog,
    LottiGeneratiLog,
    LottiUsatiLog,
    OdpRuntimeLog,
    OdpWorkGroup,
    OdpWorkGroupMember,
)
from app_odp.services.order_helpers import _json_safe, _norm_text


PAGE_SIZE_DEFAULT = 50
SCAN_LIMIT = 5000


ACTION_LABELS = {
    "presa_in_carico": "Presa in carico",
    "presa_in_carico_gruppo": "Presa in carico gruppo",
    "sospensione": "Sospensione",
    "sospensione_gruppo": "Sospensione gruppo",
    "riattivazione": "Riattivazione",
    "riattivazione_macchina": "Riattivazione macchina",
    "riattivazione_gruppo": "Riattivazione gruppo",
    "chiusura_finale": "Chiusura totale",
    "chiusura_macchina": "Chiusura macchina",
    "chiusura_parziale": "Chiusura parziale",
    "chiusura_membro_scioglie_gruppo": "Chiusura membro / scioglimento gruppo",
    "runtime_pre_gruppo": "Tempo precedente al gruppo",
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
        return {"_raw": raw}
    return parsed if isinstance(parsed, dict) else {"value": parsed}


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


def _input_event_label(row: InputOdpLog) -> str:
    partial = _norm_text(row.ChiusuraParziale).lower() in {"true", "1", "si", "yes"}
    if _norm_text(row.QuantitaConforme) or _norm_text(row.QuantitaNonConforme):
        return "Chiusura parziale" if partial else "Chiusura totale"

    post = _norm_text(row.StatoOrdinePost).lower()
    if "attiv" in post:
        return "Presa in carico"
    if "sospes" in post:
        return "Sospensione"

    note = _norm_text(row.NoteChiusura)
    return note.split("|", 1)[0].strip() if note else "Log ordine"


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
    entry["articles"].add(_norm_text(getattr(row, "CodArt", "")))
    entry["operatori"].add(_norm_text(user))
    entry["eventi"].add(event_label)
    entry["risorse"].add(
        _norm_text(getattr(row, "RisorsaAttiva", ""))
        or _norm_text(getattr(row, "CodRisorsaProd", ""))
    )
    entry["count"] += 1

    event_at = _row_event_at(row)
    if not entry["last_event_at"] or event_at > entry["last_event_at"]:
        entry["last_event_at"] = event_at
        entry["last_event"] = event_label


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
        ("evento", OdpRuntimeLog.Azione),
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
        ("evento", InputOdpLog.NoteChiusura),
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
        group_uid = _group_for_event(row, payload, member_by_order, groups_by_uid)
        entry = _event_entry(entries, row, group_uid, payload, groups_by_uid)
        _add_entry_event(entry, row, _action_label(row.Azione), row.UtenteOperazione)

        for input_log in input_by_operation.get(_norm_text(row.OperationGroupId), []):
            entry["risorse"].add(
                _norm_text(input_log.RisorsaAttiva)
                or _norm_text(input_log.CodRisorsaProd)
            )
            entry["articles"].add(_norm_text(input_log.CodArt))

    for row in input_event_rows:
        payload = {}
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
        for field in ("orders", "articles", "risorse", "operatori", "eventi"):
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
            if _norm_text(
                payload.get("group_uid")
            ) == group_uid or _event_in_group_window(row.EventAt, group):
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
            for row in candidates:
                if _order_key(row) in order_keys and (
                    group is None or _event_in_group_window(row.ClosedAt, group)
                ):
                    rows.append(row)

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
            for row in candidates:
                if _order_key(row) in order_keys and (
                    group is None or _event_in_group_window(row.ClosedAt, group)
                ):
                    rows.append(row)

    by_id = {row.log_id: row for row in rows}
    return sorted(
        by_id.values(), key=lambda row: (_norm_text(row.ClosedAt), row.log_id or 0)
    )


def _event_description(row, payload: dict) -> str:
    action = _action_label(row.Azione)
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

    note = _norm_text(row.Note or row.Motivo)
    if note:
        parts.append(note)

    return " | ".join(parts)


def _runtime_to_dict(row):
    payload = _payload(row)
    return {
        "log_id": row.log_id,
        "source": "runtime",
        "event_at": _norm_text(row.EventAt),
        "event_at_display": _format_dt(row.EventAt),
        "azione": _norm_text(row.Azione),
        "azione_label": _action_label(row.Azione),
        "descrizione": _event_description(row, payload),
        "utente": _norm_text(row.UtenteOperazione),
        "ordine": _norm_text(row.RifRegistraz) or f"{row.IdDocumento}/{row.IdRiga}",
        "id_documento": _norm_text(row.IdDocumento),
        "id_riga": _norm_text(row.IdRiga),
        "articolo": _norm_text(row.CodArt),
        "fase_pre": _norm_text(row.FasePre),
        "fase_post": _norm_text(row.FasePost),
        "stato_pre": _norm_text(row.StatoOrdinePre or row.StatoOdpPre),
        "stato_post": _norm_text(row.StatoOrdinePost or row.StatoOdpPost),
        "q_ok": _norm_text(row.QuantitaConforme),
        "q_ko": _norm_text(row.QuantitaNonConforme),
        "elapsed_seconds": _norm_text(row.ElapsedSeconds),
        "tempo_lavorazione_ore": _positive_number_text(row.TempoFunzionamentoPost),
        "tempo_non_funzionamento_minuti": _non_working_minutes(row),
        "payload": payload,
        "payload_pretty": json.dumps(payload, ensure_ascii=False, indent=2)
        if payload
        else "",
    }


def _input_to_dict(row):
    return {
        "log_id": row.log_id,
        "closed_at": _norm_text(row.ClosedAt),
        "closed_at_display": _format_dt(row.ClosedAt),
        "ordine": _norm_text(row.RifRegistraz) or f"{row.IdDocumento}/{row.IdRiga}",
        "id_documento": _norm_text(row.IdDocumento),
        "id_riga": _norm_text(row.IdRiga),
        "articolo": _norm_text(row.CodArt),
        "descrizione": _norm_text(row.DesArt),
        "fase": _norm_text(row.FaseConsuntivata or row.FaseAttiva),
        "risorsa": _norm_text(row.RisorsaAttiva or row.CodRisorsaProd),
        "q_ok": _norm_text(row.QuantitaConforme),
        "q_ko": _norm_text(row.QuantitaNonConforme),
        "tempo_finale": _norm_text(row.TempoFunzionamentoFinale),
        "tempo_non_funzionamento_minuti": _non_working_minutes(row),
        "chiusura_parziale": _norm_text(row.ChiusuraParziale),
        "note": _norm_text(row.NoteChiusura),
        "utente": _norm_text(row.ClosedBy),
        "matricola": _norm_text(row.CodMatricola),
    }


def _input_timeline_to_dict(row):
    label = _input_event_label(row)
    user = _norm_text(row.ClosedBy) or "-"
    description = f"{user} - {label}"
    state_pre = _norm_text(row.StatoOrdinePre)
    state_post = _norm_text(row.StatoOrdinePost)
    if state_pre or state_post:
        description += f" | stato {state_pre or '-'} -> {state_post or '-'}"
    if _norm_text(row.QuantitaConforme) or _norm_text(row.QuantitaNonConforme):
        description += f" | OK {_norm_text(row.QuantitaConforme) or '0'} / KO {_norm_text(row.QuantitaNonConforme) or '0'}"
    if _norm_text(row.NoteChiusura):
        description += f" | {_norm_text(row.NoteChiusura)}"

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
        "azione": "input_odp_log",
        "azione_label": label,
        "descrizione": description,
        "utente": _norm_text(row.ClosedBy),
        "ordine": _norm_text(row.RifRegistraz) or f"{row.IdDocumento}/{row.IdRiga}",
        "id_documento": _norm_text(row.IdDocumento),
        "id_riga": _norm_text(row.IdRiga),
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


def build_storico_ordini_detail(params) -> dict:
    kind = _norm_text(params.get("kind"))

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
        _input_timeline_to_dict(row)
        for row in input_logs
        if _norm_text(row.OperationGroupId) not in runtime_ops
    )
    timeline.sort(
        key=lambda row: (_norm_text(row["event_at"]), row["source"], row["log_id"])
    )

    return _json_safe(
        {
            "ok": True,
            "header": header,
            "timeline": timeline,
            "input_logs": [_input_to_dict(row) for row in input_logs],
            "lotti_usati": [_lotto_usato_to_dict(row) for row in used_lots],
            "lotti_generati": [_lotto_generato_to_dict(row) for row in generated_lots],
        }
    )
