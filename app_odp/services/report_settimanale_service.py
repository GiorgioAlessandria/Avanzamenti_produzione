from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime
from typing import Any

from sqlalchemy import and_, or_, select

from app_odp.models import db, InputOdpLog, OdpRuntimeLog, InputOdpRuntime, User

CLOSED_STATES = {"chiusa", "chiuso"}
DELETED_STATE = "eliminato dal gestionale"
ACTIVE_STATES = {"attivo", "attiva"}
STOP_STATES = {"in sospeso", "sospeso", "sospesa", "chiusa", "chiuso"}


def _norm(value: Any) -> str:
    return str(value or "").strip()


def _norm_l(value: Any) -> str:
    return _norm(value).lower()


def _to_float(value: Any) -> float:
    raw = _norm(value).replace(",", ".")
    if not raw:
        return 0.0

    try:
        return float(raw)
    except (TypeError, ValueError):
        return 0.0


def _round2(value: float) -> float:
    return round(float(value or 0), 2)


def _percent(real: float, planned: float) -> float | None:
    if not planned:
        return None

    return round((real / planned) * 100, 2)


def _parse_iso(value: Any) -> datetime | None:
    raw = _norm(value)
    if not raw:
        return None

    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def _format_dt(value: Any) -> str:
    parsed = _parse_iso(value)

    if parsed:
        return parsed.strftime("%d/%m/%Y %H:%M")

    return _norm(value)


def _num_progr_riga(row) -> str:
    return _norm(getattr(row, "NumProgrRiga", "")) or _norm(getattr(row, "IdRiga", ""))


def _work_row_key(row) -> tuple[str, str, str, str]:
    """
    Chiave finale di aggregazione report.

    Divide le righe per:
    - IdDocumento
    - IdRiga
    - Fase
    - NumProgrRiga
    """
    return (
        _norm(row.IdDocumento),
        _norm(row.IdRiga),
        _phase_value(row),
        _num_progr_riga(row),
    )


def _order_key(row) -> tuple[str, str]:
    return _norm(row.IdDocumento), _norm(row.IdRiga)


def _phase_value(row) -> str:
    """
    Fase usata per separare i tempi degli ordini multifase.

    Normalizza anche valori salvati come lista JSON:
    - "[1.0]" -> "1"
    - "[1]"   -> "1"
    """
    raw = (
        _norm(getattr(row, "NumFase", ""))
        or _norm(getattr(row, "FaseAttiva", ""))
        or _norm(getattr(row, "FaseConsuntivata", ""))
        or _norm(getattr(row, "FasePost", ""))
        or _norm(getattr(row, "FasePre", ""))
        or _norm(getattr(row, "Fase", ""))
        or _norm(getattr(row, "NumeroFase", ""))
    )

    if not raw:
        return "0"

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        parsed = None

    if isinstance(parsed, list):
        if not parsed:
            return "0"
        raw = _norm(parsed[0])
    elif parsed is not None:
        raw = _norm(parsed)

    number = _phase_number_from_value(raw)

    if number is not None:
        return str(number)

    return raw or "0"


def _num_progr_riga(row) -> str:
    return (
        _norm(getattr(row, "NumProgrRiga", ""))
        or _norm(getattr(row, "NumProgrRigaPost", ""))
        or _norm(getattr(row, "NumProgrRigaPre", ""))
        or "-"
    )


def _phase_key(row) -> tuple[str, str, str]:
    id_documento, id_riga = _order_key(row)

    return id_documento, id_riga, _phase_value(row)


def _order_key_from_phase_key(phase_key: tuple[str, str, str]) -> tuple[str, str]:
    return phase_key[0], phase_key[1]


def _phase_number_from_value(value: Any) -> int | None:
    number = _to_float(value)

    if number <= 0:
        return None

    return int(number)


def _phase_label_from_key(phase_key: tuple[str, str, str]) -> str:
    phase = _norm(phase_key[2])

    if not phase or phase == "0":
        return "-"

    number = _phase_number_from_value(phase)

    if number is not None:
        return str(number)

    return phase


def _phase_sort_key(phase_key: tuple[str, str, str]) -> tuple[int, str]:
    phase = _norm(phase_key[2])
    number = _phase_number_from_value(phase)

    if number is not None:
        return 0, f"{number:04d}"

    return 1, phase


def _is_unknown_phase(phase_key: tuple[str, str, str] | None) -> bool:
    if phase_key is None:
        return True

    return not _norm(phase_key[2]) or _norm(phase_key[2]) == "0"


def _parse_tempo_previsto_lavoraz_for_phase(
    row: InputOdpLog,
    phase_key: tuple[str, str, str] | None = None,
) -> float:
    """
    Se TempoPrevistoLavoraz è una lista JSON, prende solo il valore della fase.

    Esempio:
    - TempoPrevistoLavoraz = [1.5, 2.0]
    - fase = 1  -> 1.5
    - fase = 2  -> 2.0

    Se la fase non è ricavabile, usa la somma totale come fallback.
    """
    raw = _norm(row.TempoPrevistoLavoraz)

    if not raw:
        return 0.0

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return _to_float(raw)

    if not isinstance(parsed, list):
        return _to_float(parsed)

    selected_phase = phase_key[2] if phase_key else _phase_value(row)
    phase_number = _phase_number_from_value(selected_phase)

    if phase_number is None:
        return sum(_to_float(item) for item in parsed)

    index = phase_number - 1

    if index < 0 or index >= len(parsed):
        return 0.0

    return _to_float(parsed[index])


def _is_deleted_state(pre: Any, post: Any) -> bool:
    return _norm_l(pre) == DELETED_STATE or _norm_l(post) == DELETED_STATE


def _is_input_log_deleted(row: InputOdpLog) -> bool:
    return _is_deleted_state(row.StatoOrdinePre, row.StatoOrdinePost)


def _is_runtime_log_deleted(row: OdpRuntimeLog) -> bool:
    return _is_deleted_state(
        row.StatoOrdinePre, row.StatoOrdinePost
    ) or _is_deleted_state(row.StatoOdpPre, row.StatoOdpPost)


def _is_activation_input_log(row: InputOdpLog) -> bool:
    return _norm_l(row.StatoOrdinePost) in ACTIVE_STATES


def _is_stop_input_log(row: InputOdpLog) -> bool:
    return _norm_l(row.StatoOrdinePost) in STOP_STATES


def _is_closed_log(row) -> bool:
    return (
        _norm_l(getattr(row, "StatoOrdinePost", "")) in CLOSED_STATES
        or _norm_l(getattr(row, "StatoOdpPost", "")) in CLOSED_STATES
    )


def _runtime_by_operation_group(
    runtime_logs: list[OdpRuntimeLog],
) -> dict[str, OdpRuntimeLog]:
    out: dict[str, OdpRuntimeLog] = {}

    for row in runtime_logs:
        key = _norm(row.OperationGroupId)
        if key:
            out[key] = row

    return out


def _get_runtime_for_input_log(
    input_log: InputOdpLog,
    runtime_map: dict[str, OdpRuntimeLog],
) -> OdpRuntimeLog | None:
    return runtime_map.get(_norm(input_log.OperationGroupId))


def _runtime_delta_hours(runtime_log: OdpRuntimeLog | None) -> float:
    if runtime_log is None:
        return 0.0

    pre = _to_float(runtime_log.TempoFunzionamentoPre)
    post = _to_float(runtime_log.TempoFunzionamentoPost)

    delta = post - pre

    return delta if delta > 0 else 0.0


def _final_close_hours(input_log: InputOdpLog) -> float:
    """
    Per ordini chiusi usa il tempo finale registrato in InputOdpLog.
    """
    return _to_float(getattr(input_log, "TempoFunzionamentoFinale", None))


def _runtime_current_hours(runtime_row: InputOdpRuntime | None) -> float:
    """
    Fallback: legge il tempo attuale dalla tabella InputOdpRuntime.

    Uso getattr multipli perché il nome Python del campo potrebbe differire
    dal nome colonna DB.
    """
    if runtime_row is None:
        return 0.0

    return _to_float(
        getattr(runtime_row, "Tempo_funzionamento", None)
        or getattr(runtime_row, "TempoFunzionamento", None)
        or getattr(runtime_row, "tempo_funzionamento", None)
    )


def _worked_hours_with_fallback(
    *,
    input_log: InputOdpLog,
    runtime_map: dict[str, OdpRuntimeLog],
    runtime_current_map: dict[tuple[str, str, str], InputOdpRuntime],
    runtime_current_by_order: dict[tuple[str, str], InputOdpRuntime],
    runtime_current_by_rif: dict[str, InputOdpRuntime],
) -> float:
    """
    Prima prova OdpRuntimeLog.
    Se non trova un delta valido, usa InputOdpRuntime.Tempo_funzionamento.

    Fallback:
    1. OperationGroupId -> OdpRuntimeLog
    2. IdDocumento + IdRiga + fase
    3. IdDocumento + IdRiga
    4. RifRegistraz
    """
    if _is_closed_log(input_log):
        final_hours = _final_close_hours(input_log)

        if final_hours > 0:
            return final_hours
    runtime_row = _get_runtime_for_input_log(input_log, runtime_map)

    worked = _runtime_delta_hours(runtime_row)

    if worked > 0:
        return worked

    phase_runtime = runtime_current_map.get(_phase_key(input_log))
    phase_hours = _runtime_current_hours(phase_runtime)

    if phase_hours > 0:
        return phase_hours

    order_runtime = runtime_current_by_order.get(_order_key(input_log))
    order_hours = _runtime_current_hours(order_runtime)

    if order_hours > 0:
        return order_hours

    rif = _norm(getattr(input_log, "RifRegistraz", ""))
    rif_runtime = runtime_current_by_rif.get(rif)

    return _runtime_current_hours(rif_runtime)


def _is_macchina(row: InputOdpLog) -> bool:
    return _norm_l(row.GestioneMatricola) == "si"


def _qta_finale(row: InputOdpLog) -> float:
    """
    Quantità del componente finale CodArt.
    Per il KPI componenti uso Quantita dell'ordine, non la distinta.
    """
    return _to_float(row.Quantita)


def _qta_prodotta(row: InputOdpLog) -> float:
    """
    Quantità effettivamente prodotta:
    conforme + non conforme.

    Serve per calcolare il tempo previsto corretto, perché il gestionale
    fornisce TempoPrevistoLavoraz in minuti per singolo componente.
    """
    conforme = _to_float(getattr(row, "QuantitaConforme", None))
    non_conforme = _to_float(getattr(row, "QuantitaNonConforme", None))

    totale = conforme + non_conforme

    if totale > 0:
        return totale

    return 0.0


def _row_label(row: InputOdpLog) -> str:
    return _norm(row.RifRegistraz) or f"{_norm(row.IdDocumento)}.{_norm(row.IdRiga)}"


def _username(user: User | None) -> str:
    if user is None:
        return ""

    return _norm(getattr(user, "username", ""))


def _latest_state(row: InputOdpLog) -> str:
    return (
        _norm(getattr(row, "StatoOdpPost", ""))
        or _norm(getattr(row, "StatoOrdinePost", ""))
        or "-"
    )


def _chunks(values: list[tuple[str, str]], size: int = 400):
    for index in range(0, len(values), size):
        yield values[index : index + size]


def get_report_users() -> list[dict[str, Any]]:
    rows = db.session.execute(
        select(User.id, User.username)
        .where(User.active.is_(True))
        .order_by(User.username.asc())
    ).all()

    return [
        {
            "id": row.id,
            "username": row.username,
        }
        for row in rows
    ]


def _load_runtime_current_for_orders(
    order_keys: set[tuple[str, str]],
) -> list[InputOdpRuntime]:
    if not order_keys:
        return []

    rows: list[InputOdpRuntime] = []
    ordered_keys = sorted(order_keys)

    for chunk in _chunks(ordered_keys):
        filters = [
            and_(
                InputOdpRuntime.IdDocumento == id_documento,
                InputOdpRuntime.IdRiga == id_riga,
            )
            for id_documento, id_riga in chunk
        ]

        chunk_rows = (
            db.session.execute(
                select(InputOdpRuntime)
                .where(or_(*filters))
                .order_by(
                    InputOdpRuntime.IdDocumento.asc(),
                    InputOdpRuntime.IdRiga.asc(),
                )
            )
            .scalars()
            .all()
        )

        rows.extend(chunk_rows)

    return rows


def _runtime_current_by_phase(
    runtime_rows: list[InputOdpRuntime],
) -> dict[tuple[str, str, str], InputOdpRuntime]:
    out = {}

    for row in runtime_rows:
        out[_phase_key(row)] = row

    return out


def _runtime_current_by_order(
    runtime_rows: list[InputOdpRuntime],
) -> dict[tuple[str, str], InputOdpRuntime]:
    out: dict[tuple[str, str], InputOdpRuntime] = {}

    for row in runtime_rows:
        out[_order_key(row)] = row

    return out


def _runtime_current_by_rif(
    runtime_rows: list[InputOdpRuntime],
) -> dict[str, InputOdpRuntime]:
    out: dict[str, InputOdpRuntime] = {}

    for row in runtime_rows:
        rif = _norm(getattr(row, "RifRegistraz", ""))
        if not rif:
            continue

        current = out.get(rif)

        if current is None:
            out[rif] = row
            continue

        if _runtime_current_hours(row) > _runtime_current_hours(current):
            out[rif] = row

    return out


def _load_user_by_id(user_id: int) -> User | None:
    return db.session.get(User, int(user_id))


def _load_deleted_order_keys() -> set[tuple[str, str]]:
    """
    Esclude totalmente gli ordini che in uno dei log risultano eliminati dal gestionale.
    """
    deleted_keys: set[tuple[str, str]] = set()

    input_deleted = (
        db.session.execute(
            select(InputOdpLog).where(
                or_(
                    InputOdpLog.StatoOrdinePre == "Eliminato dal gestionale",
                    InputOdpLog.StatoOrdinePost == "Eliminato dal gestionale",
                )
            )
        )
        .scalars()
        .all()
    )

    for row in input_deleted:
        deleted_keys.add(_order_key(row))

    runtime_deleted = (
        db.session.execute(
            select(OdpRuntimeLog).where(
                or_(
                    OdpRuntimeLog.StatoOrdinePre == "Eliminato dal gestionale",
                    OdpRuntimeLog.StatoOrdinePost == "Eliminato dal gestionale",
                    OdpRuntimeLog.StatoOdpPre == "Eliminato dal gestionale",
                    OdpRuntimeLog.StatoOdpPost == "Eliminato dal gestionale",
                )
            )
        )
        .scalars()
        .all()
    )

    for row in runtime_deleted:
        deleted_keys.add(_order_key(row))

    return deleted_keys


def _load_runtime_logs_in_period(
    *,
    start_iso: str,
    end_iso: str,
    deleted_keys: set[tuple[str, str]],
) -> list[OdpRuntimeLog]:
    """
    Carica i movimenti runtime nel periodo, indipendentemente dal fatto che
    l'ordine sia chiuso oppure ancora aperto/sospeso.

    Questo è il cambio principale rispetto alla versione precedente:
    il report non parte più dagli ordini chiusi, ma dalle lavorazioni.
    """
    rows = (
        db.session.execute(
            select(OdpRuntimeLog)
            .where(
                OdpRuntimeLog.EventAt >= start_iso,
                OdpRuntimeLog.EventAt <= end_iso,
            )
            .order_by(
                OdpRuntimeLog.IdDocumento.asc(),
                OdpRuntimeLog.IdRiga.asc(),
                OdpRuntimeLog.EventAt.asc(),
                OdpRuntimeLog.log_id.asc(),
            )
        )
        .scalars()
        .all()
    )

    return [
        row
        for row in rows
        if _order_key(row) not in deleted_keys and not _is_runtime_log_deleted(row)
    ]


def _load_input_logs_in_period_for_user(
    *,
    username: str,
    start_iso: str,
    end_iso: str,
    deleted_keys: set[tuple[str, str]],
) -> list[InputOdpLog]:
    """
    Carica le righe log in cui compare direttamente l'operatore nel periodo.

    Serve per contare anche ordini dove l'utente compare nei log ma non esiste
    ancora un delta runtime consolidato, ad esempio ordine preso in carico e
    ancora attivo.
    """
    rows = (
        db.session.execute(
            select(InputOdpLog)
            .where(
                InputOdpLog.ClosedAt >= start_iso,
                InputOdpLog.ClosedAt <= end_iso,
                InputOdpLog.ClosedBy == username,
            )
            .order_by(
                InputOdpLog.IdDocumento.asc(),
                InputOdpLog.IdRiga.asc(),
                InputOdpLog.ClosedAt.asc(),
                InputOdpLog.log_id.asc(),
            )
        )
        .scalars()
        .all()
    )

    return [
        row
        for row in rows
        if _order_key(row) not in deleted_keys and not _is_input_log_deleted(row)
    ]


def _load_input_logs_for_orders(order_keys: set[tuple[str, str]]) -> list[InputOdpLog]:
    if not order_keys:
        return []

    rows: list[InputOdpLog] = []
    ordered_keys = sorted(order_keys)

    for chunk in _chunks(ordered_keys):
        filters = [
            and_(
                InputOdpLog.IdDocumento == id_documento,
                InputOdpLog.IdRiga == id_riga,
            )
            for id_documento, id_riga in chunk
        ]

        chunk_rows = (
            db.session.execute(
                select(InputOdpLog)
                .where(or_(*filters))
                .order_by(
                    InputOdpLog.IdDocumento.asc(),
                    InputOdpLog.IdRiga.asc(),
                    InputOdpLog.ClosedAt.asc(),
                    InputOdpLog.log_id.asc(),
                )
            )
            .scalars()
            .all()
        )

        rows.extend(chunk_rows)

    return rows


def _calculate_worked_hours_by_user_phase(
    *,
    input_logs: list[InputOdpLog],
    runtime_map: dict[str, OdpRuntimeLog],
    runtime_current_map: dict[tuple[str, str, str], InputOdpRuntime],
    runtime_current_by_order: dict[tuple[str, str], InputOdpRuntime],
    runtime_current_by_rif: dict[str, InputOdpRuntime],
) -> dict[str, dict[tuple[str, str, str], float]]:
    """
    Calcola le ore lavorate per utente e per fase.

    Regola:
    - l'utente viene preso dal log di attivazione/presa in carico;
    - il tempo viene letto dal runtime log collegato via OperationGroupId;
    - il delta è TempoFunzionamentoPost - TempoFunzionamentoPre;
    - la fase entra nella chiave, quindi fase 1 e fase 2 restano separate.
    """
    result: dict[str, dict[tuple[str, str, str], float]] = defaultdict(
        lambda: defaultdict(float)
    )

    active_by_operation: dict[str, tuple[str, tuple[str, str, str]]] = {}

    for row in input_logs:
        operation_group_id = _norm(row.OperationGroupId)

        if operation_group_id and _is_activation_input_log(row):
            active_by_operation[operation_group_id] = (
                _norm(row.ClosedBy),
                _phase_key(row),
            )

    consumed_operations: set[str] = set()

    for row in input_logs:
        if not _is_stop_input_log(row):
            continue

        operation_group_id = _norm(row.OperationGroupId)
        worked_hours = _worked_hours_with_fallback(
            input_log=row,
            runtime_map=runtime_map,
            runtime_current_map=runtime_current_map,
            runtime_current_by_order=runtime_current_by_order,
            runtime_current_by_rif=runtime_current_by_rif,
        )

        if worked_hours <= 0:
            continue

        active_data = active_by_operation.get(operation_group_id)

        if not active_data:
            continue

        active_username, active_phase_key = active_data
        stop_phase_key = _phase_key(row)
        phase_key = (
            active_phase_key if _is_unknown_phase(stop_phase_key) else stop_phase_key
        )

        if active_username:
            result[active_username][phase_key] += worked_hours
            consumed_operations.add(operation_group_id)

    """
    Fallback: se per qualche motivo l'attivazione non ha OperationGroupId,
    provo a ricostruire il ciclo dalla sequenza dei log dello stesso ordine.
    """
    logs_by_order: dict[tuple[str, str], list[InputOdpLog]] = defaultdict(list)

    for row in input_logs:
        logs_by_order[_order_key(row)].append(row)

    for rows in logs_by_order.values():
        active_username: str | None = None
        active_phase_key: tuple[str, str, str] | None = None

        for row in rows:
            operation_group_id = _norm(row.OperationGroupId)

            if _is_activation_input_log(row):
                active_username = _norm(row.ClosedBy)
                active_phase_key = _phase_key(row)
                continue

            if not _is_stop_input_log(row):
                continue

            if operation_group_id in consumed_operations:
                active_username = None
                active_phase_key = None
                continue

            if not active_username:
                active_username = None
                active_phase_key = None
                continue

            worked_hours = _worked_hours_with_fallback(
                input_log=row,
                runtime_map=runtime_map,
                runtime_current_map=runtime_current_map,
                runtime_current_by_order=runtime_current_by_order,
                runtime_current_by_rif=runtime_current_by_rif,
            )

            if worked_hours > 0:
                stop_phase_key = _phase_key(row)
                phase_key = (
                    active_phase_key
                    if _is_unknown_phase(stop_phase_key)
                    else stop_phase_key
                )
                result[active_username][phase_key] += worked_hours

            active_username = None
            active_phase_key = None

    return {username: dict(values) for username, values in result.items()}


def _build_snapshots(
    input_logs: list[InputOdpLog],
) -> tuple[
    dict[tuple[str, str], InputOdpLog],
    dict[tuple[str, str, str], InputOdpLog],
]:
    snapshot_by_order: dict[tuple[str, str], InputOdpLog] = {}
    snapshot_by_phase: dict[tuple[str, str, str], InputOdpLog] = {}

    for row in input_logs:
        snapshot_by_order[_order_key(row)] = row
        snapshot_by_phase[_phase_key(row)] = row

    return snapshot_by_order, snapshot_by_phase


def build_report_settimanale_for_user(
    *,
    selected_user_id: int,
    start_dt,
    end_dt,
    can_select_user: bool,
    users: list[dict[str, Any]],
) -> dict[str, Any]:
    selected_user = _load_user_by_id(selected_user_id)

    if selected_user is None:
        return {
            "ok": False,
            "error": "Utente non trovato.",
        }

    selected_username = _username(selected_user)

    start_iso = start_dt.isoformat(timespec="seconds")
    end_iso = end_dt.isoformat(timespec="seconds")

    deleted_keys = _load_deleted_order_keys()

    runtime_logs_in_period = _load_runtime_logs_in_period(
        start_iso=start_iso,
        end_iso=end_iso,
        deleted_keys=deleted_keys,
    )

    user_input_logs_in_period = _load_input_logs_in_period_for_user(
        username=selected_username,
        start_iso=start_iso,
        end_iso=end_iso,
        deleted_keys=deleted_keys,
    )

    runtime_order_keys = {_order_key(row) for row in runtime_logs_in_period}
    user_input_order_keys = {_order_key(row) for row in user_input_logs_in_period}

    candidate_order_keys = runtime_order_keys | user_input_order_keys

    input_logs_for_candidate_orders = _load_input_logs_for_orders(candidate_order_keys)
    input_logs_for_candidate_orders = [
        row
        for row in input_logs_for_candidate_orders
        if _order_key(row) not in deleted_keys and not _is_input_log_deleted(row)
    ]

    runtime_map = _runtime_by_operation_group(runtime_logs_in_period)
    runtime_current_rows = _load_runtime_current_for_orders(candidate_order_keys)
    runtime_current_map = _runtime_current_by_phase(runtime_current_rows)
    runtime_current_by_order = _runtime_current_by_order(runtime_current_rows)
    runtime_current_by_rif = _runtime_current_by_rif(runtime_current_rows)

    worked_by_user_phase = _calculate_worked_hours_by_user_phase(
        input_logs=input_logs_for_candidate_orders,
        runtime_map=runtime_map,
        runtime_current_map=runtime_current_map,
        runtime_current_by_order=runtime_current_by_order,
        runtime_current_by_rif=runtime_current_by_rif,
    )

    selected_worked_phases = dict(worked_by_user_phase.get(selected_username, {}))

    selected_logged_phases = {
        _phase_key(row)
        for row in user_input_logs_in_period
        if _norm(row.ClosedBy) == selected_username
    }

    phase_snapshot_by_key = {
        _phase_key(row): row
        for row in user_input_logs_in_period
        if _norm(row.ClosedBy) == selected_username
    }

    for phase_key in selected_logged_phases:
        if selected_worked_phases.get(phase_key, 0.0) > 0:
            continue

        runtime_current = runtime_current_map.get(phase_key)
        current_hours = _runtime_current_hours(runtime_current)

        if current_hours <= 0:
            order_key = _order_key_from_phase_key(phase_key)
            runtime_current = runtime_current_by_order.get(order_key)
            current_hours = _runtime_current_hours(runtime_current)

        if current_hours <= 0:
            input_log = phase_snapshot_by_key.get(phase_key)
            rif = _norm(getattr(input_log, "RifRegistraz", "")) if input_log else ""
            runtime_current = runtime_current_by_rif.get(rif)
            current_hours = _runtime_current_hours(runtime_current)

        if current_hours > 0:
            selected_worked_phases[phase_key] = current_hours
    selected_phase_keys = set(selected_worked_phases) | selected_logged_phases

    snapshot_by_order, snapshot_by_phase = _build_snapshots(
        input_logs_for_candidate_orders
    )

    ordini_lavorati_by_key: dict[tuple[str, str, str], dict[str, Any]] = {}

    for phase_key in sorted(selected_phase_keys, key=_phase_sort_key):
        order_key = _order_key_from_phase_key(phase_key)
        snapshot = snapshot_by_phase.get(phase_key) or snapshot_by_order.get(order_key)

        if snapshot is None:
            continue

        tipo = "Macchina" if _is_macchina(snapshot) else "Semilavorato"

        ordini_lavorati_by_key[phase_key] = {
            "rif_ordine": _row_label(snapshot),
            "id_documento": _norm(snapshot.IdDocumento),
            "id_riga": _norm(snapshot.IdRiga),
            "num_progr_riga": _num_progr_riga(snapshot),
            "cod_art": _norm(snapshot.CodArt),
            "descrizione": _norm(snapshot.DesArt),
            "tipo": tipo,
            "qta_finale": _round2(_qta_finale(snapshot)),
            "stato": _latest_state(snapshot),
            "ultimo_evento": _format_dt(getattr(snapshot, "ClosedAt", "")),
            "fase": _phase_label_from_key(phase_key),
            "ore_previste": 0.0,
            "ore_impiegate": 0.0,
        }

    for phase_key in sorted(selected_phase_keys, key=_phase_sort_key):
        order_key = _order_key_from_phase_key(phase_key)
        row_data = ordini_lavorati_by_key.get(phase_key)

        if row_data is None:
            continue

        phase_snapshot = snapshot_by_phase.get(phase_key) or snapshot_by_order.get(
            order_key
        )

        if phase_snapshot is None:
            continue

        planned = _parse_tempo_previsto_lavoraz_for_phase(
            phase_snapshot,
            phase_key=phase_key,
        )

        worked = selected_worked_phases.get(phase_key, 0.0)

        if worked <= 0:
            worked = _worked_hours_with_fallback(
                input_log=phase_snapshot,
                runtime_map=runtime_map,
                runtime_current_map=runtime_current_map,
                runtime_current_by_order=runtime_current_by_order,
                runtime_current_by_rif=runtime_current_by_rif,
            )

        row_data["ore_previste"] += planned
        row_data["ore_impiegate"] += worked

    ordini_lavorati: list[dict[str, Any]] = []

    total_planned = 0.0
    total_worked = 0.0
    total_macchine = 0.0
    total_semilavorati = 0.0

    for phase_key, row_data in sorted(
        ordini_lavorati_by_key.items(),
        key=lambda item: (
            item[0][0],
            item[0][1],
            _phase_sort_key(item[0]),
        ),
    ):
        planned = row_data["ore_previste"]
        worked = row_data["ore_impiegate"]
        delta = worked - planned

        if row_data["tipo"] == "Macchina":
            total_macchine += row_data["qta_finale"]
        else:
            total_semilavorati += row_data["qta_finale"]

        total_planned += planned
        total_worked += worked

        row_data.update(
            {
                "fasi_lavorate": row_data.get("fase") or "-",
                "ore_previste": _round2(planned),
                "ore_impiegate": _round2(worked),
                "delta_ore": _round2(delta),
                "scostamento_percentuale": _percent(worked, planned),
            }
        )

        ordini_lavorati.append(row_data)

    total_delta = total_worked - total_planned

    kpi = {
        "utente": selected_username,
        "ordini_lavorati": len(ordini_lavorati),
        "ore_previste": _round2(total_planned),
        "ore_impiegate": _round2(total_worked),
        "delta_ore": _round2(total_delta),
        "scostamento_percentuale": _percent(total_worked, total_planned),
        "componenti_macchine": _round2(total_macchine),
        "componenti_semilavorati": _round2(total_semilavorati),
    }

    return {
        "ok": True,
        "can_select_user": can_select_user,
        "selected_user_id": selected_user_id,
        "users": users,
        "periodo": {
            "start": start_dt.strftime("%d/%m/%Y %H:%M"),
            "end": end_dt.strftime("%d/%m/%Y %H:%M"),
        },
        "kpi": kpi,
        "ordini_lavorati": ordini_lavorati,
    }
