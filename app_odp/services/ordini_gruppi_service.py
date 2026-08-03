from __future__ import annotations

# app_odp/services/ordini_gruppi_service.py
from app_odp.services.order_helpers import (
    _norm_text,
    _now_rome_dt,
    _parse_iso_dt,
    _first_code_from_cell,
    _qty_da_lavorare_text,
    _tempo_to_seconds,
    _seconds_to_tempo_text,
    _sync_active_fields_for_phase,
    _parse_qty_decimal,
    _decimal_to_text,
    _normalize_indice_articolo_search,
    _ordine_ref_label,
)
from app_odp.services.ordini_service import _fase_corrente_for_export
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
import json
from uuid import uuid4
from app_odp.models import (
    db,
    InputOdp,
    InputOdpRuntime,
    OdpRuntimeLog,
    OdpWorkGroup,
    OdpWorkGroupMember,
)
from app_odp.services.session_helpers import (
    _current_user_id,
    _current_username,
)
from app_odp.services.priorita_service import (
    _consume_priorita_ordine,
    _priorita_row_for_operatore_ordine,
    _snapshot_priorita_in_runtime,
)
from app_odp.services.ordini_runtime_service import (
    _accumulate_runtime_until,
    _ensure_stop_minutes_within_elapsed,
    _ensure_stato_attivo,
    _ensure_operator_can_activate_group,
)
from app_odp.services.ordini_query_service import _base_odp_query
from app_odp.services.home_service import _get_visible_odp_by_key
from app_odp.services.erp_export_service import _build_operation_group_id
from app_odp.services.ordini_log_service import (
    _add_input_odp_suspend_log,
    _add_input_odp_takeover_log,
)

GROUP_TYPE_MULTIPLO = "MULTIPLO"
GROUP_TYPE_MASCHERATO = "MASCHERATO"

GROUP_STATUS_ATTIVO = "Attivo"
GROUP_STATUS_SOSPESO = "In Sospeso"
GROUP_STATUS_CHIUSO = "Chiuso"
GROUP_STATUS_SCIOLTO = "Sciolto"

ROLE_MEMBER = "MEMBER"
ROLE_MAIN = "MAIN"
ROLE_MASKED = "MASKED"

SHARE_SPLIT = "SPLIT"
SHARE_FULL = "FULL"
SHARE_ZERO = "ZERO"


def _round_seconds(value: float | Decimal) -> int:
    return int(Decimal(str(value)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _now_iso() -> str:
    return _now_rome_dt().isoformat(timespec="seconds")


def _order_key(ordine) -> tuple[str, str]:
    return _norm_text(ordine.IdDocumento), _norm_text(ordine.IdRiga)


def _order_kind(ordine) -> str:
    return (
        "macchina"
        if _norm_text(getattr(ordine, "GestioneMatricola", "")).lower() == "si"
        else "semilavorato"
    )


def _order_reparto(ordine) -> str:
    return _first_code_from_cell(getattr(ordine, "CodReparto", ""))


def _order_is_pianificata(ordine) -> bool:
    return _norm_text(getattr(ordine, "StatoOrdine", "")).lower() == "pianificata"


def _order_can_enter_group(ordine) -> bool:
    return _norm_text(getattr(ordine, "StatoOrdine", "")).lower() in {
        "pianificata",
        "attivo",
        "in sospeso",
    }


def _active_group_for_order(id_documento: str, id_riga: str) -> OdpWorkGroup | None:
    member = (
        OdpWorkGroupMember.query.filter_by(
            IdDocumento=_norm_text(id_documento), IdRiga=_norm_text(id_riga)
        )
        .filter(
            OdpWorkGroupMember.Status.in_([GROUP_STATUS_ATTIVO, GROUP_STATUS_SOSPESO])
        )
        .order_by(OdpWorkGroupMember.id.desc())
        .first()
    )
    if member is None:
        return None

    group = OdpWorkGroup.query.filter_by(GroupUid=member.GroupUid).first()
    if group is None:
        return None

    if _norm_text(group.Status) in {GROUP_STATUS_ATTIVO, GROUP_STATUS_SOSPESO}:
        return group

    return None


def get_active_group_for_order(id_documento: str, id_riga: str) -> OdpWorkGroup | None:
    return _active_group_for_order(id_documento, id_riga)


def _ensure_order_can_enter_group(
    ordine, *, expected_reparto: str | None = None, expected_kind: str | None = None
) -> None:
    if not _order_can_enter_group(ordine):
        raise ValueError(
            f"Ordine {_order_label(ordine)} non inseribile: stato attuale '{ordine.StatoOrdine}'."
        )

    existing_group = _active_group_for_order(ordine.IdDocumento, ordine.IdRiga)
    if existing_group is not None:
        raise ValueError(
            f"Ordine {_order_label(ordine)} già presente nel gruppo {existing_group.GroupUid}."
        )

    if expected_reparto and _order_reparto(ordine) != expected_reparto:
        raise ValueError(
            "Gli ordini del gruppo devono appartenere allo stesso reparto."
        )

    if expected_kind and _order_kind(ordine) != expected_kind:
        raise ValueError(
            "Non è possibile mescolare ordini macchina e semilavorati nello stesso gruppo."
        )


def _order_label(ordine) -> str:
    rif = _norm_text(getattr(ordine, "RifRegistraz", ""))
    num = _norm_text(getattr(ordine, "NumProgrRiga", ""))
    if rif and num:
        return f"{rif}.{num}"
    return f"{_norm_text(ordine.IdDocumento)}/{_norm_text(ordine.IdRiga)}"


def _group_uid(prefix: str, ordini: list) -> str:
    refs = []
    for ordine in ordini:
        label = _order_label(ordine)
        safe = "".join(ch for ch in label if ch.isalnum() or ch in ("-", "_", "."))
        refs.append(safe or _norm_text(ordine.IdRiga))
    joined = "+".join(refs)
    if len(joined) > 90:
        joined = joined[:90].rstrip("+")
    return f"{prefix}{joined}-{uuid4().hex[:6]}"


def _priority_for_order(ordine) -> int | None:
    row = _priorita_row_for_operatore_ordine(
        operatore_id=_current_user_id(),
        id_documento=ordine.IdDocumento,
        id_riga=ordine.IdRiga,
        fase=_fase_corrente_for_export(ordine),
    )
    return int(row.Priorita) if row is not None and row.Priorita is not None else None


def _create_member(
    group_uid: str, ordine, *, role: str, share_mode: str, now_iso: str
) -> OdpWorkGroupMember:
    fase_corrente = _fase_corrente_for_export(ordine)
    return OdpWorkGroupMember(
        GroupUid=group_uid,
        IdDocumento=_norm_text(ordine.IdDocumento),
        IdRiga=_norm_text(ordine.IdRiga),
        NumProgrRiga=_norm_text(getattr(ordine, "NumProgrRiga", "")),
        RifRegistraz=_norm_text(getattr(ordine, "RifRegistraz", "")),
        CodArt=_norm_text(getattr(ordine, "CodArt", "")),
        VarianteArt=_norm_text(getattr(ordine, "VarianteArt", "")),
        IndiceModifica=_norm_text(getattr(ordine, "IndiceModifica", "")),
        DesArt=_norm_text(getattr(ordine, "DesArt", "")),
        Fase=fase_corrente,
        Role=role,
        TimeShareMode=share_mode,
        RuntimeSecondsAssigned=0,
        Status=GROUP_STATUS_ATTIVO,
        CreatedAt=now_iso,
    )


def _activate_order_for_group(ordine, *, group_uid: str, now_dt: datetime) -> None:
    fase_corrente = _fase_corrente_for_export(ordine)
    now_iso = now_dt.isoformat(timespec="seconds")

    _sync_active_fields_for_phase(ordine, fase_corrente)

    stato = InputOdpRuntime.query.filter_by(
        IdDocumento=ordine.IdDocumento,
        IdRiga=ordine.IdRiga,
    ).first()

    stato_ordine_pre = _norm_text(ordine.StatoOrdine)
    qty_pre = _qty_da_lavorare_text(ordine)
    priorita_row = _priorita_row_for_operatore_ordine(
        operatore_id=_current_user_id(),
        id_documento=ordine.IdDocumento,
        id_riga=ordine.IdRiga,
        fase=fase_corrente,
    )

    elapsed_pre_group = 0
    if stato is not None and _norm_text(stato.Stato_odp).lower().startswith("attiv"):
        elapsed_pre_group = _accumulate_runtime_until(stato, now_dt)

    stato = _ensure_stato_attivo(
        ordine=ordine,
        stato=stato,
        username=_current_username(),
        when_dt=now_dt,
        fase_corrente=fase_corrente,
    )
    stato.RifOrdinePrinc = ""
    _snapshot_priorita_in_runtime(
        stato=stato,
        priorita_row=priorita_row,
        operatore_id=_current_user_id(),
        when_iso=now_iso,
    )
    ordine.StatoOrdine = GROUP_STATUS_ATTIVO

    _consume_priorita_ordine(
        ordine.IdDocumento,
        ordine.IdRiga,
        ordine.FaseAttiva,
    )

    _append_group_state_log(
        group_uid=group_uid,
        ordine=ordine,
        action="presa_in_carico_gruppo",
        event_at=now_iso,
        stato_pre=stato_ordine_pre,
        stato_post=GROUP_STATUS_ATTIVO,
        qty_pre=qty_pre,
        qty_post=_qty_da_lavorare_text(ordine, stato=stato),
        note="Presa in carico da gruppo ordini",
    )
    if elapsed_pre_group:
        _append_group_runtime_log(
            group_uid=group_uid,
            ordine=ordine,
            action="runtime_pre_gruppo",
            event_at=now_iso,
            stato_pre=stato_ordine_pre,
            stato_post=GROUP_STATUS_ATTIVO,
            qty_pre=qty_pre,
            qty_post=_qty_da_lavorare_text(ordine, stato=stato),
            elapsed_seconds=elapsed_pre_group,
            note="Tempo singolo maturato prima del gruppo ordini",
        )


def _group_operation_group_id(ordine, action: str, event_at: str) -> str:
    return _build_operation_group_id(ordine=ordine, action=action, when_iso=event_at)


def _append_group_runtime_log(
    *,
    group_uid: str,
    ordine,
    action: str,
    event_at: str,
    stato_pre: str = "",
    stato_post: str = "",
    qty_pre: str = "",
    qty_post: str = "",
    elapsed_seconds: int = 0,
    note: str = "",
    extra_payload: dict | None = None,
):
    runtime = getattr(ordine, "runtime_row", None)
    payload = {
        "group_uid": group_uid,
        "azione": action,
        "utente": _current_username(),
    }
    if extra_payload:
        payload.update(extra_payload)

    db.session.add(
        OdpRuntimeLog(
            OperationGroupId=_group_operation_group_id(ordine, action, event_at),
            EventSequence=1,
            Topic="ordine_gruppo",
            Scope=_order_reparto(ordine),
            CodArt=_norm_text(getattr(ordine, "CodArt", "")),
            CodReparto=_norm_text(getattr(ordine, "CodReparto", "")),
            PayloadJson=json.dumps(payload, ensure_ascii=False),
            IdDocumento=ordine.IdDocumento,
            IdRiga=ordine.IdRiga,
            RifRegistraz=_norm_text(getattr(ordine, "RifRegistraz", "")),
            Azione=action,
            Motivo=note,
            UtenteOperazione=_current_username(),
            EventAt=event_at,
            StatoOdpPre=_norm_text(stato_pre),
            StatoOdpPost=_norm_text(stato_post),
            StatoOrdinePre=_norm_text(stato_pre),
            StatoOrdinePost=_norm_text(stato_post),
            FasePre=_norm_text(getattr(runtime, "FaseAttiva", "")),
            FasePost=_norm_text(getattr(runtime, "FaseAttiva", "")),
            TempoFunzionamentoPre="",
            TempoFunzionamentoPost=_norm_text(
                getattr(runtime, "Tempo_funzionamento", "")
            ),
            ElapsedSeconds=str(elapsed_seconds or 0),
            QtyDaLavorarePre=_norm_text(qty_pre),
            QtyDaLavorarePost=_norm_text(qty_post),
            Note=note,
            VarianteArt=_norm_text(getattr(ordine, "VarianteArt", "")),
            NumProgrRiga=_norm_text(getattr(ordine, "NumProgrRiga", "")),
        )
    )


def _append_group_state_log(
    *,
    group_uid: str,
    ordine,
    action: str,
    event_at: str,
    stato_pre: str,
    stato_post: str,
    qty_pre: str,
    qty_post: str,
    note: str,
    causale: str = "",
    minuti_non_funzionamento: int | None = None,
    secondi_non_funzionamento: int | None = None,
):
    operation_group_id = _group_operation_group_id(ordine, action, event_at)
    stato_post_norm = _norm_text(stato_post)

    if stato_post_norm == GROUP_STATUS_ATTIVO:
        _add_input_odp_takeover_log(
            operation_group_id=operation_group_id,
            ordine=ordine,
            stato_ordine_pre=stato_pre,
            stato_ordine_post=stato_post,
            qty_pre=qty_pre,
            qty_post=qty_post,
            taken_by=_current_username(),
            taken_at=event_at,
            note_evento=note,
        )
    elif stato_post_norm == GROUP_STATUS_SOSPESO:
        _add_input_odp_suspend_log(
            operation_group_id=operation_group_id,
            ordine=ordine,
            stato_ordine_pre=stato_pre,
            stato_ordine_post=stato_post,
            qty_pre=qty_pre,
            qty_post=qty_post,
            suspended_by=_current_username(),
            suspended_at=event_at,
            causale=causale,
            minuti_non_funzionamento=minuti_non_funzionamento,
            secondi_non_funzionamento=secondi_non_funzionamento,
            note_evento=note,
        )

    _append_group_runtime_log(
        group_uid=group_uid,
        ordine=ordine,
        action=action,
        event_at=event_at,
        stato_pre=stato_pre,
        stato_post=stato_post,
        qty_pre=qty_pre,
        qty_post=qty_post,
        note=note,
    )


def create_multiplo_group(order_keys: list[dict], policy) -> OdpWorkGroup:
    if not isinstance(order_keys, list) or len(order_keys) < 2:
        raise ValueError("Selezionare almeno 2 ordini per un gruppo multiplo.")
    if len(order_keys) > 3:
        raise ValueError("Un gruppo multiplo può contenere massimo 3 ordini.")

    ordini = [
        _get_visible_odp_by_key(
            policy,
            _norm_text(item.get("id_documento")),
            _norm_text(item.get("id_riga")),
        )
        for item in order_keys
    ]

    seen = set()
    for ordine in ordini:
        key = _order_key(ordine)
        if key in seen:
            raise ValueError("Lo stesso ordine è stato selezionato più volte.")
        seen.add(key)

    _ensure_operator_can_activate_group(
        [_order_key(ordine) for ordine in ordini],
        _current_username(),
    )

    expected_reparto = _order_reparto(ordini[0])
    expected_kind = _order_kind(ordini[0])
    for ordine in ordini:
        _ensure_order_can_enter_group(
            ordine,
            expected_reparto=expected_reparto,
            expected_kind=expected_kind,
        )

    now_dt = _now_rome_dt()
    now_iso = now_dt.isoformat(timespec="seconds")
    group_uid = _group_uid("MULT", ordini)

    group = OdpWorkGroup(
        GroupUid=group_uid,
        GroupType=GROUP_TYPE_MULTIPLO,
        Status=GROUP_STATUS_ATTIVO,
        CreatedAt=now_iso,
        StartedAt=now_iso,
        LastActivationAt=now_iso,
        OperatoreId=_current_user_id(),
        OperatoreUsername=_current_username(),
        Reparto=expected_reparto,
        Fase="",
        InitialMemberCount=len(ordini),
        TotalRuntimeSeconds=0,
    )
    db.session.add(group)

    for ordine in ordini:
        _activate_order_for_group(ordine, group_uid=group_uid, now_dt=now_dt)
        db.session.add(
            _create_member(
                group_uid,
                ordine,
                role=ROLE_MEMBER,
                share_mode=SHARE_SPLIT,
                now_iso=now_iso,
            )
        )

    db.session.flush()
    return group


def create_misto_group(
    shared_keys: list[dict], masked_key: dict, policy
) -> OdpWorkGroup:
    if not isinstance(shared_keys, list) or len(shared_keys) != 2:
        raise ValueError(
            "Selezionare esattamente 2 ordini con tempo condiviso per il gruppo misto."
        )

    shared_ordini = [
        _get_visible_odp_by_key(
            policy,
            _norm_text(item.get("id_documento")),
            _norm_text(item.get("id_riga")),
        )
        for item in shared_keys
    ]
    masked = _get_visible_odp_by_key(
        policy,
        _norm_text(masked_key.get("id_documento")),
        _norm_text(masked_key.get("id_riga")),
    )

    ordini = [*shared_ordini, masked]
    seen = set()
    for ordine in ordini:
        key = _order_key(ordine)
        if key in seen:
            raise ValueError("Lo stesso ordine è stato selezionato più volte.")
        seen.add(key)

    _ensure_operator_can_activate_group(
        [_order_key(ordine) for ordine in ordini],
        _current_username(),
    )

    expected_reparto = _order_reparto(shared_ordini[0])
    expected_kind = _order_kind(shared_ordini[0])
    for ordine in ordini:
        _ensure_order_can_enter_group(
            ordine,
            expected_reparto=expected_reparto,
            expected_kind=expected_kind,
        )

    now_dt = _now_rome_dt()
    now_iso = now_dt.isoformat(timespec="seconds")
    group_uid = _group_uid("MIX", ordini)

    group = OdpWorkGroup(
        GroupUid=group_uid,
        GroupType=GROUP_TYPE_MULTIPLO,
        Status=GROUP_STATUS_ATTIVO,
        CreatedAt=now_iso,
        StartedAt=now_iso,
        LastActivationAt=now_iso,
        OperatoreId=_current_user_id(),
        OperatoreUsername=_current_username(),
        Reparto=expected_reparto,
        Fase="",
        Note="MISTO",
        InitialMemberCount=len(shared_ordini),
        TotalRuntimeSeconds=0,
    )
    db.session.add(group)

    for ordine in shared_ordini:
        _activate_order_for_group(ordine, group_uid=group_uid, now_dt=now_dt)
        db.session.add(
            _create_member(
                group_uid,
                ordine,
                role=ROLE_MEMBER,
                share_mode=SHARE_SPLIT,
                now_iso=now_iso,
            )
        )

    _activate_order_for_group(masked, group_uid=group_uid, now_dt=now_dt)
    db.session.add(
        _create_member(
            group_uid,
            masked,
            role=ROLE_MASKED,
            share_mode=SHARE_ZERO,
            now_iso=now_iso,
        )
    )

    db.session.flush()
    return group


def create_mascherato_group(main_key: dict, masked_keys, policy) -> OdpWorkGroup:
    if isinstance(masked_keys, dict):
        masked_keys = [masked_keys]
    if not isinstance(masked_keys, list) or not masked_keys:
        raise ValueError("Selezionare almeno un ordine mascherato.")

    main = _get_visible_odp_by_key(
        policy,
        _norm_text(main_key.get("id_documento")),
        _norm_text(main_key.get("id_riga")),
    )
    masked_ordini = [
        _get_visible_odp_by_key(
            policy,
            _norm_text(masked_key.get("id_documento")),
            _norm_text(masked_key.get("id_riga")),
        )
        for masked_key in masked_keys
        if isinstance(masked_key, dict)
    ]
    if not masked_ordini:
        raise ValueError("Selezionare almeno un ordine mascherato.")

    ordini = [main, *masked_ordini]
    seen = set()
    for ordine in ordini:
        key = _order_key(ordine)
        if key in seen:
            raise ValueError(
                "Ordine principale e ordini mascherati non possono coincidere."
            )
        seen.add(key)

    _ensure_operator_can_activate_group(
        [_order_key(ordine) for ordine in ordini],
        _current_username(),
    )

    expected_reparto = _order_reparto(main)
    expected_kind = _order_kind(main)
    _ensure_order_can_enter_group(main)
    for masked in masked_ordini:
        _ensure_order_can_enter_group(masked)
        if _order_reparto(masked) != expected_reparto:
            raise ValueError(
                "Ordine principale e mascherati devono appartenere allo stesso reparto."
            )
        if _order_kind(masked) != expected_kind:
            raise ValueError(
                "Non e' possibile mascherare insieme una macchina e un semilavorato."
            )

    now_dt = _now_rome_dt()
    now_iso = now_dt.isoformat(timespec="seconds")
    group_uid = _group_uid("MASK", ordini)

    group = OdpWorkGroup(
        GroupUid=group_uid,
        GroupType=GROUP_TYPE_MASCHERATO,
        Status=GROUP_STATUS_ATTIVO,
        CreatedAt=now_iso,
        StartedAt=now_iso,
        LastActivationAt=now_iso,
        OperatoreId=_current_user_id(),
        OperatoreUsername=_current_username(),
        Reparto=expected_reparto,
        Fase=_fase_corrente_for_export(main),
        InitialMemberCount=len(ordini),
        TotalRuntimeSeconds=0,
    )
    db.session.add(group)

    _activate_order_for_group(main, group_uid=group_uid, now_dt=now_dt)
    db.session.add(
        _create_member(
            group_uid, main, role=ROLE_MAIN, share_mode=SHARE_FULL, now_iso=now_iso
        )
    )

    for masked in masked_ordini:
        _activate_order_for_group(masked, group_uid=group_uid, now_dt=now_dt)
        db.session.add(
            _create_member(
                group_uid,
                masked,
                role=ROLE_MASKED,
                share_mode=SHARE_ZERO,
                now_iso=now_iso,
            )
        )

    db.session.flush()
    return group

def _assigned_seconds_for_member(
    group: OdpWorkGroup, member: OdpWorkGroupMember, elapsed_seconds: int
) -> int:
    mode = _norm_text(member.TimeShareMode).upper()
    if mode == SHARE_FULL:
        return int(elapsed_seconds)
    if mode == SHARE_ZERO:
        return 0
    divisor = max(1, int(group.InitialMemberCount or 1))
    return _round_seconds(Decimal(elapsed_seconds) / Decimal(divisor))


def _runtime_for_member(member: OdpWorkGroupMember) -> InputOdpRuntime | None:
    return InputOdpRuntime.query.filter_by(
        IdDocumento=member.IdDocumento,
        IdRiga=member.IdRiga,
    ).first()


def _order_for_member(member: OdpWorkGroupMember):
    return InputOdp.query.filter_by(
        IdDocumento=member.IdDocumento,
        IdRiga=member.IdRiga,
    ).first()


def _group_active_members(group: OdpWorkGroup) -> list[OdpWorkGroupMember]:
    return [m for m in group.members if _norm_text(m.Status) == GROUP_STATUS_ATTIVO]


def assign_elapsed_group_runtime(
    group: OdpWorkGroup,
    now_dt: datetime | None = None,
    *,
    minuti_non_funzionamento: int = 0,
) -> int:
    """
    Attribuisce ai membri il tempo maturato dall'ultima attivazione gruppo.

    Il tempo non funzionamento è unico di gruppo: viene sottratto una sola volta
    dal tempo reale del blocco prima di applicare SPLIT/FULL/ZERO.
    """
    now_dt = now_dt or _now_rome_dt()
    start_dt = _parse_iso_dt(group.LastActivationAt)

    if start_dt is None:
        # Fallback: usa la prima data_ultima_attivazione dei membri attivi.
        for member in _group_active_members(group):
            rt = _runtime_for_member(member)
            start_dt = _parse_iso_dt(getattr(rt, "data_ultima_attivazione", ""))
            if start_dt is not None:
                break

    if start_dt is None:
        _ensure_stop_minutes_within_elapsed(
            minuti_non_funzionamento,
            0,
        )
        group.LastActivationAt = None
        return 0

    raw_elapsed_seconds = max(0, int((now_dt - start_dt).total_seconds()))
    _ensure_stop_minutes_within_elapsed(
        minuti_non_funzionamento,
        raw_elapsed_seconds,
    )
    if raw_elapsed_seconds <= 0:
        return 0

    requested_stop_seconds = max(0, int(minuti_non_funzionamento or 0)) * 60
    removed_seconds = min(requested_stop_seconds, raw_elapsed_seconds)
    elapsed_seconds = max(0, raw_elapsed_seconds - removed_seconds)

    event_at = now_dt.isoformat(timespec="seconds")

    for member in _group_active_members(group):
        rt = _runtime_for_member(member)
        ordine = _order_for_member(member)
        assigned = _assigned_seconds_for_member(group, member, elapsed_seconds)

        member.RuntimeSecondsAssigned = (
            int(member.RuntimeSecondsAssigned or 0) + assigned
        )

        if rt is not None:
            current_seconds = _tempo_to_seconds(rt.Tempo_funzionamento)
            rt.Tempo_funzionamento = _seconds_to_tempo_text(current_seconds + assigned)
            rt.data_ultima_attivazione = None

        if ordine is not None:
            _append_group_runtime_log(
                group_uid=group.GroupUid,
                ordine=ordine,
                action="runtime_gruppo",
                event_at=event_at,
                stato_pre=_norm_text(getattr(rt, "Stato_odp", "")) if rt else "",
                stato_post=_norm_text(getattr(rt, "Stato_odp", "")) if rt else "",
                elapsed_seconds=assigned,
                note="Attribuzione tempo da gruppo ordini",
                extra_payload={
                    "elapsed_reale_gruppo_seconds": raw_elapsed_seconds,
                    "elapsed_lavorato_gruppo_seconds": elapsed_seconds,
                    "tempo_non_funzionamento_secondi": removed_seconds,
                    "time_share_mode": member.TimeShareMode,
                    "group_type": group.GroupType,
                    "runtime_seconds_assigned": assigned,
                },
            )

    group.TotalRuntimeSeconds = int(group.TotalRuntimeSeconds or 0) + elapsed_seconds
    group.LastActivationAt = None
    return elapsed_seconds


def suspend_group(
    group_uid: str, *, causale: str = "", minuti_non_funzionamento: int = 0
) -> OdpWorkGroup:
    group = _get_group_or_error(group_uid)

    if _norm_text(group.Status) != GROUP_STATUS_ATTIVO:
        raise ValueError(f"Gruppo non sospendibile: stato attuale '{group.Status}'.")

    now_dt = _now_rome_dt()
    elapsed = assign_elapsed_group_runtime(
        group,
        now_dt=now_dt,
        minuti_non_funzionamento=minuti_non_funzionamento,
    )
    now_iso = now_dt.isoformat(timespec="seconds")

    stop_seconds = max(0, int(minuti_non_funzionamento or 0)) * 60
    for member in _group_active_members(group):
        rt = _runtime_for_member(member)
        ordine = _order_for_member(member)
        stato_pre = _norm_text(getattr(ordine, "StatoOrdine", ""))
        qty_pre = _qty_da_lavorare_text(ordine, stato=rt) if ordine is not None else ""
        if rt is not None:
            rt.Stato_odp = GROUP_STATUS_SOSPESO
            rt.Utente_operazione = _current_username()
        member.Status = GROUP_STATUS_SOSPESO
        member.SuspendedAt = now_iso
        if ordine is not None:
            ordine.StatoOrdine = GROUP_STATUS_SOSPESO
            _append_group_state_log(
                group_uid=group.GroupUid,
                ordine=ordine,
                action="sospensione_gruppo",
                event_at=now_iso,
                stato_pre=stato_pre,
                stato_post=GROUP_STATUS_SOSPESO,
                qty_pre=qty_pre,
                qty_post=_qty_da_lavorare_text(ordine, stato=rt),
                note="Sospensione gruppo ordini",
                causale=_norm_text(causale),
                minuti_non_funzionamento=minuti_non_funzionamento,
                secondi_non_funzionamento=stop_seconds,
            )

    group.Status = GROUP_STATUS_SOSPESO
    group.SuspendedAt = now_iso
    group.CausaleSospensione = _norm_text(causale)
    group.OperatoreId = _current_user_id()
    group.OperatoreUsername = _current_username()
    return group


def reactivate_group(group_uid: str) -> OdpWorkGroup:
    group = _get_group_or_error(group_uid)

    if _norm_text(group.Status) != GROUP_STATUS_SOSPESO:
        raise ValueError(f"Gruppo non riattivabile: stato attuale '{group.Status}'.")

    _ensure_operator_can_activate_group(
        [
            (_norm_text(member.IdDocumento), _norm_text(member.IdRiga))
            for member in group.members
            if _norm_text(member.Status) == GROUP_STATUS_SOSPESO
        ],
        _current_username(),
    )

    now_dt = _now_rome_dt()
    now_iso = now_dt.isoformat(timespec="seconds")

    for member in [
        m for m in group.members if _norm_text(m.Status) == GROUP_STATUS_SOSPESO
    ]:
        rt = _runtime_for_member(member)
        ordine = _order_for_member(member)
        if ordine is None:
            continue

        fase_corrente = _fase_corrente_for_export(ordine)
        stato_pre = _norm_text(getattr(ordine, "StatoOrdine", ""))
        qty_pre = _qty_da_lavorare_text(ordine, stato=rt)
        rt = _ensure_stato_attivo(
            ordine=ordine,
            stato=rt,
            username=_current_username(),
            when_dt=now_dt,
            fase_corrente=fase_corrente,
        )
        rt.Stato_odp = GROUP_STATUS_ATTIVO
        rt.data_ultima_attivazione = now_iso
        member.Status = GROUP_STATUS_ATTIVO
        member.SuspendedAt = None
        ordine.StatoOrdine = GROUP_STATUS_ATTIVO
        _append_group_state_log(
            group_uid=group.GroupUid,
            ordine=ordine,
            action="riattivazione_gruppo",
            event_at=now_iso,
            stato_pre=stato_pre,
            stato_post=GROUP_STATUS_ATTIVO,
            qty_pre=qty_pre,
            qty_post=_qty_da_lavorare_text(ordine, stato=rt),
            note="Riattivazione gruppo ordini",
        )

    group.Status = GROUP_STATUS_ATTIVO
    group.LastActivationAt = now_iso
    group.SuspendedAt = None
    group.OperatoreId = _current_user_id()
    group.OperatoreUsername = _current_username()
    return group


def dissolve_group_for_single_member_close(
    group_uid: str, *, id_documento: str, id_riga: str
) -> OdpWorkGroup:
    """
    Scioglie il gruppo per consentire la chiusura singola di un membro.
    Il membro indicato resta Attivo, con il tempo gruppo già assegnato e una
    nuova attivazione da ordine singolo; gli altri membri vengono messi in
    sospeso e potranno essere gestiti singolarmente.
    """
    group = _get_group_or_error(group_uid)
    target = None
    for member in group.members:
        if member.IdDocumento == _norm_text(
            id_documento
        ) and member.IdRiga == _norm_text(id_riga):
            target = member
            break

    if target is None:
        raise ValueError("Ordine non appartenente al gruppo.")

    now_dt = _now_rome_dt()
    now_iso = now_dt.isoformat(timespec="seconds")

    if _norm_text(group.Status) == GROUP_STATUS_ATTIVO:
        assign_elapsed_group_runtime(group, now_dt=now_dt)

    for member in group.members:
        rt = _runtime_for_member(member)
        ordine = _order_for_member(member)

        stato_pre = _norm_text(getattr(ordine, "StatoOrdine", "")) if ordine is not None else ""
        qty_pre = _qty_da_lavorare_text(ordine, stato=rt) if ordine is not None else ""

        if member.id == target.id:
            member.Status = GROUP_STATUS_SCIOLTO
            member.DissolvedAt = now_iso
            if rt is not None:
                rt.Stato_odp = GROUP_STATUS_ATTIVO
                rt.data_ultima_attivazione = now_iso
            if ordine is not None:
                ordine.StatoOrdine = GROUP_STATUS_ATTIVO
                _append_group_state_log(
                    group_uid=group.GroupUid,
                    ordine=ordine,
                    action="scioglimento_gruppo_attiva_membro",
                    event_at=now_iso,
                    stato_pre=stato_pre,
                    stato_post=GROUP_STATUS_ATTIVO,
                    qty_pre=qty_pre,
                    qty_post=_qty_da_lavorare_text(ordine, stato=rt),
                    note="Scioglimento gruppo: ordine attivo per chiusura singola",
                )
            continue

        if _norm_text(member.Status) in {GROUP_STATUS_ATTIVO, GROUP_STATUS_SOSPESO}:
            member.Status = GROUP_STATUS_SCIOLTO
            member.DissolvedAt = now_iso
            if rt is not None:
                rt.Stato_odp = GROUP_STATUS_SOSPESO
                rt.data_ultima_attivazione = None
            if ordine is not None:
                ordine.StatoOrdine = GROUP_STATUS_SOSPESO
                _append_group_state_log(
                    group_uid=group.GroupUid,
                    ordine=ordine,
                    action="scioglimento_gruppo_sospende_membro",
                    event_at=now_iso,
                    stato_pre=stato_pre,
                    stato_post=GROUP_STATUS_SOSPESO,
                    qty_pre=qty_pre,
                    qty_post=_qty_da_lavorare_text(ordine, stato=rt),
                    note="Scioglimento gruppo: ordine residuo sospeso",
                )

    group.Status = GROUP_STATUS_SCIOLTO
    group.DissolvedAt = now_iso
    group.LastActivationAt = None
    return group



def prepare_group_member_for_single_closure(
    group_uid: str,
    *,
    id_documento: str,
    id_riga: str,
    minuti_non_funzionamento: int | float | None = 0,
) -> tuple[OdpWorkGroup, OdpWorkGroupMember]:
    group = _get_group_or_error(group_uid)
    if _norm_text(group.Status) not in {GROUP_STATUS_ATTIVO, GROUP_STATUS_SOSPESO}:
        raise ValueError("Il gruppo non e' chiudibile nello stato attuale.")

    id_documento_norm = _norm_text(id_documento)
    id_riga_norm = _norm_text(id_riga)
    target = next(
        (
            member
            for member in group.members
            if _norm_text(member.Status) in {GROUP_STATUS_ATTIVO, GROUP_STATUS_SOSPESO}
            and _norm_text(member.IdDocumento) == id_documento_norm
            and _norm_text(member.IdRiga) == id_riga_norm
        ),
        None,
    )
    if target is None:
        raise ValueError("Ordine non presente tra i membri aperti del gruppo.")

    now_dt = _now_rome_dt()
    if _norm_text(group.Status) == GROUP_STATUS_ATTIVO:
        assign_elapsed_group_runtime(
            group,
            now_dt=now_dt,
            minuti_non_funzionamento=minuti_non_funzionamento or 0,
        )

    rt = _runtime_for_member(target)
    if rt is not None:
        rt.Stato_odp = GROUP_STATUS_ATTIVO
        rt.data_ultima_attivazione = None
        rt.Utente_operazione = _current_username()
    ordine = _order_for_member(target)
    if ordine is not None:
        ordine.StatoOrdine = GROUP_STATUS_ATTIVO

    return group, target


def _set_open_member_state(member: OdpWorkGroupMember, status: str, *, activation_iso: str | None) -> None:
    member.Status = status
    rt = _runtime_for_member(member)
    if rt is not None:
        rt.Stato_odp = status
        rt.data_ultima_attivazione = activation_iso if status == GROUP_STATUS_ATTIVO else None
        rt.Utente_operazione = _current_username()
    ordine = _order_for_member(member)
    if ordine is not None:
        ordine.StatoOrdine = status


def finalize_group_after_single_member_closure(
    group: OdpWorkGroup,
    closed_member: OdpWorkGroupMember,
) -> OdpWorkGroup:
    now_iso = _now_iso()
    open_members = [
        member
        for member in sorted(group.members, key=lambda item: (item.id or 0))
        if _norm_text(member.Status) in {GROUP_STATUS_ATTIVO, GROUP_STATUS_SOSPESO}
    ]

    if not open_members:
        group.Status = GROUP_STATUS_CHIUSO
        group.ClosedAt = now_iso
        group.LastActivationAt = None
        return group

    if len(open_members) <= 1:
        for member in open_members:
            member.Status = GROUP_STATUS_SCIOLTO
            member.DissolvedAt = now_iso
            rt = _runtime_for_member(member)
            ordine = _order_for_member(member)
            stato_pre = _norm_text(getattr(ordine, "StatoOrdine", "")) if ordine is not None else ""
            qty_pre = _qty_da_lavorare_text(ordine, stato=rt) if ordine is not None else ""
            if rt is not None:
                rt.Stato_odp = GROUP_STATUS_SOSPESO
                rt.data_ultima_attivazione = None
                rt.Utente_operazione = _current_username()
            if ordine is not None:
                ordine.StatoOrdine = GROUP_STATUS_SOSPESO
                _append_group_state_log(
                    group_uid=group.GroupUid,
                    ordine=ordine,
                    action="chiusura_membro_scioglie_gruppo",
                    event_at=now_iso,
                    stato_pre=stato_pre,
                    stato_post=GROUP_STATUS_SOSPESO,
                    qty_pre=qty_pre,
                    qty_post=_qty_da_lavorare_text(ordine, stato=rt),
                    note="Chiusura membro gruppo: ordine residuo sospeso",
                )

        group.Status = GROUP_STATUS_SCIOLTO
        group.DissolvedAt = now_iso
        group.LastActivationAt = None
        return group

    zero_members = [
        member
        for member in open_members
        if _norm_text(member.TimeShareMode).upper() == SHARE_ZERO
    ]
    timed_members = [
        member
        for member in open_members
        if _norm_text(member.TimeShareMode).upper() != SHARE_ZERO
    ]

    if len(timed_members) == 1 and zero_members:
        main_member = timed_members[0]
        group.GroupType = GROUP_TYPE_MASCHERATO
        group.InitialMemberCount = len(open_members)
        group.Fase = _norm_text(_fase_corrente_for_export(_order_for_member(main_member)))
        main_member.Role = ROLE_MAIN
        main_member.TimeShareMode = SHARE_FULL
        for masked_member in zero_members:
            masked_member.Role = ROLE_MASKED
            masked_member.TimeShareMode = SHARE_ZERO
    else:
        group.GroupType = GROUP_TYPE_MULTIPLO
        group.InitialMemberCount = len(open_members)
        group.Fase = ""
        for member in open_members:
            member.Role = ROLE_MEMBER
            member.TimeShareMode = SHARE_SPLIT

    should_reactivate = any(_norm_text(member.Status) == GROUP_STATUS_ATTIVO for member in open_members)
    if should_reactivate:
        group.Status = GROUP_STATUS_ATTIVO
        group.LastActivationAt = now_iso
        group.SuspendedAt = None
        for member in open_members:
            _set_open_member_state(member, GROUP_STATUS_ATTIVO, activation_iso=now_iso)
    else:
        group.Status = GROUP_STATUS_SOSPESO
        group.LastActivationAt = None
        if not _norm_text(group.SuspendedAt):
            group.SuspendedAt = now_iso
        for member in open_members:
            _set_open_member_state(member, GROUP_STATUS_SOSPESO, activation_iso=None)

    return group

def _members_for_closure(group: OdpWorkGroup) -> list[OdpWorkGroupMember]:
    return [
        m
        for m in sorted(group.members, key=lambda x: x.id or 0)
        if _norm_text(m.Status) in {GROUP_STATUS_ATTIVO, GROUP_STATUS_SOSPESO}
    ]


def prepare_group_for_full_closure(
    group_uid: str,
    *,
    minuti_non_funzionamento: int = 0,
) -> OdpWorkGroup:
    """
    Prepara il gruppo alla chiusura completa:
    - richiede gruppo Attivo;
    - attribuisce il tempo maturato ai singoli membri;
    - lascia i membri in stato Attivo, così la chiusura singola interna può
      usare la stessa logica AVP dell'ordine normale.
    """
    group = _get_group_or_error(group_uid)

    group_status = _norm_text(group.Status)
    if group_status not in {GROUP_STATUS_ATTIVO, GROUP_STATUS_SOSPESO}:
        raise ValueError(f"Gruppo non chiudibile: stato attuale '{group.Status}'.")

    now_dt = _now_rome_dt()
    if group_status == GROUP_STATUS_ATTIVO:
        assign_elapsed_group_runtime(
            group,
            now_dt=now_dt,
            minuti_non_funzionamento=minuti_non_funzionamento,
        )

    for member in _members_for_closure(group):
        rt = _runtime_for_member(member)
        ordine = _order_for_member(member)
        if rt is not None:
            rt.Stato_odp = GROUP_STATUS_ATTIVO
            rt.data_ultima_attivazione = None
            rt.Utente_operazione = _current_username()
        if ordine is not None:
            ordine.StatoOrdine = GROUP_STATUS_ATTIVO

    group.Status = GROUP_STATUS_ATTIVO
    group.SuspendedAt = None
    return group


def member_payload_key(member: OdpWorkGroupMember) -> str:
    return f"{member.IdDocumento}|{member.IdRiga}"


def group_members_for_close_payload(group: OdpWorkGroup) -> list[OdpWorkGroupMember]:
    return _members_for_closure(group)


def _store_member_closure_quantities(
    member: OdpWorkGroupMember,
    *,
    q_ok: str = "",
    q_nok: str = "",
    note: str = "",
) -> None:
    member.QtyConforme = _norm_text(q_ok)
    member.QtyNonConforme = _norm_text(q_nok)
    member.Note = _norm_text(note)


def mark_group_member_closed(
    member: OdpWorkGroupMember,
    *,
    q_ok: str = "",
    q_nok: str = "",
    note: str = "",
) -> None:
    now_iso = _now_iso()
    member.Status = GROUP_STATUS_CHIUSO
    member.ClosedAt = now_iso
    member.SuspendedAt = None
    _store_member_closure_quantities(member, q_ok=q_ok, q_nok=q_nok, note=note)


def mark_group_member_partial_closed(
    member: OdpWorkGroupMember,
    *,
    q_ok: str = "",
    q_nok: str = "",
    note: str = "",
) -> None:
    """
    Registra una chiusura parziale di un membro gruppo.

    Il membro non viene marcato Chiuso perché l'ordine resta lavorabile sulla
    stessa fase con quantità residua. Lo stato sospeso impedisce che il gruppo
    venga finalizzato erroneamente dopo una chiusura parziale.
    """
    now_iso = _now_iso()
    member.Status = GROUP_STATUS_SOSPESO
    member.SuspendedAt = now_iso
    member.ClosedAt = None
    _store_member_closure_quantities(member, q_ok=q_ok, q_nok=q_nok, note=note)


def finalize_group_after_member_closures(group: OdpWorkGroup) -> None:
    open_members = [
        m
        for m in group.members
        if _norm_text(m.Status) in {GROUP_STATUS_ATTIVO, GROUP_STATUS_SOSPESO}
    ]

    now_iso = _now_iso()

    if not open_members:
        group.Status = GROUP_STATUS_CHIUSO
        group.ClosedAt = now_iso
        group.LastActivationAt = None
        return

    if any(_norm_text(m.Status) == GROUP_STATUS_ATTIVO for m in open_members):
        group.Status = GROUP_STATUS_ATTIVO
        group.SuspendedAt = None
        return

    group.Status = GROUP_STATUS_SOSPESO
    group.SuspendedAt = group.SuspendedAt or now_iso
    group.LastActivationAt = None


def _get_group_or_error(group_uid: str) -> OdpWorkGroup:
    group = OdpWorkGroup.query.filter_by(GroupUid=_norm_text(group_uid)).first()
    if group is None:
        raise ValueError("Gruppo ordini non trovato.")
    return group


def group_to_dict(group: OdpWorkGroup) -> dict:
    return {
        "group_uid": group.GroupUid,
        "group_type": group.GroupType,
        "status": group.Status,
        "initial_member_count": group.InitialMemberCount,
        "total_runtime_seconds": group.TotalRuntimeSeconds,
        "members": [
            {
                "id_documento": member.IdDocumento,
                "id_riga": member.IdRiga,
                "ordine": ".".join(
                    x for x in [member.RifRegistraz, member.NumProgrRiga] if x
                ),
                "cod_art": member.CodArt,
                "variante": member.VarianteArt,
                "revisione": member.IndiceModifica,
                "descrizione": member.DesArt,
                "fase": member.Fase,
                "role": member.Role,
                "time_share_mode": member.TimeShareMode,
                "runtime_seconds_assigned": member.RuntimeSecondsAssigned,
                "status": member.Status,
                "distinta": _norm_text(
                    getattr(_order_for_member(member), "DistintaMateriale", "[]")
                ),
            }
            for member in sorted(group.members, key=lambda x: x.id or 0)
        ],
    }


def first_order_for_group(group: OdpWorkGroup):
    for member in group.members:
        ordine = _order_for_member(member)
        if ordine is not None:
            return ordine
    return None


def available_orders_payload(policy, *, only_pianificata: bool = True) -> list[dict]:
    # Usa la query base e poi verifica singolarmente l'accesso con _get_visible_odp_by_key.
    out = []
    for ordine in _base_odp_query().all():
        if not _order_can_enter_group(ordine):
            continue

        if only_pianificata and not _order_is_pianificata(ordine):
            continue

        if _active_group_for_order(ordine.IdDocumento, ordine.IdRiga) is not None:
            continue

        try:
            _get_visible_odp_by_key(policy, ordine.IdDocumento, ordine.IdRiga)
        except Exception:
            continue

        priority = _priority_for_order(ordine)
        out.append(
            {
                "id_documento": _norm_text(ordine.IdDocumento),
                "id_riga": _norm_text(ordine.IdRiga),
                "num_progr_riga": _norm_text(getattr(ordine, "NumProgrRiga", "")),
                "rif_registraz": _norm_text(getattr(ordine, "RifRegistraz", "")),
                "ordine": _order_label(ordine),
                "cod_art": _norm_text(getattr(ordine, "CodArt", "")),
                "variante": _norm_text(getattr(ordine, "VarianteArt", "")),
                "revisione": _norm_text(getattr(ordine, "IndiceModifica", "")),
                "descrizione": _norm_text(getattr(ordine, "DesArt", "")),
                "fase": _fase_corrente_for_export(ordine),
                "reparto": _order_reparto(ordine),
                "kind": _order_kind(ordine),
                "priorita": priority,
                "quantita": _qty_da_lavorare_text(ordine),
                "stato": _norm_text(ordine.StatoOrdine),
            }
        )

    out.sort(
        key=lambda x: (
            x.get("priorita") if x.get("priorita") is not None else 9,
            x.get("ordine", ""),
            x.get("cod_art", ""),
        )
    )
    return out


class WorkGroupHomeRow:
    """
    Riga virtuale per visualizzare un gruppo ordini come una sola riga nella home.

    La riga espone gli attributi minimi usati dai partial Jinja esistenti e fa fallback
    sull'ordine principale/di riferimento per i campi non ridefiniti.
    """

    IsWorkGroup = True

    def __init__(
        self,
        group: OdpWorkGroup,
        members: list[OdpWorkGroupMember],
        ordini: list[InputOdp],
    ):
        self._group = group
        self._members = list(members or [])
        self._ordini = list(ordini or [])

        member_order = {
            (_norm_text(m.IdDocumento), _norm_text(m.IdRiga)): idx
            for idx, m in enumerate(self._members)
        }
        self._ordini.sort(
            key=lambda o: member_order.get(
                (_norm_text(o.IdDocumento), _norm_text(o.IdRiga)),
                9999,
            )
        )

        members_by_key = {
            (_norm_text(m.IdDocumento), _norm_text(m.IdRiga)): m for m in self._members
        }

        main_member = next(
            (m for m in self._members if _norm_text(m.Role) == "MAIN"), None
        )
        if main_member is not None:
            self._main = next(
                (
                    o
                    for o in self._ordini
                    if _norm_text(o.IdDocumento) == _norm_text(main_member.IdDocumento)
                    and _norm_text(o.IdRiga) == _norm_text(main_member.IdRiga)
                ),
                self._ordini[0] if self._ordini else None,
            )
        else:
            self._main = self._ordini[0] if self._ordini else None

        self.WorkGroupUid = _norm_text(group.GroupUid)
        self.WorkGroupType = _norm_text(group.GroupType)
        self.IsMixedWorkGroup = self.WorkGroupType == GROUP_TYPE_MULTIPLO and any(
            _norm_text(getattr(m, "TimeShareMode", "")).upper() == SHARE_ZERO
            for m in self._members
        )
        self.WorkGroupDisplayType = "MISTO" if self.IsMixedWorkGroup else self.WorkGroupType
        self.WorkGroupTypeLabel = (
            "Misto"
            if self.IsMixedWorkGroup
            else ("Multiplo" if self.WorkGroupType == "MULTIPLO" else "Mascherato")
        )
        self.WorkGroupStatus = _norm_text(group.Status)
        self.WorkGroupMembers = self._members
        self.WorkGroupOrdini = self._ordini

        self.IdDocumento = f"GROUP:{self.WorkGroupUid}"
        self.IdRiga = "GROUP"
        self.NumProgrRiga = ""
        self.RifRegistraz = self.WorkGroupUid

        self.StatoOrdine = self.WorkGroupStatus
        self.Stato_odp = self.WorkGroupStatus

        self.CodArt = ""
        self.VarianteArt = ""
        self.IndiceModifica = ""

        self.DesArt = (
            "Ordine misto"
            if self.IsMixedWorkGroup
            else (
                "Ordine multiplo"
                if self.WorkGroupType == "MULTIPLO"
                else "Ordine mascherato"
            )
        )
        self.TipoOrdineVisuale = self.WorkGroupTypeLabel

        self.OrdineVisuale = " + ".join(_ordine_ref_label(o) for o in self._ordini)

        codici = []
        for ordine in self._ordini:
            cod = _norm_text(getattr(ordine, "CodArt", ""))
            if cod and cod not in codici:
                codici.append(cod)
        self.CodiciVisuale = " + ".join(codici)

        self.DettaglioGruppoVisuale = []
        for ordine in self._ordini:
            key = (_norm_text(ordine.IdDocumento), _norm_text(ordine.IdRiga))
            member = members_by_key.get(key)

            stato = getattr(ordine, "runtime_row", None)

            fase_attiva = (
                _norm_text(getattr(stato, "FaseAttiva", ""))
                or _norm_text(getattr(ordine, "FaseAttiva", ""))
                or "1"
            )
            gestione_lotto = (
                _norm_text(getattr(ordine, "GestioneLotto", "")).lower() == "si"
            )
            gestione_matricola = (
                _norm_text(getattr(ordine, "GestioneMatricola", "")).lower() == "si"
            )

            self.DettaglioGruppoVisuale.append(
                {
                    "id_documento": _norm_text(getattr(ordine, "IdDocumento", "")),
                    "id_riga": _norm_text(getattr(ordine, "IdRiga", "")),
                    "num_progr_riga": _norm_text(getattr(ordine, "NumProgrRiga", "")),
                    "ordine": _order_label(ordine),
                    "rif_registraz": _norm_text(getattr(ordine, "RifRegistraz", "")),
                    "cod_art": _norm_text(getattr(ordine, "CodArt", "")),
                    "variante": _norm_text(getattr(ordine, "VarianteArt", "")),
                    "revisione": _normalize_indice_articolo_search(
                        getattr(ordine, "IndiceModifica", "")
                    ),
                    "descrizione": _norm_text(getattr(ordine, "DesArt", "")),
                    # Fase singola effettiva dell'ordine nel runtime.
                    # Questa è quella da usare per filtrare la distinta.
                    "fase_attiva": fase_attiva,
                    # Mantieni anche "fase" per compatibilità con il modal dettaglio.
                    "fase": fase_attiva,
                    "matricola": _norm_text(getattr(ordine, "CodMatricola", "")),
                    # Distinta completa del singolo ordine.
                    # Il filtro per fase lo fa il frontend sul campo NumFase dei componenti.
                    "distinta_materiale": _norm_text(
                        getattr(ordine, "DistintaMateriale", "")
                    )
                    or "[]",
                    "quantita": _qty_da_lavorare_text(
                        ordine,
                        stato=stato,
                    ),
                    "gestione_lotto": gestione_lotto,
                    "gestione_matricola": gestione_matricola,
                    "is_macchina": gestione_matricola,
                    "richiede_lotti": gestione_lotto or gestione_matricola,
                    "modalita_lotti": "m" if gestione_matricola else "sl",
                    "ruolo": _norm_text(getattr(member, "Role", "")),
                    "time_share_mode": _norm_text(getattr(member, "TimeShareMode", "")),
                }
            )

        self.PrioritaNumero = self._calc_priorita()
        self.PrioritaVisuale = self.PrioritaNumero

        self.Quantita = self._calc_quantita()
        self.QtyDaLavorare = self.Quantita

        self.DataFineSched = self._calc_data_consegna()
        self.CodRisorsaProd = self._calc_risorsa()
        self.RisorsaAttiva = self.CodRisorsaProd

        self.AttrezzaggioAttivo = ""
        self.CodMatricola = self._calc_matricole()
        self.FaseAttiva = self._calc_fase()
        self.DistintaMateriale = "[]"

        self.OperatoreUsername = (
            _norm_text(getattr(group, "OperatoreUsername", ""))
            or self._main_operatore()
        )

    def __getattr__(self, name):
        if self._main is not None:
            return getattr(self._main, name)
        raise AttributeError(name)

    def _main_operatore(self) -> str:
        runtime = (
            getattr(self._main, "runtime_row", None) if self._main is not None else None
        )
        return _norm_text(getattr(runtime, "Utente_operazione", ""))

    def _calc_matricole(self) -> str:
        matricole = []
        for ordine in self._ordini:
            matricola = _norm_text(getattr(ordine, "CodMatricola", ""))
            if matricola and matricola not in matricole:
                matricole.append(matricola)
        return " + ".join(matricole) or "Gruppo"

    def _calc_priorita(self):
        priorities = []

        for ordine in self._ordini:
            raw = getattr(ordine, "PrioritaNumero", None)

            if raw in (None, ""):
                runtime = getattr(ordine, "runtime_row", None)
                raw = (
                    getattr(runtime, "PrioritaInCarico", None)
                    if runtime is not None
                    else None
                )

            try:
                pr = int(raw)
            except (TypeError, ValueError):
                continue

            if pr in (1, 2, 3):
                priorities.append(pr)

        return min(priorities) if priorities else None

    def _calc_quantita(self):
        if self.WorkGroupType == "MASCHERATO":
            if self._main is None:
                return ""

            runtime = getattr(self._main, "runtime_row", None)
            return _qty_da_lavorare_text(self._main, stato=runtime)

        totale = Decimal("0")
        found = False

        for ordine in self._ordini:
            runtime = getattr(ordine, "runtime_row", None)
            try:
                totale += _parse_qty_decimal(
                    _qty_da_lavorare_text(ordine, stato=runtime)
                )
                found = True
            except ValueError:
                pass

        return _decimal_to_text(totale) if found else ""

    def _calc_data_consegna(self):
        if self.WorkGroupType == "MASCHERATO":
            return (
                _norm_text(getattr(self._main, "DataFineSched", ""))
                if self._main is not None
                else ""
            )

        dates = [
            _norm_text(getattr(o, "DataFineSched", ""))
            for o in self._ordini
            if _norm_text(getattr(o, "DataFineSched", ""))
        ]

        return min(dates) if dates else ""

    def _calc_risorsa(self):
        if self.WorkGroupType != "MASCHERATO" or self._main is None:
            return ""

        return _norm_text(getattr(self._main, "RisorsaAttiva", "")) or _norm_text(
            getattr(self._main, "CodRisorsaProd", "")
        )

    def _calc_fase(self):
        if self.WorkGroupType == "MASCHERATO" and self._main is not None:
            return _norm_text(getattr(self._main, "FaseAttiva", ""))

        fasi = []
        for ordine in self._ordini:
            fase = _norm_text(getattr(ordine, "FaseAttiva", ""))
            if fase and fase not in fasi:
                fasi.append(fase)

        return " + ".join(fasi)


def _collapse_work_group_rows_for_home(odp: list[InputOdp]) -> list:
    """
    Sostituisce i membri di gruppi Attivo/In Sospeso con una sola riga virtuale.

    Le righe reali degli ordini restano nel DB/runtime, ma non vengono mostrate
    separatamente nella home quando appartengono a un gruppo operativo aperto.
    """
    if not odp:
        return odp

    order_by_key = {(_norm_text(o.IdDocumento), _norm_text(o.IdRiga)): o for o in odp}

    visible_keys = set(order_by_key.keys())

    active_groups = OdpWorkGroup.query.filter(
        OdpWorkGroup.Status.in_(["Attivo", "In Sospeso"])
    ).all()

    if not active_groups:
        return odp

    groups_by_uid = {
        _norm_text(group.GroupUid): group
        for group in active_groups
        if _norm_text(group.GroupUid)
    }

    if not groups_by_uid:
        return odp

    active_members = (
        OdpWorkGroupMember.query.filter(
            OdpWorkGroupMember.GroupUid.in_(list(groups_by_uid.keys()))
        )
        .filter(OdpWorkGroupMember.Status.in_(["Attivo", "In Sospeso"]))
        .order_by(OdpWorkGroupMember.id.asc())
        .all()
    )

    members_by_group: dict[str, list[OdpWorkGroupMember]] = {}

    for member in active_members:
        key = (_norm_text(member.IdDocumento), _norm_text(member.IdRiga))

        if key not in visible_keys:
            continue

        group_uid = _norm_text(member.GroupUid)
        if group_uid not in groups_by_uid:
            continue

        members_by_group.setdefault(group_uid, []).append(member)

    if not members_by_group:
        return odp

    member_keys_to_hide = set()
    group_rows_by_uid = {}

    for group_uid, members in members_by_group.items():
        group = groups_by_uid.get(group_uid)
        if group is None:
            continue

        group_ordini = []

        for member in members:
            key = (_norm_text(member.IdDocumento), _norm_text(member.IdRiga))
            ordine = order_by_key.get(key)
            if ordine is None:
                continue

            member_keys_to_hide.add(key)
            group_ordini.append(ordine)

        if not group_ordini:
            continue

        group_rows_by_uid[group_uid] = WorkGroupHomeRow(
            group=group,
            members=members,
            ordini=group_ordini,
        )

    if not group_rows_by_uid:
        return odp

    out = []
    inserted_groups = set()

    member_group_by_key = {}
    for group_uid, members in members_by_group.items():
        for member in members:
            member_group_by_key[
                (_norm_text(member.IdDocumento), _norm_text(member.IdRiga))
            ] = group_uid

    for ordine in odp:
        key = (_norm_text(ordine.IdDocumento), _norm_text(ordine.IdRiga))

        if key not in member_keys_to_hide:
            out.append(ordine)
            continue

        group_uid = member_group_by_key.get(key)
        if not group_uid or group_uid in inserted_groups:
            continue

        group_row = group_rows_by_uid.get(group_uid)
        if group_row is not None:
            out.append(group_row)
            inserted_groups.add(group_uid)

    return out
