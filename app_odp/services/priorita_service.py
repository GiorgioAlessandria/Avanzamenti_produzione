# app_odp/services/priorita_service.py

from sqlalchemy import func, select

from app_odp.models import db, Roles, User, user_roles, InputOdp
from app_odp.policy.policy import RbacPolicy
from app_odp.operator_session import active_user, active_policy
from flask import abort, current_app
from app_odp.models import OdpPriorita
from app_odp.services.order_helpers import _norm_text, _now_rome_dt
from app_odp.services.ordini_query_service import _base_odp_query
from app_odp.services.order_helpers import _norm_text, _ordine_ref_label
from app_odp.services.session_helpers import _current_username

PRIORITA_2_MAX_DEFAULT = 5
PRIORITA_HIDDEN_ROLE_NAMES = {"admin"}


def _make_ordine_fase_key(id_documento, id_riga, fase) -> tuple[str, str, str]:
    return (
        _norm_text(id_documento),
        _norm_text(id_riga),
        _norm_text(fase) or "1",
    )


def _priority_now_iso() -> str:
    return _now_rome_dt().isoformat(timespec="seconds")


def _priorita_2_max() -> int:
    try:
        return int(current_app.config.get("PRIORITA_2_MAX", PRIORITA_2_MAX_DEFAULT))
    except (TypeError, ValueError):
        return PRIORITA_2_MAX_DEFAULT


def _priorita_rows_for_operatore(operatore_id: int) -> list[OdpPriorita]:
    return (
        OdpPriorita.query.filter_by(operatore_id=int(operatore_id))
        .order_by(
            OdpPriorita.Priorita.asc(),
            OdpPriorita.Posizione.asc(),
            OdpPriorita.id.asc(),
        )
        .all()
    )


def _compact_priorita_operatore(operatore_id: int) -> None:
    rows = _priorita_rows_for_operatore(operatore_id)
    max_p2 = _priorita_2_max()

    for index, row in enumerate(rows):
        if index == 0:
            row.Priorita = 1
            row.Posizione = 1
        elif index <= max_p2:
            row.Priorita = 2
            row.Posizione = index
        else:
            row.Priorita = 3
            row.Posizione = index - max_p2

        row.updated_at = _priority_now_iso()


def _consume_priorita_ordine(id_documento: str, id_riga: str, fase: str) -> None:
    key = _make_ordine_fase_key(id_documento, id_riga, fase)

    rows = OdpPriorita.query.filter_by(
        IdDocumento=key[0],
        IdRiga=key[1],
        Fase=key[2],
    ).all()

    operatori_coinvolti = {row.operatore_id for row in rows}

    for row in rows:
        db.session.delete(row)

    db.session.flush()

    for operatore_id in operatori_coinvolti:
        _compact_priorita_operatore(operatore_id)


def _priorita_row_for_operatore_ordine(
    operatore_id: int,
    id_documento: str,
    id_riga: str,
    fase: str,
) -> OdpPriorita | None:
    key = _make_ordine_fase_key(id_documento, id_riga, fase)

    return (
        OdpPriorita.query.filter_by(
            operatore_id=int(operatore_id),
            IdDocumento=key[0],
            IdRiga=key[1],
            Fase=key[2],
        )
        .order_by(
            OdpPriorita.Priorita.asc(),
            OdpPriorita.Posizione.asc(),
            OdpPriorita.id.asc(),
        )
        .first()
    )


def _snapshot_priorita_in_runtime(
    stato, priorita_row, operatore_id: int, when_iso: str
) -> None:
    if stato is None:
        return

    if priorita_row is None:
        stato.PrioritaInCarico = None
        stato.PrioritaOperatoreIdInCarico = None
        stato.PrioritaPresaInCaricoAt = None
        return

    stato.PrioritaInCarico = int(priorita_row.Priorita)
    stato.PrioritaOperatoreIdInCarico = int(operatore_id)
    stato.PrioritaPresaInCaricoAt = when_iso


def _priorita_hidden_user_ids() -> set[int]:
    """
    Utenti da non mostrare nella gestione priorità.
    Esempio: admin.
    """
    hidden_role_names = {name.lower() for name in PRIORITA_HIDDEN_ROLE_NAMES}

    rows = (
        db.session.query(User.id)
        .join(user_roles, user_roles.c.user_id == User.id)
        .join(Roles, Roles.id == user_roles.c.role_id)
        .filter(func.lower(Roles.name).in_(hidden_role_names))
        .all()
    )

    return {int(row[0]) for row in rows}


def _priorita_manageable_role_ids_for_user(user: User) -> set[int]:
    """
    Restituisce tutti i ruoli sottostanti gestibili dall'utente,
    usando la gerarchia roles_manageable_roles.

    Non include i ruoli dell'utente stesso.
    """
    out: set[int] = set()

    for role in getattr(user, "roles", None) or []:
        for managed_role in getattr(role, "iter_manageable_roles", lambda: [])():
            if managed_role is not None and managed_role.id is not None:
                out.add(int(managed_role.id))

    return out


def _priorita_visible_operator_ids_for_current_user() -> set[int]:
    """
    Operatori visibili nella pagina modifica priorità.

    Regole:
    - chi ha priorita_tutti_operatori vede tutti gli utenti attivi,
      tranne se stesso e tranne gli admin
    - gli altri vedono se stessi + utenti sottostanti nella gerarchia roles_manageable_roles
    - gli admin non vengono mai mostrati
    """
    user = active_user()
    policy = active_policy()

    hidden_user_ids = _priorita_hidden_user_ids()

    if policy.can("priorita_tutti_operatori"):
        return {
            int(user_id)
            for user_id in db.session.execute(
                select(User.id)
                .where(User.active.is_(True))
                .where(User.id != user.id)
                .where(~User.id.in_(hidden_user_ids))
            )
            .scalars()
            .all()
        }

    visible_ids: set[int] = {int(user.id)}

    manageable_role_ids = _priorita_manageable_role_ids_for_user(user)

    if manageable_role_ids:
        users_with_managed_roles = set(
            db.session.execute(
                select(user_roles.c.user_id).where(
                    user_roles.c.role_id.in_(manageable_role_ids)
                )
            )
            .scalars()
            .all()
        )

        users_with_not_managed_roles = set(
            db.session.execute(
                select(user_roles.c.user_id).where(
                    ~user_roles.c.role_id.in_(manageable_role_ids)
                )
            )
            .scalars()
            .all()
        )

        visible_ids.update(
            int(user_id)
            for user_id in users_with_managed_roles - users_with_not_managed_roles
        )

    visible_ids.difference_update(hidden_user_ids)

    return visible_ids


def _get_priorita_visible_operatore_or_403(operatore_id: int) -> User:
    """
    Recupera l'operatore solo se è visibile all'utente corrente.
    Serve per proteggere anche le chiamate manuali agli endpoint.
    """
    try:
        operatore_id = int(operatore_id)
    except (TypeError, ValueError):
        abort(404)

    visible_ids = _priorita_visible_operator_ids_for_current_user()

    if operatore_id not in visible_ids:
        abort(403)

    operatore = User.query.filter(
        User.id == operatore_id,
        User.active.is_(True),
    ).first()

    if operatore is None:
        abort(404)

    return operatore


def _cleanup_priorita_operatore(operatore: User) -> None:
    valid_keys = _priorita_valid_keys_for_operatore(operatore)

    for row in _priorita_rows_for_operatore(operatore.id):
        key = _make_ordine_fase_key(row.IdDocumento, row.IdRiga, row.Fase)
        if key not in valid_keys:
            db.session.delete(row)


def _ordine_fase_key(ordine) -> tuple[str, str, str]:
    return (
        _norm_text(ordine.IdDocumento),
        _norm_text(ordine.IdRiga),
        _norm_text(ordine.FaseAttiva) or "1",
    )


def _priorita_map_for_operatore(
    operatore_id: int,
) -> dict[tuple[str, str, str], OdpPriorita]:
    return {
        _make_ordine_fase_key(row.IdDocumento, row.IdRiga, row.Fase): row
        for row in _priorita_rows_for_operatore(operatore_id)
    }


def _priorita_valid_keys_for_operatore(
    operatore: User,
) -> dict[tuple[str, str, str], InputOdp]:
    return {
        _ordine_fase_key(ordine): ordine
        for ordine in _ordini_pianificata_visibili_per_operatore(operatore)
    }


def _is_ordine_pianificata(ordine) -> bool:
    return _norm_text(getattr(ordine, "StatoOrdine", "")).lower() == "pianificata"


def _ordini_pianificata_visibili_per_operatore(operatore: User) -> list[InputOdp]:
    policy_operatore = RbacPolicy(operatore)

    q = _base_odp_query()
    q = policy_operatore.filter_input_odp(q)
    ordini = q.all()

    ordini = policy_operatore.filter_montaggio_macchine_famiglia_rows(ordini)

    return [ordine for ordine in ordini if _is_ordine_pianificata(ordine)]


def _ordine_priorita_payload(ordine, priorita_row: OdpPriorita | None = None) -> dict:
    fase = _norm_text(ordine.FaseAttiva) or "1"

    return {
        "key": f"{ordine.IdDocumento}|{ordine.IdRiga}|{fase}",
        "id_documento": _norm_text(ordine.IdDocumento),
        "id_riga": _norm_text(ordine.IdRiga),
        "fase": fase,
        "ordine": _ordine_ref_label(ordine),
        "codice": _norm_text(ordine.CodArt),
        "variante": _norm_text(ordine.VarianteArt),
        "revisione": _norm_text(ordine.IndiceModifica),
        "descrizione": _norm_text(ordine.DesArt),
        "quantita": _norm_text(getattr(ordine, "QtyDaLavorare", ""))
        or _norm_text(ordine.Quantita),
        "risorsa": _norm_text(getattr(ordine, "RisorsaAttiva", "")),
        "lavorazione": _norm_text(getattr(ordine, "LavorazioneAttiva", "")),
        "priorita": priorita_row.Priorita if priorita_row else None,
        "matricola": _norm_text(getattr(ordine, "CodMatricola", "")),
        "posizione": priorita_row.Posizione if priorita_row else None,
    }


def _restore_priorita_for_next_phase_from_runtime(
    stato,
    ordine,
    next_phase: str | None,
) -> None:
    """
    Se un ordine prioritizzato avanza alla fase successiva,
    ricrea la priorità sulla nuova fase per lo stesso operatore.

    Non compatta la coda: mantiene il numero priorità originale.
    """
    if stato is None or not next_phase:
        return

    priorita = getattr(stato, "PrioritaInCarico", None)
    operatore_id = getattr(stato, "PrioritaOperatoreIdInCarico", None)

    if priorita not in (1, 2, 3) or not operatore_id:
        return

    key = _make_ordine_fase_key(
        ordine.IdDocumento,
        ordine.IdRiga,
        next_phase,
    )

    existing = OdpPriorita.query.filter_by(
        operatore_id=int(operatore_id),
        IdDocumento=key[0],
        IdRiga=key[1],
        Fase=key[2],
    ).first()

    now_iso = _priority_now_iso()

    max_posizione = (
        db.session.query(func.max(OdpPriorita.Posizione))
        .filter_by(
            operatore_id=int(operatore_id),
            Priorita=int(priorita),
        )
        .scalar()
        or 0
    )

    if existing is not None:
        existing.Priorita = int(priorita)
        existing.Posizione = int(max_posizione) + 1
        existing.updated_at = now_iso
        existing.updated_by = _current_username("priorita_fase_successiva")
        return

    db.session.add(
        OdpPriorita(
            operatore_id=int(operatore_id),
            IdDocumento=key[0],
            IdRiga=key[1],
            Fase=key[2],
            Priorita=int(priorita),
            Posizione=int(max_posizione) + 1,
            created_at=now_iso,
            updated_at=now_iso,
            updated_by=_current_username("priorita_fase_successiva"),
        )
    )


def _priority_sort_key(ordine):
    pr = getattr(ordine, "PrioritaNumero", None)
    pos = getattr(ordine, "PrioritaPosizione", None)

    if pr in (1, 2, 3):
        return (
            0,
            pr,
            pos or 999999,
            _norm_text(ordine.DataFineSched),
            _norm_text(ordine.RifRegistraz),
        )

    return (
        1,
        99,
        999999,
        _norm_text(ordine.DataFineSched),
        _norm_text(ordine.RifRegistraz),
    )


def _apply_priorita_to_ordini(
    ordini: list[InputOdp],
    operatore_id: int,
    *,
    sort_result: bool = True,
) -> list[InputOdp]:
    priorita_map = _priorita_map_for_operatore(operatore_id)

    for ordine in ordini:
        row = priorita_map.get(_ordine_fase_key(ordine))

        if row is None:
            ordine.PrioritaNumero = None
            ordine.PrioritaPosizione = None
        else:
            ordine.PrioritaNumero = row.Priorita
            ordine.PrioritaPosizione = row.Posizione

    if sort_result:
        return sorted(ordini, key=_priority_sort_key)

    return ordini
