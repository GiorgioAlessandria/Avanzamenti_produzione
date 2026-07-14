# app_odp/services/manutenzioni_service.py

from __future__ import annotations

from typing import Any

from sqlalchemy import func, or_, false
from sqlalchemy.exc import IntegrityError

from app_odp.manutenzioni_models import Macchinario
from app_odp.models import Reparti, db
from app_odp.policy.policy import RbacPolicy


PERM_MANUTENZIONI_VISUALIZZA = "manutenzioni_visualizza"
PERM_MANUTENZIONI_GESTISCI_MACCHINARI = "manutenzioni_gestisci_macchinari"
PERM_MANUTENZIONI_VISUALIZZA_TUTTI_REPARTI = "manutenzioni_visualizza_tutti_reparti"
PERM_MANUTENZIONI_AMMINISTRAZIONE = "manutenzioni_amministrazione"


class ManutenzioniServiceError(ValueError):
    """Errore applicativo generico del modulo manutenzioni."""


class MacchinarioNonTrovatoError(ManutenzioniServiceError):
    """Il macchinario richiesto non esiste."""


class RepartoNonValidoError(ManutenzioniServiceError):
    """Il codice reparto non esiste nel database RBAC."""


class CodiceMacchinarioDuplicatoError(ManutenzioniServiceError):
    """Il codice macchinario è già utilizzato."""


class PermessoManutenzioniError(PermissionError):
    """L'utente non dispone del permesso richiesto."""


class RepartoNonAutorizzatoError(PermissionError):
    """L'utente non può operare sul reparto richiesto."""


def _norm_text(value: Any) -> str:
    return str(value or "").strip()


def _norm_optional_text(value: Any) -> str | None:
    normalized = _norm_text(value)
    return normalized or None


def _norm_required_text(
    value: Any,
    field_name: str,
) -> str:
    normalized = _norm_text(value)

    if not normalized:
        raise ManutenzioniServiceError(f"Il campo '{field_name}' è obbligatorio.")

    return normalized


def _norm_code(
    value: Any,
    field_name: str,
) -> str:
    return _norm_required_text(
        value,
        field_name,
    ).upper()


def _parse_bool(
    value: Any,
    *,
    default: bool | None = None,
) -> bool:
    if value is None or value == "":
        if default is not None:
            return default

        raise ManutenzioniServiceError("Valore booleano non specificato.")

    if isinstance(value, bool):
        return value

    if isinstance(value, int):
        if value in (0, 1):
            return bool(value)

    normalized = _norm_text(value).lower()

    if normalized in {
        "1",
        "true",
        "vero",
        "si",
        "sì",
        "yes",
        "on",
        "attivo",
    }:
        return True

    if normalized in {
        "0",
        "false",
        "falso",
        "no",
        "off",
        "inattivo",
    }:
        return False

    raise ManutenzioniServiceError(f"Valore booleano non valido: {value!r}.")


def _policy_can(
    policy: RbacPolicy | None,
    permission: str,
) -> bool:
    return bool(policy is not None and policy.can(permission))


def _is_maintenance_admin(
    policy: RbacPolicy | None,
) -> bool:
    return _policy_can(
        policy,
        PERM_MANUTENZIONI_AMMINISTRAZIONE,
    )


def _can_view_all_departments(
    policy: RbacPolicy | None,
) -> bool:
    return _is_maintenance_admin(policy) or _policy_can(
        policy,
        PERM_MANUTENZIONI_VISUALIZZA_TUTTI_REPARTI,
    )


def _allowed_department_codes(
    policy: RbacPolicy | None,
) -> set[str]:
    if policy is None:
        return set()

    return {
        _norm_code(code, "reparto_codice")
        for code in policy.allowed_reparti
        if _norm_text(code)
    }


def _assert_can_view(
    policy: RbacPolicy | None,
) -> None:
    if _is_maintenance_admin(policy):
        return

    if not _policy_can(
        policy,
        PERM_MANUTENZIONI_VISUALIZZA,
    ):
        raise PermessoManutenzioniError(
            "Utente non autorizzato a visualizzare le manutenzioni."
        )


def _assert_can_manage_machines(
    policy: RbacPolicy | None,
) -> None:
    if _is_maintenance_admin(policy):
        return

    if not _policy_can(
        policy,
        PERM_MANUTENZIONI_GESTISCI_MACCHINARI,
    ):
        raise PermessoManutenzioniError(
            "Utente non autorizzato a gestire l'anagrafica dei macchinari."
        )


def _assert_department_visible(
    policy: RbacPolicy | None,
    reparto_codice: str,
) -> None:
    if _can_view_all_departments(policy):
        return

    allowed_codes = _allowed_department_codes(policy)

    if reparto_codice not in allowed_codes:
        raise RepartoNonAutorizzatoError(
            "Il reparto del macchinario non rientra tra quelli visibili all'utente."
        )


def _assert_department_manageable(
    policy: RbacPolicy | None,
    reparto_codice: str,
) -> None:
    if _is_maintenance_admin(policy):
        return

    allowed_codes = _allowed_department_codes(policy)

    if reparto_codice not in allowed_codes:
        raise RepartoNonAutorizzatoError(
            f"L'utente non può gestire macchinari del reparto {reparto_codice}."
        )


def get_reparto_by_code(
    reparto_codice: Any,
) -> Reparti:
    """
    Recupera un reparto dal database RBAC.

    Il collegamento con il database manutenzioni è logico:
    Macchinario.reparto_codice contiene Reparti.Codice.
    """
    normalized = _norm_code(
        reparto_codice,
        "reparto_codice",
    )

    reparto = Reparti.query.filter(
        func.upper(func.trim(Reparti.Codice)) == normalized
    ).first()

    if reparto is None:
        raise RepartoNonValidoError(f"Il reparto '{normalized}' non esiste.")

    return reparto


def list_reparti_manutenzioni(
    policy: RbacPolicy,
    *,
    for_management: bool = False,
) -> list[dict[str, Any]]:
    """
    Restituisce i reparti selezionabili dall'utente.

    Un amministratore vede tutti i reparti.
    Gli altri utenti vedono solo i reparti assegnati tramite RBAC.
    """
    if for_management:
        _assert_can_manage_machines(policy)
    else:
        _assert_can_view(policy)

    query = Reparti.query

    if not _is_maintenance_admin(policy):
        allowed_codes = _allowed_department_codes(policy)

        if not allowed_codes:
            return []

        query = query.filter(func.upper(func.trim(Reparti.Codice)).in_(allowed_codes))

    reparti = query.order_by(
        func.lower(
            func.coalesce(
                Reparti.Descrizione,
                Reparti.Codice,
            )
        ),
        func.lower(Reparti.Codice),
    ).all()

    return [
        {
            "id": reparto.id,
            "codice": _norm_text(reparto.Codice),
            "descrizione": _norm_text(reparto.Descrizione),
        }
        for reparto in reparti
    ]


def _apply_machine_scope(
    query,
    policy: RbacPolicy,
):
    if _can_view_all_departments(policy):
        return query

    allowed_codes = _allowed_department_codes(policy)

    if not allowed_codes:
        return query.filter(false())

    return query.filter(Macchinario.reparto_codice.in_(allowed_codes))


def list_macchinari(
    policy: RbacPolicy,
    *,
    reparto_codice: str | None = None,
    search: str | None = None,
    include_inactive: bool = False,
) -> list[Macchinario]:
    """
    Restituisce i macchinari visibili all'utente.

    Filtri disponibili:
    - reparto;
    - ricerca per codice, descrizione, matricola,
      costruttore, modello o ubicazione;
    - inclusione dei macchinari inattivi.
    """
    _assert_can_view(policy)

    query = Macchinario.query
    query = _apply_machine_scope(query, policy)

    if not include_inactive:
        query = query.filter(Macchinario.attivo.is_(True))

    if reparto_codice:
        reparto = get_reparto_by_code(reparto_codice)
        normalized_reparto = _norm_code(
            reparto.Codice,
            "reparto_codice",
        )

        _assert_department_visible(
            policy,
            normalized_reparto,
        )

        query = query.filter(Macchinario.reparto_codice == normalized_reparto)

    normalized_search = _norm_text(search)

    if normalized_search:
        pattern = f"%{normalized_search.lower()}%"

        query = query.filter(
            or_(
                func.lower(
                    func.coalesce(
                        Macchinario.codice,
                        "",
                    )
                ).like(pattern),
                func.lower(
                    func.coalesce(
                        Macchinario.descrizione,
                        "",
                    )
                ).like(pattern),
                func.lower(
                    func.coalesce(
                        Macchinario.matricola,
                        "",
                    )
                ).like(pattern),
                func.lower(
                    func.coalesce(
                        Macchinario.costruttore,
                        "",
                    )
                ).like(pattern),
                func.lower(
                    func.coalesce(
                        Macchinario.modello,
                        "",
                    )
                ).like(pattern),
                func.lower(
                    func.coalesce(
                        Macchinario.ubicazione,
                        "",
                    )
                ).like(pattern),
            )
        )

    return query.order_by(
        Macchinario.reparto_codice,
        func.lower(Macchinario.descrizione),
        func.lower(Macchinario.codice),
    ).all()


def get_macchinario(
    macchinario_id: int | str,
    policy: RbacPolicy,
    *,
    require_management: bool = False,
) -> Macchinario:
    try:
        normalized_id = int(macchinario_id)
    except (TypeError, ValueError) as exc:
        raise MacchinarioNonTrovatoError(
            "Identificativo macchinario non valido."
        ) from exc

    macchinario = db.session.get(
        Macchinario,
        normalized_id,
    )

    if macchinario is None:
        raise MacchinarioNonTrovatoError("Macchinario non trovato.")

    if require_management:
        _assert_can_manage_machines(policy)
        _assert_department_manageable(
            policy,
            macchinario.reparto_codice,
        )
    else:
        _assert_can_view(policy)
        _assert_department_visible(
            policy,
            macchinario.reparto_codice,
        )

    return macchinario


def _machine_code_exists(
    codice: str,
    *,
    exclude_id: int | None = None,
) -> bool:
    query = Macchinario.query.filter(
        func.upper(func.trim(Macchinario.codice)) == codice
    )

    if exclude_id is not None:
        query = query.filter(Macchinario.id != exclude_id)

    return query.first() is not None


def create_macchinario(
    data: dict[str, Any],
    policy: RbacPolicy,
) -> Macchinario:
    _assert_can_manage_machines(policy)

    codice = _norm_code(
        data.get("codice"),
        "codice",
    )

    descrizione = _norm_required_text(
        data.get("descrizione"),
        "descrizione",
    )

    reparto = get_reparto_by_code(data.get("reparto_codice"))

    reparto_codice = _norm_code(
        reparto.Codice,
        "reparto_codice",
    )

    _assert_department_manageable(
        policy,
        reparto_codice,
    )

    if _machine_code_exists(codice):
        raise CodiceMacchinarioDuplicatoError(
            f"Il codice macchinario '{codice}' è già utilizzato."
        )

    macchinario = Macchinario(
        codice=codice,
        descrizione=descrizione,
        matricola=_norm_optional_text(data.get("matricola")),
        reparto_codice=reparto_codice,
        costruttore=_norm_optional_text(data.get("costruttore")),
        modello=_norm_optional_text(data.get("modello")),
        ubicazione=_norm_optional_text(data.get("ubicazione")),
        attivo=_parse_bool(
            data.get("attivo"),
            default=True,
        ),
        note=_norm_optional_text(data.get("note")),
    )

    db.session.add(macchinario)

    try:
        db.session.commit()
    except IntegrityError as exc:
        db.session.rollback()

        message = str(getattr(exc, "orig", exc)).lower()

        if "macchinari.codice" in message or "uq_macchinari_codice" in message:
            raise CodiceMacchinarioDuplicatoError(
                f"Il codice macchinario '{codice}' è già utilizzato."
            ) from exc

        raise

    return macchinario


def update_macchinario(
    macchinario_id: int | str,
    data: dict[str, Any],
    policy: RbacPolicy,
) -> Macchinario:
    macchinario = get_macchinario(
        macchinario_id,
        policy,
        require_management=True,
    )

    if "codice" in data:
        codice = _norm_code(
            data.get("codice"),
            "codice",
        )

        if _machine_code_exists(
            codice,
            exclude_id=macchinario.id,
        ):
            raise CodiceMacchinarioDuplicatoError(
                f"Il codice macchinario '{codice}' è già utilizzato."
            )

        macchinario.codice = codice

    if "descrizione" in data:
        macchinario.descrizione = _norm_required_text(
            data.get("descrizione"),
            "descrizione",
        )

    if "reparto_codice" in data:
        reparto = get_reparto_by_code(data.get("reparto_codice"))

        new_reparto_codice = _norm_code(
            reparto.Codice,
            "reparto_codice",
        )

        _assert_department_manageable(
            policy,
            new_reparto_codice,
        )

        macchinario.reparto_codice = new_reparto_codice

    optional_fields = {
        "matricola",
        "costruttore",
        "modello",
        "ubicazione",
        "note",
    }

    for field_name in optional_fields:
        if field_name in data:
            setattr(
                macchinario,
                field_name,
                _norm_optional_text(data.get(field_name)),
            )

    if "attivo" in data:
        macchinario.attivo = _parse_bool(data.get("attivo"))

    try:
        db.session.commit()
    except IntegrityError as exc:
        db.session.rollback()

        message = str(getattr(exc, "orig", exc)).lower()

        if "macchinari.codice" in message or "uq_macchinari_codice" in message:
            raise CodiceMacchinarioDuplicatoError(
                "Il codice macchinario è già utilizzato."
            ) from exc

        raise

    return macchinario


def set_macchinario_attivo(
    macchinario_id: int | str,
    attivo: bool,
    policy: RbacPolicy,
) -> Macchinario:
    """
    Attiva o disattiva un macchinario.

    Non viene eseguita una cancellazione fisica, così vengono
    mantenute le manutenzioni e lo storico collegati.
    """
    macchinario = get_macchinario(
        macchinario_id,
        policy,
        require_management=True,
    )

    macchinario.attivo = bool(attivo)

    db.session.commit()

    return macchinario


def serialize_macchinario(
    macchinario: Macchinario,
    *,
    reparto_descrizione: str | None = None,
) -> dict[str, Any]:
    return {
        "id": macchinario.id,
        "codice": macchinario.codice,
        "descrizione": macchinario.descrizione,
        "matricola": macchinario.matricola,
        "reparto_codice": (macchinario.reparto_codice),
        "reparto_descrizione": (reparto_descrizione or ""),
        "costruttore": macchinario.costruttore,
        "modello": macchinario.modello,
        "ubicazione": macchinario.ubicazione,
        "attivo": bool(macchinario.attivo),
        "note": macchinario.note,
        "created_at": (
            macchinario.created_at.isoformat() if macchinario.created_at else None
        ),
        "updated_at": (
            macchinario.updated_at.isoformat() if macchinario.updated_at else None
        ),
    }


def serialize_macchinari(
    macchinari: list[Macchinario],
) -> list[dict[str, Any]]:
    reparto_codes = {
        macchinario.reparto_codice
        for macchinario in macchinari
        if macchinario.reparto_codice
    }

    if not reparto_codes:
        return [serialize_macchinario(macchinario) for macchinario in macchinari]

    reparti = Reparti.query.filter(Reparti.Codice.in_(reparto_codes)).all()

    descriptions = {
        _norm_code(
            reparto.Codice,
            "reparto_codice",
        ): _norm_text(reparto.Descrizione)
        for reparto in reparti
    }

    return [
        serialize_macchinario(
            macchinario,
            reparto_descrizione=descriptions.get(
                macchinario.reparto_codice,
                "",
            ),
        )
        for macchinario in macchinari
    ]
