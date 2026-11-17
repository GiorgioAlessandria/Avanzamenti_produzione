from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

from sqlalchemy import and_, func

from app_odp.manutenzioni_models import (
    ManutenzioneGiornoNonLavorativo,
)
from app_odp.models import db
from app_odp.policy.policy import RbacPolicy
from app_odp.services.manutenzioni_service import (
    ManutenzioniServiceError,
    PermessoManutenzioniError,
)


PERM_MANUTENZIONI_GESTISCI_PIANI = (
    "manutenzioni_gestisci_piani"
)
PERM_MANUTENZIONI_AMMINISTRAZIONE = (
    "manutenzioni_amministrazione"
)

TIPI_GIORNO_NON_LAVORATIVO = {
    "CHIUSURA_AZIENDALE": "Chiusura aziendale",
    "FESTIVITA_LOCALE": "Festività locale o patronale",
    "PONTE": "Ponte aziendale",
    "INVENTARIO": "Inventario o fermo programmato",
    "ALTRO": "Altro",
}


class GiornoNonLavorativoNonTrovatoError(
    ManutenzioniServiceError
):
    """Il giorno non lavorativo richiesto non esiste."""


class GiornoNonLavorativoDuplicatoError(
    ManutenzioniServiceError
):
    """Esiste già una chiusura equivalente."""


def _assert_can_manage_calendar(
    policy: RbacPolicy,
) -> None:
    if policy.can(
        PERM_MANUTENZIONI_AMMINISTRAZIONE
    ) or policy.can(
        PERM_MANUTENZIONI_GESTISCI_PIANI
    ):
        return

    raise PermessoManutenzioniError(
        "Utente non autorizzato a gestire "
        "i giorni non lavorativi."
    )


def _norm_text(value: Any) -> str:
    return str(value or "").strip()


def _norm_required_text(
    value: Any,
    field_name: str,
    *,
    max_length: int | None = None,
) -> str:
    normalized = _norm_text(value)

    if not normalized:
        raise ManutenzioniServiceError(
            f"Il campo '{field_name}' è obbligatorio."
        )

    if (
        max_length is not None
        and len(normalized) > max_length
    ):
        raise ManutenzioniServiceError(
            f"Il campo '{field_name}' non può superare "
            f"{max_length} caratteri."
        )

    return normalized


def _parse_date(
    value: Any,
) -> date:
    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    normalized = _norm_text(value)

    if not normalized:
        raise ManutenzioniServiceError(
            "Il campo 'data' è obbligatorio."
        )

    try:
        return date.fromisoformat(normalized)
    except ValueError as exc:
        raise ManutenzioniServiceError(
            "La data deve avere il formato YYYY-MM-DD."
        ) from exc


def _parse_bool(
    value: Any,
    *,
    default: bool,
) -> bool:
    if value is None or value == "":
        return default

    if isinstance(value, bool):
        return value

    if isinstance(value, int) and value in {
        0,
        1,
    }:
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

    raise ManutenzioniServiceError(
        f"Valore booleano non valido: {value!r}."
    )


def _normalize_type(
    value: Any,
) -> str:
    normalized = (
        _norm_text(value)
        or "CHIUSURA_AZIENDALE"
    ).upper()

    if normalized not in TIPI_GIORNO_NON_LAVORATIVO:
        raise ManutenzioniServiceError(
            "Tipo di giorno non lavorativo non valido."
        )

    return normalized


def _duplicate_exists(
    *,
    giorno: date,
    descrizione: str,
    ricorrente_annuale: bool,
    exclude_id: int | None = None,
) -> bool:
    query = (
        ManutenzioneGiornoNonLavorativo.query
        .filter(
            ManutenzioneGiornoNonLavorativo.data
            == giorno,
            func.lower(
                func.trim(
                    ManutenzioneGiornoNonLavorativo.descrizione
                )
            )
            == descrizione.lower(),
            ManutenzioneGiornoNonLavorativo
            .ricorrente_annuale
            .is_(bool(ricorrente_annuale)),
        )
    )

    if exclude_id is not None:
        query = query.filter(
            ManutenzioneGiornoNonLavorativo.id
            != exclude_id
        )

    return query.first() is not None


def list_giorni_non_lavorativi(
    policy: RbacPolicy,
    *,
    include_inactive: bool = True,
) -> list[ManutenzioneGiornoNonLavorativo]:
    _assert_can_manage_calendar(policy)

    query = ManutenzioneGiornoNonLavorativo.query

    if not include_inactive:
        query = query.filter(
            ManutenzioneGiornoNonLavorativo.attivo
            .is_(True)
        )

    return query.order_by(
        ManutenzioneGiornoNonLavorativo.data.asc(),
        func.lower(
            ManutenzioneGiornoNonLavorativo.descrizione
        ),
        ManutenzioneGiornoNonLavorativo.id.asc(),
    ).all()


def get_giorno_non_lavorativo(
    item_id: int | str,
    policy: RbacPolicy,
) -> ManutenzioneGiornoNonLavorativo:
    _assert_can_manage_calendar(policy)

    try:
        normalized_id = int(item_id)
    except (TypeError, ValueError) as exc:
        raise GiornoNonLavorativoNonTrovatoError(
            "Identificativo non valido."
        ) from exc

    item = db.session.get(
        ManutenzioneGiornoNonLavorativo,
        normalized_id,
    )

    if item is None:
        raise GiornoNonLavorativoNonTrovatoError(
            "Giorno non lavorativo non trovato."
        )

    return item


def create_giorno_non_lavorativo(
    data: dict[str, Any],
    policy: RbacPolicy,
    *,
    commit: bool = True,
) -> ManutenzioneGiornoNonLavorativo:
    _assert_can_manage_calendar(policy)

    giorno = _parse_date(
        data.get("data")
    )

    descrizione = _norm_required_text(
        data.get("descrizione"),
        "descrizione",
        max_length=255,
    )

    tipo = _normalize_type(
        data.get("tipo")
    )

    ricorrente_annuale = _parse_bool(
        data.get("ricorrente_annuale"),
        default=False,
    )

    attivo = _parse_bool(
        data.get("attivo"),
        default=True,
    )

    if _duplicate_exists(
        giorno=giorno,
        descrizione=descrizione,
        ricorrente_annuale=ricorrente_annuale,
    ):
        raise GiornoNonLavorativoDuplicatoError(
            "Esiste già un giorno non lavorativo "
            "con la stessa data e descrizione."
        )

    item = ManutenzioneGiornoNonLavorativo(
        data=giorno,
        descrizione=descrizione,
        tipo=tipo,
        ricorrente_annuale=ricorrente_annuale,
        attivo=attivo,
    )

    db.session.add(item)

    if commit:
        db.session.commit()
    else:
        db.session.flush()

    return item


def create_giorni_non_lavorativi(
    data: dict[str, Any],
    policy: RbacPolicy,
    *,
    commit: bool = True,
) -> list[ManutenzioneGiornoNonLavorativo]:
    data_inizio = _parse_date(data.get("data"))
    data_fine = _parse_date(data.get("data_fine") or data_inizio)

    if data_fine < data_inizio:
        raise ManutenzioniServiceError(
            "La data finale non può precedere la data iniziale."
        )

    items: list[ManutenzioneGiornoNonLavorativo] = []
    giorno = data_inizio

    while giorno <= data_fine:
        payload = dict(data)
        payload["data"] = giorno
        payload.pop("data_fine", None)
        items.append(
            create_giorno_non_lavorativo(
                payload,
                policy,
                commit=False,
            )
        )
        giorno += timedelta(days=1)

    if commit:
        db.session.commit()

    return items


def update_giorno_non_lavorativo(
    item_id: int | str,
    data: dict[str, Any],
    policy: RbacPolicy,
    *,
    commit: bool = True,
) -> ManutenzioneGiornoNonLavorativo:
    item = get_giorno_non_lavorativo(
        item_id,
        policy,
    )

    giorno = _parse_date(
        data.get(
            "data",
            item.data,
        )
    )

    descrizione = _norm_required_text(
        data.get(
            "descrizione",
            item.descrizione,
        ),
        "descrizione",
        max_length=255,
    )

    tipo = _normalize_type(
        data.get(
            "tipo",
            item.tipo,
        )
    )

    ricorrente_annuale = _parse_bool(
        data.get(
            "ricorrente_annuale",
            item.ricorrente_annuale,
        ),
        default=bool(item.ricorrente_annuale),
    )

    attivo = _parse_bool(
        data.get(
            "attivo",
            item.attivo,
        ),
        default=bool(item.attivo),
    )

    if _duplicate_exists(
        giorno=giorno,
        descrizione=descrizione,
        ricorrente_annuale=ricorrente_annuale,
        exclude_id=item.id,
    ):
        raise GiornoNonLavorativoDuplicatoError(
            "Esiste già un giorno non lavorativo "
            "con la stessa data e descrizione."
        )

    item.data = giorno
    item.descrizione = descrizione
    item.tipo = tipo
    item.ricorrente_annuale = ricorrente_annuale
    item.attivo = attivo

    if commit:
        db.session.commit()
    else:
        db.session.flush()

    return item


def set_giorno_non_lavorativo_attivo(
    item_id: int | str,
    attivo: Any,
    policy: RbacPolicy,
    *,
    commit: bool = True,
) -> ManutenzioneGiornoNonLavorativo:
    item = get_giorno_non_lavorativo(
        item_id,
        policy,
    )

    item.attivo = _parse_bool(
        attivo,
        default=bool(item.attivo),
    )

    if commit:
        db.session.commit()
    else:
        db.session.flush()

    return item


def delete_giorno_non_lavorativo(
    item_id: int | str,
    policy: RbacPolicy,
    *,
    commit: bool = True,
) -> None:
    item = get_giorno_non_lavorativo(
        item_id,
        policy,
    )

    db.session.delete(item)

    if commit:
        db.session.commit()
    else:
        db.session.flush()


def serialize_giorno_non_lavorativo(
    item: ManutenzioneGiornoNonLavorativo,
) -> dict[str, Any]:
    return {
        "id": item.id,
        "data": (
            item.data.isoformat()
            if item.data
            else None
        ),
        "descrizione": item.descrizione,
        "tipo": item.tipo,
        "tipo_descrizione": (
            TIPI_GIORNO_NON_LAVORATIVO.get(
                item.tipo,
                item.tipo,
            )
        ),
        "ricorrente_annuale": bool(
            item.ricorrente_annuale
        ),
        "attivo": bool(item.attivo),
        "created_at": (
            item.created_at.isoformat()
            if item.created_at
            else None
        ),
        "updated_at": (
            item.updated_at.isoformat()
            if item.updated_at
            else None
        ),
    }


def serialize_giorni_non_lavorativi(
    items: list[
        ManutenzioneGiornoNonLavorativo
    ],
) -> list[dict[str, Any]]:
    return [
        serialize_giorno_non_lavorativo(item)
        for item in items
    ]
