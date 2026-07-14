from __future__ import annotations

from datetime import date, datetime
from typing import Any

from sqlalchemy import func
from sqlalchemy.exc import IntegrityError

from app_odp.manutenzioni_models import (
    ManutenzioneRicorrente,
)
from app_odp.models import db
from app_odp.policy.policy import RbacPolicy
from app_odp.services.manutenzioni_service import (
    ManutenzioniServiceError,
    PermessoManutenzioniError,
    get_macchinario,
)

FREQUENZE_AMMESSE = {
    "giorni",
    "settimane",
    "mesi",
    "anni",
}
GIORNI_SETTIMANA_LAVORATIVI = {
    "MO",
    "TU",
    "WE",
    "TH",
    "FR",
}
PERM_MANUTENZIONI_GESTISCI_PIANI = "manutenzioni_gestisci_piani"

PERM_MANUTENZIONI_AMMINISTRAZIONE = "manutenzioni_amministrazione"


GIORNI_SETTIMANA_LABELS = {
    "MO": "Lunedì",
    "TU": "Martedì",
    "WE": "Mercoledì",
    "TH": "Giovedì",
    "FR": "Venerdì",
    "SA": "Sabato",
    "SU": "Domenica",
}


class PianoManutenzioneNonTrovatoError(ManutenzioniServiceError):
    """Il piano di manutenzione richiesto non esiste."""


class CodicePianoDuplicatoError(ManutenzioniServiceError):
    """Il codice del piano è già usato sul macchinario."""


def _norm_text(value: Any) -> str:
    return str(value or "").strip()


def _norm_optional_text(
    value: Any,
) -> str | None:
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


def _parse_positive_int(
    value: Any,
    field_name: str,
    *,
    minimum: int = 1,
) -> int:
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise ManutenzioniServiceError(
            f"Il campo '{field_name}' deve essere un numero intero."
        ) from exc

    if normalized < minimum:
        raise ManutenzioniServiceError(
            f"Il campo '{field_name}' deve essere maggiore o uguale a {minimum}."
        )

    return normalized


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

    if isinstance(value, int) and value in (0, 1):
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


def _parse_date(
    value: Any,
    field_name: str,
    *,
    required: bool,
) -> date | None:
    if value is None or value == "":
        if required:
            raise ManutenzioniServiceError(f"Il campo '{field_name}' è obbligatorio.")

        return None

    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    normalized = _norm_text(value)

    try:
        return date.fromisoformat(normalized)
    except ValueError as exc:
        raise ManutenzioniServiceError(
            f"Il campo '{field_name}' deve avere il formato YYYY-MM-DD."
        ) from exc


def _normalize_weekdays(
    value: Any,
) -> list[str]:
    """
    Normalizza i giorni scelti per una manutenzione
    con frequenza settimanale.

    Sono ammessi esclusivamente i giorni lavorativi:
    lunedì, martedì, mercoledì, giovedì e venerdì.
    """
    if value in (None, ""):
        return []

    if isinstance(value, str):
        raw_values = value.split(",")

    elif isinstance(value, (list, tuple, set)):
        raw_values = list(value)

    else:
        raise ManutenzioniServiceError("Formato dei giorni della settimana non valido.")

    normalized_values: list[str] = []

    for item in raw_values:
        normalized = str(item or "").strip().upper()

        if not normalized:
            continue

        if normalized not in GIORNI_SETTIMANA_LAVORATIVI:
            raise ManutenzioniServiceError(
                "Le manutenzioni settimanali possono "
                "essere pianificate solamente "
                "da lunedì a venerdì."
            )

        if normalized not in normalized_values:
            normalized_values.append(normalized)

    return normalized_values


def _assert_can_manage_plans(
    policy: RbacPolicy,
) -> None:
    if policy.can(PERM_MANUTENZIONI_AMMINISTRAZIONE) or policy.can(
        PERM_MANUTENZIONI_GESTISCI_PIANI
    ):
        return

    raise PermessoManutenzioniError(
        "Utente non autorizzato a gestire i piani di manutenzione."
    )


def _validate_frequency(
    frequenza_unita: str,
    giorni_settimana: list[str],
) -> None:
    if frequenza_unita not in FREQUENZE_AMMESSE:
        raise ManutenzioniServiceError(
            "Unità di frequenza non valida."
        )

    if frequenza_unita == "settimane":
        if not giorni_settimana:
            raise ManutenzioniServiceError(
                "Selezionare almeno un giorno lavorativo "
                "per la frequenza settimanale."
            )
        return

    if giorni_settimana:
        raise ManutenzioniServiceError(
            "I giorni della settimana possono essere "
            "specificati solamente per una frequenza "
            "settimanale."
        )


def _piano_code_exists(
    macchinario_id: int,
    codice: str,
    *,
    exclude_id: int | None = None,
) -> bool:
    query = ManutenzioneRicorrente.query.filter(
        ManutenzioneRicorrente.macchinario_id == macchinario_id,
        func.upper(func.trim(ManutenzioneRicorrente.codice)) == codice.upper(),
    )

    if exclude_id is not None:
        query = query.filter(ManutenzioneRicorrente.id != exclude_id)

    return query.first() is not None


def list_piani_macchinario(
    macchinario_id: int | str,
    policy: RbacPolicy,
    *,
    include_inactive: bool = True,
) -> list[ManutenzioneRicorrente]:
    macchinario = get_macchinario(
        macchinario_id,
        policy,
    )

    query = ManutenzioneRicorrente.query.filter(
        ManutenzioneRicorrente.macchinario_id == macchinario.id
    )

    if not include_inactive:
        query = query.filter(ManutenzioneRicorrente.attiva.is_(True))

    return query.order_by(
        ManutenzioneRicorrente.attiva.desc(),
        func.lower(ManutenzioneRicorrente.titolo),
        ManutenzioneRicorrente.id,
    ).all()


def get_piano_manutenzione(
    piano_id: int | str,
    policy: RbacPolicy,
    *,
    require_management: bool = False,
) -> ManutenzioneRicorrente:
    try:
        normalized_id = int(piano_id)
    except (TypeError, ValueError) as exc:
        raise PianoManutenzioneNonTrovatoError(
            "Identificativo piano non valido."
        ) from exc

    piano = db.session.get(
        ManutenzioneRicorrente,
        normalized_id,
    )

    if piano is None:
        raise PianoManutenzioneNonTrovatoError("Piano di manutenzione non trovato.")

    if require_management:
        _assert_can_manage_plans(policy)

    get_macchinario(
        piano.macchinario_id,
        policy,
    )

    return piano


def create_piano_manutenzione(
    macchinario_id: int | str,
    data: dict[str, Any],
    policy: RbacPolicy,
    *,
    created_by=None,
) -> ManutenzioneRicorrente:
    _assert_can_manage_plans(policy)

    macchinario = get_macchinario(
        macchinario_id,
        policy,
    )

    codice = _norm_optional_text(data.get("codice"))

    if codice:
        codice = codice.upper()

        if _piano_code_exists(
            macchinario.id,
            codice,
        ):
            raise CodicePianoDuplicatoError(
                f"Il codice piano '{codice}' è già utilizzato per questo macchinario."
            )

    titolo = _norm_required_text(
        data.get("titolo"),
        "titolo",
    )

    frequenza_unita = _norm_required_text(
        data.get("frequenza_unita"),
        "frequenza_unita",
    ).lower()

    frequenza_intervallo = _parse_positive_int(
        data.get("frequenza_intervallo"),
        "frequenza_intervallo",
    )

    giorni_settimana = _normalize_weekdays(
        data.get("giorni_settimana")
    )

    data_inizio = _parse_date(
        data.get("data_inizio"),
        "data_inizio",
        required=True,
    )

    preavviso_giorni = _parse_positive_int(
        data.get("preavviso_giorni", 7),
        "preavviso_giorni",
        minimum=0,
    )

    _validate_frequency(
        frequenza_unita,
        giorni_settimana,
    )

    piano = ManutenzioneRicorrente(
        macchinario_id=macchinario.id,
        codice=codice,
        titolo=titolo,
        descrizione=_norm_optional_text(data.get("descrizione")),
        frequenza_unita=frequenza_unita,
        frequenza_intervallo=(frequenza_intervallo),
        data_inizio=data_inizio,
        preavviso_giorni=preavviso_giorni,
        attiva=_parse_bool(
            data.get("attiva"),
            default=True,
        ),
        created_by_public_id=_norm_optional_text(
            getattr(created_by, "public_id", None)
        ),
        created_by_username=_norm_optional_text(getattr(created_by, "username", None)),
    )

    piano.giorni_settimana_list = giorni_settimana

    db.session.add(piano)

    try:
        db.session.commit()
    except IntegrityError as exc:
        db.session.rollback()

        message = str(getattr(exc, "orig", exc)).lower()

        if (
            "uq_manutenzioni_ricorrenti_codice" in message
            or ("manutenzioni_ricorrenti.macchinario_id") in message
        ):
            raise CodicePianoDuplicatoError(
                "Il codice del piano è già utilizzato per questo macchinario."
            ) from exc

        raise

    return piano


def update_piano_manutenzione(
    piano_id: int | str,
    data: dict[str, Any],
    policy: RbacPolicy,
) -> ManutenzioneRicorrente:
    piano = get_piano_manutenzione(
        piano_id,
        policy,
        require_management=True,
    )

    if "codice" in data:
        codice = _norm_optional_text(data.get("codice"))

        if codice:
            codice = codice.upper()

            if _piano_code_exists(
                piano.macchinario_id,
                codice,
                exclude_id=piano.id,
            ):
                raise CodicePianoDuplicatoError(
                    f"Il codice piano '{codice}' è già utilizzato."
                )

        piano.codice = codice

    if "titolo" in data:
        piano.titolo = _norm_required_text(
            data.get("titolo"),
            "titolo",
        )

    if "descrizione" in data:
        piano.descrizione = _norm_optional_text(data.get("descrizione"))

    frequenza_unita = _norm_required_text(
        data.get(
            "frequenza_unita",
            piano.frequenza_unita,
        ),
        "frequenza_unita",
    ).lower()

    frequenza_intervallo = _parse_positive_int(
        data.get(
            "frequenza_intervallo",
            piano.frequenza_intervallo,
        ),
        "frequenza_intervallo",
    )

    if "giorni_settimana" in data:
        giorni_settimana = _normalize_weekdays(
            data.get("giorni_settimana")
        )
    elif frequenza_unita == "settimane":
        giorni_settimana = piano.giorni_settimana_list
    else:
        giorni_settimana = []

    data_inizio = _parse_date(
        data.get(
            "data_inizio",
            piano.data_inizio,
        ),
        "data_inizio",
        required=True,
    )

    preavviso_giorni = _parse_positive_int(
        data.get(
            "preavviso_giorni",
            piano.preavviso_giorni,
        ),
        "preavviso_giorni",
        minimum=0,
    )

    _validate_frequency(
        frequenza_unita,
        giorni_settimana,
    )

    piano.frequenza_unita = frequenza_unita
    piano.frequenza_intervallo = frequenza_intervallo
    piano.giorni_settimana_list = giorni_settimana
    piano.data_inizio = data_inizio
    piano.preavviso_giorni = preavviso_giorni

    if "attiva" in data:
        piano.attiva = _parse_bool(data.get("attiva"))

    try:
        db.session.commit()
    except IntegrityError as exc:
        db.session.rollback()

        raise CodicePianoDuplicatoError(
            "Il codice del piano è già utilizzato per questo macchinario."
        ) from exc

    return piano


def set_piano_attivo(
    piano_id: int | str,
    attiva: bool,
    policy: RbacPolicy,
) -> ManutenzioneRicorrente:
    piano = get_piano_manutenzione(
        piano_id,
        policy,
        require_management=True,
    )

    piano.attiva = bool(attiva)
    db.session.commit()

    return piano


def serialize_piano_manutenzione(
    piano: ManutenzioneRicorrente,
) -> dict[str, Any]:
    giorni = piano.giorni_settimana_list

    return {
        "id": piano.id,
        "macchinario_id": piano.macchinario_id,
        "codice": piano.codice,
        "titolo": piano.titolo,
        "descrizione": piano.descrizione,
        "frequenza_unita": (piano.frequenza_unita),
        "frequenza_intervallo": (piano.frequenza_intervallo),
        "giorni_settimana": giorni,
        "giorni_settimana_descrizione": [
            GIORNI_SETTIMANA_LABELS.get(
                giorno,
                giorno,
            )
            for giorno in giorni
        ],
        "descrizione_frequenza": (piano.descrizione_frequenza),
        "data_inizio": (piano.data_inizio.isoformat() if piano.data_inizio else None),
        "preavviso_giorni": (piano.preavviso_giorni),
        "attiva": bool(piano.attiva),
        "created_by_public_id": (piano.created_by_public_id),
        "created_by_username": (piano.created_by_username),
        "created_at": (piano.created_at.isoformat() if piano.created_at else None),
        "updated_at": (piano.updated_at.isoformat() if piano.updated_at else None),
    }


def serialize_piani_manutenzione(
    piani: list[ManutenzioneRicorrente],
) -> list[dict[str, Any]]:
    return [serialize_piano_manutenzione(piano) for piano in piani]
