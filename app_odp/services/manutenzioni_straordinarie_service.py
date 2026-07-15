from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import func
from sqlalchemy.orm import joinedload

from app_odp.manutenzioni_models import (
    ESITI_MANUTENZIONE_STRAORDINARIA,
    ManutenzioneStraordinaria,
)
from app_odp.models import db
from app_odp.policy.policy import RbacPolicy
from app_odp.services.manutenzioni_service import (
    ManutenzioniServiceError,
    PermessoManutenzioniError,
    get_macchinario,
    list_macchinari,
    serialize_macchinari,
)


ROME_TIMEZONE = ZoneInfo("Europe/Rome")

PERM_MANUTENZIONI_ESEGUI = "manutenzioni_esegui"
PERM_MANUTENZIONI_AMMINISTRAZIONE = (
    "manutenzioni_amministrazione"
)


class ManutenzioneStraordinariaNonTrovataError(
    ManutenzioniServiceError
):
    """L'intervento straordinario richiesto non esiste."""


def _now_rome_naive() -> datetime:
    return datetime.now(
        ROME_TIMEZONE
    ).replace(
        tzinfo=None,
        second=0,
        microsecond=0,
    )


def _assert_can_manage(
    policy: RbacPolicy,
) -> None:
    if policy.can(
        PERM_MANUTENZIONI_ESEGUI
    ) or policy.can(
        PERM_MANUTENZIONI_AMMINISTRAZIONE
    ):
        return

    raise PermessoManutenzioniError(
        "Utente non autorizzato a registrare "
        "o modificare gli interventi straordinari."
    )


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
        raise ManutenzioniServiceError(
            f"Il campo '{field_name}' è obbligatorio."
        )

    return normalized


def _parse_datetime(
    value: Any,
    field_name: str,
) -> datetime:
    if isinstance(value, datetime):
        parsed = value

    elif isinstance(value, date):
        parsed = datetime.combine(
            value,
            time.min,
        )

    else:
        normalized = _norm_text(value)

        if not normalized:
            raise ManutenzioniServiceError(
                f"Il campo '{field_name}' è obbligatorio."
            )

        parsed = None

        for format_string in (
            "%Y-%m-%dT%H:%M",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d %H:%M:%S",
        ):
            try:
                parsed = datetime.strptime(
                    normalized,
                    format_string,
                )
                break
            except ValueError:
                continue

        if parsed is None:
            raise ManutenzioniServiceError(
                f"Il campo '{field_name}' deve avere "
                "il formato YYYY-MM-DDTHH:MM."
            )

    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(
            ROME_TIMEZONE
        ).replace(
            tzinfo=None,
        )

    parsed = parsed.replace(
        second=0,
        microsecond=0,
    )

    if parsed > _now_rome_naive():
        raise ManutenzioniServiceError(
            "La data dell'intervento non può essere futura."
        )

    return parsed


def _parse_minutes(
    value: Any,
    field_name: str,
) -> int | None:
    if value is None or value == "":
        return None

    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise ManutenzioniServiceError(
            f"Il campo '{field_name}' deve essere "
            "espresso in minuti interi."
        ) from exc

    if normalized < 0:
        raise ManutenzioniServiceError(
            f"Il campo '{field_name}' non può essere negativo."
        )

    return normalized


def _parse_esito(
    value: Any,
) -> str:
    normalized = _norm_text(value).upper()

    if normalized not in ESITI_MANUTENZIONE_STRAORDINARIA:
        raise ManutenzioniServiceError(
            "Esito non valido. Valori ammessi: "
            + ", ".join(
                sorted(
                    ESITI_MANUTENZIONE_STRAORDINARIA
                )
            )
            + "."
        )

    return normalized


def _user_snapshot(
    user,
) -> tuple[str | None, str | None]:
    return (
        _norm_optional_text(
            getattr(
                user,
                "public_id",
                None,
            )
        ),
        _norm_optional_text(
            getattr(
                user,
                "username",
                None,
            )
        ),
    )


def list_straordinarie_macchinario(
    macchinario_id: int | str,
    policy: RbacPolicy,
) -> list[ManutenzioneStraordinaria]:
    macchinario = get_macchinario(
        macchinario_id,
        policy,
    )

    return (
        ManutenzioneStraordinaria.query
        .options(
            joinedload(
                ManutenzioneStraordinaria.macchinario
            )
        )
        .filter(
            ManutenzioneStraordinaria.macchinario_id
            == macchinario.id
        )
        .order_by(
            ManutenzioneStraordinaria.data_intervento
            .desc(),
            ManutenzioneStraordinaria.id.desc(),
        )
        .all()
    )


def get_manutenzione_straordinaria(
    intervento_id: int | str,
    policy: RbacPolicy,
    *,
    require_management: bool = False,
) -> ManutenzioneStraordinaria:
    try:
        normalized_id = int(
            intervento_id
        )
    except (TypeError, ValueError) as exc:
        raise ManutenzioneStraordinariaNonTrovataError(
            "Identificativo intervento non valido."
        ) from exc

    intervento = (
        ManutenzioneStraordinaria.query
        .options(
            joinedload(
                ManutenzioneStraordinaria.macchinario
            )
        )
        .filter(
            ManutenzioneStraordinaria.id
            == normalized_id
        )
        .first()
    )

    if intervento is None:
        raise ManutenzioneStraordinariaNonTrovataError(
            "Intervento straordinario non trovato."
        )

    get_macchinario(
        intervento.macchinario_id,
        policy,
    )

    if require_management:
        _assert_can_manage(policy)

    return intervento


def _apply_payload(
    intervento: ManutenzioneStraordinaria,
    data: dict[str, Any],
    *,
    user,
    creating: bool,
) -> None:
    intervento.data_intervento = _parse_datetime(
        data.get(
            "data_intervento",
            intervento.data_intervento,
        ),
        "data_intervento",
    )

    intervento.titolo = _norm_required_text(
        data.get(
            "titolo",
            intervento.titolo,
        ),
        "titolo",
    )

    intervento.descrizione_problema = (
        _norm_required_text(
            data.get(
                "descrizione_problema",
                intervento.descrizione_problema,
            ),
            "descrizione_problema",
        )
    )

    intervento.causa = _norm_optional_text(
        data.get(
            "causa",
            intervento.causa,
        )
    )

    intervento.intervento_eseguito = (
        _norm_required_text(
            data.get(
                "intervento_eseguito",
                intervento.intervento_eseguito,
            ),
            "intervento_eseguito",
        )
    )

    intervento.esito = _parse_esito(
        data.get(
            "esito",
            intervento.esito,
        )
    )

    intervento.fermo_macchina_minuti = (
        _parse_minutes(
            data.get(
                "fermo_macchina_minuti",
                intervento.fermo_macchina_minuti,
            ),
            "fermo_macchina_minuti",
        )
    )

    intervento.durata_intervento_minuti = (
        _parse_minutes(
            data.get(
                "durata_intervento_minuti",
                intervento.durata_intervento_minuti,
            ),
            "durata_intervento_minuti",
        )
    )

    intervento.note = _norm_optional_text(
        data.get(
            "note",
            intervento.note,
        )
    )

    tecnico_esterno = _norm_optional_text(
        data.get(
            "eseguito_da_esterno",
            intervento.eseguito_da_esterno,
        )
    )

    public_id, username = _user_snapshot(
        user
    )

    intervento.eseguito_da_esterno = (
        tecnico_esterno
    )

    if tecnico_esterno:
        intervento.eseguito_da_public_id = None
        intervento.eseguito_da_username = None
    elif (
        creating
        or not intervento.eseguito_da_username
    ):
        intervento.eseguito_da_public_id = (
            public_id
        )
        intervento.eseguito_da_username = (
            username
        )

    if creating:
        intervento.registrato_da_public_id = (
            public_id
        )
        intervento.registrato_da_username = (
            username
        )


def create_manutenzione_straordinaria(
    macchinario_id: int | str,
    data: dict[str, Any],
    policy: RbacPolicy,
    *,
    user,
) -> ManutenzioneStraordinaria:
    _assert_can_manage(policy)

    macchinario = get_macchinario(
        macchinario_id,
        policy,
    )

    intervento = ManutenzioneStraordinaria(
        macchinario_id=macchinario.id,
        data_intervento=_now_rome_naive(),
        titolo="Temporaneo",
        descrizione_problema="Temporaneo",
        intervento_eseguito="Temporaneo",
        esito="DA_VERIFICARE",
    )

    _apply_payload(
        intervento,
        data,
        user=user,
        creating=True,
    )

    db.session.add(intervento)
    db.session.commit()

    return intervento


def update_manutenzione_straordinaria(
    intervento_id: int | str,
    data: dict[str, Any],
    policy: RbacPolicy,
    *,
    user,
) -> ManutenzioneStraordinaria:
    intervento = get_manutenzione_straordinaria(
        intervento_id,
        policy,
        require_management=True,
    )

    _apply_payload(
        intervento,
        data,
        user=user,
        creating=False,
    )

    if (
        intervento.evento_manutenzione is not None
        and intervento.intervento_eseguito.strip().upper() == "DA COMPLETARE"
    ):
        raise ManutenzioniServiceError(
            "Descrivere l'intervento straordinario eseguito."
        )

    if intervento.evento_manutenzione is not None:
        intervento.evento_manutenzione.stato = "COMPLETATA"

    db.session.commit()

    return intervento


def serialize_manutenzione_straordinaria(
    intervento: ManutenzioneStraordinaria,
) -> dict[str, Any]:
    macchinario = intervento.macchinario

    esecutore = (
        intervento.eseguito_da_esterno
        or intervento.eseguito_da_username
        or "-"
    )

    return {
        "id": intervento.id,
        "macchinario_id": intervento.macchinario_id,
        "evento_manutenzione_id": intervento.evento_manutenzione_id,
        "macchinario_codice": (
            macchinario.codice
            if macchinario is not None
            else None
        ),
        "macchinario_descrizione": (
            macchinario.descrizione
            if macchinario is not None
            else None
        ),
        "reparto_codice": (
            macchinario.reparto_codice
            if macchinario is not None
            else None
        ),
        "macchinario_attivo": (
            bool(macchinario.attivo)
            if macchinario is not None
            else False
        ),
        "data_intervento": (
            intervento.data_intervento.strftime(
                "%Y-%m-%dT%H:%M"
            )
            if intervento.data_intervento
            else None
        ),
        "data_intervento_visuale": (
            intervento.data_intervento.strftime(
                "%d/%m/%Y %H:%M"
            )
            if intervento.data_intervento
            else "-"
        ),
        "titolo": intervento.titolo,
        "descrizione_problema": (
            intervento.descrizione_problema
        ),
        "causa": intervento.causa,
        "intervento_eseguito": (
            intervento.intervento_eseguito
        ),
        "da_completare": (
            intervento.intervento_eseguito.strip().upper()
            == "DA COMPLETARE"
        ),
        "esito": intervento.esito,
        "fermo_macchina_minuti": (
            intervento.fermo_macchina_minuti
        ),
        "durata_intervento_minuti": (
            intervento.durata_intervento_minuti
        ),
        "eseguito_da_public_id": (
            intervento.eseguito_da_public_id
        ),
        "eseguito_da_username": (
            intervento.eseguito_da_username
        ),
        "eseguito_da_esterno": (
            intervento.eseguito_da_esterno
        ),
        "esecutore": esecutore,
        "registrato_da_public_id": (
            intervento.registrato_da_public_id
        ),
        "registrato_da_username": (
            intervento.registrato_da_username
        ),
        "note": intervento.note,
        "created_at": (
            intervento.created_at.isoformat()
            if intervento.created_at
            else None
        ),
        "updated_at": (
            intervento.updated_at.isoformat()
            if intervento.updated_at
            else None
        ),
    }


def serialize_manutenzioni_straordinarie(
    interventi: list[
        ManutenzioneStraordinaria
    ],
) -> list[dict[str, Any]]:
    return [
        serialize_manutenzione_straordinaria(
            intervento
        )
        for intervento in interventi
    ]


def build_registro_straordinarie(
    policy: RbacPolicy,
    *,
    reparto_codice: str | None = None,
    data_dal: date | None = None,
    data_fino: date | None = None,
    esito: str | None = None,
    search: str | None = None,
) -> dict[str, Any]:
    macchinari = list_macchinari(
        policy,
        reparto_codice=reparto_codice,
        include_inactive=True,
    )

    macchinari_rows = serialize_macchinari(
        macchinari
    )

    machine_by_id = {
        row["id"]: row
        for row in macchinari_rows
    }

    if not machine_by_id:
        return {
            "rows": [],
            "summary": {
                "totale": 0,
                "risolti": 0,
                "parzialmente_risolti": 0,
                "da_verificare": 0,
                "non_risolti": 0,
                "fermo_macchina_minuti": 0,
                "durata_intervento_minuti": 0,
            },
        }

    query = (
        ManutenzioneStraordinaria.query
        .options(
            joinedload(
                ManutenzioneStraordinaria.macchinario
            )
        )
        .filter(
            ManutenzioneStraordinaria.macchinario_id
            .in_(machine_by_id.keys())
        )
    )

    if data_dal is not None:
        query = query.filter(
            ManutenzioneStraordinaria.data_intervento
            >= datetime.combine(
                data_dal,
                time.min,
            )
        )

    if data_fino is not None:
        query = query.filter(
            ManutenzioneStraordinaria.data_intervento
            < datetime.combine(
                data_fino + timedelta(days=1),
                time.min,
            )
        )

    normalized_esito = _norm_text(
        esito
    ).upper()

    if normalized_esito:
        if (
            normalized_esito
            not in ESITI_MANUTENZIONE_STRAORDINARIA
        ):
            raise ManutenzioniServiceError(
                "Filtro esito non valido."
            )

        query = query.filter(
            ManutenzioneStraordinaria.esito
            == normalized_esito
        )

    interventi = query.order_by(
        ManutenzioneStraordinaria.data_intervento
        .desc(),
        ManutenzioneStraordinaria.id.desc(),
    ).all()

    normalized_search = _norm_text(
        search
    ).lower()

    rows = []

    for intervento in interventi:
        row = serialize_manutenzione_straordinaria(
            intervento
        )

        machine = machine_by_id.get(
            intervento.macchinario_id,
            {},
        )

        row.update(
            {
                "macchinario_codice": machine.get(
                    "codice"
                ),
                "macchinario_descrizione": machine.get(
                    "descrizione"
                ),
                "reparto_codice": machine.get(
                    "reparto_codice"
                ),
                "reparto_descrizione": machine.get(
                    "reparto_descrizione"
                ),
                "ubicazione": machine.get(
                    "ubicazione"
                ),
                "macchinario_attivo": bool(
                    machine.get("attivo")
                ),
            }
        )

        if normalized_search:
            searchable = " ".join(
                str(value or "")
                for value in (
                    row.get("macchinario_codice"),
                    row.get("macchinario_descrizione"),
                    row.get("titolo"),
                    row.get("descrizione_problema"),
                    row.get("causa"),
                    row.get("intervento_eseguito"),
                    row.get("esito"),
                    row.get("esecutore"),
                    row.get("registrato_da_username"),
                    row.get("note"),
                    row.get("ubicazione"),
                )
            ).lower()

            if normalized_search not in searchable:
                continue

        rows.append(row)

    summary = {
        "totale": len(rows),
        "risolti": sum(
            1
            for row in rows
            if row["esito"] == "RISOLTO"
        ),
        "parzialmente_risolti": sum(
            1
            for row in rows
            if row["esito"]
            == "PARZIALMENTE_RISOLTO"
        ),
        "da_verificare": sum(
            1
            for row in rows
            if row["esito"] == "DA_VERIFICARE"
        ),
        "non_risolti": sum(
            1
            for row in rows
            if row["esito"] == "NON_RISOLTO"
        ),
        "fermo_macchina_minuti": sum(
            int(
                row.get(
                    "fermo_macchina_minuti"
                )
                or 0
            )
            for row in rows
        ),
        "durata_intervento_minuti": sum(
            int(
                row.get(
                    "durata_intervento_minuti"
                )
                or 0
            )
            for row in rows
        ),
    }

    return {
        "rows": rows,
        "summary": summary,
    }
