from __future__ import annotations

import calendar
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import joinedload

from app_odp.services.giorni_lavorativi_service import (
    normalizza_data_manutenzione,
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
from app_odp.manutenzioni_models import (
    ESITI_EVENTO_MANUTENZIONE,
    EventoManutenzione,
    ManutenzioneRicorrente,
)

ROME_TIMEZONE = ZoneInfo("Europe/Rome")

DEFAULT_HORIZON_MONTHS = 12
MAX_EVENTI_PER_GENERAZIONE = 5000

WEEKDAY_INDEX = {
    "MO": 0,
    "TU": 1,
    "WE": 2,
    "TH": 3,
    "FR": 4,
}


class GenerazioneEventiError(ValueError):
    """Errore durante il calcolo o la generazione degli eventi."""


def today_rome() -> date:
    return datetime.now(ROME_TIMEZONE).date()


def add_months(
    base_date: date,
    months: int,
) -> date:
    """
    Aggiunge mesi mantenendo il giorno originale quando possibile.

    Esempio:
    31/01 + 1 mese = 28/02
    31/01 + 2 mesi = 31/03
    """
    absolute_month = base_date.year * 12 + base_date.month - 1 + months

    year, month_index = divmod(
        absolute_month,
        12,
    )

    month = month_index + 1

    last_day = calendar.monthrange(
        year,
        month,
    )[1]

    day = min(
        base_date.day,
        last_day,
    )

    return date(
        year,
        month,
        day,
    )


def add_years(
    base_date: date,
    years: int,
) -> date:
    target_year = base_date.year + years

    last_day = calendar.monthrange(
        target_year,
        base_date.month,
    )[1]

    return date(
        target_year,
        base_date.month,
        min(base_date.day, last_day),
    )


def default_horizon_date(
    reference_date: date | None = None,
) -> date:
    reference = reference_date or today_rome()

    return add_months(
        reference,
        DEFAULT_HORIZON_MONTHS,
    )


def _append_date(
    results: list[date],
    value: date,
) -> None:
    if len(results) >= MAX_EVENTI_PER_GENERAZIONE:
        raise GenerazioneEventiError(
            "Il piano genera troppi eventi. Controllare la frequenza e il periodo."
        )

    results.append(value)


def _calculate_daily_dates(
    anchor: date,
    interval: int,
    start: date,
    end: date,
) -> list[date]:
    """
    Calcola le date teoriche senza modificarle in base al calendario.
    """
    results: list[date] = []


    elapsed_days = max(0, (start - anchor).days)
    steps = (elapsed_days + interval - 1) // interval
    candidate = anchor + timedelta(days=steps * interval)

    while candidate <= end:
        _append_date(
            results,
            candidate,
        )

        candidate += timedelta(days=interval)

    return results


def _calculate_weekly_dates(
    piano: ManutenzioneRicorrente,
    start: date,
    end: date,
) -> list[date]:
    results: list[date] = []

    anchor = piano.data_inizio
    interval = piano.frequenza_intervallo

    selected_codes = piano.giorni_settimana_list

    if not selected_codes:
        raise GenerazioneEventiError(
            "Il piano settimanale non contiene "
            "giorni lavorativi configurati."
        )

    invalid_codes = [
        code
        for code in selected_codes
        if code not in WEEKDAY_INDEX
    ]

    if invalid_codes:
        raise GenerazioneEventiError(
            "Il piano contiene giorni non lavorativi: "
            + ", ".join(invalid_codes)
            + "."
        )

    weekday_indexes = sorted(
        {
            WEEKDAY_INDEX[code]
            for code in selected_codes
        }
    )

    anchor_week_start = anchor - timedelta(days=anchor.weekday())

    elapsed_weeks = max(
        0,
        (start - anchor_week_start).days // 7,
    )

    first_cycle = max(
        0,
        elapsed_weeks // interval - 1,
    )

    cycle = first_cycle

    while True:
        week_start = anchor_week_start + timedelta(weeks=cycle * interval)

        if week_start > end:
            break

        for weekday_index in weekday_indexes:
            candidate = week_start + timedelta(days=weekday_index)

            if candidate < anchor:
                continue

            if candidate < start:
                continue

            if candidate > end:
                continue

            _append_date(
                results,
                candidate,
            )

        cycle += 1

    return sorted(set(results))


def _calculate_monthly_dates(
    anchor: date,
    interval: int,
    start: date,
    end: date,
) -> list[date]:
    results: list[date] = []

    elapsed_months = (start.year - anchor.year) * 12 + start.month - anchor.month

    first_cycle = max(
        0,
        elapsed_months // interval - 1,
    )

    cycle = first_cycle

    while True:
        candidate = add_months(
            anchor,
            cycle * interval,
        )

        if candidate > end:
            break

        if candidate >= start:
            _append_date(
                results,
                candidate,
            )

        cycle += 1

    return results


def _calculate_yearly_dates(
    anchor: date,
    interval: int,
    start: date,
    end: date,
) -> list[date]:
    results: list[date] = []

    elapsed_years = max(
        0,
        start.year - anchor.year,
    )

    first_cycle = max(
        0,
        elapsed_years // interval - 1,
    )

    cycle = first_cycle

    while True:
        candidate = add_years(
            anchor,
            cycle * interval,
        )

        if candidate > end:
            break

        if candidate >= start:
            _append_date(
                results,
                candidate,
            )

        cycle += 1

    return results


def calculate_piano_dates(
    piano: ManutenzioneRicorrente,
    *,
    data_fino: date,
    data_dal: date | None = None,
) -> list[date]:
    """
    Calcola le date previste dal piano senza scrivere nel database.
    """
    if piano.data_inizio is None:
        raise GenerazioneEventiError("Il piano non ha una data iniziale.")

    if piano.frequenza_intervallo <= 0:
        raise GenerazioneEventiError("L'intervallo deve essere maggiore di zero.")

    start = max(
        piano.data_inizio,
        data_dal or piano.data_inizio,
    )

    end = data_fino

    if end < start:
        return []

    if piano.frequenza_unita == "giorni":
        return _calculate_daily_dates(
            piano.data_inizio,
            piano.frequenza_intervallo,
            start,
            end,
        )

    if piano.frequenza_unita == "settimane":
        return _calculate_weekly_dates(
            piano,
            start,
            end,
        )

    if piano.frequenza_unita == "mesi":
        return _calculate_monthly_dates(
            piano.data_inizio,
            piano.frequenza_intervallo,
            start,
            end,
        )

    if piano.frequenza_unita == "anni":
        return _calculate_yearly_dates(
            piano.data_inizio,
            piano.frequenza_intervallo,
            start,
            end,
        )

    raise GenerazioneEventiError(
        f"Unità di frequenza non supportata: {piano.frequenza_unita!r}."
    )


def sync_eventi_piano(
    piano: ManutenzioneRicorrente,
    *,
    data_fino: date | None = None,
    data_dal: date | None = None,
    commit: bool = True,
) -> dict[str, int]:
    """
    Genera e riallinea gli eventi del piano.

    La ricorrenza viene identificata tramite data_teorica.
    La data_programmata viene adattata al calendario lavorativo.

    Vengono aggiornati solamente gli eventi ancora PROGRAMMATI.
    Gli eventi completati, saltati o annullati non vengono spostati.
    """
    empty_result = {
        "created": 0,
        "updated": 0,
        "rescheduled": 0,
        "existing": 0,
        "duplicates": 0,
        "deleted": 0,
    }

    horizon = data_fino or default_horizon_date()

    theoretical_dates = (
        calculate_piano_dates(
            piano,
            data_fino=horizon,
            data_dal=data_dal,
        )
        if piano.attiva
        else []
    )

    deleted = 0
    desired_dates = set(theoretical_dates)

    if data_dal is not None:
        future_events = EventoManutenzione.query.filter(
            EventoManutenzione.manutenzione_ricorrente_id == piano.id,
            EventoManutenzione.stato == "PROGRAMMATA",
            EventoManutenzione.data_programmata >= data_dal,
        ).all()

        for evento in future_events:
            if evento.data_teorica not in desired_dates:
                db.session.delete(evento)
                deleted += 1

    if not theoretical_dates:
        empty_result["deleted"] = deleted
        if commit:
            db.session.commit()
        return empty_result

    minimum_date = min(theoretical_dates)
    maximum_date = max(theoretical_dates)

    existing_events = EventoManutenzione.query.filter(
        EventoManutenzione.manutenzione_ricorrente_id == piano.id,
        EventoManutenzione.data_teorica >= minimum_date,
        EventoManutenzione.data_teorica <= maximum_date,
    ).all()

    existing_by_theoretical_date: dict[
        date,
        EventoManutenzione,
    ] = {}

    for evento in existing_events:
        key = evento.data_teorica

        if key in existing_by_theoretical_date:
            raise GenerazioneEventiError(
                "Sono presenti più eventi per lo stesso piano "
                f"e la stessa data teorica: {key.isoformat()}."
            )

        existing_by_theoretical_date[key] = evento

    created = 0
    updated = 0
    rescheduled = 0
    existing = 0
    duplicates = 0

    for theoretical_date in theoretical_dates:
        (
            scheduled_date,
            shift_reason,
        ) = normalizza_data_manutenzione(
            theoretical_date,
            reference_date=today_rome(),
        )

        current_event = existing_by_theoretical_date.get(theoretical_date)

        if current_event is not None:
            existing += 1

            if data_dal is not None and current_event.data_programmata < data_dal:
                continue

            if current_event.stato != "PROGRAMMATA":
                continue

            changed = False

            if current_event.titolo_snapshot != piano.titolo:
                current_event.titolo_snapshot = piano.titolo
                changed = True

            if current_event.descrizione_snapshot != piano.descrizione:
                current_event.descrizione_snapshot = piano.descrizione
                changed = True

            if current_event.data_programmata != scheduled_date:
                current_event.data_programmata = scheduled_date
                rescheduled += 1
                changed = True

            should_be_shifted = scheduled_date != theoretical_date

            if bool(current_event.data_spostata) != should_be_shifted:
                current_event.data_spostata = should_be_shifted
                changed = True

            if current_event.motivo_spostamento != shift_reason:
                current_event.motivo_spostamento = shift_reason
                changed = True

            if changed:
                updated += 1

            continue

        event = EventoManutenzione(
            manutenzione_ricorrente_id=piano.id,
            data_teorica=theoretical_date,
            data_programmata=scheduled_date,
            data_spostata=(scheduled_date != theoretical_date),
            motivo_spostamento=shift_reason,
            stato="PROGRAMMATA",
            titolo_snapshot=piano.titolo,
            descrizione_snapshot=piano.descrizione,
        )

        try:
            with db.session.begin_nested():
                db.session.add(event)
                db.session.flush()

            created += 1

        except IntegrityError:
            duplicates += 1

    if commit:
        db.session.commit()

    return {
        "created": created,
        "updated": updated,
        "rescheduled": rescheduled,
        "existing": existing,
        "duplicates": duplicates,
        "deleted": deleted,
    }


def sync_eventi_macchinario(
    macchinario_id: int | str,
    policy: RbacPolicy,
    *,
    data_fino: date | None = None,
    data_dal: date | None = None,
) -> dict[str, int]:
    macchinario = get_macchinario(
        macchinario_id,
        policy,
    )

    piani = ManutenzioneRicorrente.query.filter(
        ManutenzioneRicorrente.macchinario_id == macchinario.id,
        ManutenzioneRicorrente.attiva.is_(True),
    ).all()

    totals = {
        "created": 0,
        "updated": 0,
        "rescheduled": 0,
        "existing": 0,
        "duplicates": 0,
        "deleted": 0,
        "plans": len(piani),
    }

    try:
        for piano in piani:
            result = sync_eventi_piano(
                piano,
                data_fino=data_fino,
                data_dal=data_dal,
                commit=False,
            )

            for key in (
                "created",
                "updated",
                "rescheduled",
                "existing",
                "duplicates",
                "deleted",
            ):
                totals[key] += result[key]

        db.session.commit()

    except Exception:
        db.session.rollback()
        raise

    return totals


def sync_all_active_plans(
    *,
    data_fino: date | None = None,
    data_dal: date | None = None,
) -> dict[str, int]:
    """Sincronizza i piani attivi come operazione interna senza policy."""
    piani = ManutenzioneRicorrente.query.filter(
        ManutenzioneRicorrente.attiva.is_(True)
    ).all()

    totals = {
        "created": 0,
        "updated": 0,
        "rescheduled": 0,
        "existing": 0,
        "duplicates": 0,
        "deleted": 0,
        "plans": len(piani),
    }
    try:
        for piano in piani:
            result = sync_eventi_piano(
                piano,
                data_fino=data_fino,
                data_dal=data_dal,
                commit=False,
            )

            for key in (
                "created",
                "updated",
                "rescheduled",
                "existing",
                "duplicates",
                "deleted",
            ):
                totals[key] += result[key]

        db.session.commit()

    except Exception:
        db.session.rollback()
        raise

    return totals


def list_eventi_macchinario(
    macchinario_id: int | str,
    policy: RbacPolicy,
    *,
    data_dal: date | None = None,
    data_fino: date | None = None,
) -> list[EventoManutenzione]:
    macchinario = get_macchinario(
        macchinario_id,
        policy,
    )

    query = (
        EventoManutenzione.query.join(
            ManutenzioneRicorrente,
            EventoManutenzione.manutenzione_ricorrente_id == ManutenzioneRicorrente.id,
        )
        .options(joinedload(EventoManutenzione.manutenzione_ricorrente))
        .filter(ManutenzioneRicorrente.macchinario_id == macchinario.id)
    )

    if data_dal is not None:
        query = query.filter(EventoManutenzione.data_programmata >= data_dal)

    if data_fino is not None:
        query = query.filter(EventoManutenzione.data_programmata <= data_fino)

    return query.order_by(
        EventoManutenzione.data_programmata.asc(),
        EventoManutenzione.id.asc(),
    ).all()


def get_stato_visuale_evento(
    evento: EventoManutenzione,
    *,
    reference_date: date | None = None,
) -> str:
    if evento.stato != "PROGRAMMATA":
        return evento.stato

    today = reference_date or today_rome()

    if evento.data_programmata < today:
        return "SCADUTA"

    piano = evento.manutenzione_ricorrente

    preavviso = piano.preavviso_giorni if piano is not None else 0

    if evento.data_programmata <= (today + timedelta(days=preavviso)):
        return "IN_SCADENZA"

    return "PROGRAMMATA"


def serialize_evento_manutenzione(
    evento: EventoManutenzione,
) -> dict[str, Any]:
    piano = evento.manutenzione_ricorrente

    return {
        "id": evento.id,
        "manutenzione_ricorrente_id": (evento.manutenzione_ricorrente_id),
        "macchinario_id": (piano.macchinario_id if piano is not None else None),
        "piano_codice": (piano.codice if piano is not None else None),
        "piano_attivo": (bool(piano.attiva) if piano is not None else False),
        "data_programmata": (evento.data_programmata.isoformat()),
        "stato": evento.stato,
        "stato_visuale": (get_stato_visuale_evento(evento)),
        "titolo": evento.titolo_snapshot,
        "descrizione": (evento.descrizione_snapshot),
        "data_esecuzione": (
            evento.data_esecuzione.isoformat() if evento.data_esecuzione else None
        ),
        "eseguito_da_username": (evento.eseguito_da_username),
        "esito": evento.esito,
        "descrizione_intervento": (evento.descrizione_intervento),
        "note": evento.note,
        "durata_minuti": evento.durata_minuti,
        "registrato_da_public_id": (evento.registrato_da_public_id),
        "registrato_da_username": (evento.registrato_da_username),
        "chiuso": evento.stato != "PROGRAMMATA",
        "data_teorica": (
            evento.data_teorica.isoformat() if evento.data_teorica else None
        ),
        "data_spostata": bool(evento.data_spostata),
        "motivo_spostamento": (evento.motivo_spostamento),
    }


def serialize_eventi_manutenzione(
    eventi: list[EventoManutenzione],
) -> list[dict[str, Any]]:
    return [serialize_evento_manutenzione(evento) for evento in eventi]


class EventoManutenzioneNonTrovatoError(ManutenzioniServiceError):
    """L'evento di manutenzione richiesto non esiste."""


class EventoManutenzioneChiusoError(ManutenzioniServiceError):
    """L'evento è già stato completato, saltato o annullato."""


def _norm_text(value: Any) -> str:
    return str(value or "").strip()


def _norm_optional_text(
    value: Any,
) -> str | None:
    normalized = _norm_text(value)
    return normalized or None


def _assert_can_execute_events(
    policy: RbacPolicy,
) -> None:
    if policy.can("manutenzioni_esegui") or policy.can("manutenzioni_amministrazione"):
        return

    raise PermessoManutenzioniError(
        "Utente non autorizzato a registrare le manutenzioni eseguite."
    )


def _parse_execution_date(
    value: Any,
) -> date:
    if value in (None, ""):
        return today_rome()

    if isinstance(value, datetime):
        parsed = value.date()

    elif isinstance(value, date):
        parsed = value

    else:
        try:
            parsed = date.fromisoformat(_norm_text(value))
        except ValueError as exc:
            raise ManutenzioniServiceError(
                "La data di esecuzione deve avere il formato YYYY-MM-DD."
            ) from exc

    if parsed > today_rome():
        raise ManutenzioniServiceError("La data di esecuzione non può essere futura.")

    return parsed


def _parse_duration_minutes(
    value: Any,
) -> int | None:
    if value in (None, ""):
        return None

    try:
        duration = int(value)
    except (TypeError, ValueError) as exc:
        raise ManutenzioniServiceError(
            "La durata deve essere espressa in minuti interi."
        ) from exc

    if duration < 0:
        raise ManutenzioniServiceError("La durata non può essere negativa.")

    return duration


def _user_snapshot(
    user,
) -> tuple[str | None, str | None]:
    return (
        _norm_optional_text(getattr(user, "public_id", None)),
        _norm_optional_text(getattr(user, "username", None)),
    )


def get_evento_manutenzione(
    evento_id: int | str,
    policy: RbacPolicy,
    *,
    require_execution: bool = False,
) -> EventoManutenzione:
    try:
        normalized_id = int(evento_id)
    except (TypeError, ValueError) as exc:
        raise EventoManutenzioneNonTrovatoError(
            "Identificativo evento non valido."
        ) from exc

    evento = (
        EventoManutenzione.query.options(
            joinedload(EventoManutenzione.manutenzione_ricorrente)
        )
        .filter(EventoManutenzione.id == normalized_id)
        .first()
    )

    if evento is None:
        raise EventoManutenzioneNonTrovatoError("Evento di manutenzione non trovato.")

    piano = evento.manutenzione_ricorrente

    if piano is None:
        raise EventoManutenzioneNonTrovatoError(
            "Il piano associato all'evento non esiste."
        )

    # Applica lo scope reparto già previsto dal service macchinari.
    get_macchinario(
        piano.macchinario_id,
        policy,
    )

    if require_execution:
        _assert_can_execute_events(policy)

    return evento


def _assert_evento_programmato(
    evento: EventoManutenzione,
) -> None:
    if evento.stato != "PROGRAMMATA":
        raise EventoManutenzioneChiusoError(
            f"L'evento è già stato chiuso con stato '{evento.stato}'."
        )


def completa_evento_manutenzione(
    evento_id: int | str,
    data: dict[str, Any],
    policy: RbacPolicy,
    *,
    user,
) -> EventoManutenzione:
    evento = get_evento_manutenzione(
        evento_id,
        policy,
        require_execution=True,
    )

    _assert_evento_programmato(evento)

    esito = _norm_text(data.get("esito")).upper()

    if esito not in ESITI_EVENTO_MANUTENZIONE:
        raise ManutenzioniServiceError(
            "Esito non valido. Valori ammessi: "
            + ", ".join(sorted(ESITI_EVENTO_MANUTENZIONE))
            + "."
        )

    descrizione_intervento = _norm_optional_text(data.get("descrizione_intervento"))

    if (
        esito
        in {
            "ANOMALIA",
            "INTERVENTO_RICHIESTO",
        }
        and not descrizione_intervento
    ):
        raise ManutenzioniServiceError(
            "La descrizione dell'intervento è obbligatoria "
            "in caso di anomalia o intervento richiesto."
        )

    execution_date = _parse_execution_date(data.get("data_esecuzione"))

    duration = _parse_duration_minutes(data.get("durata_minuti"))

    public_id, username = _user_snapshot(user)

    evento.data_esecuzione = execution_date

    evento.eseguito_da_public_id = public_id
    evento.eseguito_da_username = username

    evento.registrato_da_public_id = public_id
    evento.registrato_da_username = username

    evento.esito = esito
    evento.descrizione_intervento = descrizione_intervento
    evento.note = _norm_optional_text(data.get("note"))
    evento.durata_minuti = duration

    # Impostato dopo gli altri campi per rispettare il vincolo:
    # COMPLETATA richiede data_esecuzione.
    evento.stato = "COMPLETATA"

    db.session.commit()

    return evento


def chiudi_evento_senza_esecuzione(
    evento_id: int | str,
    data: dict[str, Any],
    policy: RbacPolicy,
    *,
    user,
    stato: str,
) -> EventoManutenzione:
    evento = get_evento_manutenzione(
        evento_id,
        policy,
        require_execution=True,
    )

    _assert_evento_programmato(evento)

    if stato not in {
        "SALTATA",
        "ANNULLATA",
    }:
        raise ManutenzioniServiceError("Stato di chiusura non valido.")

    note = _norm_required_event_note(
        data.get("note"),
        stato,
    )

    public_id, username = _user_snapshot(user)

    evento.registrato_da_public_id = public_id
    evento.registrato_da_username = username

    evento.data_esecuzione = None
    evento.eseguito_da_public_id = None
    evento.eseguito_da_username = None
    evento.esito = None
    evento.descrizione_intervento = None
    evento.durata_minuti = None

    evento.note = note
    evento.stato = stato

    db.session.commit()

    return evento


def _norm_required_event_note(
    value: Any,
    stato: str,
) -> str:
    normalized = _norm_text(value)

    if not normalized:
        label = "salto" if stato == "SALTATA" else "annullamento"

        raise ManutenzioniServiceError(f"Indicare il motivo del {label}.")

    return normalized


def gestisci_evento_manutenzione(
    evento_id: int | str,
    data: dict[str, Any],
    policy: RbacPolicy,
    *,
    user,
) -> EventoManutenzione:
    action = _norm_text(data.get("action")).lower()

    if action == "completa":
        return completa_evento_manutenzione(
            evento_id,
            data,
            policy,
            user=user,
        )

    if action == "salta":
        return chiudi_evento_senza_esecuzione(
            evento_id,
            data,
            policy,
            user=user,
            stato="SALTATA",
        )

    if action == "annulla":
        return chiudi_evento_senza_esecuzione(
            evento_id,
            data,
            policy,
            user=user,
            stato="ANNULLATA",
        )

    raise ManutenzioniServiceError("Azione evento non valida.")


def build_scadenziario_manutenzioni(
    policy: RbacPolicy,
    *,
    reparto_codice: str | None = None,
    data_dal: date | None = None,
    data_fino: date | None = None,
    stato: str | None = None,
    search: str | None = None,
) -> dict[str, Any]:
    """
    Costruisce lo scadenziario degli eventi relativi ai macchinari
    visibili dall'utente.

    Gli eventi dei macchinari inattivi vengono mantenuti visibili,
    perché possono rappresentare dati storici importanti.
    """
    normalized_reparto = str(reparto_codice or "").strip()

    normalized_search = str(search or "").strip().lower()

    normalized_stato = str(stato or "").strip().upper()

    macchinari = list_macchinari(
        policy,
        reparto_codice=(normalized_reparto or None),
        include_inactive=True,
    )

    macchinari_rows = serialize_macchinari(macchinari)

    rows: list[dict[str, Any]] = []

    for macchinario in macchinari_rows:
        eventi = list_eventi_macchinario(
            macchinario["id"],
            policy,
            data_dal=data_dal,
            data_fino=data_fino,
        )

        for evento in eventi:
            row = serialize_evento_manutenzione(evento)

            row.update(
                {
                    "macchinario_codice": (macchinario.get("codice")),
                    "macchinario_descrizione": (macchinario.get("descrizione")),
                    "macchinario_attivo": bool(macchinario.get("attivo")),
                    "reparto_codice": (macchinario.get("reparto_codice")),
                    "reparto_descrizione": (macchinario.get("reparto_descrizione")),
                    "ubicazione": (macchinario.get("ubicazione")),
                }
            )

            if normalized_search:
                searchable_values = [
                    row.get("macchinario_codice"),
                    row.get("macchinario_descrizione"),
                    row.get("piano_codice"),
                    row.get("titolo"),
                    row.get("descrizione"),
                    row.get("esito"),
                    row.get("eseguito_da_username"),
                    row.get("registrato_da_username"),
                    row.get("ubicazione"),
                ]

                haystack = " ".join(
                    str(value or "") for value in searchable_values
                ).lower()

                if normalized_search not in haystack:
                    continue

            rows.append(row)

    summary = {
        "totale": len(rows),
        "aperte": sum(1 for row in rows if row["stato"] == "PROGRAMMATA"),
        "scadute": sum(1 for row in rows if row["stato_visuale"] == "SCADUTA"),
        "in_scadenza": sum(1 for row in rows if row["stato_visuale"] == "IN_SCADENZA"),
        "programmate": sum(1 for row in rows if row["stato_visuale"] == "PROGRAMMATA"),
        "completate": sum(1 for row in rows if row["stato"] == "COMPLETATA"),
        "saltate": sum(1 for row in rows if row["stato"] == "SALTATA"),
        "annullate": sum(1 for row in rows if row["stato"] == "ANNULLATA"),
    }

    if normalized_stato:
        if normalized_stato == "APERTI":
            rows = [row for row in rows if row["stato"] == "PROGRAMMATA"]

        elif normalized_stato in {
            "SCADUTA",
            "IN_SCADENZA",
            "PROGRAMMATA",
        }:
            rows = [row for row in rows if (row["stato_visuale"] == normalized_stato)]

        elif normalized_stato in {
            "COMPLETATA",
            "SALTATA",
            "ANNULLATA",
        }:
            rows = [row for row in rows if row["stato"] == normalized_stato]

    rows.sort(
        key=lambda row: (
            row.get("data_programmata") or "",
            row.get("macchinario_codice") or "",
            row.get("titolo") or "",
            row.get("id") or 0,
        )
    )

    return {
        "rows": rows,
        "summary": summary,
    }
