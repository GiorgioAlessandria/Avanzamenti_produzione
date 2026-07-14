from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from app_odp.manutenzioni_models import (
    ManutenzioneGiornoNonLavorativo,
)


FESTIVITA_NAZIONALI_FISSE = {
    (1, 1): "Capodanno",
    (1, 6): "Epifania",
    (4, 25): "Festa della Liberazione",
    (5, 1): "Festa dei Lavoratori",
    (6, 2): "Festa della Repubblica",
    (8, 15): "Ferragosto",
    (11, 1): "Tutti i Santi",
    (12, 8): "Immacolata Concezione",
    (12, 25): "Natale",
    (12, 26): "Santo Stefano",
}


def _pasqua(year: int) -> date:
    """
    Calcolo della domenica di Pasqua secondo
    l'algoritmo gregoriano di Meeus/Jones/Butcher.
    """
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3

    h = (19 * a + b - d - g + 15) % 30

    i = c // 4
    k = c % 4

    l = (32 + 2 * e + 2 * i - h - k) % 7

    m = (a + 11 * h + 22 * l) // 451

    month = (h + l - 7 * m + 114) // 31

    day = ((h + l - 7 * m + 114) % 31) + 1

    return date(
        year,
        month,
        day,
    )


def festivita_nazionali(
    year: int,
) -> dict[date, str]:
    holidays = {
        date(year, month, day): description
        for (
            month,
            day,
        ), description in FESTIVITA_NAZIONALI_FISSE.items()
    }

    pasqua = _pasqua(year)

    holidays[pasqua + timedelta(days=1)] = "Lunedì dell'Angelo"

    return holidays


def _giorni_non_lavorativi_personalizzati(
    giorno: date,
) -> list[ManutenzioneGiornoNonLavorativo]:
    rows = ManutenzioneGiornoNonLavorativo.query.filter(
        ManutenzioneGiornoNonLavorativo.attivo.is_(True)
    ).all()

    matches = []

    for row in rows:
        if row.data is None:
            continue

        if row.ricorrente_annuale:
            if row.data.month == giorno.month and row.data.day == giorno.day:
                matches.append(row)

        elif row.data == giorno:
            matches.append(row)

    return matches


def motivo_non_lavorativo(
    giorno: date,
) -> str | None:
    if giorno.weekday() == 5:
        return "Sabato"

    if giorno.weekday() == 6:
        return "Domenica"

    national_holidays = festivita_nazionali(giorno.year)

    national_description = national_holidays.get(giorno)

    if national_description:
        return national_description

    custom_holidays = _giorni_non_lavorativi_personalizzati(giorno)

    if custom_holidays:
        descriptions = [row.descrizione for row in custom_holidays if row.descrizione]

        return ", ".join(descriptions) or "Giorno non lavorativo"

    return None


def is_giorno_lavorativo(
    giorno: date,
) -> bool:
    return motivo_non_lavorativo(giorno) is None


def giorno_lavorativo_precedente(
    giorno: date,
    *,
    includi_giorno: bool = True,
) -> date:
    candidate = giorno

    if not includi_giorno:
        candidate -= timedelta(days=1)

    for _ in range(370):
        if is_giorno_lavorativo(candidate):
            return candidate

        candidate -= timedelta(days=1)

    raise RuntimeError("Impossibile trovare un giorno lavorativo precedente.")


def giorno_lavorativo_successivo(
    giorno: date,
    *,
    includi_giorno: bool = True,
) -> date:
    candidate = giorno

    if not includi_giorno:
        candidate += timedelta(days=1)

    for _ in range(370):
        if is_giorno_lavorativo(candidate):
            return candidate

        candidate += timedelta(days=1)

    raise RuntimeError("Impossibile trovare un giorno lavorativo successivo.")


def normalizza_data_manutenzione(
    data_teorica: date,
    *,
    reference_date: date | None = None,
) -> tuple[date, str | None]:
    motivo = motivo_non_lavorativo(data_teorica)

    if motivo is None:
        return data_teorica, None

    data_precedente = giorno_lavorativo_precedente(
        data_teorica,
        includi_giorno=False,
    )

    reference = reference_date or date.today()
    if data_precedente > reference:
        return data_precedente, motivo

    data_successiva = giorno_lavorativo_successivo(
        data_teorica,
        includi_giorno=False,
    )

    return data_successiva, motivo


def aggiungi_giorni_lavorativi(
    giorno: date,
    numero_giorni: int,
) -> date:
    try:
        remaining = int(numero_giorni)
    except (TypeError, ValueError) as exc:
        raise ValueError("Il numero di giorni deve essere intero.") from exc

    if remaining == 0:
        return giorno_lavorativo_precedente(giorno)

    direction = 1 if remaining > 0 else -1
    remaining = abs(remaining)

    candidate = giorno

    while remaining > 0:
        candidate += timedelta(days=direction)

        if is_giorno_lavorativo(candidate):
            remaining -= 1

    return candidate

def _add_non_working_day(
    result: dict[date, dict[str, Any]],
    *,
    giorno: date,
    descrizione: str,
    tipo: str,
    ricorrente_annuale: bool = False,
) -> None:
    normalized_description = str(
        descrizione or "Giorno non lavorativo"
    ).strip()

    entry = result.setdefault(
        giorno,
        {
            "data": giorno.isoformat(),
            "descrizioni": [],
            "tipi": [],
            "ricorrente_annuale": False,
        },
    )

    if normalized_description not in entry["descrizioni"]:
        entry["descrizioni"].append(
            normalized_description
        )

    if tipo not in entry["tipi"]:
        entry["tipi"].append(tipo)

    entry["ricorrente_annuale"] = (
        entry["ricorrente_annuale"]
        or bool(ricorrente_annuale)
    )


def giorni_non_lavorativi_nel_periodo(
    data_dal: date,
    data_fino: date,
) -> list[dict[str, Any]]:
    """
    Restituisce festività nazionali e chiusure personalizzate
    comprese nell'intervallo indicato.

    Sabato e domenica non vengono restituiti come singoli record:
    il calendario li evidenzia direttamente tramite il giorno
    della settimana.
    """
    if not isinstance(data_dal, date):
        raise ValueError(
            "data_dal deve essere un oggetto datetime.date."
        )

    if not isinstance(data_fino, date):
        raise ValueError(
            "data_fino deve essere un oggetto datetime.date."
        )

    if data_fino < data_dal:
        data_dal, data_fino = data_fino, data_dal

    result: dict[date, dict[str, Any]] = {}

    for year in range(
        data_dal.year,
        data_fino.year + 1,
    ):
        for giorno, descrizione in (
            festivita_nazionali(year).items()
        ):
            if data_dal <= giorno <= data_fino:
                _add_non_working_day(
                    result,
                    giorno=giorno,
                    descrizione=descrizione,
                    tipo="FESTIVITA_NAZIONALE",
                )

    custom_rows = (
        ManutenzioneGiornoNonLavorativo.query
        .filter(
            ManutenzioneGiornoNonLavorativo.attivo.is_(
                True
            )
        )
        .all()
    )

    for row in custom_rows:
        if row.data is None:
            continue

        if row.ricorrente_annuale:
            for year in range(
                data_dal.year,
                data_fino.year + 1,
            ):
                try:
                    giorno = date(
                        year,
                        row.data.month,
                        row.data.day,
                    )
                except ValueError:
                    # Gestisce, per esempio, il 29 febbraio
                    # negli anni non bisestili.
                    continue

                if data_dal <= giorno <= data_fino:
                    _add_non_working_day(
                        result,
                        giorno=giorno,
                        descrizione=row.descrizione,
                        tipo=(
                            row.tipo
                            or "CHIUSURA_AZIENDALE"
                        ),
                        ricorrente_annuale=True,
                    )

            continue

        if data_dal <= row.data <= data_fino:
            _add_non_working_day(
                result,
                giorno=row.data,
                descrizione=row.descrizione,
                tipo=(
                    row.tipo
                    or "CHIUSURA_AZIENDALE"
                ),
                ricorrente_annuale=False,
            )

    rows = []

    for giorno in sorted(result):
        entry = result[giorno]

        rows.append(
            {
                "data": entry["data"],
                "descrizione": ", ".join(
                    entry["descrizioni"]
                ),
                "descrizioni": entry["descrizioni"],
                "tipi": entry["tipi"],
                "ricorrente_annuale": bool(
                    entry["ricorrente_annuale"]
                ),
            }
        )

    return rows

