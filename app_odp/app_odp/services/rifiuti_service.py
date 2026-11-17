from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from io import BytesIO
import re
from typing import Any
from zoneinfo import ZoneInfo

from openpyxl import Workbook
from openpyxl.styles import Font
from sqlalchemy import update

from app_odp.models import db
from app_odp.rifiuti_models import (
    RifiutiCarico,
    RifiutiCer,
)


ROME_TIMEZONE = ZoneInfo("Europe/Rome")
PESO_QUANTUM = Decimal("0.001")
PESO_MASSIMO_KG = Decimal("999999999.999")
PESO_PATTERN = re.compile(r"^\d+(?:\.\d{1,3})?$")
EXPORT_HEADERS = (
    "ID",
    "Data caricamento",
    "Codice CER",
    "Descrizione CER",
    "Peso kg",
    "Stato",
    "Caricato da",
    "Data smaltimento",
    "Smaltito da",
    "Note",
)
STOCK_EXPORT_HEADERS = (
    "Codice CER",
    "Descrizione CER",
    "Peso totale kg",
)


class RifiutiServiceError(ValueError):
    pass


class CodiceCerNonValidoError(RifiutiServiceError):
    pass


class PesoRifiutoNonValidoError(RifiutiServiceError):
    pass


class CaricoRifiutoNonValidoError(RifiutiServiceError):
    pass


def _now_rome_iso() -> str:
    return datetime.now(
        ROME_TIMEZONE
    ).isoformat(timespec="seconds")


def _parse_peso_kg(value: Any) -> Decimal:
    """
    Accetta sia il separatore decimale italiano sia quello con il punto.
    Il valore viene memorizzato con tre cifre decimali.
    """
    raw = str(value or "").strip().replace(",", ".")

    if not raw:
        raise PesoRifiutoNonValidoError(
            "Inserire il peso del materiale."
        )

    if not PESO_PATTERN.fullmatch(raw):
        raise PesoRifiutoNonValidoError(
            "Il peso deve essere un numero con massimo tre decimali."
        )

    try:
        peso = Decimal(raw)
    except InvalidOperation as exc:
        raise PesoRifiutoNonValidoError(
            "Il peso deve essere un numero valido."
        ) from exc

    if not peso.is_finite():
        raise PesoRifiutoNonValidoError(
            "Il peso deve essere un numero finito."
        )

    if peso <= 0:
        raise PesoRifiutoNonValidoError(
            "Il peso deve essere maggiore di zero."
        )

    if peso > PESO_MASSIMO_KG:
        raise PesoRifiutoNonValidoError(
            "Il peso inserito supera il limite consentito."
        )

    return peso.quantize(
        PESO_QUANTUM,
        rounding=ROUND_HALF_UP,
    )


def _normalize_note(value: Any) -> str | None:
    note = str(value or "").strip()
    return note or None


def _actor_snapshot(user) -> tuple[int | None, str]:
    user_id = getattr(user, "id", None)

    username = str(
        getattr(user, "username", "")
        or getattr(user, "nome", "")
        or getattr(user, "name", "")
        or ""
    ).strip()

    if not username:
        username = "Operatore sconosciuto"

    return user_id, username


def list_codici_cer_attivi() -> list[RifiutiCer]:
    return (
        RifiutiCer.query
        .filter(
            RifiutiCer.attivo.is_(True)
        )
        .order_by(
            RifiutiCer.codice.asc(),
            RifiutiCer.descrizione.asc(),
        )
        .all()
    )


def get_codice_cer_attivo(
    codice_cer_id: int | str | None,
) -> RifiutiCer:
    try:
        normalized_id = int(codice_cer_id)
    except (TypeError, ValueError) as exc:
        raise CodiceCerNonValidoError(
            "Selezionare un codice CER valido."
        ) from exc

    codice = (
        RifiutiCer.query
        .filter(
            RifiutiCer.id == normalized_id,
            RifiutiCer.attivo.is_(True),
        )
        .first()
    )

    if codice is None:
        raise CodiceCerNonValidoError(
            "Il codice CER selezionato non è disponibile."
        )

    return codice

def _normalize_cer_text(value: Any, field_name: str) -> str:
    normalized = " ".join(str(value or "").split())
    if not normalized:
        raise CodiceCerNonValidoError(f"{field_name} obbligatoria.")
    return normalized


def _get_codice_cer_attivo(codice_cer_id: Any) -> RifiutiCer:
    try:
        normalized_id = int(codice_cer_id)
    except (TypeError, ValueError) as exc:
        raise CodiceCerNonValidoError("Codice CER non valido.") from exc

    codice = db.session.get(RifiutiCer, normalized_id)
    if codice is None or not codice.attivo:
        raise CodiceCerNonValidoError("Codice CER non disponibile.")
    return codice


def create_codice_cer(
    *,
    codice: Any,
    descrizione: Any,
    commit: bool = True,
) -> RifiutiCer:
    normalized_code = _normalize_cer_text(codice, "Codice CER")
    normalized_description = _normalize_cer_text(descrizione, "Descrizione")
    existing = RifiutiCer.query.filter(
        RifiutiCer.codice == normalized_code,
        RifiutiCer.descrizione == normalized_description,
    ).first()

    if existing is not None and existing.attivo:
        raise CodiceCerNonValidoError(
            "La combinazione codice CER e descrizione è già presente."
        )

    if existing is None:
        existing = RifiutiCer(
            codice=normalized_code,
            descrizione=normalized_description,
            attivo=True,
        )
        db.session.add(existing)
    else:
        existing.attivo = True
        existing.aggiornato_il = _now_rome_iso()

    if commit:
        db.session.commit()
    return existing


def update_codice_cer(
    *,
    codice_cer_id: Any,
    codice: Any,
    descrizione: Any,
    commit: bool = True,
) -> RifiutiCer:
    cer = _get_codice_cer_attivo(codice_cer_id)
    normalized_code = _normalize_cer_text(codice, "Codice CER")
    normalized_description = _normalize_cer_text(descrizione, "Descrizione")
    duplicate = (
        RifiutiCer.query
        .filter(
            RifiutiCer.codice == normalized_code,
            RifiutiCer.descrizione == normalized_description,
            RifiutiCer.id != cer.id,
        )
        .first()
    )
    if duplicate is not None:
        raise CodiceCerNonValidoError(
            "La combinazione codice CER e descrizione è già presente."
        )

    cer.codice = normalized_code
    cer.descrizione = normalized_description
    cer.aggiornato_il = _now_rome_iso()

    if commit:
        db.session.commit()
    return cer


def deactivate_codice_cer(
    codice_cer_id: Any,
    *,
    commit: bool = True,
) -> RifiutiCer:
    cer = _get_codice_cer_attivo(codice_cer_id)
    cer.attivo = False
    cer.aggiornato_il = _now_rome_iso()

    if commit:
        db.session.commit()
    return cer


def create_carico_rifiuto(
    *,
    codice_cer_id: int | str | None,
    peso_kg: Any,
    note: Any = None,
    user,
    commit: bool = True,
) -> RifiutiCarico:
    codice = get_codice_cer_attivo(
        codice_cer_id
    )
    peso = _parse_peso_kg(
        peso_kg
    )
    user_id, username = _actor_snapshot(
        user
    )

    carico = RifiutiCarico(
        cer_id=codice.id,
        peso_kg=peso,
        stato="PRESENTE",
        caricato_il=_now_rome_iso(),
        caricato_da_id=user_id,
        caricato_da_nome=username,
        note=_normalize_note(note),
    )

    db.session.add(carico)

    if commit:
        db.session.commit()

    return carico


def list_carichi_presenti() -> list[RifiutiCarico]:
    return (
        RifiutiCarico.query
        .filter(
            RifiutiCarico.stato == "PRESENTE"
        )
        .order_by(
            RifiutiCarico.caricato_il.desc(),
            RifiutiCarico.id.desc(),
        )
        .all()
    )


def list_carichi_smaltiti(limit: int | None = None) -> list[RifiutiCarico]:
    query = (
        RifiutiCarico.query
        .filter(RifiutiCarico.stato == "SMALTITO")
        .order_by(
            RifiutiCarico.smaltito_il.desc(),
            RifiutiCarico.id.desc(),
        )
    )
    if limit is not None:
        query = query.limit(limit)
    return query.all()


def list_carichi_tutti() -> list[RifiutiCarico]:
    return (
        RifiutiCarico.query
        .order_by(
            RifiutiCarico.caricato_il.desc(),
            RifiutiCarico.id.desc(),
        )
        .all()
    )


def format_peso_kg(value: Any) -> str:
    peso = Decimal(
        str(value or 0)
    ).quantize(
        Decimal("0.1"),
        rounding=ROUND_HALF_UP,
    )

    return format(
        peso,
        ".1f",
    ).replace(".", ",")


def format_datetime_it(value: Any) -> str:
    raw = str(value or "").strip()

    if not raw:
        return ""

    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return raw

    return parsed.strftime(
        "%d/%m/%Y %H:%M"
    )


def serialize_carico(
    carico: RifiutiCarico,
) -> dict[str, Any]:
    return {
        "id": carico.id,
        "codice_cer": carico.cer.codice,
        "descrizione_cer": carico.cer.descrizione,
        "peso_kg": format_peso_kg(
            carico.peso_kg
        ),
        "peso_kg_raw": str(
            carico.peso_kg
        ),
        "stato": carico.stato,
        "caricato_il": format_datetime_it(
            carico.caricato_il
        ),
        "caricato_da": carico.caricato_da_nome,
        "smaltito_il": format_datetime_it(carico.smaltito_il),
        "smaltito_da": carico.smaltito_da_nome or "",
        "note": carico.note or "",
    }


def build_carichi_presenti_rows() -> list[dict[str, Any]]:
    return [
        serialize_carico(carico)
        for carico in list_carichi_presenti()
    ]


def build_carichi_smaltiti_rows(limit: int = 20) -> list[dict[str, Any]]:
    return [
        serialize_carico(carico)
        for carico in list_carichi_smaltiti(limit=limit)
    ]


def _normalize_carico_ids(values: Any) -> list[int]:
    raw_values = values if isinstance(values, (list, tuple, set)) else [values]
    normalized = []

    for value in raw_values:
        try:
            carico_id = int(value)
        except (TypeError, ValueError) as exc:
            raise CaricoRifiutoNonValidoError(
                "La selezione contiene un carico non valido."
            ) from exc

        if carico_id <= 0:
            raise CaricoRifiutoNonValidoError(
                "La selezione contiene un carico non valido."
            )

        if carico_id not in normalized:
            normalized.append(carico_id)

    if not normalized:
        raise CaricoRifiutoNonValidoError(
            "Selezionare almeno un carico da smaltire."
        )

    return normalized


def smaltisci_carichi(
    *,
    carico_ids: Any,
    user,
    commit: bool = True,
) -> list[RifiutiCarico]:
    ids = _normalize_carico_ids(carico_ids)
    carichi = RifiutiCarico.query.filter(RifiutiCarico.id.in_(ids)).all()
    by_id = {carico.id: carico for carico in carichi}

    if set(by_id) != set(ids):
        raise CaricoRifiutoNonValidoError(
            "Uno o più carichi selezionati non esistono."
        )

    if any(carico.stato != "PRESENTE" for carico in carichi):
        raise CaricoRifiutoNonValidoError(
            "Uno o più carichi risultano già smaltiti."
        )

    user_id, username = _actor_snapshot(user)
    smaltito_il = _now_rome_iso()

    result = db.session.execute(
        update(RifiutiCarico)
        .where(
            RifiutiCarico.id.in_(ids),
            RifiutiCarico.stato == "PRESENTE",
        )
        .values(
            stato="SMALTITO",
            smaltito_il=smaltito_il,
            smaltito_da_id=user_id,
            smaltito_da_nome=username,
        )
        .execution_options(synchronize_session="fetch")
    )

    if result.rowcount != len(ids):
        raise CaricoRifiutoNonValidoError(
            "Uno o più carichi sono stati smaltiti da un altro operatore."
        )

    if commit:
        db.session.commit()

    return [by_id[carico_id] for carico_id in ids]


def delete_carico_rifiuto(
    carico_id: Any,
    *,
    commit: bool = True,
) -> RifiutiCarico:
    try:
        normalized_id = int(carico_id)
    except (TypeError, ValueError) as exc:
        raise CaricoRifiutoNonValidoError("Carico non valido.") from exc

    carico = db.session.get(RifiutiCarico, normalized_id)
    if carico is None:
        raise CaricoRifiutoNonValidoError("Carico non trovato.")
    if carico.stato != "PRESENTE":
        raise CaricoRifiutoNonValidoError(
            "È possibile cancellare solo materiale ancora presente nello stock."
        )

    db.session.delete(carico)
    if commit:
        db.session.commit()
    return carico


def build_rifiuti_export(carichi: list[RifiutiCarico]) -> BytesIO:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Rifiuti"
    sheet.append(EXPORT_HEADERS)

    for cell in sheet[1]:
        cell.font = Font(bold=True)

    for carico in carichi:
        sheet.append(
            (
                carico.id,
                format_datetime_it(carico.caricato_il),
                carico.cer.codice,
                carico.cer.descrizione,
                float(carico.peso_kg),
                carico.stato,
                carico.caricato_da_nome,
                format_datetime_it(carico.smaltito_il),
                carico.smaltito_da_nome or "",
                carico.note or "",
            )
        )

    sheet.freeze_panes = "A2"
    sheet.auto_filter.ref = sheet.dimensions
    sheet.column_dimensions["B"].width = 20
    sheet.column_dimensions["C"].width = 16
    sheet.column_dimensions["D"].width = 40
    sheet.column_dimensions["E"].width = 14
    sheet.column_dimensions["G"].width = 24
    sheet.column_dimensions["H"].width = 20
    sheet.column_dimensions["I"].width = 24
    sheet.column_dimensions["J"].width = 40

    output = BytesIO()
    workbook.save(output)
    output.seek(0)
    return output


def build_rifiuti_stock_export(carichi: list[RifiutiCarico]) -> BytesIO:
    totals: dict[tuple[str, str], Decimal] = {}
    for carico in carichi:
        key = (carico.cer.codice, carico.cer.descrizione)
        totals[key] = (
            totals.get(key, Decimal("0"))
            + Decimal(str(carico.peso_kg))
        )

    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Stock rifiuti"
    sheet.append(STOCK_EXPORT_HEADERS)

    for cell in sheet[1]:
        cell.font = Font(bold=True)

    for (codice, descrizione), totale in sorted(totals.items()):
        sheet.append((codice, descrizione, float(totale)))

    sheet.freeze_panes = "A2"
    sheet.auto_filter.ref = sheet.dimensions
    sheet.column_dimensions["A"].width = 16
    sheet.column_dimensions["B"].width = 40
    sheet.column_dimensions["C"].width = 18

    output = BytesIO()
    workbook.save(output)
    output.seek(0)
    return output


def calculate_totale_presente(
    carichi: list[RifiutiCarico] | None = None,
) -> Decimal:
    rows = (
        carichi
        if carichi is not None
        else list_carichi_presenti()
    )

    totale = sum(
        (
            Decimal(str(row.peso_kg))
            for row in rows
        ),
        Decimal("0"),
    )

    return totale.quantize(
        PESO_QUANTUM,
        rounding=ROUND_HALF_UP,
    )
