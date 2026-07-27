from __future__ import annotations

import calendar
import json
from datetime import date, datetime
from pathlib import Path
from uuid import uuid4
from zoneinfo import ZoneInfo

from flask import current_app
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from werkzeug.utils import secure_filename

from app_odp.models import Reparti, db
from app_odp.tarature_models import (
    EventoTaratura,
    SpedizioneTaratura,
    SpedizioneTaraturaStrumento,
    StrumentoMisura,
    TaraturaLog,
    TipologiaStrumento,
)


MAX_CERTIFICATO_PDF_BYTES = 20 * 1024 * 1024


def _text(value, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field} è obbligatorio.")
    return normalized


def _optional_text(value) -> str | None:
    normalized = str(value or "").strip()
    return normalized or None


def _numero_seriale(value, codice_interno) -> str:
    normalized = _optional_text(value)
    if normalized in {None, "-"}:
        return f"__NO_SERIAL__:{_text(codice_interno, 'codice interno').upper()}"
    return normalized.upper()


def _read_certificato_pdf(file_storage) -> tuple[str, bytes]:
    if file_storage is None or not str(file_storage.filename or "").strip():
        raise ValueError("Il certificato PDF è obbligatorio.")

    filename = secure_filename(file_storage.filename)
    if not filename or not filename.lower().endswith(".pdf"):
        raise ValueError("Il certificato deve essere un file PDF.")

    payload = file_storage.read(MAX_CERTIFICATO_PDF_BYTES + 1)
    if len(payload) > MAX_CERTIFICATO_PDF_BYTES:
        raise ValueError("Il certificato PDF non può superare 20 MB.")
    if not payload.startswith(b"%PDF-"):
        raise ValueError("Il file caricato non è un PDF valido.")
    return filename, payload


def _save_certificato_pdf(directory, payload: bytes) -> str:
    base_dir = Path(directory)
    filename = f"{uuid4().hex}.pdf"
    target = base_dir / filename
    try:
        base_dir.mkdir(parents=True, exist_ok=True)
        target.write_bytes(payload)
    except OSError as exc:
        try:
            target.unlink(missing_ok=True)
        except OSError:
            pass
        raise ValueError("Impossibile salvare il certificato PDF.") from exc
    return filename


def _checked(value) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "on", "yes"}


def _richiede_taratura_esterna(strumento: StrumentoMisura) -> bool:
    return (
        strumento.tipologia.taratura_esterna_attiva is not False
        and not strumento.solo_verifica_interna
    )


def _positive_int(value, field: str, *, optional: bool = False) -> int | None:
    if optional and (value is None or str(value).strip() == ""):
        return None
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} deve essere un numero intero.") from exc
    if normalized <= 0:
        raise ValueError(f"{field} deve essere maggiore di zero.")
    return normalized


def parse_date(value, field: str) -> date:
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value or "").strip())
    except ValueError as exc:
        raise ValueError(f"{field} non è una data valida.") from exc


def add_months(day: date, months: int) -> date:
    """Somma mesi di calendario mantenendo, quando possibile, il giorno."""
    zero_based = day.month - 1 + months
    year = day.year + zero_based // 12
    month = zero_based % 12 + 1
    return date(year, month, min(day.day, calendar.monthrange(year, month)[1]))


def today_rome() -> date:
    return datetime.now(ZoneInfo("Europe/Rome")).date()


def _actor(user) -> tuple[str | None, str | None]:
    return (
        getattr(user, "public_id", None),
        getattr(user, "username", None) or getattr(user, "name", None),
    )


def _log(evento: str, entita: str, entita_id: int | None, data, user) -> None:
    public_id, username = _actor(user)
    db.session.add(
        TaraturaLog(
            evento=evento,
            entita=entita,
            entita_id=entita_id,
            dettaglio=json.dumps(data, ensure_ascii=False, default=str),
            utente_public_id=public_id,
            utente_username=username,
        )
    )


def _commit() -> None:
    try:
        db.session.commit()
    except IntegrityError as exc:
        db.session.rollback()
        raise ValueError("Codice, numero seriale o nome già presente.") from exc


def _get(model, row_id: int, label: str):
    row = db.session.get(model, row_id)
    if row is None:
        raise ValueError(f"{label} non trovato.")
    return row


def _validate_reparto(codice: str) -> str:
    normalized = _text(codice, "reparto").upper()
    exists = Reparti.query.filter(
        func.upper(func.trim(Reparti.Codice)) == normalized
    ).first()
    if exists is None:
        raise ValueError("Reparto non valido.")
    return str(exists.Codice).strip()


def list_reparti() -> list[dict]:
    return [
        {
            "codice": str(row.Codice or "").strip(),
            "descrizione": str(row.Descrizione or "").strip(),
        }
        for row in Reparti.query.order_by(
            func.lower(func.coalesce(Reparti.Descrizione, Reparti.Codice))
        ).all()
    ]


def due_for_instrument(strumento: StrumentoMisura, today: date | None = None) -> dict:
    today = today or today_rome()
    conformi = [row for row in strumento.eventi if row.esito == "CONFORME"]
    if not conformi:
        tipo = "ESTERNA" if _richiede_taratura_esterna(strumento) else "INTERNA"
        return {"tipo": tipo, "data": today, "classe": "SCADUTA", "giorni": 0}

    esterne = [
        row.data_evento
        for row in conformi
        if row.tipo in {"INIZIALE", "ESTERNA"}
    ]
    if _richiede_taratura_esterna(strumento) and not esterne:
        return {"tipo": "ESTERNA", "data": today, "classe": "SCADUTA"}

    scadenza_esterna = None
    if _richiede_taratura_esterna(strumento):
        scadenza_esterna = add_months(
            max(esterne),
            strumento.tipologia.frequenza_esterna_mesi,
        )
    scadenza_interna = None
    if strumento.tipologia.frequenza_interna_mesi:
        ultima_verifica = max(row.data_evento for row in conformi)
        scadenza_interna = add_months(
            ultima_verifica,
            strumento.tipologia.frequenza_interna_mesi,
        )

    if not _richiede_taratura_esterna(strumento):
        tipo, prossima = "INTERNA", scadenza_interna
    # In caso di coincidenza o sorpasso, la taratura esterna prevale.
    elif scadenza_interna is None or scadenza_esterna <= scadenza_interna:
        tipo, prossima = "ESTERNA", scadenza_esterna
    else:
        tipo, prossima = "INTERNA", scadenza_interna

    days = (prossima - today).days
    classe = "SCADUTA" if days < 0 else "ENTRO_30" if days <= 30 else "ENTRO_60" if days <= 60 else "REGOLARE"
    return {"tipo": tipo, "data": prossima, "classe": classe, "giorni": days}


def alerts_summary(today: date | None = None) -> dict[str, int]:
    summary = {"scadute": 0, "entro_30": 0, "entro_60": 0, "totale": 0}
    for strumento in StrumentoMisura.query.filter_by(stato="IN_USO").all():
        classe = due_for_instrument(strumento, today)["classe"]
        key = {
            "SCADUTA": "scadute",
            "ENTRO_30": "entro_30",
            "ENTRO_60": "entro_60",
        }.get(classe)
        if key:
            summary[key] += 1
            summary["totale"] += 1
    return summary


def create_tipologia(data, user) -> TipologiaStrumento:
    esterna = _positive_int(
        data.get("frequenza_esterna_mesi"),
        "frequenza esterna",
        optional=True,
    )
    interna = _positive_int(
        data.get("frequenza_interna_mesi"),
        "frequenza interna",
        optional=True,
    )
    if esterna is None and interna is None:
        raise ValueError("Indica almeno una frequenza, interna o esterna.")
    row = TipologiaStrumento(
        nome=_text(data.get("nome"), "nome"),
        frequenza_esterna_mesi=esterna or 1,
        frequenza_interna_mesi=interna,
        taratura_esterna_attiva=esterna is not None,
    )
    db.session.add(row)
    db.session.flush()
    _log("TIPOLOGIA_CREATA", "TIPOLOGIA", row.id, {"nome": row.nome}, user)
    _commit()
    return row


def update_tipologia(tipologia_id: int, data, user) -> TipologiaStrumento:
    row = _get(TipologiaStrumento, tipologia_id, "Tipologia")
    before = {
        "nome": row.nome,
        "frequenza_esterna_mesi": row.frequenza_esterna_mesi,
        "frequenza_interna_mesi": row.frequenza_interna_mesi,
        "taratura_esterna_attiva": row.taratura_esterna_attiva,
    }
    esterna = _positive_int(
        data.get("frequenza_esterna_mesi"),
        "frequenza esterna",
        optional=True,
    )
    interna = _positive_int(
        data.get("frequenza_interna_mesi"),
        "frequenza interna",
        optional=True,
    )
    if esterna is None and interna is None:
        raise ValueError("Indica almeno una frequenza, interna o esterna.")
    row.nome = _text(data.get("nome"), "nome")
    row.frequenza_esterna_mesi = esterna or row.frequenza_esterna_mesi or 1
    row.frequenza_interna_mesi = interna
    row.taratura_esterna_attiva = esterna is not None
    _log("TIPOLOGIA_MODIFICATA", "TIPOLOGIA", row.id, {"prima": before}, user)
    _commit()
    return row


def create_strumento(data, user, *, data_inserimento: date | None = None) -> StrumentoMisura:
    tipologia = _get(
        TipologiaStrumento,
        int(data.get("tipologia_id") or 0),
        "Tipologia",
    )
    stato = str(data.get("stato") or "IN_USO").strip().upper()
    if stato not in {"IN_USO", "NON_IN_USO"}:
        raise ValueError("Un nuovo strumento può essere In uso o Non in uso.")
    solo_verifica_interna = _checked(data.get("solo_verifica_interna"))
    if solo_verifica_interna and not tipologia.frequenza_interna_mesi:
        raise ValueError("La tipologia deve avere una frequenza di verifica interna.")

    data_ultima_taratura = str(data.get("data_ultima_taratura") or "").strip()
    ultima_taratura = data_inserimento or (
        parse_date(data_ultima_taratura, "ultima taratura")
        if data_ultima_taratura
        else today_rome()
    )
    if ultima_taratura > today_rome():
        raise ValueError("La data dell'ultima taratura non può essere futura.")

    public_id, username = _actor(user)
    row = StrumentoMisura(
        codice_interno=data.get("codice_interno"),
        numero_seriale=_numero_seriale(
            data.get("numero_seriale"),
            data.get("codice_interno"),
        ),
        descrizione=data.get("descrizione"),
        costruttore=data.get("costruttore"),
        solo_verifica_interna=solo_verifica_interna,
        tipologia=tipologia,
        reparto_codice=_validate_reparto(data.get("reparto_codice")),
        stato=stato,
    )
    db.session.add(row)
    db.session.flush()
    db.session.add(
        EventoTaratura(
            strumento=row,
            tipo="INIZIALE",
            data_evento=ultima_taratura,
            esito="CONFORME",
            note=(
                "Ultima taratura indicata all'inserimento."
                if data_inserimento or data_ultima_taratura
                else "Taratura iniziale eseguita dalla casa madre."
            ),
            registrato_da_public_id=public_id,
            registrato_da_username=username,
        )
    )
    _log(
        "STRUMENTO_CREATO",
        "STRUMENTO",
        row.id,
        {"codice": row.codice_interno, "stato": row.stato},
        user,
    )
    _commit()
    return row


def update_strumento(strumento_id: int, data, user) -> StrumentoMisura:
    row = _get(StrumentoMisura, strumento_id, "Strumento")
    before = {
        "codice_interno": row.codice_interno,
        "numero_seriale": row.numero_seriale,
        "descrizione": row.descrizione,
        "costruttore": row.costruttore,
        "solo_verifica_interna": row.solo_verifica_interna,
        "tipologia_id": row.tipologia_id,
        "reparto_codice": row.reparto_codice,
    }
    tipologia = _get(
        TipologiaStrumento,
        int(data.get("tipologia_id") or 0),
        "Tipologia",
    )
    solo_verifica_interna = _checked(data.get("solo_verifica_interna"))
    if solo_verifica_interna and not tipologia.frequenza_interna_mesi:
        raise ValueError("La tipologia deve avere una frequenza di verifica interna.")

    row.codice_interno = data.get("codice_interno")
    row.numero_seriale = _numero_seriale(
        data.get("numero_seriale"),
        data.get("codice_interno"),
    )
    row.descrizione = data.get("descrizione")
    row.costruttore = data.get("costruttore")
    row.solo_verifica_interna = solo_verifica_interna
    row.tipologia = tipologia
    row.reparto_codice = _validate_reparto(data.get("reparto_codice"))
    _log("STRUMENTO_MODIFICATO", "STRUMENTO", row.id, {"prima": before}, user)
    _commit()
    return row


def set_stato(strumento_id: int, stato: str, user, *, today: date | None = None) -> StrumentoMisura:
    row = _get(StrumentoMisura, strumento_id, "Strumento")
    nuovo = str(stato or "").strip().upper()
    if row.stato == "IN_TARATURA":
        raise ValueError("Per chiudere una taratura registra rapporto ed esito.")
    if nuovo not in {"IN_USO", "NON_IN_USO"}:
        raise ValueError("Lo stato In taratura si imposta creando una spedizione.")
    if nuovo == "IN_USO" and due_for_instrument(row, today)["classe"] == "SCADUTA":
        raise ValueError("Lo strumento non può essere riattivato: la taratura è scaduta.")
    precedente = row.stato
    row.stato = nuovo
    _log(
        "STATO_MODIFICATO",
        "STRUMENTO",
        row.id,
        {"da": precedente, "a": nuovo},
        user,
    )
    _commit()
    return row


def record_internal_check(strumento_id: int, data, user) -> EventoTaratura:
    row = _get(StrumentoMisura, strumento_id, "Strumento")
    if row.stato != "IN_USO":
        raise ValueError("La verifica interna è consentita solo per strumenti In uso.")
    data_evento = parse_date(data.get("data_evento"), "data verifica")
    if data_evento > today_rome():
        raise ValueError("La data della verifica non può essere futura.")
    esito = str(data.get("esito") or "").strip().upper()
    if esito not in {"CONFORME", "NON_CONFORME"}:
        raise ValueError("Esito non valido.")
    public_id, username = _actor(user)
    evento = EventoTaratura(
        strumento=row,
        tipo="INTERNA",
        data_evento=data_evento,
        esito=esito,
        note=_optional_text(data.get("note")),
        registrato_da_public_id=public_id,
        registrato_da_username=username,
    )
    db.session.add(evento)
    row.stato = "IN_USO" if esito == "CONFORME" else "NON_IN_USO"
    db.session.flush()
    _log(
        "VERIFICA_INTERNA_REGISTRATA",
        "STRUMENTO",
        row.id,
        {"evento_id": evento.id, "data": data_evento, "esito": esito},
        user,
    )
    _commit()
    return evento


def create_spedizione(data, strumento_ids, user) -> SpedizioneTaratura:
    ids = list(dict.fromkeys(int(value) for value in strumento_ids if str(value).strip()))
    if not ids:
        raise ValueError("Seleziona almeno uno strumento.")
    strumenti = StrumentoMisura.query.filter(StrumentoMisura.id.in_(ids)).all()
    if len(strumenti) != len(ids):
        raise ValueError("Uno o più strumenti non esistono.")
    if any(row.stato == "IN_TARATURA" for row in strumenti):
        raise ValueError("Uno o più strumenti sono già In taratura.")
    if any(not _richiede_taratura_esterna(row) for row in strumenti):
        raise ValueError("Gli strumenti con sola verifica interna non possono essere spediti.")

    public_id, username = _actor(user)
    spedizione = SpedizioneTaratura(
        data_spedizione=parse_date(data.get("data_spedizione"), "data spedizione"),
        laboratorio=_text(data.get("laboratorio"), "laboratorio"),
        note=_optional_text(data.get("note")),
        created_by_public_id=public_id,
        created_by_username=username,
    )
    db.session.add(spedizione)
    db.session.flush()
    for row in strumenti:
        spedizione.strumenti.append(
            SpedizioneTaraturaStrumento(
                strumento=row,
                stato_precedente=row.stato,
            )
        )
        row.stato = "IN_TARATURA"
        _log(
            "STRUMENTO_SPEDITO",
            "STRUMENTO",
            row.id,
            {"spedizione": spedizione.numero},
            user,
        )
    _log(
        "SPEDIZIONE_CREATA",
        "SPEDIZIONE",
        spedizione.id,
        {"numero": spedizione.numero, "strumenti": ids},
        user,
    )
    _commit()
    return spedizione


def record_external_calibration(strumento_id: int, data, user, certificato) -> EventoTaratura:
    row = _get(StrumentoMisura, strumento_id, "Strumento")
    if row.stato != "IN_TARATURA":
        raise ValueError("Lo strumento non è In taratura.")
    certificato_nome, certificato_contenuto = _read_certificato_pdf(certificato)
    data_evento = parse_date(data.get("data_evento"), "data taratura")
    if data_evento > today_rome():
        raise ValueError("La data della taratura non può essere futura.")
    rapporto = _text(data.get("rapporto_riferimento"), "rapporto di taratura")
    esito = str(data.get("esito") or "").strip().upper()
    if esito not in {"CONFORME", "NON_CONFORME"}:
        raise ValueError("Esito non valido.")

    pending = (
        SpedizioneTaraturaStrumento.query.join(SpedizioneTaratura)
        .filter(
            SpedizioneTaraturaStrumento.strumento_id == row.id,
            SpedizioneTaraturaStrumento.evento_id.is_(None),
        )
        .order_by(SpedizioneTaratura.data_spedizione.desc(), SpedizioneTaratura.id.desc())
        .first()
    )
    if pending and data_evento < pending.spedizione.data_spedizione:
        raise ValueError("La taratura non può precedere la data di spedizione.")

    public_id, username = _actor(user)
    evento = EventoTaratura(
        strumento=row,
        tipo="ESTERNA",
        data_evento=data_evento,
        esito=esito,
        rapporto_riferimento=rapporto,
        note=_optional_text(data.get("note")),
        certificato_nome=certificato_nome,
        registrato_da_public_id=public_id,
        registrato_da_username=username,
    )
    db.session.add(evento)
    db.session.flush()
    if pending:
        pending.evento_id = evento.id
        pending.chiuso_at = datetime.now()
    row.stato = "IN_USO" if esito == "CONFORME" else "NON_IN_USO"
    _log(
        "TARATURA_ESTERNA_REGISTRATA",
        "STRUMENTO",
        row.id,
        {
            "evento_id": evento.id,
            "data": data_evento,
            "esito": esito,
            "rapporto": rapporto,
            "certificato": certificato_nome,
        },
        user,
    )
    directory = current_app.config["TARATURE_CERTIFICATI_DIR"]
    certificato_file = _save_certificato_pdf(directory, certificato_contenuto)
    evento.certificato_file = certificato_file
    try:
        _commit()
    except Exception:
        try:
            (Path(directory) / certificato_file).unlink(missing_ok=True)
        except OSError:
            pass
        raise
    return evento


def build_page_context(today: date | None = None) -> dict:
    today = today or today_rome()
    strumenti = StrumentoMisura.query.order_by(
        func.lower(StrumentoMisura.codice_interno)
    ).all()
    instrument_rows = [
        {"strumento": row, "scadenza": due_for_instrument(row, today)}
        for row in strumenti
    ]
    return {
        "oggi": today,
        "strumenti_rows": instrument_rows,
        "tipologie": TipologiaStrumento.query.order_by(
            func.lower(TipologiaStrumento.nome)
        ).all(),
        "reparti": list_reparti(),
        "spedizioni": SpedizioneTaratura.query.order_by(
            SpedizioneTaratura.data_spedizione.desc(),
            SpedizioneTaratura.id.desc(),
        ).all(),
        "logs": TaraturaLog.query.order_by(TaraturaLog.created_at.desc()).limit(200).all(),
        "alerts": alerts_summary(today),
    }
