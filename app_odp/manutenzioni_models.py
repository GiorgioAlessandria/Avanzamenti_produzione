# app_odp/manutenzioni_models.py

from __future__ import annotations

import json
from datetime import date, datetime
from zoneinfo import ZoneInfo

from sqlalchemy.orm import validates

from app_odp.models import db


MANUTENZIONI_BIND_KEY = "manutenzioni"
ROME_TIMEZONE = ZoneInfo("Europe/Rome")

UNITA_FREQUENZA = {
    "giorni",
    "settimane",
    "mesi",
    "anni",
}

GIORNI_SETTIMANA = {
    "MO",
    "TU",
    "WE",
    "TH",
    "FR",
}

STATI_EVENTO_MANUTENZIONE = {
    "PROGRAMMATA",
    "COMPLETATA",
    "SALTATA",
    "ANNULLATA",
}

ESITI_EVENTO_MANUTENZIONE = {
    "POSITIVO",
    "ANOMALIA",
    "INTERVENTO_RICHIESTO",
    "NON_APPLICABILE",
}

ESITI_MANUTENZIONE_STRAORDINARIA = {
    "RISOLTO",
    "PARZIALMENTE_RISOLTO",
    "DA_VERIFICARE",
    "NON_RISOLTO",
}


def _now_rome_dt() -> datetime:
    """
    Restituisce la data e ora locale italiana senza timezone associata.

    SQLite non conserva in modo affidabile le informazioni timezone nelle
    colonne DateTime. L'applicazione considera quindi tutti i DateTime di
    questo database come Europe/Rome.
    """
    return datetime.now(ROME_TIMEZONE).replace(tzinfo=None)


def _normalize_required_text(value: str | None, field_name: str) -> str:
    normalized = str(value or "").strip()

    if not normalized:
        raise ValueError(f"Il campo {field_name} è obbligatorio.")

    return normalized


def _normalize_optional_text(value: str | None) -> str | None:
    normalized = str(value or "").strip()
    return normalized or None


class Macchinario(db.Model):
    """
    Anagrafica dei macchinari soggetti a manutenzione.

    reparto_codice è un riferimento logico a Reparti.Codice in RBAC.db.
    Non è una foreign key SQL perché il reparto si trova in un altro
    database SQLite.
    """

    __bind_key__ = MANUTENZIONI_BIND_KEY
    __tablename__ = "macchinari"

    id = db.Column(
        db.Integer,
        primary_key=True,
        autoincrement=True,
    )

    codice = db.Column(
        db.Text,
        nullable=False,
    )

    descrizione = db.Column(
        db.Text,
        nullable=False,
    )

    matricola = db.Column(
        db.Text,
    )

    reparto_codice = db.Column(
        db.Text,
        nullable=False,
    )

    costruttore = db.Column(
        db.Text,
    )

    modello = db.Column(
        db.Text,
    )

    ubicazione = db.Column(
        db.Text,
    )

    attivo = db.Column(
        db.Boolean,
        nullable=False,
        default=True,
        server_default=db.text("1"),
    )

    note = db.Column(
        db.Text,
    )

    created_at = db.Column(
        db.DateTime,
        nullable=False,
        default=_now_rome_dt,
    )

    updated_at = db.Column(
        db.DateTime,
        nullable=False,
        default=_now_rome_dt,
        onupdate=_now_rome_dt,
    )

    manutenzioni_ricorrenti = db.relationship(
        "ManutenzioneRicorrente",
        back_populates="macchinario",
        lazy="selectin",
        passive_deletes=True,
    )

    manutenzioni_straordinarie = db.relationship(
        "ManutenzioneStraordinaria",
        back_populates="macchinario",
        lazy="selectin",
        passive_deletes=True,
    )

    __table_args__ = (
        db.UniqueConstraint(
            "codice",
            name="uq_macchinari_codice",
        ),
        db.CheckConstraint(
            "attivo IN (0, 1)",
            name="ck_macchinari_attivo",
        ),
        db.Index(
            "ix_macchinari_reparto_codice",
            "reparto_codice",
        ),
        db.Index(
            "ix_macchinari_attivo",
            "attivo",
        ),
    )

    @validates("codice")
    def validate_codice(self, key: str, value: str | None) -> str:
        return _normalize_required_text(value, "codice")

    @validates("descrizione")
    def validate_descrizione(self, key: str, value: str | None) -> str:
        return _normalize_required_text(value, "descrizione")

    @validates("reparto_codice")
    def validate_reparto_codice(
        self,
        key: str,
        value: str | None,
    ) -> str:
        return _normalize_required_text(value, "reparto_codice")

    @validates(
        "matricola",
        "costruttore",
        "modello",
        "ubicazione",
        "note",
    )
    def normalize_optional_fields(
        self,
        key: str,
        value: str | None,
    ) -> str | None:
        return _normalize_optional_text(value)

    def __repr__(self) -> str:
        return (
            f"<Macchinario "
            f"id={self.id} "
            f"codice={self.codice!r} "
            f"reparto={self.reparto_codice!r}>"
        )


class ManutenzioneRicorrente(db.Model):
    """
    Regola di manutenzione ricorrente associata a un macchinario.

    Questa tabella definisce la frequenza e le istruzioni. Le singole
    scadenze sono registrate in eventi_manutenzione.
    """

    __bind_key__ = MANUTENZIONI_BIND_KEY
    __tablename__ = "manutenzioni_ricorrenti"

    id = db.Column(
        db.Integer,
        primary_key=True,
        autoincrement=True,
    )

    macchinario_id = db.Column(
        db.Integer,
        db.ForeignKey(
            "macchinari.id",
            onupdate="CASCADE",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )

    codice = db.Column(
        db.Text,
    )

    titolo = db.Column(
        db.Text,
        nullable=False,
    )

    descrizione = db.Column(
        db.Text,
    )

    frequenza_unita = db.Column(
        db.Text,
        nullable=False,
    )

    frequenza_intervallo = db.Column(
        db.Integer,
        nullable=False,
        default=1,
        server_default=db.text("1"),
    )

    giorni_settimana = db.Column(
        db.Text,
    )

    data_inizio = db.Column(
        db.Date,
        nullable=False,
    )

    preavviso_giorni = db.Column(
        db.Integer,
        nullable=False,
        default=7,
        server_default=db.text("7"),
    )

    attiva = db.Column(
        db.Boolean,
        nullable=False,
        default=True,
        server_default=db.text("1"),
    )

    created_by_public_id = db.Column(
        db.Text,
    )

    created_by_username = db.Column(
        db.Text,
    )

    created_at = db.Column(
        db.DateTime,
        nullable=False,
        default=_now_rome_dt,
    )

    updated_at = db.Column(
        db.DateTime,
        nullable=False,
        default=_now_rome_dt,
        onupdate=_now_rome_dt,
    )

    macchinario = db.relationship(
        "Macchinario",
        back_populates="manutenzioni_ricorrenti",
        lazy="joined",
    )

    eventi = db.relationship(
        "EventoManutenzione",
        back_populates="manutenzione_ricorrente",
        lazy="selectin",
        passive_deletes=True,
        order_by="EventoManutenzione.data_programmata",
    )

    __table_args__ = (
        db.UniqueConstraint(
            "macchinario_id",
            "codice",
            name="uq_manutenzioni_ricorrenti_codice",
        ),
        db.CheckConstraint(
            """
                frequenza_unita IN (
                    'giorni',
                    'settimane',
                    'mesi',
                    'anni'
                )
                """,
            name="ck_manutenzioni_ricorrenti_frequenza_unita",
        ),
        db.CheckConstraint(
            "frequenza_intervallo > 0",
            name="ck_manutenzioni_ricorrenti_intervallo",
        ),
        db.CheckConstraint(
            "preavviso_giorni >= 0",
            name="ck_manutenzioni_ricorrenti_preavviso",
        ),
        db.CheckConstraint(
            "attiva IN (0, 1)",
            name="ck_manutenzioni_ricorrenti_attiva",
        ),
        db.Index(
            "ix_manutenzioni_ricorrenti_macchinario",
            "macchinario_id",
        ),
        db.Index(
            "ix_manutenzioni_ricorrenti_attiva",
            "attiva",
        ),
        db.Index(
            "ix_manutenzioni_ricorrenti_data_inizio",
            "data_inizio",
        ),
    )

    @validates("titolo")
    def validate_titolo(self, key: str, value: str | None) -> str:
        return _normalize_required_text(value, "titolo")

    @validates("codice", "descrizione")
    def normalize_optional_fields(
        self,
        key: str,
        value: str | None,
    ) -> str | None:
        return _normalize_optional_text(value)

    @validates("frequenza_unita")
    def validate_frequenza_unita(
        self,
        key: str,
        value: str | None,
    ) -> str:
        normalized = str(value or "").strip().lower()

        if normalized not in UNITA_FREQUENZA:
            raise ValueError(
                "frequenza_unita deve essere uno dei seguenti valori: "
                + ", ".join(sorted(UNITA_FREQUENZA))
            )

        return normalized

    @validates("frequenza_intervallo")
    def validate_frequenza_intervallo(
        self,
        key: str,
        value: int | str | None,
    ) -> int:
        try:
            normalized = int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "frequenza_intervallo deve essere un numero intero."
            ) from exc

        if normalized <= 0:
            raise ValueError("frequenza_intervallo deve essere maggiore di zero.")

        return normalized

    @validates("preavviso_giorni")
    def validate_preavviso_giorni(
        self,
        key: str,
        value: int | str | None,
    ) -> int:
        try:
            normalized = int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError("preavviso_giorni deve essere un numero intero.") from exc

        if normalized < 0:
            raise ValueError("preavviso_giorni non può essere negativo.")

        return normalized

    @validates("data_inizio")
    def validate_date(
        self,
        key: str,
        value: date | None,
    ) -> date | None:
        if value is not None and not isinstance(value, date):
            raise ValueError(f"{key} deve essere un oggetto datetime.date.")

        return value

    @property
    def giorni_settimana_list(self) -> list[str]:
        if not self.giorni_settimana:
            return []

        try:
            parsed = json.loads(self.giorni_settimana)
        except (TypeError, json.JSONDecodeError):
            return []

        if not isinstance(parsed, list):
            return []

        result: list[str] = []

        for value in parsed:
            normalized = str(value or "").strip().upper()

            if normalized in GIORNI_SETTIMANA:
                result.append(normalized)

        return list(dict.fromkeys(result))

    @giorni_settimana_list.setter
    def giorni_settimana_list(
        self,
        values: list[str] | tuple[str, ...] | set[str] | None,
    ) -> None:
        if not values:
            self.giorni_settimana = None
            return

        normalized_values: list[str] = []

        for value in values:
            normalized = str(value or "").strip().upper()

            if normalized not in GIORNI_SETTIMANA:
                raise ValueError(f"Giorno della settimana non valido: {value!r}.")

            if normalized not in normalized_values:
                normalized_values.append(normalized)

        self.giorni_settimana = json.dumps(
            normalized_values,
            ensure_ascii=False,
        )

    @property
    def descrizione_frequenza(self) -> str:
        intervallo = self.frequenza_intervallo

        if self.frequenza_unita == "giorni":
            return "Ogni giorno" if intervallo == 1 else f"Ogni {intervallo} giorni"

        if self.frequenza_unita == "settimane":
            if self.giorni_settimana_list:
                giorni = ", ".join(self.giorni_settimana_list)

                if intervallo == 1:
                    return f"Ogni settimana: {giorni}"

                return f"Ogni {intervallo} settimane: {giorni}"

            return (
                "Ogni settimana" if intervallo == 1 else f"Ogni {intervallo} settimane"
            )

        if self.frequenza_unita == "mesi":
            return "Ogni mese" if intervallo == 1 else f"Ogni {intervallo} mesi"

        if self.frequenza_unita == "anni":
            return "Ogni anno" if intervallo == 1 else f"Ogni {intervallo} anni"

        return ""

    def __repr__(self) -> str:
        return (
            f"<ManutenzioneRicorrente "
            f"id={self.id} "
            f"macchinario_id={self.macchinario_id} "
            f"titolo={self.titolo!r}>"
        )


class EventoManutenzione(db.Model):
    """
    Singola scadenza generata da una manutenzione ricorrente.

    Una manutenzione ricorrente può generare più eventi. Il vincolo
    univoco impedisce di generare due volte lo stesso evento per la
    stessa data.
    """

    __bind_key__ = MANUTENZIONI_BIND_KEY
    __tablename__ = "eventi_manutenzione"

    id = db.Column(
        db.Integer,
        primary_key=True,
        autoincrement=True,
    )

    manutenzione_ricorrente_id = db.Column(
        db.Integer,
        db.ForeignKey(
            "manutenzioni_ricorrenti.id",
            onupdate="CASCADE",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )

    data_teorica = db.Column(
        db.Date,
        nullable=False,
        index=True,
    )

    data_programmata = db.Column(
        db.Date,
        nullable=False,
    )

    data_spostata = db.Column(
        db.Boolean,
        nullable=False,
        default=False,
        server_default=db.text("0"),
    )

    motivo_spostamento = db.Column(
        db.String(255),
        nullable=True,
    )

    stato = db.Column(
        db.Text,
        nullable=False,
        default="PROGRAMMATA",
        server_default=db.text("'PROGRAMMATA'"),
    )

    titolo_snapshot = db.Column(
        db.Text,
        nullable=False,
    )

    descrizione_snapshot = db.Column(
        db.Text,
    )

    data_esecuzione = db.Column(
        db.DateTime,
    )

    eseguito_da_public_id = db.Column(
        db.Text,
    )

    eseguito_da_username = db.Column(
        db.Text,
    )

    registrato_da_public_id = db.Column(
        db.Text,
    )

    registrato_da_username = db.Column(
        db.Text,
    )

    esito = db.Column(
        db.Text,
    )

    descrizione_intervento = db.Column(
        db.Text,
    )

    note = db.Column(
        db.Text,
    )

    durata_minuti = db.Column(
        db.Integer,
    )

    created_at = db.Column(
        db.DateTime,
        nullable=False,
        default=_now_rome_dt,
    )

    updated_at = db.Column(
        db.DateTime,
        nullable=False,
        default=_now_rome_dt,
        onupdate=_now_rome_dt,
    )

    manutenzione_ricorrente = db.relationship(
        "ManutenzioneRicorrente",
        back_populates="eventi",
        lazy="joined",
    )
    __table_args__ = (
        db.UniqueConstraint(
            "manutenzione_ricorrente_id",
            "data_teorica",
            name="uq_eventi_manutenzione_piano_data_teorica",
        ),
        db.CheckConstraint(
            "data_spostata IN (0, 1)",
            name="ck_eventi_manutenzione_data_spostata",
        ),
        db.CheckConstraint(
            """
                stato IN (
                    'PROGRAMMATA',
                    'COMPLETATA',
                    'SALTATA',
                    'ANNULLATA'
                )
                """,
            name="ck_eventi_manutenzione_stato",
        ),
        db.CheckConstraint(
            """
                esito IS NULL
                OR esito IN (
                    'POSITIVO',
                    'ANOMALIA',
                    'INTERVENTO_RICHIESTO',
                    'NON_APPLICABILE'
                )
                """,
            name="ck_eventi_manutenzione_esito",
        ),
        db.CheckConstraint(
            """
                durata_minuti IS NULL
                OR durata_minuti >= 0
                """,
            name="ck_eventi_manutenzione_durata",
        ),
        db.CheckConstraint(
            """
                stato <> 'COMPLETATA'
                OR data_esecuzione IS NOT NULL
                """,
            name="ck_eventi_completati_data",
        ),
        db.Index(
            "ix_eventi_manutenzione_piano",
            "manutenzione_ricorrente_id",
        ),
        db.Index(
            "ix_eventi_manutenzione_data_programmata",
            "data_programmata",
        ),
        db.Index(
            "ix_eventi_manutenzione_stato",
            "stato",
        ),
        db.Index(
            "ix_eventi_manutenzione_stato_data",
            "stato",
            "data_programmata",
        ),
    )

    @validates("stato")
    def validate_stato(
        self,
        key: str,
        value: str | None,
    ) -> str:
        normalized = str(value or "").strip().upper()

        if normalized not in STATI_EVENTO_MANUTENZIONE:
            raise ValueError(
                "Stato evento non valido. Valori consentiti: "
                + ", ".join(sorted(STATI_EVENTO_MANUTENZIONE))
            )

        return normalized

    @validates("esito")
    def validate_esito(
        self,
        key: str,
        value: str | None,
    ) -> str | None:
        if value is None or not str(value).strip():
            return None

        normalized = str(value).strip().upper()

        if normalized not in ESITI_EVENTO_MANUTENZIONE:
            raise ValueError(
                "Esito evento non valido. Valori consentiti: "
                + ", ".join(sorted(ESITI_EVENTO_MANUTENZIONE))
            )

        return normalized

    @validates("titolo_snapshot")
    def validate_titolo_snapshot(
        self,
        key: str,
        value: str | None,
    ) -> str:
        return _normalize_required_text(value, "titolo_snapshot")

    @validates("durata_minuti")
    def validate_durata_minuti(
        self,
        key: str,
        value: int | str | None,
    ) -> int | None:
        if value is None or value == "":
            return None

        try:
            normalized = int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError("durata_minuti deve essere un numero intero.") from exc

        if normalized < 0:
            raise ValueError("durata_minuti non può essere negativa.")

        return normalized

    def __repr__(self) -> str:
        return (
            f"<EventoManutenzione "
            f"id={self.id} "
            f"piano_id={self.manutenzione_ricorrente_id} "
            f"data={self.data_programmata} "
            f"stato={self.stato!r}>"
        )


class ManutenzioneStraordinaria(db.Model):
    """
    Registro degli interventi straordinari non generati da un piano
    ricorrente.
    """

    __bind_key__ = MANUTENZIONI_BIND_KEY
    __tablename__ = "manutenzioni_straordinarie"

    id = db.Column(
        db.Integer,
        primary_key=True,
        autoincrement=True,
    )

    macchinario_id = db.Column(
        db.Integer,
        db.ForeignKey(
            "macchinari.id",
            onupdate="CASCADE",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )

    data_intervento = db.Column(
        db.DateTime,
        nullable=False,
    )

    titolo = db.Column(
        db.Text,
        nullable=False,
    )

    descrizione_problema = db.Column(
        db.Text,
        nullable=False,
    )

    causa = db.Column(
        db.Text,
    )

    intervento_eseguito = db.Column(
        db.Text,
        nullable=False,
    )

    esito = db.Column(
        db.Text,
        nullable=False,
    )

    fermo_macchina_minuti = db.Column(
        db.Integer,
    )

    durata_intervento_minuti = db.Column(
        db.Integer,
    )

    eseguito_da_public_id = db.Column(
        db.Text,
    )

    eseguito_da_username = db.Column(
        db.Text,
    )

    eseguito_da_esterno = db.Column(
        db.Text,
    )

    registrato_da_public_id = db.Column(
        db.Text,
    )

    registrato_da_username = db.Column(
        db.Text,
    )

    note = db.Column(
        db.Text,
    )

    created_at = db.Column(
        db.DateTime,
        nullable=False,
        default=_now_rome_dt,
    )

    updated_at = db.Column(
        db.DateTime,
        nullable=False,
        default=_now_rome_dt,
        onupdate=_now_rome_dt,
    )

    macchinario = db.relationship(
        "Macchinario",
        back_populates="manutenzioni_straordinarie",
        lazy="joined",
    )

    __table_args__ = (
        db.CheckConstraint(
            """
                esito IN (
                    'RISOLTO',
                    'PARZIALMENTE_RISOLTO',
                    'DA_VERIFICARE',
                    'NON_RISOLTO'
                )
                """,
            name="ck_manutenzioni_straordinarie_esito",
        ),
        db.CheckConstraint(
            """
                fermo_macchina_minuti IS NULL
                OR fermo_macchina_minuti >= 0
                """,
            name="ck_manutenzioni_straordinarie_fermo",
        ),
        db.CheckConstraint(
            """
                durata_intervento_minuti IS NULL
                OR durata_intervento_minuti >= 0
                """,
            name="ck_manutenzioni_straordinarie_durata",
        ),
        db.Index(
            "ix_manutenzioni_straordinarie_macchinario",
            "macchinario_id",
        ),
        db.Index(
            "ix_manutenzioni_straordinarie_data",
            "data_intervento",
        ),
        db.Index(
            "ix_manutenzioni_straordinarie_esito",
            "esito",
        ),
    )

    @validates(
        "titolo",
        "descrizione_problema",
        "intervento_eseguito",
    )
    def validate_required_text(
        self,
        key: str,
        value: str | None,
    ) -> str:
        return _normalize_required_text(value, key)

    @validates("esito")
    def validate_esito(
        self,
        key: str,
        value: str | None,
    ) -> str:
        normalized = str(value or "").strip().upper()

        if normalized not in ESITI_MANUTENZIONE_STRAORDINARIA:
            raise ValueError(
                "Esito manutenzione straordinaria non valido. "
                "Valori consentiti: "
                + ", ".join(sorted(ESITI_MANUTENZIONE_STRAORDINARIA))
            )

        return normalized

    @validates(
        "fermo_macchina_minuti",
        "durata_intervento_minuti",
    )
    def validate_minutes(
        self,
        key: str,
        value: int | str | None,
    ) -> int | None:
        if value is None or value == "":
            return None

        try:
            normalized = int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{key} deve essere un numero intero.") from exc

        if normalized < 0:
            raise ValueError(f"{key} non può essere negativo.")

        return normalized

    def __repr__(self) -> str:
        return (
            f"<ManutenzioneStraordinaria "
            f"id={self.id} "
            f"macchinario_id={self.macchinario_id} "
            f"data={self.data_intervento} "
            f"esito={self.esito!r}>"
        )


class ManutenzioneGiornoNonLavorativo(db.Model):
    __bind_key__ = MANUTENZIONI_BIND_KEY
    __tablename__ = "manutenzione_giorni_non_lavorativi"

    id = db.Column(
        db.Integer,
        primary_key=True,
    )

    data = db.Column(
        db.Date,
        nullable=False,
        index=True,
    )

    descrizione = db.Column(
        db.String(255),
        nullable=False,
    )

    tipo = db.Column(
        db.String(40),
        nullable=False,
        default="CHIUSURA_AZIENDALE",
    )

    ricorrente_annuale = db.Column(
        db.Boolean,
        nullable=False,
        default=False,
    )

    attivo = db.Column(
        db.Boolean,
        nullable=False,
        default=True,
    )

    created_at = db.Column(
        db.DateTime,
        nullable=False,
        default=_now_rome_dt,
    )

    updated_at = db.Column(
        db.DateTime,
        nullable=False,
        default=_now_rome_dt,
        onupdate=_now_rome_dt,
    )
