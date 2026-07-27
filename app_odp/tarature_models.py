from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from sqlalchemy.orm import validates

from app_odp.models import db


TARATURE_BIND_KEY = "manutenzioni"
STATI_STRUMENTO = {"IN_USO", "NON_IN_USO", "IN_TARATURA"}
TIPI_EVENTO_TARATURA = {"INIZIALE", "INTERNA", "ESTERNA"}
ESITI_TARATURA = {"CONFORME", "NON_CONFORME"}


def _now_rome() -> datetime:
    return datetime.now(ZoneInfo("Europe/Rome")).replace(tzinfo=None)


def _required(value, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field} è obbligatorio.")
    return normalized


class TipologiaStrumento(db.Model):
    __bind_key__ = TARATURE_BIND_KEY
    __tablename__ = "tipologie_strumento"

    id = db.Column(db.Integer, primary_key=True)
    nome = db.Column(db.String(120), nullable=False, unique=True)
    frequenza_esterna_mesi = db.Column(db.Integer, nullable=False)
    frequenza_interna_mesi = db.Column(db.Integer)
    taratura_esterna_attiva = db.Column(
        db.Boolean,
        nullable=False,
        default=True,
        server_default="1",
    )
    created_at = db.Column(db.DateTime, nullable=False, default=_now_rome)
    updated_at = db.Column(
        db.DateTime,
        nullable=False,
        default=_now_rome,
        onupdate=_now_rome,
    )

    strumenti = db.relationship(
        "StrumentoMisura",
        back_populates="tipologia",
        lazy="selectin",
    )

    __table_args__ = (
        db.CheckConstraint(
            "frequenza_esterna_mesi > 0",
            name="ck_tipologie_strumento_frequenza_esterna",
        ),
        db.CheckConstraint(
            "frequenza_interna_mesi IS NULL OR frequenza_interna_mesi > 0",
            name="ck_tipologie_strumento_frequenza_interna",
        ),
    )

    @validates("nome")
    def validate_nome(self, key, value):
        return _required(value, "nome")


class StrumentoMisura(db.Model):
    __bind_key__ = TARATURE_BIND_KEY
    __tablename__ = "strumenti_misura"

    id = db.Column(db.Integer, primary_key=True)
    codice_interno = db.Column(db.String(80), nullable=False, unique=True)
    numero_seriale = db.Column(db.String(120), nullable=False, unique=True)
    descrizione = db.Column(db.Text, nullable=False)
    costruttore = db.Column(db.String(160), nullable=False)
    solo_verifica_interna = db.Column(
        db.Boolean,
        nullable=False,
        default=False,
        server_default="0",
    )
    tipologia_id = db.Column(
        db.Integer,
        db.ForeignKey("tipologie_strumento.id", ondelete="RESTRICT"),
        nullable=False,
    )
    reparto_codice = db.Column(db.Text, nullable=False)
    stato = db.Column(
        db.String(20),
        nullable=False,
        default="IN_USO",
        server_default="IN_USO",
    )
    created_at = db.Column(db.DateTime, nullable=False, default=_now_rome)
    updated_at = db.Column(
        db.DateTime,
        nullable=False,
        default=_now_rome,
        onupdate=_now_rome,
    )

    tipologia = db.relationship("TipologiaStrumento", back_populates="strumenti")
    eventi = db.relationship(
        "EventoTaratura",
        back_populates="strumento",
        lazy="selectin",
        order_by=lambda: EventoTaratura.data_evento.desc(),
    )
    spedizioni = db.relationship(
        "SpedizioneTaraturaStrumento",
        back_populates="strumento",
        lazy="selectin",
    )

    __table_args__ = (
        db.CheckConstraint(
            "stato IN ('IN_USO', 'NON_IN_USO', 'IN_TARATURA')",
            name="ck_strumenti_misura_stato",
        ),
        db.Index("ix_strumenti_misura_stato", "stato"),
        db.Index("ix_strumenti_misura_reparto", "reparto_codice"),
        db.Index("ix_strumenti_misura_tipologia", "tipologia_id"),
    )

    @validates("codice_interno", "numero_seriale")
    def validate_identifier(self, key, value):
        return _required(value, key).upper()

    @validates("descrizione", "costruttore", "reparto_codice")
    def validate_required_text(self, key, value):
        return _required(value, key)

    @validates("stato")
    def validate_stato(self, key, value):
        normalized = _required(value, key).upper()
        if normalized not in STATI_STRUMENTO:
            raise ValueError("Stato strumento non valido.")
        return normalized


class SpedizioneTaratura(db.Model):
    __bind_key__ = TARATURE_BIND_KEY
    __tablename__ = "spedizioni_taratura"

    id = db.Column(db.Integer, primary_key=True)
    data_spedizione = db.Column(db.Date, nullable=False)
    laboratorio = db.Column(db.Text, nullable=False)
    note = db.Column(db.Text)
    created_by_public_id = db.Column(db.Text)
    created_by_username = db.Column(db.Text)
    created_at = db.Column(db.DateTime, nullable=False, default=_now_rome)

    strumenti = db.relationship(
        "SpedizioneTaraturaStrumento",
        back_populates="spedizione",
        lazy="selectin",
        cascade="all, delete-orphan",
        order_by="SpedizioneTaraturaStrumento.strumento_id",
    )

    @property
    def numero(self) -> str:
        return f"SP-{self.id:05d}" if self.id else "SP-NUOVA"

    @property
    def chiusa(self) -> bool:
        return bool(self.strumenti) and all(row.evento_id for row in self.strumenti)

    @validates("laboratorio")
    def validate_laboratorio(self, key, value):
        return _required(value, key)


class SpedizioneTaraturaStrumento(db.Model):
    __bind_key__ = TARATURE_BIND_KEY
    __tablename__ = "spedizioni_taratura_strumenti"

    spedizione_id = db.Column(
        db.Integer,
        db.ForeignKey("spedizioni_taratura.id", ondelete="CASCADE"),
        primary_key=True,
    )
    strumento_id = db.Column(
        db.Integer,
        db.ForeignKey("strumenti_misura.id", ondelete="RESTRICT"),
        primary_key=True,
    )
    evento_id = db.Column(
        db.Integer,
        db.ForeignKey("eventi_taratura.id", ondelete="SET NULL"),
        unique=True,
    )
    stato_precedente = db.Column(db.String(20), nullable=False)
    chiuso_at = db.Column(db.DateTime)

    spedizione = db.relationship("SpedizioneTaratura", back_populates="strumenti")
    strumento = db.relationship("StrumentoMisura", back_populates="spedizioni")
    evento = db.relationship("EventoTaratura", foreign_keys=[evento_id])


class EventoTaratura(db.Model):
    __bind_key__ = TARATURE_BIND_KEY
    __tablename__ = "eventi_taratura"

    id = db.Column(db.Integer, primary_key=True)
    strumento_id = db.Column(
        db.Integer,
        db.ForeignKey("strumenti_misura.id", ondelete="RESTRICT"),
        nullable=False,
    )
    tipo = db.Column(db.String(20), nullable=False)
    data_evento = db.Column(db.Date, nullable=False)
    esito = db.Column(db.String(20), nullable=False)
    rapporto_riferimento = db.Column(db.Text)
    note = db.Column(db.Text)
    certificato_nome = db.Column(db.Text)
    certificato_file = db.Column(db.Text)
    registrato_da_public_id = db.Column(db.Text)
    registrato_da_username = db.Column(db.Text)
    created_at = db.Column(db.DateTime, nullable=False, default=_now_rome)

    strumento = db.relationship("StrumentoMisura", back_populates="eventi")

    __table_args__ = (
        db.CheckConstraint(
            "tipo IN ('INIZIALE', 'INTERNA', 'ESTERNA')",
            name="ck_eventi_taratura_tipo",
        ),
        db.CheckConstraint(
            "esito IN ('CONFORME', 'NON_CONFORME')",
            name="ck_eventi_taratura_esito",
        ),
        db.Index("ix_eventi_taratura_strumento", "strumento_id"),
        db.Index("ix_eventi_taratura_data", "data_evento"),
    )


class TaraturaLog(db.Model):
    __bind_key__ = TARATURE_BIND_KEY
    __tablename__ = "tarature_log"

    id = db.Column(db.Integer, primary_key=True)
    evento = db.Column(db.String(60), nullable=False)
    entita = db.Column(db.String(40), nullable=False)
    entita_id = db.Column(db.Integer)
    dettaglio = db.Column(db.Text, nullable=False)
    utente_public_id = db.Column(db.Text)
    utente_username = db.Column(db.Text)
    created_at = db.Column(db.DateTime, nullable=False, default=_now_rome)

    __table_args__ = (
        db.Index("ix_tarature_log_created_at", "created_at"),
        db.Index("ix_tarature_log_entita", "entita", "entita_id"),
    )
