from __future__ import annotations

from app_odp.models import db


RIFIUTI_BIND_KEY = "rifiuti"

STATI_RIFIUTO = {
    "PRESENTE",
    "SMALTITO",
}


class RifiutiCer(db.Model):
    """
    Anagrafica dei codici CER disponibili nel modulo rifiuti.
    """

    __bind_key__ = RIFIUTI_BIND_KEY
    __tablename__ = "rifiuti_cer"

    id = db.Column(
        db.Integer,
        primary_key=True,
        autoincrement=True,
    )

    codice = db.Column(
        db.Text,
        nullable=False,
        index=True,
    )

    descrizione = db.Column(
        db.Text,
        nullable=False,
    )

    attivo = db.Column(
        db.Boolean,
        nullable=False,
        default=True,
        server_default=db.text("1"),
        index=True,
    )

    creato_il = db.Column(
        db.Text,
        nullable=False,
        server_default=db.text("CURRENT_TIMESTAMP"),
    )

    aggiornato_il = db.Column(
        db.Text,
        nullable=True,
    )

    carichi = db.relationship(
        "RifiutiCarico",
        back_populates="cer",
        lazy="select",
    )

    __table_args__ = (
        db.UniqueConstraint(
            "codice", "descrizione",
            name="uq_rifiuti_cer_codice_descrizione",
        ),
    )

    def __repr__(self) -> str:
        return f"<RifiutiCer {self.codice!r}>"


class RifiutiCarico(db.Model):
    """
    Singolo carico di materiale inserito nello stock virtuale.
    In questa prima fase vengono gestiti solamente i record PRESENTE.
    """

    __bind_key__ = RIFIUTI_BIND_KEY
    __tablename__ = "rifiuti_carichi"

    id = db.Column(
        db.Integer,
        primary_key=True,
        autoincrement=True,
    )

    cer_id = db.Column(
        db.Integer,
        db.ForeignKey(
            "rifiuti_cer.id",
            onupdate="CASCADE",
            ondelete="RESTRICT",
        ),
        nullable=False,
        index=True,
    )

    peso_kg = db.Column(
        db.Numeric(12, 3),
        nullable=False,
    )

    stato = db.Column(
        db.Text,
        nullable=False,
        default="PRESENTE",
        server_default=db.text("'PRESENTE'"),
        index=True,
    )

    caricato_il = db.Column(
        db.Text,
        nullable=False,
        server_default=db.text("CURRENT_TIMESTAMP"),
        index=True,
    )

    caricato_da_id = db.Column(
        db.Integer,
        nullable=True,
    )

    caricato_da_nome = db.Column(
        db.Text,
        nullable=False,
    )

    smaltito_il = db.Column(
        db.Text,
        nullable=True,
        index=True,
    )

    smaltito_da_id = db.Column(
        db.Integer,
        nullable=True,
    )

    smaltito_da_nome = db.Column(
        db.Text,
        nullable=True,
    )

    note = db.Column(
        db.Text,
        nullable=True,
    )

    cer = db.relationship(
        "RifiutiCer",
        back_populates="carichi",
        lazy="joined",
    )

    __table_args__ = (
        db.CheckConstraint(
            "peso_kg > 0",
            name="ck_rifiuti_carichi_peso_positivo",
        ),
        db.CheckConstraint(
            "stato IN ('PRESENTE', 'SMALTITO')",
            name="ck_rifiuti_carichi_stato",
        ),
        db.Index(
            "ix_rifiuti_carichi_stato_cer",
            "stato",
            "cer_id",
        ),
    )

    @property
    def presente(self) -> bool:
        return self.stato == "PRESENTE"

    def __repr__(self) -> str:
        return (
            f"<RifiutiCarico id={self.id!r} "
            f"cer_id={self.cer_id!r} peso_kg={self.peso_kg!r}>"
        )
