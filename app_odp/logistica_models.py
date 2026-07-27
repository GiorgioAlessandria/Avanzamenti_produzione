from __future__ import annotations

from app_odp.models import db


LOGISTICA_BIND_KEY = "logistica"


class VettoreTrasporto(db.Model):
    __bind_key__ = LOGISTICA_BIND_KEY
    __tablename__ = "vettori"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    nome = db.Column(
        db.String(120, collation="NOCASE"),
        nullable=False,
        unique=True,
    )

    movimenti = db.relationship(
        "MovimentoLogistico",
        back_populates="vettore",
        lazy="select",
    )


class MovimentoLogistico(db.Model):
    __bind_key__ = LOGISTICA_BIND_KEY
    __tablename__ = "movimenti"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    vettore_id = db.Column(
        db.Integer,
        db.ForeignKey("vettori.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    movimento = db.Column(db.String(10), nullable=False)
    tipologia = db.Column(db.String(10), nullable=False)
    controparte = db.Column(db.String(160), nullable=False)
    data = db.Column(db.Date, nullable=False, index=True)
    materiale = db.Column(db.String(300), nullable=False)
    note = db.Column(db.String(1000), nullable=True)
    sollecitato_il = db.Column(db.DateTime, nullable=True)
    completato_il = db.Column(db.DateTime, nullable=True, index=True)
    completato_da_id = db.Column(db.Integer, nullable=True)
    completato_da_nome = db.Column(db.String(120), nullable=True)
    creato_il = db.Column(
        db.DateTime,
        nullable=False,
        server_default=db.func.current_timestamp(),
    )
    creato_da_id = db.Column(db.Integer, nullable=True)
    creato_da_nome = db.Column(db.String(120), nullable=False)

    vettore = db.relationship(
        "VettoreTrasporto",
        back_populates="movimenti",
        lazy="joined",
    )

    __table_args__ = (
        db.CheckConstraint(
            "movimento IN ('CARICO', 'SCARICO')",
            name="ck_movimenti_movimento",
        ),
        db.CheckConstraint(
            "tipologia IN ('CLIENTE', 'FORNITORE')",
            name="ck_movimenti_tipologia",
        ),
    )
