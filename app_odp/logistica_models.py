from __future__ import annotations

from types import SimpleNamespace

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


class ClientePackingList(db.Model):
    __bind_key__ = LOGISTICA_BIND_KEY
    __tablename__ = "packing_clienti"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    nome = db.Column(
        db.String(160, collation="NOCASE"),
        nullable=False,
        index=True,
    )
    indirizzo = db.Column(db.String(300), nullable=False)
    provincia = db.Column(db.String(100), nullable=False)
    paese = db.Column(db.String(100), nullable=False)
    creato_il = db.Column(
        db.DateTime,
        nullable=False,
        server_default=db.func.current_timestamp(),
    )

    packing_lists = db.relationship(
        "PackingList",
        back_populates="cliente",
        lazy="select",
    )


class PackingList(db.Model):
    __bind_key__ = LOGISTICA_BIND_KEY
    __tablename__ = "packing_lists"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    cliente_id = db.Column(
        db.Integer,
        db.ForeignKey("packing_clienti.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    transport_document = db.Column(db.String(120), nullable=False)
    invoice_number = db.Column(db.String(120), nullable=False)
    invoice_date = db.Column(db.Date, nullable=False, index=True)
    total_pallets = db.Column(db.Integer, nullable=False)
    total_net_weight = db.Column(db.Numeric(12, 3), nullable=False)
    total_gross_weight = db.Column(db.Numeric(12, 3), nullable=False)
    comments = db.Column(db.String(2000), nullable=True)
    delivery_nome = db.Column(db.String(160), nullable=True)
    delivery_indirizzo = db.Column(db.String(300), nullable=True)
    delivery_provincia = db.Column(db.String(100), nullable=True)
    delivery_paese = db.Column(db.String(100), nullable=True)
    delivery_terms = db.Column(db.String(200), nullable=False)
    forwarder = db.Column(db.String(200), nullable=False)
    creato_il = db.Column(
        db.DateTime,
        nullable=False,
        server_default=db.func.current_timestamp(),
        index=True,
    )
    creato_da_id = db.Column(db.Integer, nullable=True)
    creato_da_nome = db.Column(db.String(120), nullable=False)

    cliente = db.relationship(
        "ClientePackingList",
        back_populates="packing_lists",
        lazy="joined",
    )
    righe = db.relationship(
        "RigaPackingList",
        back_populates="packing_list",
        cascade="all, delete-orphan",
        lazy="selectin",
        order_by="RigaPackingList.posizione",
    )

    @property
    def delivery(self):
        return SimpleNamespace(
            nome=self.delivery_nome or self.cliente.nome,
            indirizzo=self.delivery_indirizzo or self.cliente.indirizzo,
            provincia=self.delivery_provincia or self.cliente.provincia,
            paese=self.delivery_paese or self.cliente.paese,
        )

    __table_args__ = (
        db.CheckConstraint(
            "total_pallets >= 0",
            name="ck_packing_lists_total_pallets",
        ),
        db.CheckConstraint(
            "total_net_weight >= 0",
            name="ck_packing_lists_total_net_weight",
        ),
        db.CheckConstraint(
            "total_gross_weight >= total_net_weight",
            name="ck_packing_lists_total_gross_weight",
        ),
    )


class RigaPackingList(db.Model):
    __bind_key__ = LOGISTICA_BIND_KEY
    __tablename__ = "packing_list_righe"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    packing_list_id = db.Column(
        db.Integer,
        db.ForeignKey("packing_lists.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    posizione = db.Column(db.Integer, nullable=False)
    codice = db.Column(db.String(120), nullable=False)
    descrizione = db.Column(db.String(500), nullable=False)
    numero_seriale = db.Column(db.String(200), nullable=True)
    quantita = db.Column(db.Numeric(12, 3), nullable=False)

    packing_list = db.relationship(
        "PackingList",
        back_populates="righe",
    )

    __table_args__ = (
        db.CheckConstraint(
            "posizione > 0",
            name="ck_packing_list_righe_posizione",
        ),
        db.CheckConstraint(
            "quantita > 0",
            name="ck_packing_list_righe_quantita",
        ),
        db.UniqueConstraint(
            "packing_list_id",
            "posizione",
            name="uq_packing_list_righe_posizione",
        ),
    )
