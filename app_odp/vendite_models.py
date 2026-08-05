from datetime import datetime
from zoneinfo import ZoneInfo

from app_odp.models import db


ROME_TZ = ZoneInfo("Europe/Rome")


def _rome_iso_now() -> str:
    return datetime.now(ROME_TZ).isoformat(timespec="seconds")


class VenditeOrdineCliente(db.Model):
    __tablename__ = "vendite_ordini_cliente"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    cliente_nome = db.Column(
        db.String(160, collation="NOCASE"),
        nullable=False,
    )
    cliente_chiave = db.Column(db.String(320), nullable=False)
    numero_ordine = db.Column(
        db.String(120, collation="NOCASE"),
        nullable=False,
    )
    numero_ordine_chiave = db.Column(db.String(240), nullable=False)
    creato_il = db.Column(
        db.Text,
        nullable=False,
        default=_rome_iso_now,
        index=True,
    )
    creato_da_id = db.Column(
        db.Integer,
        db.ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    creato_da_nome = db.Column(db.String(120), nullable=False)

    righe = db.relationship(
        "VenditeOrdineClienteRiga",
        back_populates="ordine_cliente",
        cascade="all, delete-orphan",
        lazy="selectin",
        order_by="VenditeOrdineClienteRiga.posizione",
        passive_deletes=True,
    )

    __table_args__ = (
        db.UniqueConstraint(
            "cliente_chiave",
            "numero_ordine_chiave",
            name="uq_vendite_ordine_cliente_numero",
        ),
    )


class VenditeOrdineClienteRiga(db.Model):
    __tablename__ = "vendite_ordini_cliente_righe"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    ordine_cliente_id = db.Column(
        db.Integer,
        db.ForeignKey("vendite_ordini_cliente.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    posizione = db.Column(db.Integer, nullable=False)
    modello_codice = db.Column(db.String(160), nullable=False, index=True)
    modello_variante = db.Column(db.String(120), nullable=False, default="")
    modello_descrizione = db.Column(db.String(500), nullable=True)
    note = db.Column(db.String(1000), nullable=True)
    data_consegna = db.Column(db.Date, nullable=False, index=True)
    versione = db.Column(db.Integer, nullable=False, default=1)

    # Snapshot dell'ODP: nessuna FK, perché la sincronizzazione elimina gli ODP chiusi.
    odp_id_documento = db.Column(db.Text, nullable=True)
    odp_id_riga = db.Column(db.Text, nullable=True)
    odp_rif_registraz = db.Column(db.Text, nullable=True)
    odp_num_progr_riga = db.Column(db.Text, nullable=True)
    odp_matricola = db.Column(db.String(200), nullable=True)
    assegnata_il = db.Column(db.Text, nullable=True)
    assegnata_da_id = db.Column(
        db.Integer,
        db.ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    assegnata_da_nome = db.Column(db.String(120), nullable=True)

    ordine_cliente = db.relationship(
        "VenditeOrdineCliente",
        back_populates="righe",
    )

    __table_args__ = (
        db.CheckConstraint(
            "posizione > 0",
            name="ck_vendite_ordine_cliente_riga_posizione",
        ),
        db.CheckConstraint(
            "versione > 0",
            name="ck_vendite_ordine_cliente_riga_versione",
        ),
        db.CheckConstraint(
            "((odp_id_documento IS NULL AND odp_id_riga IS NULL) OR "
            "(odp_id_documento IS NOT NULL AND odp_id_riga IS NOT NULL))",
            name="ck_vendite_ordine_cliente_riga_odp",
        ),
        db.UniqueConstraint(
            "ordine_cliente_id",
            "posizione",
            name="uq_vendite_ordine_cliente_riga_posizione",
        ),
        db.UniqueConstraint(
            "odp_id_documento",
            "odp_id_riga",
            name="uq_vendite_ordine_cliente_riga_odp",
        ),
    )

    __mapper_args__ = {"version_id_col": versione}
