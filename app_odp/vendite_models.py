from datetime import datetime
from zoneinfo import ZoneInfo

from app_odp.models import db


ROME_TZ = ZoneInfo("Europe/Rome")
VENDITE_INTERNAL_REFERENCES = ("ITALIA", "ESTERO", "EXTRACEE")
VENDITE_DEFAULT_PACKAGING_NOTES = {
    "ITALIA": "",
    "ESTERO": "",
    "EXTRACEE": "Inserire sacchetto anti-umidità",
}


def _rome_iso_now() -> str:
    return datetime.now(ROME_TZ).isoformat(timespec="seconds")


class VenditeRaggruppamento(db.Model):
    __tablename__ = "vendite_raggruppamenti"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    nome = db.Column(db.String(80), nullable=False)
    nome_chiave = db.Column(db.String(160), nullable=False, unique=True)
    famiglie = db.Column(db.JSON, nullable=False, default=list)
    versione = db.Column(db.Integer, nullable=False, default=1)

    __mapper_args__ = {"version_id_col": versione}


class VenditeNotaProduzioneMacchina(db.Model):
    __tablename__ = "vendite_note_produzione_macchina"

    # Indipendente dagli ODP, che la sincronizzazione può eliminare.
    matricola = db.Column(db.String(200), primary_key=True)
    note = db.Column(db.String(1000), nullable=False, default="")
    versione = db.Column(db.Integer, nullable=False, default=1)

    __mapper_args__ = {"version_id_col": versione}


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
    riferimento_interno = db.Column(
        db.String(20),
        nullable=False,
        default="ITALIA",
        index=True,
    )
    data_spedizione = db.Column(db.Date, nullable=True, index=True)
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
    confermato_il = db.Column(db.Text, nullable=True)
    confermato_da_nome = db.Column(db.String(120), nullable=True)

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
    note_commerciali = db.Column(db.String(1000), nullable=True)
    note_produzione = db.Column(db.String(1000), nullable=True)
    note_per_produzione = db.Column(db.String(1000), nullable=True)
    note_spedizione = db.Column(db.String(1000), nullable=True)
    data_disponibile = db.Column(db.Date, nullable=True, index=True)
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
    assegnazione_automatica = db.Column(
        db.Boolean,
        nullable=False,
        default=False,
    )

    ordine_cliente = db.relationship(
        "VenditeOrdineCliente",
        back_populates="righe",
    )
    spedizione = db.relationship(
        "VenditeSpedizioneConfermata",
        back_populates="riga_ordine_cliente",
        cascade="all, delete-orphan",
        passive_deletes=True,
        uselist=False,
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


class VenditeMacchinaStock(db.Model):
    __tablename__ = "vendite_macchine_stock"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    odp_id_documento = db.Column(db.Text, nullable=False)
    odp_id_riga = db.Column(db.Text, nullable=False)
    odp_rif_registraz = db.Column(db.Text, nullable=True)
    odp_num_progr_riga = db.Column(db.Text, nullable=True)
    modello_codice = db.Column(db.String(160), nullable=False, index=True)
    modello_variante = db.Column(db.String(120), nullable=False, default="")
    modello_descrizione = db.Column(db.String(500), nullable=True)
    matricola = db.Column(db.String(6), nullable=False, unique=True, index=True)
    inserita_il = db.Column(
        db.Text,
        nullable=False,
        default=_rome_iso_now,
        index=True,
    )
    inserita_da_nome = db.Column(db.String(120), nullable=False)

    __table_args__ = (
        db.CheckConstraint(
            "length(matricola) = 6 AND matricola NOT GLOB '*[^0-9]*'",
            name="ck_vendite_macchina_stock_matricola",
        ),
        db.UniqueConstraint(
            "odp_id_documento",
            "odp_id_riga",
            name="uq_vendite_macchina_stock_odp",
        ),
    )


class VenditeImballoMacchina(db.Model):
    __tablename__ = "vendite_imballi_macchina"

    # La conferma segue la matricola anche quando passa da STOCK a un ordine cliente.
    matricola = db.Column(db.String(200), primary_key=True)
    confermata_il = db.Column(db.Text, nullable=False, default=_rome_iso_now)
    confermata_da_id = db.Column(
        db.Integer,
        db.ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    confermata_da_nome = db.Column(db.String(120), nullable=False)


class VenditeSpedizioneConfermata(db.Model):
    __tablename__ = "vendite_spedizioni_confermate"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    ordine_cliente_riga_id = db.Column(
        db.Integer,
        db.ForeignKey("vendite_ordini_cliente_righe.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True,
    )
    cliente_nome = db.Column(db.String(160), nullable=False)
    numero_ordine = db.Column(db.String(120), nullable=False)
    riferimento_interno = db.Column(db.String(20), nullable=False)
    data_spedizione = db.Column(db.Date, nullable=True)
    data_disponibile = db.Column(db.Date, nullable=True)
    ordine_cliente_creato_il = db.Column(db.Text, nullable=False)
    ordine_cliente_creato_da_nome = db.Column(db.String(120), nullable=False)
    posizione = db.Column(db.Integer, nullable=False)
    modello_codice = db.Column(db.String(160), nullable=False)
    modello_variante = db.Column(db.String(120), nullable=False, default="")
    modello_descrizione = db.Column(db.String(500), nullable=True)
    data_consegna = db.Column(db.Date, nullable=False)
    note_vendita = db.Column(db.String(1000), nullable=True)
    note_produzione = db.Column(db.String(1000), nullable=True)
    note_spedizione = db.Column(db.String(1000), nullable=True)
    odp_id_documento = db.Column(db.Text, nullable=False)
    odp_id_riga = db.Column(db.Text, nullable=False)
    odp_rif_registraz = db.Column(db.Text, nullable=True)
    odp_num_progr_riga = db.Column(db.Text, nullable=True)
    odp_matricola = db.Column(db.String(200), nullable=False)
    assegnata_il = db.Column(db.Text, nullable=True)
    assegnata_da_nome = db.Column(db.String(120), nullable=True)
    assegnazione_automatica = db.Column(db.Boolean, nullable=False, default=False)
    confermata_il = db.Column(
        db.Text,
        nullable=False,
        default=_rome_iso_now,
        index=True,
    )
    confermata_da_id = db.Column(
        db.Integer,
        db.ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    confermata_da_nome = db.Column(db.String(120), nullable=False)

    riga_ordine_cliente = db.relationship(
        "VenditeOrdineClienteRiga",
        back_populates="spedizione",
    )


class VenditeNotaImballaggio(db.Model):
    __tablename__ = "vendite_note_imballaggio"

    riferimento_interno = db.Column(db.String(20), primary_key=True)
    note = db.Column(db.String(1000), nullable=True)
    aggiornato_il = db.Column(db.Text, nullable=True)
    aggiornato_da_id = db.Column(
        db.Integer,
        db.ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    aggiornato_da_nome = db.Column(db.String(120), nullable=True)
