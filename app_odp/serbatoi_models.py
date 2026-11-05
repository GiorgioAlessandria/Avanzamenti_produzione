"""Archivio operativo persistente; nessuna FK verso le cache ERP/ODP eliminabili."""
from app_odp.models import db


class SerbatoiArticolo(db.Model):
    __tablename__ = "serbatoi_articoli"
    id = db.Column(db.Integer, primary_key=True)
    codice_oss = db.Column(db.String(120), nullable=False, unique=True)
    codice_lav = db.Column(db.String(120), nullable=False, unique=True)
    codice_spezzone = db.Column(db.String(120), nullable=False)
    descrizione = db.Column(db.String(300), nullable=False)
    fase_inizio = db.Column(db.String(20), nullable=True)
    fase_fine = db.Column(db.String(20), nullable=True)
    attivo = db.Column(db.Boolean, nullable=False, default=True)


class SerbatoiCliente(db.Model):
    __tablename__ = "serbatoi_clienti_ordini"
    id = db.Column(db.Integer, primary_key=True)
    id_documento = db.Column(db.Text, nullable=False)
    id_riga = db.Column(db.Text, nullable=False)
    riferimento = db.Column(db.Text, nullable=False)
    cliente = db.Column(db.Text, nullable=False)
    articolo_id = db.Column(db.Integer, db.ForeignKey("serbatoi_articoli.id"), index=True, nullable=False)
    quantita = db.Column(db.Integer, nullable=False)
    saldo_erp = db.Column(db.Integer, nullable=False)
    consegna = db.Column(db.Text, nullable=False, default="")
    anomalia = db.Column(db.Text, nullable=False, default="")
    __table_args__ = (db.UniqueConstraint("id_documento", "id_riga"), db.CheckConstraint("quantita > 0"))


class SerbatoiProduzione(db.Model):
    __tablename__ = "serbatoi_produzioni"
    id = db.Column(db.Integer, primary_key=True)
    id_documento = db.Column(db.Text, nullable=False)
    id_riga = db.Column(db.Text, nullable=False)
    riferimento = db.Column(db.Text, nullable=False)
    riferimento_stock = db.Column(db.String(40), nullable=False, unique=True)
    articolo_id = db.Column(db.Integer, db.ForeignKey("serbatoi_articoli.id"), index=True, nullable=False)
    quantita = db.Column(db.Integer, nullable=False)
    fase_inizio = db.Column(db.String(20), nullable=False)
    fase_fine = db.Column(db.String(20), nullable=False)
    avviata = db.Column(db.Boolean, nullable=False, default=False)
    anomalia = db.Column(db.Text, nullable=False, default="")
    __table_args__ = (db.UniqueConstraint("id_documento", "id_riga"), db.CheckConstraint("quantita > 0"))


class SerbatoiImpegno(db.Model):
    __tablename__ = "serbatoi_impegni"
    id = db.Column(db.Integer, primary_key=True)
    cliente_id = db.Column(db.Integer, db.ForeignKey("serbatoi_clienti_ordini.id"), index=True, nullable=False)
    produzione_id = db.Column(db.Integer, db.ForeignKey("serbatoi_produzioni.id"), index=True, nullable=False)
    quantita = db.Column(db.Integer, nullable=False)
    realizzata = db.Column(db.Integer, nullable=False, default=0)
    __table_args__ = (db.CheckConstraint("quantita > 0 AND realizzata >= 0 AND realizzata <= quantita"),)


class SerbatoiBarre(db.Model):
    __tablename__ = "serbatoi_ordini_barre"
    id = db.Column(db.Integer, primary_key=True)
    fornitore_tipo = db.Column(db.Text, nullable=False)
    fornitore_codice = db.Column(db.Text, nullable=False)
    fornitore_nome = db.Column(db.Text, nullable=False)
    riferimento = db.Column(db.Text, nullable=False)
    codice = db.Column(db.String(120), nullable=False)
    lunghezza_mm = db.Column(db.Integer, nullable=False)
    quantita = db.Column(db.Integer, nullable=False)
    __table_args__ = (db.CheckConstraint("quantita > 0 AND lunghezza_mm > 0"),)


class SerbatoiLotto(db.Model):
    __tablename__ = "serbatoi_lotti"
    id = db.Column(db.Integer, primary_key=True)
    riferimento = db.Column(db.String(50), nullable=False, unique=True)
    tipo = db.Column(db.String(20), nullable=False)
    codice = db.Column(db.String(120), nullable=False)
    articolo_id = db.Column(db.Integer, db.ForeignKey("serbatoi_articoli.id"), index=True, nullable=True)
    stato = db.Column(db.String(30), nullable=False)
    ubicazione = db.Column(db.String(60), nullable=False, default="Bernardi")
    quantita = db.Column(db.Integer, nullable=False)
    iniziale = db.Column(db.Integer, nullable=False)
    padre_id = db.Column(db.Integer, db.ForeignKey("serbatoi_lotti.id"), index=True, nullable=True)
    produzione_id = db.Column(db.Integer, db.ForeignKey("serbatoi_produzioni.id"), index=True, nullable=True)
    cliente_id = db.Column(db.Integer, db.ForeignKey("serbatoi_clienti_ordini.id"), index=True, nullable=True)
    barre_id = db.Column(db.Integer, db.ForeignKey("serbatoi_ordini_barre.id"), index=True, nullable=True)
    origine = db.Column(db.Text, nullable=False, default="")
    __table_args__ = (db.CheckConstraint("quantita >= 0 AND iniziale > 0"),)


class SerbatoiConsumo(db.Model):
    __tablename__ = "serbatoi_consumi"
    id = db.Column(db.Integer, primary_key=True)
    produzione_id = db.Column(db.Integer, db.ForeignKey("serbatoi_produzioni.id"), index=True, nullable=False)
    lotto_id = db.Column(db.Integer, db.ForeignKey("serbatoi_lotti.id"), index=True, nullable=False)
    quantita = db.Column(db.Integer, nullable=False)
    __table_args__ = (db.CheckConstraint("quantita > 0"),)


class SerbatoiMovimento(db.Model):
    __tablename__ = "serbatoi_movimenti"
    id = db.Column(db.Integer, primary_key=True)
    tipo = db.Column(db.String(30), nullable=False)
    data = db.Column(db.Text, nullable=False)
    operatore = db.Column(db.Text, nullable=False)
    quantita = db.Column(db.Integer, nullable=False)
    origine_id = db.Column(db.Integer, db.ForeignKey("serbatoi_lotti.id"), index=True, nullable=True)
    destinazione_id = db.Column(db.Integer, db.ForeignKey("serbatoi_lotti.id"), index=True, nullable=True)
    produzione_id = db.Column(db.Integer, db.ForeignKey("serbatoi_produzioni.id"), index=True, nullable=True)
    ddt = db.Column(db.String(60), nullable=False, default="")
    note = db.Column(db.Text, nullable=False, default="")


class SerbatoiEvento(db.Model):
    __tablename__ = "serbatoi_eventi_lct"
    id = db.Column(db.Integer, primary_key=True)
    chiave = db.Column(db.String(250), nullable=False, unique=True)
    produzione_id = db.Column(db.Integer, db.ForeignKey("serbatoi_produzioni.id"), index=True, nullable=False)
    dati = db.Column(db.Text, nullable=False)


class SerbatoiRichiesta(db.Model):
    __tablename__ = "serbatoi_richieste"
    chiave = db.Column(db.String(64), primary_key=True)
    dati = db.Column(db.String(64), nullable=False)
