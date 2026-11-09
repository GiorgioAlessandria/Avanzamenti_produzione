"""Archivio operativo persistente; nessuna FK verso le cache ERP/ODP eliminabili."""
from app_odp.models import db


class SerbatoiParametri(db.Model):
    __tablename__ = "serbatoi_parametri"
    id = db.Column(db.Integer, primary_key=True)
    fornitore_barre_tipo = db.Column(db.Text, nullable=False, default="")
    fornitore_barre_codice = db.Column(db.Text, nullable=False, default="")
    codice_barre = db.Column(db.String(120), nullable=False, default="")
    unita_barre = db.Column(db.String(20), nullable=False, default="M")
    lunghezza_barre_mm = db.Column(db.Integer, nullable=False, default=6000)
    fornitore_spezzoni_tipo = db.Column(db.Text, nullable=False, default="")
    fornitore_spezzoni_codice = db.Column(db.Text, nullable=False, default="")
    codici_spezzoni = db.Column(db.JSON, nullable=False, default=list)
    sfrido_mm = db.Column(db.Integer, nullable=False, default=0)
    __table_args__ = (db.CheckConstraint("id = 1 AND lunghezza_barre_mm > 0 AND sfrido_mm >= 0"),)


class SerbatoiTempistiche(db.Model):
    __tablename__ = "serbatoi_tempistiche"
    id = db.Column(db.Integer, primary_key=True)
    giorni = db.Column(db.JSON, nullable=False)
    lotti = db.Column(db.JSON, nullable=False)
    __table_args__ = (db.CheckConstraint("id = 1"),)


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


class SerbatoiRettificaVista(db.Model):
    __tablename__ = "serbatoi_rettifiche_vista"
    produzione_id = db.Column(db.Integer, db.ForeignKey("serbatoi_produzioni.id"), primary_key=True)
    operatore = db.Column(db.Text, nullable=False)
    data = db.Column(db.Text, nullable=False)
    note = db.Column(db.Text, nullable=False)


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


class SerbatoiBarreOrigine(db.Model):
    __tablename__ = "serbatoi_barre_origini"
    id_documento = db.Column(db.Text, primary_key=True)
    id_riga = db.Column(db.Text, primary_key=True)
    barre_id = db.Column(db.Integer, db.ForeignKey("serbatoi_ordini_barre.id"), unique=True, nullable=False)
    quantita_erp = db.Column(db.Integer, nullable=False)
    quantita_originale = db.Column(db.Text, nullable=False, default="")
    unita_originale = db.Column(db.String(20), nullable=False, default="")


class SerbatoiBarreManuale(db.Model):
    __tablename__ = "serbatoi_barre_manuali"
    barre_id = db.Column(db.Integer, db.ForeignKey("serbatoi_ordini_barre.id"), primary_key=True)
    data = db.Column(db.Text, nullable=False)
    operatore = db.Column(db.Text, nullable=False)
    note = db.Column(db.Text, nullable=False)


class SerbatoiPrezzoBarre(db.Model):
    __tablename__ = "serbatoi_prezzi_barre"
    barre_id = db.Column(db.Integer, db.ForeignKey("serbatoi_ordini_barre.id"), primary_key=True)
    centesimi_kg = db.Column(db.Integer, nullable=False)
    data_prezzo = db.Column(db.Date, nullable=False)
    __table_args__ = (db.CheckConstraint("centesimi_kg > 0"),)


class SerbatoiRiferimentoBarre(db.Model):
    __tablename__ = "serbatoi_riferimenti_prezzo_barre"
    fornitore_tipo = db.Column(db.Text, primary_key=True)
    fornitore_codice = db.Column(db.Text, primary_key=True)
    codice = db.Column(db.String(120), primary_key=True)
    centesimi_kg = db.Column(db.Integer, nullable=False)
    soglia_centesimi_percento = db.Column(db.Integer, nullable=False)
    __table_args__ = (db.CheckConstraint("centesimi_kg > 0 AND soglia_centesimi_percento > 0"),)


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


class SerbatoiPianoCQ(db.Model):
    __tablename__ = "serbatoi_piani_cq"
    codice = db.Column(db.String(120), primary_key=True)
    revisione = db.Column(db.String(40), nullable=False)
    controlli = db.Column(db.JSON, nullable=False)


class SerbatoiControlloCQ(db.Model):
    __tablename__ = "serbatoi_controlli_cq"
    id = db.Column(db.Integer, primary_key=True)
    fase = db.Column(db.String(20), nullable=False)
    produzione_id = db.Column(db.Integer, db.ForeignKey("serbatoi_produzioni.id"), index=True)
    cliente_id = db.Column(db.Integer, db.ForeignKey("serbatoi_clienti_ordini.id"), index=True)
    lotto_id = db.Column(db.Integer, db.ForeignKey("serbatoi_lotti.id"), unique=True)
    lotto_ko_id = db.Column(db.Integer, db.ForeignKey("serbatoi_lotti.id"))
    operazione = db.Column(db.String(128), unique=True)
    piano = db.Column(db.JSON, nullable=False)
    risultati = db.Column(db.JSON)
    quantita = db.Column(db.Integer, nullable=False)
    ko = db.Column(db.Integer)
    operatore = db.Column(db.Text)
    data = db.Column(db.Text)
    contabilizzato_da = db.Column(db.Text)
    contabilizzato_il = db.Column(db.Text)
    __table_args__ = (db.CheckConstraint("quantita > 0 AND (ko IS NULL OR (ko >= 0 AND ko <= quantita))"),)
