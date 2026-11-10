import hashlib
from collections import Counter
from io import BytesIO
import json
import re
from zipfile import BadZipFile, ZipFile
from xml.etree.ElementTree import ParseError

from openpyxl import load_workbook
from openpyxl.utils.exceptions import InvalidFileException
from sqlalchemy import update

from app_odp.models import db
from app_odp.serbatoi_models import SerbatoiControlloCQ as Controllo, SerbatoiPianoCQ as Piano
from app_odp.services import serbatoi_service as s

STATO_CQ = "In controllo CQ"
GIACENZA_KO = "CQ da contabilizzare"
CAMPI_CONTROLLO = {"descrizione": 500, "frequenza": 30, "strumento": 200,
                  "valore": 300, "reazione": 300, "note": 300}


def normalizza_controllo(riga, etichetta):
    controllo = {campo: s._norm_text(riga[campo]) for campo in CAMPI_CONTROLLO}
    for campo, massimo in CAMPI_CONTROLLO.items():
        if len(controllo[campo]) > massimo:
            raise s.SerbatoiError(f"{etichetta}: {campo} può contenere al massimo {massimo} caratteri.")
    controllo["descrizione"] = s.testo(controllo["descrizione"], f"{etichetta}: descrizione", 500)
    frequenza = controllo["frequenza"]
    campione = re.fullmatch(r"([1-9]\d{0,5})\s+ogni\s+lotto", frequenza, flags=re.I)
    if frequenza.replace(" ", "") != "100%" and not campione:
        raise s.SerbatoiError(f"{etichetta}: usare 100% o N ogni lotto come frequenza.")
    controllo["campione"] = int(campione[1]) if campione else None
    return controllo


def importa_piano(file):
    if not file or not file.filename.lower().endswith(".xlsx"):
        raise s.SerbatoiError("Selezionare un file .xlsx della procedura controlli.")
    limite = 2_000_000
    dati = file.read(limite + 1)
    if len(dati) > limite:
        raise s.SerbatoiError("Il file Excel supera 2 MB.")
    try:
        with ZipFile(BytesIO(dati)) as archivio:
            if sum(x.file_size for x in archivio.infolist()) > 10_000_000:
                raise s.SerbatoiError("Il file Excel contiene troppi dati.")
        libro = load_workbook(BytesIO(dati), read_only=True, data_only=True)
        try:
            righe = libro.active.iter_rows(values_only=True)
            intestazione = next(righe, ())
            colonne = {s._norm_text(c): i for i, c in enumerate(intestazione)}
            if not {"Codice_C", "Controllo_T", "Frequenza_T"} <= colonne.keys():
                raise s.SerbatoiError("Nell’Excel mancano Codice_C, Controllo_T o Frequenza_T.")
            controlli = []
            identita = set()
            for numero, riga in enumerate(righe, 2):
                if not any(v is not None for v in riga):
                    continue
                def campo(nome):
                    i = colonne.get(nome, len(riga))
                    valore = s._norm_text(riga[i]) if i < len(riga) else ""
                    return "" if valore == "/" else valore

                codice_rev = s.testo(campo("Codice_C"), f"Riga {numero}: codice")
                codice, *rev = re.split(r"\s+-\s+Rev\.?\s+", codice_rev, maxsplit=1, flags=re.I)
                identita.add((codice.strip().upper(), s.testo(rev[0], "Revisione", 40) if rev else ""))
                controlli.append(normalizza_controllo(dict(
                    descrizione=campo("Controllo_T"), frequenza=campo("Frequenza_T"),
                    strumento=campo("Strumento_T")[:200], valore=campo("Valore_T")[:300],
                    reazione=campo("PianoReazione_T")[:300], note=campo("Note_T")[:300],
                ), f"Riga {numero}"))
                if len(controlli) > 50:
                    raise s.SerbatoiError("Inserire al massimo 50 controlli per codice.")
        finally:
            libro.close()
    except (BadZipFile, InvalidFileException, OSError, KeyError, ParseError) as exc:
        raise s.SerbatoiError("Il file Excel non è leggibile.") from exc
    if len(identita) != 1 or not controlli:
        raise s.SerbatoiError("Caricare un piano con un solo codice, una sola revisione e almeno un controllo.")
    codice, revisione = identita.pop()
    if s.articolo_per_codice(codice) is None:
        raise s.SerbatoiError(f"Il codice {codice} non è configurato tra gli articoli Serbatoi attivi.")
    piano = db.session.get(Piano, codice)
    if piano is None:
        piano = Piano(codice=codice)
        db.session.add(piano)
    piano.revisione, piano.controlli = revisione, controlli
    return piano


def piano_codice(codice):
    piano = db.session.get(Piano, s._norm_text(codice).upper())
    if piano is None:
        return None
    contenuto = dict(codice=piano.codice, revisione=piano.revisione, controlli=piano.controlli)
    contenuto["versione"] = hashlib.sha256(json.dumps(contenuto, sort_keys=True).encode()).hexdigest()
    return contenuto


def modifica_piano(codice, dati):
    campi = {nome: dati.getlist(nome) for nome in CAMPI_CONTROLLO}
    numero = len(campi["descrizione"])
    if not 1 <= numero <= 50 or any(len(valori) != numero for valori in campi.values()):
        raise s.SerbatoiError("Il piano deve contenere da 1 a 50 controlli completi.")
    controlli = [
        normalizza_controllo({nome: valori[i] for nome, valori in campi.items()}, f"Controllo {i + 1}")
        for i in range(numero)
    ]
    revisione = s._norm_text(dati.get("revisione"))
    if len(revisione) > 40:
        raise s.SerbatoiError("La revisione può contenere al massimo 40 caratteri.")
    # Il lock impedisce un salvataggio concorrente fra il controllo della versione e la scrittura.
    db.session.execute(update(Piano).where(Piano.codice == codice).values(revisione=Piano.revisione))
    piano = piano_codice(codice)
    if piano is None or piano["versione"] != dati.get("versione"):
        raise s.SerbatoiError("Il piano è stato modificato nel frattempo. Riaprire il piano e riprovare.")
    record = db.session.get(Piano, codice)
    record.revisione, record.controlli = revisione, controlli


def valida_esiti(piano, payload, quantita, ko):
    if ko is None or ko == "":
        raise s.SerbatoiError("Indicare il totale dei pezzi KO.")
    quantita, ko = s.intero(quantita), s.intero(ko, "Totale KO", zero=True)
    if ko > quantita:
        raise s.SerbatoiError("I pezzi KO superano la quantità del lotto.")
    if not isinstance(payload, dict) or payload.get("versione") != piano["versione"]:
        raise s.SerbatoiError("Compilare il piano CQ aggiornato prima di confermare.")
    righe = payload.get("esiti")
    if not isinstance(righe, list) or len(righe) != len(piano["controlli"]):
        raise s.SerbatoiError("Compilare tutti i controlli del piano CQ.")
    risultati = []
    for controllo, riga in zip(piano["controlli"], righe):
        if (not isinstance(riga, dict) or riga.get("eseguito") is not True
                or riga.get("verificati") in (None, "") or riga.get("ko") in (None, "")):
            raise s.SerbatoiError("Confermare ogni controllo e indicare i pezzi verificati e i KO.")
        verificati = s.intero(riga["verificati"], "Pezzi verificati")
        difetti = s.intero(riga["ko"], "Pezzi KO", zero=True)
        minimo = min(quantita, controllo["campione"] or quantita)
        if not minimo <= verificati <= quantita or difetti > verificati:
            raise s.SerbatoiError(f"Verificare da {minimo} a {quantita} pezzi e indicare i KO rilevati.")
        note = s._norm_text(riga.get("note"))
        if len(note) > 300:
            raise s.SerbatoiError("Le note di ciascun controllo possono contenere al massimo 300 caratteri.")
        risultati.append(dict(verificati=verificati, ko=difetti, note=note))
    difetti = [r["ko"] for r in risultati]
    if not max(difetti) <= ko <= sum(difetti):
        raise s.SerbatoiError("Il totale dei pezzi KO deve corrispondere ai KO dei controlli, contando una sola volta ogni pezzo.")
    return risultati


def registra_fresatura(ordine, operazione, piano, esiti, q_ok, q_ko, utente):
    p = s.Produzione.query.filter_by(id_documento=ordine.IdDocumento, id_riga=ordine.IdRiga).one()
    db.session.add(Controllo(
        fase="fresatura", produzione_id=p.id, operazione=operazione, piano=piano, risultati=esiti,
        quantita=int(q_ok + q_ko), ko=int(q_ko), operatore=utente, data=s._now_rome_dt().isoformat(),
    ))


def piano_rientro(lotto):
    if lotto.produzione_id:
        fresatura = Controllo.query.filter_by(produzione_id=lotto.produzione_id, fase="fresatura").order_by(
            Controllo.id.desc()
        ).first()
        if fresatura:
            return piano_codice(fresatura.piano["codice"]) or fresatura.piano
    return piano_codice(lotto.codice)


def accoda_rientro(lotto, piano):
    db.session.add(Controllo(fase="rientro", produzione_id=lotto.produzione_id, cliente_id=lotto.cliente_id,
                             lotto_id=lotto.id, piano=piano or {}, quantita=lotto.quantita))


def piano_controllo_rientro(controllo, lotto):
    return controllo.piano or piano_rientro(lotto)


def conferma_rientro(controllo, payload, ko, utente):
    lotto = s.blocca(s.record(s.Lotto, controllo.lotto_id))
    db.session.refresh(controllo)
    if controllo.fase != "rientro" or controllo.risultati is not None or lotto.stato != STATO_CQ or lotto.quantita != controllo.quantita:
        raise s.SerbatoiError("Questo controllo di rientro non è più in attesa.")
    piano = piano_controllo_rientro(controllo, lotto)
    if not piano:
        raise s.SerbatoiError(f"Manca il piano di controllo per il codice {lotto.codice}.")
    risultati = valida_esiti(piano, payload, controllo.quantita, ko)
    ko = s.intero(ko, "Totale KO", zero=True)
    if ko:
        scarto = s.sposta(lotto, ko, "Scarto", GIACENZA_KO, utente, "CQ_KO", cliente_id=lotto.cliente_id)
        controllo.lotto_ko_id = scarto.id
    if lotto.quantita:
        s.sposta(lotto, lotto.quantita, "Ossidato", "Bernardi", utente, "CQ_OK", cliente_id=lotto.cliente_id)
    controllo.piano = piano
    controllo.risultati, controllo.ko = risultati, ko
    controllo.operatore, controllo.data = utente, s._now_rome_dt().isoformat()


def contabilizza_ko(controllo, utente):
    if controllo.lotto_ko_id is None:
        raise s.SerbatoiError("Nessuna giacenza KO da contabilizzare.")
    lotto = s.blocca(s.record(s.Lotto, controllo.lotto_ko_id))
    db.session.refresh(controllo)
    if controllo.contabilizzato_il:
        return
    quantita = lotto.quantita
    if not quantita or lotto.ubicazione != GIACENZA_KO:
        raise s.SerbatoiError("Giacenza CQ da verificare.")
    lotto.quantita = 0
    controllo.contabilizzato_da, controllo.contabilizzato_il = utente, s._now_rome_dt().isoformat()
    s.movimento("CQ_CONTABILIZZATO", quantita, utente, origine=lotto)


def dati_cqc(cliente):
    rientri = Controllo.query.filter_by(cliente_id=cliente.id, fase="rientro").order_by(Controllo.id).all()
    conclusi = [r for r in rientri if r.risultati is not None]
    lotti_cq = {r.lotto_id: r for r in conclusi}
    conformi_per_odp = Counter()
    for lotto in s.Lotto.query.filter_by(cliente_id=cliente.id, tipo="Materiale").filter(
        s.Lotto.quantita > 0, s.Lotto.stato.in_(("Ossidato", "Spedito"))
    ):
        origine = lotto
        while origine is not None and origine.id not in lotti_cq:
            origine = db.session.get(s.Lotto, origine.padre_id) if origine.padre_id else None
        if origine is not None:
            conformi_per_odp[lotti_cq[origine.id].produzione_id] += lotto.quantita
    if sum(conformi_per_odp.values()) < cliente.quantita:
        raise s.SerbatoiError("La quantità conforme verificata al rientro non copre ancora tutta la riga cliente.")
    fresature = []
    for produzione_id, quantita in sorted(conformi_per_odp.items(), key=lambda r: r[0] or 0):
        if produzione_id is None:
            raise s.SerbatoiError("Manca la provenienza ODP di un lotto conforme.")
        controlli = Controllo.query.filter_by(produzione_id=produzione_id, fase="fresatura").order_by(Controllo.id).all()
        if sum(r.quantita - r.ko for r in controlli) < quantita:
            raise s.SerbatoiError("Il controllo Fresatura non copre tutti i pezzi conformi rientrati.")
        fresature.extend(controlli)
    return fresature, conclusi
