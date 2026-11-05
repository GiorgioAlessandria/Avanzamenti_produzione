from __future__ import annotations

import hashlib
import json
from collections import Counter
from decimal import InvalidOperation
from uuid import uuid4

from sqlalchemy import or_, update

from app_odp.models import db, InputOdp, AcqOrdineClienteAperto, AcqClienteFornitore, AcqOrdineFornitoreAperto
from app_odp.serbatoi_models import (
    SerbatoiArticolo as Articolo, SerbatoiCliente as Cliente,
    SerbatoiProduzione as Produzione, SerbatoiImpegno as Impegno,
    SerbatoiBarre as Barre, SerbatoiLotto as Lotto,
    SerbatoiConsumo as Consumo, SerbatoiMovimento as Movimento,
    SerbatoiEvento as Evento,
)
from app_odp.services.order_helpers import (
    _norm_text, _parse_qty_decimal, _phase_sequence_for_ordine,
    _now_rome_dt, _ordine_ref_label,
)


class SerbatoiError(ValueError):
    pass


def intero(value, label="Quantità", *, zero=False):
    try:
        n = _parse_qty_decimal(value)
    except (ValueError, TypeError, InvalidOperation):
        raise SerbatoiError(f"{label}: inserire un numero intero valido.") from None
    if not n.is_finite() or n != n.to_integral_value() or n < (0 if zero else 1):
        raise SerbatoiError(f"{label}: inserire un intero {'non negativo' if zero else 'positivo'}.")
    return int(n)


def testo(value, label, massimo=120):
    value = _norm_text(value)
    if not value or len(value) > massimo:
        raise SerbatoiError(f"{label}: valore obbligatorio, massimo {massimo} caratteri.")
    return value


def record(model, key):
    obj = db.session.get(model, intero(key, "Riferimento"))
    if obj is None:
        raise SerbatoiError("Riferimento non trovato. Aggiornare la pagina.")
    return obj


def blocca(obj):
    # SQLite serializza gli scrittori: rileggere i saldi dopo aver ottenuto il lock.
    model = type(obj)
    db.session.execute(update(model).where(model.id == obj.id).values(quantita=model.quantita))
    db.session.refresh(obj)
    return obj


def movimento(tipo, q, utente, *, origine=None, destinazione=None, produzione=None, ddt="", note=""):
    db.session.flush()
    obj = Movimento(tipo=tipo, quantita=q, data=_now_rome_dt().isoformat(),
                    operatore=utente, origine_id=origine.id if origine else None,
                    destinazione_id=destinazione.id if destinazione else None,
                    produzione_id=produzione.id if produzione else None, ddt=ddt, note=note)
    db.session.add(obj)
    return obj


def nuovo_lotto(tipo, codice, q, **kwargs):
    obj = Lotto(riferimento=f"LOT-{uuid4().hex.upper()}", tipo=tipo, codice=codice,
                quantita=q, iniziale=q, **kwargs)
    db.session.add(obj)
    db.session.flush()
    obj.riferimento = f"LOT-{obj.id:06d}"
    return obj


def sposta(lotto, q, stato, ubicazione, utente, tipo, *, cliente_id=None, ddt="", note=""):
    lotto = blocca(lotto)
    if q > lotto.quantita:
        raise SerbatoiError("Quantità superiore alla disponibilità del lotto.")
    lotto.quantita -= q
    codice = record(Articolo, lotto.articolo_id).codice_oss if stato == "Ossidato" and lotto.articolo_id else lotto.codice
    dest = nuovo_lotto(lotto.tipo, codice, q, stato=stato, ubicazione=ubicazione,
                       articolo_id=lotto.articolo_id, padre_id=lotto.id,
                       produzione_id=lotto.produzione_id, barre_id=lotto.barre_id,
                       cliente_id=cliente_id, origine=lotto.origine)
    movimento(tipo, q, utente, origine=lotto, destinazione=dest, ddt=ddt, note=note)
    return dest


def configura_articolo(data):
    oss = testo(data.get("codice_oss"), "Codice ossidato").upper()
    lav = testo(data.get("codice_lav"), "Codice lavorato").upper()
    if Articolo.query.filter(or_(Articolo.codice_oss.in_([oss, lav]), Articolo.codice_lav.in_([oss, lav]))).first():
        raise SerbatoiError("Uno dei codici è già configurato. Non sono ammesse conversioni ambigue.")
    if oss == lav:
        raise SerbatoiError("Indicare distintamente codice lavorato e ossidato.")
    fasi = []
    for name in ("fase_inizio", "fase_fine"):
        raw = _norm_text(data.get(name))
        fasi.append(str(intero(raw, "Fase")) if raw else None)
    a = Articolo(codice_oss=oss, codice_lav=lav,
                 codice_spezzone=testo(data.get("codice_spezzone"), "Codice spezzone").upper(),
                 descrizione=testo(data.get("descrizione"), "Descrizione", 300),
                 fase_inizio=fasi[0], fase_fine=fasi[1])
    db.session.add(a)
    return a


def articolo_per_codice(codice):
    code = _norm_text(codice).upper()
    return Articolo.query.filter(Articolo.attivo.is_(True),
                                or_(Articolo.codice_lav == code, Articolo.codice_oss == code)).first()


def acquisisci_odp(ordine):
    p = Produzione.query.filter_by(id_documento=ordine.IdDocumento, id_riga=ordine.IdRiga).first()
    if p:
        a = record(Articolo, p.articolo_id)
        if _norm_text(ordine.CodArt).upper() not in (a.codice_oss, a.codice_lav):
            p.anomalia = "Articolo ODP modificato: verificare la produzione collegata."
            raise SerbatoiError(p.anomalia)
        return p
    a = articolo_per_codice(ordine.CodArt)
    if a is None:
        return None
    fasi = _phase_sequence_for_ordine(ordine)
    if not fasi:
        raise SerbatoiError("ODP Serbatoi privo di una sequenza di fasi valida.")
    start, end = a.fase_inizio or fasi[0], a.fase_fine or fasi[-1]
    if start not in fasi or end not in fasi or fasi.index(start) > fasi.index(end):
        raise SerbatoiError("Le fasi Serbatoi configurate non corrispondono al ciclo dell'ODP.")
    p = Produzione(id_documento=ordine.IdDocumento, id_riga=ordine.IdRiga,
                   riferimento=_ordine_ref_label(ordine),
                   riferimento_stock=f"STK-{uuid4().hex.upper()}", articolo_id=a.id,
                   quantita=intero(ordine.Quantita), fase_inizio=start, fase_fine=end)
    db.session.add(p)
    db.session.flush()
    return p


def sincronizza():
    count = 0
    avvisi = []
    for row in AcqOrdineClienteAperto.query.all():
        a = articolo_per_codice(row.CodArt)
        if not a:
            continue
        try:
            if row.QTA_SALDO_DOC is None:
                raise SerbatoiError("Saldo ERP assente.")
            q = intero(row.QTA_SALDO_DOC, zero=True)
        except (SerbatoiError, InvalidOperation):
            avvisi.append(f"Riga cliente {row.IdDocumento}/{row.IdRigaDoc}: quantità non valida.")
            continue
        c = Cliente.query.filter_by(id_documento=row.IdDocumento, id_riga=row.IdRigaDoc).first()
        if c is None:
            if q == 0:
                continue
            c = Cliente(id_documento=row.IdDocumento, id_riga=row.IdRigaDoc,
                        riferimento=_norm_text(row.NumDocOriginale or row.NumRegistraz or row.IdDocumento),
                        cliente=_norm_text(row.CodCliFor), articolo_id=a.id, quantita=q, saldo_erp=q,
                        consegna=_norm_text(row.DataConsegna))
            db.session.add(c)
            count += 1
        elif c.articolo_id != a.id:
            message = "Articolo ERP modificato: verificare le assegnazioni esistenti."
            count += int(c.anomalia != message)
            c.anomalia = message
        elif c.saldo_erp != q:
            shipped = sum(l.quantita for l in Lotto.query.filter_by(cliente_id=c.id, stato="Spedito"))
            c.saldo_erp = q
            if q != c.quantita - shipped:
                c.anomalia = f"Saldo ERP aggiornato a {q}: verificare il fabbisogno iniziale e le spedizioni locali."
            count += 1
    for row in InputOdp.query.all():
        if articolo_per_codice(row.CodArt) is None:
            continue
        existed = Produzione.query.filter_by(id_documento=row.IdDocumento, id_riga=row.IdRiga).first()
        try:
            p = acquisisci_odp(row)
            if not existed:
                count += 1
                if _norm_text(row.StatoOrdine).lower() not in ("pianificata", ""):
                    p.anomalia = "ODP acquisito a lavorazione già iniziata: riconciliare quantità pregresse prima di inviare materiale."
            elif intero(row.Quantita) != p.quantita:
                message = "Quantità ODP modificata: riconciliare il fabbisogno prima di inviare materiale."
                count += int(p.anomalia != message)
                p.anomalia = message
        except SerbatoiError as exc:
            avvisi.append(f"ODP {row.IdDocumento}/{row.IdRiga}: {exc}")
    db.session.flush()
    return count, avvisi


def lotti_produzione(p, stato=None):
    query = Lotto.query.filter_by(produzione_id=p.id, tipo="Materiale").filter(Lotto.quantita > 0)
    if stato:
        query = query.filter_by(stato=stato)
    return query.order_by(Lotto.id).all()


def capacita_produzione(p):
    if not p.avviata:
        return p.quantita
    return sum(l.quantita for l in lotti_produzione(p, "In lavorazione"))


def quote_future(p):
    capacita = capacita_produzione(p)
    quote = {}
    for i in Impegno.query.filter_by(produzione_id=p.id).order_by(Impegno.id):
        q = min(i.quantita - i.realizzata, capacita)
        quote[i.id] = q
        capacita -= q
    return quote


def copertura(c):
    q = sum(l.quantita for l in Lotto.query.filter_by(cliente_id=c.id, tipo="Materiale")
            if l.stato not in ("Scarto", "Consumato"))
    for i in Impegno.query.filter_by(cliente_id=c.id):
        q += quote_future(record(Produzione, i.produzione_id)).get(i.id, 0)
    return q


def stati_cliente(c):
    stati = Counter()
    for l in Lotto.query.filter_by(cliente_id=c.id, tipo="Materiale").filter(Lotto.quantita > 0):
        if l.stato not in ("Scarto", "Consumato"):
            stati[f"{l.stato} · {l.ubicazione}"] += l.quantita
    for i in Impegno.query.filter_by(cliente_id=c.id):
        p = record(Produzione, i.produzione_id)
        q = quote_future(p).get(i.id, 0)
        if q:
            stati["In lavorazione · Bernardi" if p.avviata else "Da produrre · ODP"] += q
    return dict(stati)


def assegna_produzione(cliente_id, produzione_id, quantita, utente):
    c = blocca(record(Cliente, cliente_id))
    p = blocca(record(Produzione, produzione_id))
    q = intero(quantita)
    if p.articolo_id != c.articolo_id:
        raise SerbatoiError("Articoli non compatibili.")
    free = capacita_produzione(p) - sum(quote_future(p).values())
    if q > free or q > c.quantita - copertura(c):
        raise SerbatoiError("Quantità superiore al fabbisogno cliente o alla capacità ODP libera. Per materiale già prodotto usare l'assegnazione da magazzino.")
    db.session.add(Impegno(cliente_id=c.id, produzione_id=p.id, quantita=q, realizzata=0))
    movimento("IMPEGNO_ODP", q, utente, produzione=p, note=f"Cliente #{c.id}")


def assegna_magazzino(cliente_id, lotto_id, quantita, utente):
    c = blocca(record(Cliente, cliente_id))
    l = blocca(record(Lotto, lotto_id))
    q = intero(quantita)
    if l.tipo != "Materiale" or l.articolo_id != c.articolo_id or l.cliente_id is not None:
        raise SerbatoiError("Selezionare materiale libero e compatibile con l'ordine cliente.")
    if l.stato not in ("Grezzo", "Lavorato", "Ossidato"):
        raise SerbatoiError("Lo stato del materiale non consente l'assegnazione diretta.")
    if q > c.quantita - copertura(c):
        raise SerbatoiError("Quantità superiore al fabbisogno ancora da coprire.")
    return sposta(l, q, l.stato, l.ubicazione, utente, "ASSEGNAZIONE", cliente_id=c.id)


def libera_impegno(impegno_id, quantita, utente):
    i = blocca(record(Impegno, impegno_id))
    p = blocca(record(Produzione, i.produzione_id))
    c = blocca(record(Cliente, i.cliente_id))
    q = intero(quantita)
    if q > i.quantita - i.realizzata:
        raise SerbatoiError("È possibile liberare soltanto la quota non ancora prodotta.")
    i.quantita -= q
    if i.quantita == 0:
        db.session.delete(i)
    movimento("LIBERA_IMPEGNO", q, utente, produzione=p, note=f"Ordine cliente {c.riferimento}")


def libera_materiale(lotto_id, quantita, utente):
    l = blocca(record(Lotto, lotto_id))
    if l.tipo != "Materiale" or not l.cliente_id or (l.stato, l.ubicazione) != ("Lavorato", "Bernardi"):
        raise SerbatoiError("Può tornare a stock solo il materiale lavorato presso Bernardi, prima dell'invio al terzista.")
    return sposta(l, intero(quantita), "Lavorato", "Bernardi", utente, "RITORNO_STOCK")


def consuma_spezzoni(produzione_id, lotto_id, quantita, utente):
    p = blocca(record(Produzione, produzione_id))
    l = blocca(record(Lotto, lotto_id))
    q = intero(quantita)
    a = record(Articolo, p.articolo_id)
    used = sum(x.quantita for x in Consumo.query.filter_by(produzione_id=p.id))
    if l.tipo != "Spezzone" or l.stato != "Disponibile" or l.codice != a.codice_spezzone:
        raise SerbatoiError("Spezzone non compatibile con la produzione.")
    if used + q > p.quantita:
        raise SerbatoiError("Consumo superiore alla quantità dell'ODP.")
    dest = sposta(l, q, "Consumato", "Bernardi", utente, "PRELIEVO_SPEZZONI")
    db.session.add(Consumo(produzione_id=p.id, lotto_id=dest.id, quantita=q))
    movimento("ORIGINE_ODP", q, utente, origine=dest, produzione=p)


def _avvia_materiale(p, utente):
    if p.avviata:
        return
    a = record(Articolo, p.articolo_id)
    l = nuovo_lotto("Materiale", a.codice_lav, p.quantita, stato="In lavorazione",
                    ubicazione="Bernardi", articolo_id=a.id, produzione_id=p.id)
    p.avviata = True
    movimento("AVVIO_LCT", p.quantita, utente, destinazione=l, produzione=p)


def evento_lct(ordine, *, tipo, operazione, fase, utente, q_ok=0, q_nok=0, finale=False):
    p = acquisisci_odp(ordine)
    if p is None:
        return
    blocca(p)
    fasi = _phase_sequence_for_ordine(ordine)
    if p.fase_inizio not in fasi or p.fase_fine not in fasi:
        raise SerbatoiError("Ciclo ODP modificato: verificare le fasi Serbatoi collegate.")
    if fase not in fasi or not (fasi.index(p.fase_inizio) <= fasi.index(fase) <= fasi.index(p.fase_fine)):
        return
    key = hashlib.sha256(json.dumps([operazione, ordine.IdDocumento, ordine.IdRiga, fase, tipo]).encode()).hexdigest()
    good, bad = intero(q_ok, zero=True), intero(q_nok, zero=True)
    payload = json.dumps([good, bad, finale], separators=(",", ":"))
    previous = Evento.query.filter_by(chiave=key).first()
    if previous:
        if previous.dati != payload:
            raise SerbatoiError("Evento LCT già registrato con quantità diverse.")
        return
    db.session.add(Evento(chiave=key, produzione_id=p.id, dati=payload))
    _avvia_materiale(p, utente)
    if tipo == "AVVIO":
        return
    working = lotti_produzione(p, "In lavorazione")
    available = sum(l.quantita for l in working)
    if good + bad > available:
        raise SerbatoiError("Conferma LCT superiore al materiale ancora in lavorazione Serbatoi.")
    if finale and fase == p.fase_fine and good + bad != available:
        raise SerbatoiError("La chiusura finale LCT lascia un residuo Serbatoi: verificare le quantità oppure usare la chiusura parziale.")
    if not working:
        if good or bad:
            raise SerbatoiError("Materiale in lavorazione Serbatoi non disponibile.")
        return
    # Il lotto di lavorazione è unico; le origini multiple restano nei consumi ODP.
    l = working[0]
    if bad:
        sposta(l, bad, "Scarto", "Bernardi", utente, "SCARTO_LCT")
    if fase != p.fase_fine:
        return
    remaining = good
    for i in Impegno.query.filter_by(produzione_id=p.id).order_by(Impegno.id):
        n = min(remaining, i.quantita - i.realizzata)
        if n:
            sposta(l, n, "Lavorato", "Bernardi", utente, "COMPLETAMENTO_LCT", cliente_id=i.cliente_id)
            i.realizzata += n
            remaining -= n
    if remaining:
        sposta(l, remaining, "Lavorato", "Bernardi", utente, "COMPLETAMENTO_STOCK")


def crea_ordine_barre(data, utente):
    tipo, sep, codice = _norm_text(data.get("fornitore")).partition("|")
    f = next((r for r in fornitori() if (r.TipoAnagrafica, r.CodCliFor) == (tipo, codice)), None) if sep else None
    if f is None:
        raise SerbatoiError("Selezionare un fornitore valido dall'anagrafica.")
    b = Barre(fornitore_tipo=f.TipoAnagrafica, fornitore_codice=f.CodCliFor,
              fornitore_nome=_norm_text(f.RagioneSociale),
              riferimento=testo(data.get("riferimento"), "Ordine barre"),
              codice=testo(data.get("codice"), "Articolo barre").upper(),
              lunghezza_mm=intero(data.get("lunghezza_mm"), "Lunghezza mm"),
              quantita=intero(data.get("quantita")))
    db.session.add(b)
    movimento("ORDINE_BARRE", b.quantita, utente, note=f"{b.riferimento} · {b.fornitore_nome}")
    return b


def fornitori():
    tipi = {_norm_text(r.TipoAnagrafica).upper() for r in AcqOrdineFornitoreAperto.query.all()}
    return [f for f in AcqClienteFornitore.query.order_by(AcqClienteFornitore.RagioneSociale)
            if _norm_text(f.TipoAnagrafica).upper() in ("F", "FORNITORE", "FORNITORI")
            or (_norm_text(f.TipoAnagrafica) and _norm_text(f.TipoAnagrafica).upper() in tipi)]


def ricevi_barre(barre_id, quantita, ddt, utente):
    b = blocca(record(Barre, barre_id))
    q = intero(quantita)
    doc = documento(ddt)
    received = sum(l.iniziale for l in Lotto.query.filter_by(barre_id=b.id, padre_id=None, tipo="Barra"))
    if received + q > b.quantita:
        raise SerbatoiError("Ricezione superiore alla quantità ordinata.")
    l = nuovo_lotto("Barra", b.codice, q, stato="Disponibile", ubicazione="Bernardi", barre_id=b.id)
    movimento("RICEZIONE_BARRE", q, utente, destinazione=l, ddt=doc)
    return l


def taglia_barre(lotto_id, numero_barre, codice_spezzone, pezzi, lunghezza_mm, sfrido_mm, utente):
    l = blocca(record(Lotto, lotto_id))
    n, q = intero(numero_barre, "Barre consumate"), intero(pezzi, "Spezzoni prodotti")
    lunghezza, sfrido = intero(lunghezza_mm, "Lunghezza spezzone"), intero(sfrido_mm, "Sfrido", zero=True)
    code = testo(codice_spezzone, "Codice spezzone").upper()
    if l.tipo != "Barra" or l.stato != "Disponibile":
        raise SerbatoiError("Selezionare un lotto barre disponibile.")
    if not Articolo.query.filter_by(codice_spezzone=code, attivo=True).first():
        raise SerbatoiError("Codice spezzone non configurato nel catalogo Serbatoi.")
    b = record(Barre, l.barre_id)
    if q > n * (b.lunghezza_mm // (lunghezza + sfrido)):
        raise SerbatoiError("Gli spezzoni superano la resa delle barre indicate, considerando lo sfrido per taglio.")
    used = sposta(l, n, "Consumato", "Bernardi", utente, "CONSUMO_BARRE")
    out = nuovo_lotto("Spezzone", code, q, stato="Disponibile", ubicazione="Bernardi", padre_id=used.id)
    movimento("TAGLIO", q, utente, origine=used, destinazione=out,
               note=f"{n} barre; pezzo {lunghezza} mm; sfrido/taglio {sfrido} mm; lunghezza non trasformata {n * b.lunghezza_mm - q * (lunghezza + sfrido)} mm")
    return out


def documento(ddt):
    doc = testo(ddt, "DDT", 60)
    if not doc.isascii() or not doc.isdigit():
        raise SerbatoiError("DDT: inserire il numero, come nel programma precedente.")
    return doc


def origine_completa(l, visited=None):
    visited = set() if visited is None else visited
    if l.id in visited:
        return False
    visited.add(l.id)
    if l.padre_id:
        return origine_completa(record(Lotto, l.padre_id), visited)
    if l.barre_id:
        return True
    if l.produzione_id:
        p = record(Produzione, l.produzione_id)
        consumi = Consumo.query.filter_by(produzione_id=p.id).all()
        return bool(consumi) and sum(x.quantita for x in consumi) >= p.quantita and all(
            origine_completa(record(Lotto, x.lotto_id), set(visited)) for x in consumi)
    return False


def avanza_materiale(lotto_id, quantita, azione, ddt, scarti, utente):
    l = blocca(record(Lotto, lotto_id))
    q, bad = intero(quantita), intero(scarti or 0, "Scarti", zero=True)
    doc = documento(ddt)
    transitions = {
        "gaser": ("Lavorato", "Bernardi", "Lavorato", "Gaser"),
        "rientro": ("Lavorato", "Gaser", "Ossidato", "Bernardi"),
        "spedisci": ("Ossidato", "Bernardi", "Spedito", "Cliente"),
    }
    if azione not in transitions:
        raise SerbatoiError("Azione non prevista.")
    old, place, new, target = transitions[azione]
    if l.tipo != "Materiale" or (l.stato, l.ubicazione) != (old, place):
        raise SerbatoiError("Stato o ubicazione non compatibili con il passaggio richiesto.")
    if l.cliente_id is None:
        raise SerbatoiError("Materiale di stock: assegnare prima la quantità a un ordine cliente. Invio a Gaser e spedizione non consentiti.")
    c = record(Cliente, l.cliente_id)
    if c.anomalia:
        raise SerbatoiError(c.anomalia)
    if l.produzione_id and record(Produzione, l.produzione_id).anomalia:
        raise SerbatoiError("ODP da riconciliare: verificare le quantità pregresse.")
    if q + bad > l.quantita:
        raise SerbatoiError("Quantità e scarti superiori alla disponibilità.")
    if not origine_completa(l):
        raise SerbatoiError("Provenienza incompleta: registrare i prelievi spezzoni e gli ordini barre di origine prima di proseguire.")
    if bad:
        sposta(l, bad, "Scarto", place, utente, "SCARTO", cliente_id=c.id, ddt=doc)
    return sposta(l, q, new, target, utente, azione.upper(), cliente_id=c.id, ddt=doc)


def tracciabilita(lotto):
    nodes, seen = [], set()

    def visit(l):
        if l.id in seen:
            return
        seen.add(l.id)
        nodes.append(l)
        if l.padre_id:
            visit(record(Lotto, l.padre_id))
        if l.produzione_id:
            for c in Consumo.query.filter_by(produzione_id=l.produzione_id):
                visit(record(Lotto, c.lotto_id))

    visit(lotto)
    docs = Movimento.query.filter(or_(Movimento.origine_id.in_(seen), Movimento.destinazione_id.in_(seen))).order_by(Movimento.id).all()
    return nodes, docs


def riconcilia_produzione(produzione_id, quantita, note, utente):
    p = blocca(record(Produzione, produzione_id))
    q = intero(quantita)
    note = testo(note, "Esito della verifica", 1000)
    if q != p.quantita and Lotto.query.filter_by(produzione_id=p.id).filter(
        or_(Lotto.ubicazione == "Gaser", Lotto.stato.in_(["Ossidato", "Spedito"]))
    ).first():
        raise SerbatoiError("Movimenti esterni già registrati: mantenere immutabile il lotto e gestire il nuovo fabbisogno con un nuovo ODP.")
    used = sum(x.quantita for x in Consumo.query.filter_by(produzione_id=p.id))
    finished = sum(l.quantita for l in lotti_produzione(p) if l.stato != "In lavorazione")
    if q < max(used, finished):
        raise SerbatoiError("Quantità inferiore ai consumi o al materiale già registrato. Non è possibile annullare questi movimenti con una riconciliazione.")
    if p.avviata and q != p.quantita:
        working = lotti_produzione(p, "In lavorazione")
        if not working:
            raise SerbatoiError("Lavorazione completata: gestire l'eventuale nuovo fabbisogno con un nuovo ODP.")
        working[0].quantita += q - p.quantita
    p.quantita = q
    p.anomalia = ""
    movimento("RICONCILIAZIONE", q, utente, produzione=p, note=note)


def riconcilia_cliente(cliente_id, quantita, note, utente):
    c = blocca(record(Cliente, cliente_id))
    q = intero(quantita)
    sorgente = db.session.get(AcqOrdineClienteAperto, (c.id_documento, c.id_riga))
    if sorgente:
        a = articolo_per_codice(sorgente.CodArt)
        if a is None or a.id != c.articolo_id:
            raise SerbatoiError("Articolo ERP diverso: la riconciliazione delle quantità non può cambiare la provenienza dell'ordine.")
    if q < copertura(c):
        raise SerbatoiError("Il fabbisogno non può scendere sotto le quantità già assegnate. Verificare prima gli impegni esistenti.")
    note = testo(note, "Esito della verifica", 1000)
    c.quantita = q
    c.anomalia = ""
    movimento("RICONCILIA_CLIENTE", q, utente, note=f"Ordine {c.riferimento}, riga {c.id_riga}: {note}")
