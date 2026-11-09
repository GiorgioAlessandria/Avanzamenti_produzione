from __future__ import annotations

import hashlib
import json
from collections import Counter
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import re
from uuid import uuid4

from sqlalchemy import or_, update

from app_odp.models import db, InputOdp, AcqOrdineClienteAperto, AcqClienteFornitore, AcqOrdineFornitoreAperto, AcqArticoli, AcqArticoliLookup
from app_odp.serbatoi_models import (
    SerbatoiArticolo as Articolo, SerbatoiCliente as Cliente,
    SerbatoiProduzione as Produzione, SerbatoiImpegno as Impegno,
    SerbatoiBarre as Barre, SerbatoiLotto as Lotto,
    SerbatoiConsumo as Consumo, SerbatoiMovimento as Movimento,
    SerbatoiEvento as Evento,
    SerbatoiParametri as Parametri, SerbatoiBarreOrigine as BarreOrigine,
    SerbatoiBarreManuale as BarreManuale,
    SerbatoiPrezzoBarre as PrezzoBarre, SerbatoiRiferimentoBarre as RiferimentoBarre,
    SerbatoiTempistiche as Tempistiche,
)
from app_odp.services.acquisti_service import _parse_acquisti_date
from app_odp.services.order_helpers import (
    _norm_text, _parse_qty_decimal, _phase_sequence_for_ordine,
    _now_rome_dt, _ordine_ref_label,
    _normalize_udm,
    _decimal_to_text,
)


class SerbatoiError(ValueError):
    pass


def intero(value, label="Quantità", *, zero=False):
    try:
        n = _parse_qty_decimal(value)
    except (ValueError, TypeError, InvalidOperation):
        raise SerbatoiError(f"{label}: inserire un numero intero valido.") from None
    if not n.is_finite() or n != n.to_integral_value() or n < (0 if zero else 1) or n > 2**63 - 1:
        raise SerbatoiError(f"{label}: inserire un intero {'non negativo' if zero else 'positivo'}.")
    return int(n)


def testo(value, label, massimo=120):
    value = _norm_text(value)
    if not value or len(value) > massimo:
        raise SerbatoiError(f"{label}: valore obbligatorio, massimo {massimo} caratteri.")
    return value


def parametri():
    return db.session.get(Parametri, 1) or Parametri(
        id=1, fornitore_barre_tipo="", fornitore_barre_codice="", codice_barre="", unita_barre="M",
        lunghezza_barre_mm=6000, fornitore_spezzoni_tipo="", fornitore_spezzoni_codice="",
        codici_spezzoni=[], sfrido_mm=0,
    )


TEMPI_SERBATOI = {
    "grezzo": ("Grezzo per lotto", 7),
    "in_lavorazione": ("In lavorazione per lotto", 7),
    "lavorato": ("Lavorato per lotto, anche a Gaser", 4),
    "ossidato": ("Ossidato per lotto", 2),
    "trasporto_bernardi": ("Singola tratta da Bernardi", 4),
    "trasporto_gaser": ("Rientro da Gaser", 2),
    "preavviso": ("Preavviso prima della data limite", 7),
}
LOTTI_SERBATOI = {
    "SCB100907": 288, "SCB100978": 192, "SC4117261": 288,
    "SC0090596": 192, "SC0090495": 288, "SC0090494": 192,
    "SC0090592": 288, "SC0090595": 288, "SC0090597": 192,
}
COLORI_SCADENZA = {
    "rosso": "Consegna oggi o in ritardo",
    "arancione": "In scadenza",
    "giallo": "Preavviso di scadenza",
    "verde": "In anticipo",
    "grigio": "Stima non disponibile",
}


def tempistiche():
    return db.session.get(Tempistiche, 1) or Tempistiche(
        id=1, giorni={key: default for key, (_, default) in TEMPI_SERBATOI.items()},
        lotti=LOTTI_SERBATOI.copy(),
    )


def salva_tempistiche(data, utente):
    giorni = {key: intero(data.get(key), label, zero=True) for key, (label, _) in TEMPI_SERBATOI.items()}
    cfg = tempistiche()
    lotti = cfg.lotti.copy()
    for articolo in Articolo.query.all():
        key = f"lotto_{articolo.id}"
        if key not in data:
            continue
        raw = _norm_text(data.get(key))
        if raw:
            lotti[articolo.codice_oss] = intero(raw, f"Lotto {articolo.codice_oss}")
        else:
            lotti.pop(articolo.codice_oss, None)
    cfg.giorni, cfg.lotti = giorni, lotti
    db.session.add(cfg)
    movimento("TEMPISTICHE", 0, utente, note="Aggiornati tempi di consegna e lotti standard.")


def scadenza_cliente(consegna, codice, fasi, cfg, oggi):
    data = _parse_acquisti_date(consegna)
    if data is None:
        try:
            data = datetime.strptime(_norm_text(consegna), "%d/%m/%Y").date()
        except ValueError:
            return dict(colore="grigio", testo="Data di consegna da verificare")
    giorni = cfg.giorni
    lotto = cfg.lotti.get(codice)
    residui = {
        "grezzo": (fasi["grezzo"] + fasi["da_assegnare"], 2 * giorni["trasporto_bernardi"]),
        "in_lavorazione": (fasi["in_lavorazione"], 2 * giorni["trasporto_bernardi"]),
        "lavorato": (fasi["lavorato"], 2 * giorni["trasporto_bernardi"]),
        "gaser": (fasi["gaser"], giorni["trasporto_gaser"]),
        "ossidato": (fasi["ossidato"], 0),
    }
    lead = None
    if lotto:
        # Una frazione residua impegna un lotto. Fa fede la quota con più giorni residui.
        lead = max((giorni["lavorato" if fase == "gaser" else fase] * ((q + lotto - 1) // lotto) + trasporto
                    for fase, (q, trasporto) in residui.items() if q > 0), default=None)
    alla_consegna = (data - oggi).days
    if alla_consegna <= 0:
        colore = "rosso"
        testo_scadenza = "Consegna oggi" if alla_consegna == 0 else "In ritardo"
    elif lead is None:
        colore = "grigio"
        testo_scadenza = "Lotto standard da configurare" if not lotto else COLORI_SCADENZA[colore]
    else:
        margine = alla_consegna - lead
        colore = "arancione" if margine <= 0 else "giallo" if margine <= giorni["preavviso"] else "verde"
        testo_scadenza = COLORI_SCADENZA[colore]
    limite = data.toordinal() - lead if lead is not None else 0
    return dict(colore=colore, testo=testo_scadenza, consegna=data, giorni=lead,
                limite=date.fromordinal(limite) if limite > 0 else None)


def fornitore_selezionato(value):
    tipo, sep, code = _norm_text(value).partition("|")
    f = next((r for r in fornitori() if (r.TipoAnagrafica, r.CodCliFor) == (tipo, code)), None) if sep else None
    if f is None:
        raise SerbatoiError("Selezionare un fornitore dall'anagrafica fornitori.")
    return f


def salva_parametri(data, utente):
    barre = fornitore_selezionato(data.get("fornitore_barre"))
    spezzoni = fornitore_selezionato(data.get("fornitore_spezzoni"))
    code = testo(data.get("codice_barre"), "Codice barre").upper()
    unit = _normalize_udm(testo(data.get("unita_barre"), "Unità delle barre", 20))
    if unit not in {"M","MT","METRO","METRI","PZ","N","NR","BARRE","BARRA"}:
        raise SerbatoiError("Unità barre non supportata: usare metri oppure un conteggio di barre. I kg non vengono convertiti automaticamente.")
    length = intero(data.get("lunghezza_barre_mm"), "Lunghezza barra")
    scrap = intero(data.get("sfrido_mm"), "Sfrido", zero=True)
    raw_codes = testo(data.get("codici_spezzoni"), "Codici spezzoni", 24000)
    codes = list(dict.fromkeys(testo(c, "Codice spezzone").upper() for c in raw_codes.splitlines() if c.strip()))
    if not codes or len(codes) > 200:
        raise SerbatoiError("Indicare da 1 a 200 codici spezzoni, uno per riga.")
    cfg = parametri()
    cfg.fornitore_barre_tipo, cfg.fornitore_barre_codice = barre.TipoAnagrafica, barre.CodCliFor
    cfg.fornitore_spezzoni_tipo, cfg.fornitore_spezzoni_codice = spezzoni.TipoAnagrafica, spezzoni.CodCliFor
    cfg.codice_barre, cfg.unita_barre, cfg.lunghezza_barre_mm = code, unit, length
    cfg.codici_spezzoni, cfg.sfrido_mm = codes, scrap
    db.session.add(cfg)
    movimento("PARAMETRI", 0, utente, note=f"Barre: {barre.RagioneSociale} / {code}; spezzoni: {spezzoni.RagioneSociale}; codici: {', '.join(codes)}")
    return cfg


def corrisponde_barre(row, cfg):
    tipo = row.TipoAnagrafica if isinstance(row, AcqOrdineFornitoreAperto) else row.fornitore_tipo
    code = row.CodCliFor if isinstance(row, AcqOrdineFornitoreAperto) else row.fornitore_codice
    art = row.CodArt if isinstance(row, AcqOrdineFornitoreAperto) else row.codice
    return bool(cfg.codice_barre and cfg.fornitore_barre_codice
                and (tipo, code) == (cfg.fornitore_barre_tipo, cfg.fornitore_barre_codice)
                and _norm_text(art).upper() == cfg.codice_barre)


def _centesimi(value, label):
    raw = _norm_text(value)
    if not re.fullmatch(r"[0-9]{1,9}(?:[.,][0-9]{1,2})?", raw):
        raise SerbatoiError(f"{label}: inserire un numero positivo con al massimo due decimali.")
    return intero(_parse_qty_decimal(raw) * 100, label)


def formatta_centesimi(value):
    return f"{Decimal(value) / 100:.2f}".replace(".", ",")


def salva_prezzo_barre(barra, data, utente):
    centesimi = _centesimi(data.get("prezzo_kg"), "Prezzo €/kg")
    data_prezzo = _parse_acquisti_date(data.get("data_prezzo"))
    if data_prezzo is None or data_prezzo.isoformat() != _norm_text(data.get("data_prezzo")):
        raise SerbatoiError("Indicare una data del prezzo valida.")
    prezzo = db.session.get(PrezzoBarre, barra.id)
    if prezzo and (prezzo.centesimi_kg, prezzo.data_prezzo) == (centesimi, data_prezzo):
        return
    prima = f"{formatta_centesimi(prezzo.centesimi_kg)} €/kg ({prezzo.data_prezzo.isoformat()})" if prezzo else "—"
    if prezzo is None:
        prezzo = PrezzoBarre(barre_id=barra.id)
        db.session.add(prezzo)
    prezzo.centesimi_kg, prezzo.data_prezzo = centesimi, data_prezzo
    movimento("PREZZO_BARRE", 0, utente, note=f"{barra.riferimento} · {prima} → {formatta_centesimi(centesimi)} €/kg ({data_prezzo.isoformat()})")


def salva_riferimento_barre(data, utente):
    cfg = parametri()
    chiave = (cfg.fornitore_barre_tipo, cfg.fornitore_barre_codice, cfg.codice_barre)
    if not all(chiave) or chiave != (data.get("fornitore_tipo"), data.get("fornitore_codice"), data.get("codice")):
        raise SerbatoiError("Parametri barre modificati. Ricaricare la pagina prima di salvare il riferimento.")
    centesimi = _centesimi(data.get("prezzo_kg"), "Prezzo di riferimento €/kg")
    soglia = _centesimi(data.get("soglia_percento"), "Soglia percentuale")
    riferimento = db.session.get(RiferimentoBarre, chiave)
    if riferimento and (riferimento.centesimi_kg, riferimento.soglia_centesimi_percento) == (centesimi, soglia):
        return
    prima = f"{formatta_centesimi(riferimento.centesimi_kg)} €/kg (±{formatta_centesimi(riferimento.soglia_centesimi_percento)}%)" if riferimento else "—"
    if riferimento is None:
        riferimento = RiferimentoBarre(fornitore_tipo=chiave[0], fornitore_codice=chiave[1], codice=chiave[2])
        db.session.add(riferimento)
    riferimento.centesimi_kg, riferimento.soglia_centesimi_percento = centesimi, soglia
    movimento("RIFERIMENTO_PREZZO_BARRE", 0, utente,
              note=f"{chiave[1]} · {chiave[2]} · {prima} → {formatta_centesimi(centesimi)} €/kg (±{formatta_centesimi(soglia)}%)")


def scostamento_prezzo_barre(prezzo, riferimento):
    base = riferimento.centesimi_kg
    differenza = prezzo.centesimi_kg - base
    percentuale = (Decimal(differenza) * 100 / base).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return {
        "percentuale": f"{percentuale:+.2f}".replace(".", ",") if percentuale else "0,00",
        "soglia_raggiunta": abs(differenza) * 10000 >= base * riferimento.soglia_centesimi_percento,
        "aumento": differenza > 0,
    }


def descrizioni_articoli():
    descriptions = {}
    for a in Articolo.query.all():
        descriptions.update({a.codice_oss: a.descrizione, a.codice_lav: a.descrizione})
    for row in AcqOrdineFornitoreAperto.query.all():
        if _norm_text(row.DesArt):
            descriptions[_norm_text(row.CodArt).upper()] = row.DesArt
    for row in AcqArticoliLookup.query.filter_by(VarianteArt="").order_by(AcqArticoliLookup.IndiceModifica):
        if _norm_text(row.DesArt):
            descriptions[_norm_text(row.CodArt).upper()] = row.DesArt
    for row in AcqArticoli.query.all():
        if _norm_text(row.DesArt):
            descriptions[_norm_text(row.CodArt).upper()] = row.DesArt
    return descriptions


def riferimenti_barre_erp(cfg):
    return [r for r in AcqOrdineFornitoreAperto.query.all() if corrisponde_barre(r, cfg)]


def _riferimento_fornitore(row):
    return " · ".join(filter(None, [row.CodTipoDoc, row.CodSerie, row.NumRegistraz or row.NumDocOriginale or row.IdDocumento, f"riga {row.IdRigaDoc}"]))


def quantita_barre_erp(row, cfg):
    meters = {"M","MT","METRO","METRI"}
    counts = {"PZ","N","NR","BARRE","BARRA"}
    unit, configured = _normalize_udm(row.UmDoc), _normalize_udm(cfg.unita_barre)
    if not ((unit in meters and configured in meters) or (unit in counts and configured in counts)):
        raise SerbatoiError(f"unità ERP {row.UmDoc or 'assente'} diversa dal parametro {cfg.unita_barre}")
    try:
        qty = _parse_qty_decimal(row.QTA_ORD)
        converted = qty * 1000 / cfg.lunghezza_barre_mm if unit in meters else qty
        n = intero(converted)
    except (SerbatoiError,ValueError,InvalidOperation,ZeroDivisionError):
        raise SerbatoiError("quantità non valida o metri non multipli della lunghezza barra; verificare senza arrotondare") from None
    return n


def _origine_barre(row, b, q):
    return BarreOrigine(id_documento=row.IdDocumento,id_riga=row.IdRigaDoc,barre_id=b.id,quantita_erp=q,
                         quantita_originale=_decimal_to_text(_parse_qty_decimal(row.QTA_ORD)),unita_originale=_normalize_udm(row.UmDoc))


def acquisisci_barre_erp(cfg):
    count, avvisi = 0, []
    known = {x.barre_id for x in BarreOrigine.query.all()} | {x.barre_id for x in BarreManuale.query.all()}
    pending = [b for b in Barre.query.all() if corrisponde_barre(b,cfg) and b.id not in known]
    if pending:
        return 0, ["Prima di acquisire gli ordini barre ERP, collegare i riferimenti precedenti oppure confermarli come pregressi/manuali: " + ", ".join(b.riferimento for b in pending)]
    for row in riferimenti_barre_erp(cfg):
        key = row.IdDocumento, row.IdRigaDoc
        old = db.session.get(BarreOrigine, key)
        try:
            q = quantita_barre_erp(row,cfg)
            if row.QTA_SALDO_DOC is not None:
                balance = _parse_qty_decimal(row.QTA_SALDO_DOC)
                if not balance.is_finite():
                    raise SerbatoiError("saldo ERP non valido")
                if balance <= 0:
                    continue
        except (SerbatoiError,ValueError,InvalidOperation) as exc:
            avvisi.append(f"Barre {_riferimento_fornitore(row)}: {exc}.")
            continue
        if old:
            previous = record(Barre,old.barre_id)
            if old.quantita_erp != q or not corrisponde_barre(previous,cfg) or previous.lunghezza_mm != cfg.lunghezza_barre_mm:
                avvisi.append(f"Barre {_riferimento_fornitore(row)}: dati ERP o lunghezza modificati; verificare l'ordine senza sovrascrivere i carichi.")
            continue
        ref = _riferimento_fornitore(row)
        if Barre.query.filter_by(fornitore_tipo=cfg.fornitore_barre_tipo, fornitore_codice=cfg.fornitore_barre_codice,
                                 codice=cfg.codice_barre, riferimento=ref).first():
            avvisi.append(f"Barre {ref}: ordine già presente; collegare il riferimento ERP nel dettaglio.")
            continue
        b = crea_ordine_barre(dict(riferimento=ref, quantita=q), "Sincronizzazione ERP",da_erp=True)
        db.session.flush()
        db.session.add(_origine_barre(row,b,q))
        count += 1
    return count, avvisi


def collega_barre_erp(barre_id, id_documento, id_riga, utente):
    b = blocca(record(Barre, barre_id))
    row = db.session.get(AcqOrdineFornitoreAperto, (id_documento, id_riga))
    cfg = parametri()
    if row is None or not corrisponde_barre(row, cfg) or not corrisponde_barre(b, cfg):
        raise SerbatoiError("Ordine non compatibile con fornitore e codice barre configurati.")
    if quantita_barre_erp(row,cfg) != b.quantita or b.lunghezza_mm != cfg.lunghezza_barre_mm:
        raise SerbatoiError("Quantità o unità dell'ordine ERP diverse: verificare prima il collegamento.")
    db.session.add(_origine_barre(row,b,b.quantita))
    movimento("COLLEGA_BARRE_ERP", b.quantita, utente, note=f"{b.riferimento} → {_riferimento_fornitore(row)}")


def conferma_barre_manuali(barre_id, note, utente):
    b = blocca(record(Barre,barre_id))
    note = testo(note,"Motivo del riferimento pregresso/manuale",1000)
    if db.session.get(BarreManuale,b.id) is None:
        db.session.add(BarreManuale(barre_id=b.id,data=_now_rome_dt().isoformat(),operatore=utente,note=note))
        movimento("CONFERMA_BARRE_MANUALI",b.quantita,utente,note=f"{b.riferimento}: {note}")


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


def usa_export_lct_due_fasi(ordine):
    if _phase_sequence_for_ordine(ordine) != ["1", "2"]:
        return False
    produzione = Produzione.query.filter_by(
        id_documento=ordine.IdDocumento, id_riga=ordine.IdRiga
    ).first()
    if produzione is not None:
        return (produzione.fase_inizio, produzione.fase_fine) == ("1", "2")
    articolo = articolo_per_codice(ordine.CodArt)
    return (
        articolo is not None
        and (articolo.fase_inizio or "1", articolo.fase_fine or "2") == ("1", "2")
    )


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
    count, avvisi = acquisisci_barre_erp(parametri())
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
        consegna = _norm_text(row.DataConsegna)
        if c is not None and c.articolo_id == a.id and c.consegna != consegna:
            c.consegna = consegna
            count += 1
        if c is None:
            if q == 0:
                continue
            c = Cliente(id_documento=row.IdDocumento, id_riga=row.IdRigaDoc,
                        riferimento=_norm_text(row.NumDocOriginale or row.NumRegistraz or row.IdDocumento),
                        cliente=_norm_text(row.CodCliFor), articolo_id=a.id, quantita=q, saldo_erp=q,
                        consegna=consegna)
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


def verifica_assegnazione_produzione(c, p):
    q = c.quantita
    live = db.session.get(InputOdp, (p.id_documento,p.id_riga))
    if live is None:
        raise SerbatoiError("ODP non più presente negli ordini aperti: per materiale già prodotto usare il magazzino.")
    if _norm_text(live.StatoOrdine).casefold() == "chiusa":
        raise SerbatoiError("ODP chiuso: per materiale già prodotto usare il magazzino.")
    if p.anomalia or c.anomalia:
        raise SerbatoiError("ODP non operativo o quantità da verificare: riconciliare l'ordine prima dell'assegnazione.")
    articolo = articolo_per_codice(live.CodArt)
    if articolo is None or articolo.id != p.articolo_id or intero(live.Quantita) != p.quantita:
        raise SerbatoiError("Dati ODP modificati o articolo disattivato: aggiornare e verificare l'ordine prima dell'assegnazione.")
    if copertura(c):
        raise SerbatoiError("La riga è già coperta, anche parzialmente. Liberare l'assegnazione prima di collegare l'intero lotto a un ODP.")
    if p.articolo_id != c.articolo_id:
        raise SerbatoiError("Articoli non compatibili.")
    free = capacita_produzione(p) - sum(quote_future(p).values())
    if q > free or q > c.quantita - copertura(c):
        raise SerbatoiError("Quantità superiore al fabbisogno cliente o alla capacità ODP libera. Per materiale già prodotto usare l'assegnazione da magazzino.")
    return free


def assegna_produzione(cliente_id, produzione_id, utente):
    c = blocca(record(Cliente, cliente_id))
    p = blocca(record(Produzione, produzione_id))
    verifica_assegnazione_produzione(c, p)
    q = c.quantita
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


def quantita_da_tracciare(p):
    eccedenza = db.session.query(db.func.sum(Movimento.quantita)).filter_by(
        produzione_id=p.id, tipo="SOVRAPRODUZIONE_LCT"
    ).scalar() or 0
    return p.quantita + eccedenza


def consuma_spezzoni(produzione_id, lotto_id, quantita, utente):
    p = blocca(record(Produzione, produzione_id))
    l = blocca(record(Lotto, lotto_id))
    q = intero(quantita)
    a = record(Articolo, p.articolo_id)
    if l.codice not in parametri().codici_spezzoni:
        raise SerbatoiError("Codice spezzone non abilitato nei parametri.")
    used = sum(x.quantita for x in Consumo.query.filter_by(produzione_id=p.id))
    if l.tipo != "Spezzone" or l.stato != "Disponibile" or l.codice != a.codice_spezzone:
        raise SerbatoiError("Spezzone non compatibile con la produzione.")
    if used + q > quantita_da_tracciare(p):
        raise SerbatoiError("Consumo superiore alla quantità da tracciare per l'ODP.")
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


def evento_lct(ordine, *, tipo, operazione, fase, utente, q_ok=0, q_nok=0, finale=False,
               consenti_sovrapproduzione=False):
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
    eccedenza = good + bad - available
    if eccedenza > 0:
        if not (finale and consenti_sovrapproduzione):
            raise SerbatoiError("Conferma LCT superiore al materiale ancora in lavorazione Serbatoi.")
        lotto = working[0] if working else Lotto.query.filter_by(
            produzione_id=p.id, tipo="Materiale", stato="In lavorazione", padre_id=None
        ).first()
        if lotto is None:
            raise SerbatoiError("Lotto di lavorazione Serbatoi non disponibile.")
        lotto.quantita += eccedenza
        movimento("SOVRAPRODUZIONE_LCT", eccedenza, utente, destinazione=lotto, produzione=p)
        working = [lotto]
    if finale and fase == p.fase_fine and eccedenza < 0:
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


def crea_ordine_barre(data, utente, *, da_erp=False):
    cfg = parametri()
    if not cfg.codice_barre or not cfg.fornitore_barre_codice:
        raise SerbatoiError("Configurare prima fornitore e codice barre nei Parametri.")
    expected_supplier = f"{cfg.fornitore_barre_tipo}|{cfg.fornitore_barre_codice}"
    if data.get("fornitore", expected_supplier) != expected_supplier or _norm_text(data.get("codice", cfg.codice_barre)).upper() != cfg.codice_barre:
        raise SerbatoiError("Fornitore o codice barre diversi dai parametri del modulo.")
    if data.get("lunghezza_mm") is not None and intero(data["lunghezza_mm"]) != cfg.lunghezza_barre_mm:
        raise SerbatoiError("Lunghezza barre diversa dai Parametri. Ricaricare la pagina.")
    f = fornitore_selezionato(expected_supplier)
    ref = testo(data.get("riferimento"), "Ordine barre")
    if Barre.query.filter_by(fornitore_tipo=f.TipoAnagrafica,fornitore_codice=f.CodCliFor,codice=cfg.codice_barre,riferimento=ref).first():
        raise SerbatoiError("Ordine barre già presente per lo stesso fornitore, codice e riferimento.")
    b = Barre(fornitore_tipo=f.TipoAnagrafica, fornitore_codice=f.CodCliFor,
              fornitore_nome=_norm_text(f.RagioneSociale),
              riferimento=ref,
              codice=cfg.codice_barre, lunghezza_mm=cfg.lunghezza_barre_mm,
              quantita=intero(data.get("quantita")))
    db.session.add(b)
    movimento("ORDINE_BARRE", b.quantita, utente, note=f"{b.riferimento} · {b.fornitore_nome}")
    if not da_erp:
        db.session.add(BarreManuale(barre_id=b.id,data=_now_rome_dt().isoformat(),operatore=utente,note="Registrazione manuale dell'ordine"))
    return b


def fornitori():
    tipi = {_norm_text(r.TipoAnagrafica).upper() for r in AcqOrdineFornitoreAperto.query.all()}
    return [f for f in AcqClienteFornitore.query.order_by(AcqClienteFornitore.RagioneSociale)
            if _norm_text(f.TipoAnagrafica).upper() in ("F", "FORNITORE", "FORNITORI")
            or (_norm_text(f.TipoAnagrafica) and _norm_text(f.TipoAnagrafica).upper() in tipi)]


def ricevi_barre(barre_id, quantita, ddt, utente):
    b = blocca(record(Barre, barre_id))
    if not corrisponde_barre(b,parametri()):
        raise SerbatoiError("L'ordine barre non corrisponde al fornitore/codice dei Parametri.")
    q = intero(quantita)
    doc = documento(ddt)
    received = sum(l.iniziale for l in Lotto.query.filter_by(barre_id=b.id, padre_id=None, tipo="Barra"))
    if received + q > b.quantita:
        raise SerbatoiError("Ricezione superiore alla quantità ordinata.")
    l = nuovo_lotto("Barra", b.codice, q, stato="Disponibile", ubicazione="Bernardi", barre_id=b.id)
    movimento("RICEZIONE_BARRE", q, utente, destinazione=l, ddt=doc)
    return l


def taglia_barre(lotto_id, numero_barre, codice_spezzone, pezzi, lunghezza_mm, sfrido_mm, utente, *, fornitore_atteso=None):
    l = blocca(record(Lotto, lotto_id))
    n, q = intero(numero_barre, "Barre consumate"), intero(pezzi, "Spezzoni prodotti")
    lunghezza, sfrido = intero(lunghezza_mm, "Lunghezza spezzone"), intero(sfrido_mm, "Sfrido", zero=True)
    code = testo(codice_spezzone, "Codice spezzone").upper()
    cfg = parametri()
    if sfrido != cfg.sfrido_mm:
        raise SerbatoiError("Sfrido diverso dai Parametri. Ricaricare la pagina.")
    if fornitore_atteso is not None and fornitore_atteso != f"{cfg.fornitore_spezzoni_tipo}|{cfg.fornitore_spezzoni_codice}":
        raise SerbatoiError("Fornitore spezzoni modificato nei Parametri. Ricaricare la pagina.")
    if code not in cfg.codici_spezzoni:
        raise SerbatoiError("Codice spezzone non abilitato nei Parametri.")
    supplier = fornitore_selezionato(f"{cfg.fornitore_spezzoni_tipo}|{cfg.fornitore_spezzoni_codice}")
    if l.tipo != "Barra" or l.stato != "Disponibile":
        raise SerbatoiError("Selezionare un lotto barre disponibile.")
    b = record(Barre, l.barre_id)
    if q > n * (b.lunghezza_mm // (lunghezza + sfrido)):
        raise SerbatoiError("Gli spezzoni superano la resa delle barre indicate, considerando lo sfrido per taglio.")
    used = sposta(l, n, "Consumato", "Bernardi", utente, "CONSUMO_BARRE")
    out = nuovo_lotto("Spezzone", code, q, stato="Disponibile", ubicazione="Bernardi", padre_id=used.id)
    movimento("TAGLIO", q, utente, origine=used, destinazione=out,
               note=f"{n} barre; pezzo {lunghezza} mm; sfrido/taglio {sfrido} mm; fornitore spezzoni {supplier.RagioneSociale} ({supplier.CodCliFor}); lunghezza non trasformata {n * b.lunghezza_mm - q * (lunghezza + sfrido)} mm")
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
        return bool(consumi) and sum(x.quantita for x in consumi) >= quantita_da_tracciare(p) and all(
            origine_completa(record(Lotto, x.lotto_id), set(visited)) for x in consumi)
    return False


def avanza_materiale(lotto_id, quantita, azione, ddt, scarti, utente):
    from app_odp.services import serbatoi_cq_service as cq

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
    if azione == "rientro":
        piano = cq.piano_rientro(l)
        if piano:
            if bad:
                raise SerbatoiError("Registrare tutti i pezzi rientrati. I KO si indicano nel controllo qualità.")
            rientro = sposta(l, q, cq.STATO_CQ, target, utente, "RIENTRO", cliente_id=c.id, ddt=doc)
            cq.accoda_rientro(rientro, piano)
            return rientro
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
