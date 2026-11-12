import hashlib
import json
from collections import Counter, defaultdict
from functools import cache
from pathlib import Path
import secrets
from uuid import uuid4

from flask import abort, current_app, flash, jsonify, redirect, render_template, request, send_file, session, url_for
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy import func
from werkzeug.utils import secure_filename

from app_odp.models import db, InputOdp, AcqOrdineClienteAperto, AcqOrdineFornitoreAperto
from app_odp.operator_session import active_policy, active_user, active_token
from app_odp.policy.decorator import require_active_any_perm, require_active_perm
from app_odp.policy.serbatoi import SERBATOI_OPERATIVI, SERBATOI_QUALITA
from app_odp.routes_blueprint import main_bp
from app_odp.serbatoi_models import SerbatoiRichiesta, SerbatoiRettificaVista
from app_odp.services import serbatoi_service as s
from app_odp.services import serbatoi_cq_service as cq
from app_odp.services.serbatoi_cqc_pdf import build_cqc_pdf
from app_odp.services.session_helpers import _current_username
from app_odp.services.lct_service import lct_sections
from app_odp.services.serbatoi_mercato_service import mercato_alluminio


TABS = {"clienti": "Ordini cliente", "produzione": "ODP e stock", "barre": "Ordini barre",
        "spezzoni": "Spezzoni", "magazzino": "Magazzino materiale", "storico": "Spedizioni",
        "cqc": "Stampa CQC", "articoli": "Parametri"}

TABS_PRODUZIONE = {key: TABS[key] for key in ("clienti", "produzione", "storico")}

FASI = (
    {"codice": "grezzo", "nome": "Grezzo", "stato": "Grezzo", "ubicazione": "Bernardi"},
    {"codice": "in_lavorazione", "nome": "In lavorazione", "stato": "In lavorazione", "ubicazione": "Bernardi"},
    {"codice": "lavorato", "nome": "Lavorato", "stato": "Lavorato", "ubicazione": "Bernardi"},
    {"codice": "gaser", "nome": "Gaser", "stato": "Lavorato", "ubicazione": "Gaser"},
    {"codice": "ossidato", "nome": "Ossidato", "stato": "Ossidato", "ubicazione": "Bernardi"},
    {"codice": "spedito", "nome": "Spedito", "stato": "Spedito", "ubicazione": "Cliente"},
)

FASI_MAGAZZINO = (*FASI[:4],
                  {"codice": "cq", "nome": "Controllo qualità", "stato": cq.STATO_CQ, "ubicazione": "Bernardi"},
                  FASI[4])

FASI_ODP = (
    {"codice": "grezzo", "nome": "Grezzo", "stato": "Grezzo", "ubicazione": "Bernardi"},
    {"codice": "fresatura", "nome": "Fresatura", "stato": "Fresatura", "ubicazione": "Bernardi"},
    {"codice": "buratto", "nome": "Buratto", "stato": "Buratto", "ubicazione": "Bernardi"},
    {"codice": "lavorati", "nome": "Lavorati", "stato": "Lavorati", "ubicazione": "Bernardi"},
)

FASI_SPEDIZIONI = (
    dict(FASI[2], nome="Lavorato Bernardi"),
    dict(FASI[3], nome="Gaser · conto terzi"),
    dict(FASI[4], nome="Ossidato · da spedire"),
    FASI[5],
)
SPEDIZIONI_PER_STATO = {(f["stato"], f["ubicazione"]): f for f in FASI_SPEDIZIONI}

STOCK_INTERNO = {"colore": "blu", "testo": "Stock interno"}
PRIORITA_SCADENZA = {colore: i for i, colore in enumerate(("verde", "grigio", "giallo", "arancione", "rosso"))}


def _fasi_produzione(p, ordine):
    fasi = {f["codice"]: 0 for f in FASI_ODP}
    fasi["lavorati"] = db.session.query(func.coalesce(func.sum(s.Movimento.quantita), 0)).join(
        s.Lotto, s.Lotto.id == s.Movimento.destinazione_id
    ).filter(s.Lotto.produzione_id == p.id, s.Movimento.tipo.in_(
        ["COMPLETAMENTO_LCT", "COMPLETAMENTO_STOCK", "COMPLETAMENTO_RETTIFICA"]
    )).scalar()
    remaining = s.capacita_produzione(p)
    if not remaining:
        return fasi, ""
    sections = lct_sections([ordine]) if ordine else {}
    if sections.get("buratto_da_eseguire") or sections.get("buratto_in_corso"):
        fasi["buratto"] = remaining
    elif sections.get("fresa_da_eseguire") or sections.get("fresa_in_corso"):
        fasi["fresatura" if p.avviata or ordine.StatoOrdine != "Pianificata" else "grezzo"] = remaining
    else:
        return fasi, "Fase LCT non disponibile o fuori dal ciclo fresatura/buratto: verificare l'ODP."
    return fasi, ""


def _fasi_da_stati(stati):
    result = {f["codice"]: stati.get(f'{f["stato"]} · {f["ubicazione"]}', 0) for f in FASI}
    result["grezzo"] += stati.get("Da produrre · ODP", 0)
    result["in_lavorazione"] += stati.get(f"{cq.STATO_CQ} · Bernardi", 0)
    return result


def _ordini_per_fase(rows, tab, ubicazione, fasi):
    result = {}
    for fase in fasi:
        gruppi = {}
        if ubicazione and fase["ubicazione"] != ubicazione:
            result[fase["codice"]] = []
            continue
        for row in rows:
            q = row.get("fasi", {}).get(fase["codice"], 0)
            if not q:
                continue
            obj = row["oggetto"]
            titolo, target_tab, target_id = row["titolo"], tab, row["id"]
            info = obj.cliente if tab == "clienti" else row["info"] if tab == "storico" else ""
            key = target_tab, target_id
            if key not in gruppi:
                gruppi[key] = dict(titolo=titolo, info=info, codice=row["codice"],
                                   descrizione=row.get("descrizione", ""),
                                   fase=fase["codice"],
                                   tab=target_tab, id=target_id, quantita=0,
                                   rettifica=bool(row.get("pezzi_rettificati")) and fase["codice"] == "lavorati",
                                   scadenza=row.get("scadenza") if tab != "clienti" or fase["codice"] != "spedito" else None)
            gruppi[key]["quantita"] += q
        result[fase["codice"]] = list(gruppi.values())
    return result


@main_bp.app_errorhandler(s.SerbatoiError)
def serbatoi_errore_lct(exc):
    db.session.rollback()
    return jsonify(ok=False, error=str(exc)), 409


def _accesso():
    if not getattr(active_user(), "active", False):
        abort(403)


def _url_cq(**kwargs):
    if active_token():
        kwargs["tab_session"] = active_token()
    return url_for("main.serbatoi_qualita", **kwargs)


@main_bp.route("/serbatoi/qualita", methods=["GET", "POST"])
@require_active_any_perm(*SERBATOI_QUALITA)
def serbatoi_qualita():
    _accesso()
    policy = active_policy()
    can_piani = any(policy.can(code) for code in SERBATOI_OPERATIVI)
    can_contabilizza = policy.can("all_serbatoi")
    session.setdefault("serbatoi_csrf", secrets.token_urlsafe(32))
    if request.method == "POST":
        dati = request.form
        if not secrets.compare_digest(dati.get("csrf", "").encode(), session["serbatoi_csrf"].encode()):
            abort(400)
        azione = dati.get("azione")
        if azione not in {"piano", "modifica_piano", "controlla", "contabilizza"}:
            abort(400)
        if (azione in {"piano", "modifica_piano"} and not can_piani) or (azione == "contabilizza" and not can_contabilizza):
            abort(403)
        id_selezionato = None
        codice_piano = None
        try:
            if azione == "piano":
                piano = cq.importa_piano(request.files.get("piano"))
                messaggio = f"Piano CQ salvato per {piano.codice}."
            elif azione == "modifica_piano":
                codice_piano = s.testo(dati.get("codice"), "Codice piano")
                cq.modifica_piano(codice_piano, dati)
                messaggio = f"Piano CQ aggiornato per {codice_piano}."
            else:
                controllo = s.record(cq.Controllo, dati.get("controllo_id"))
                if controllo.fase != "rientro":
                    abort(404)
                id_selezionato = controllo.id
                if azione == "controlla":
                    lotto = s.record(s.Lotto, controllo.lotto_id)
                    piano = cq.piano_controllo_rientro(controllo, lotto)
                    if not piano:
                        raise s.SerbatoiError(f"Manca il piano di controllo per il codice {lotto.codice}.")
                    esiti = [
                        dict(verificati=dati.get(f"verificati_{i}"), ko=dati.get(f"ko_{i}"),
                             note=dati.get(f"note_{i}"), eseguito=dati.get(f"eseguito_{i}") == "1")
                        for i in range(len(piano["controlli"]))
                    ]
                    cq.conferma_rientro(controllo, dict(versione=dati.get("versione"), esiti=esiti),
                                       dati.get("ko_totale"), _current_username())
                    if not can_piani:
                        id_selezionato = None
                    messaggio = "Controllo registrato. I pezzi conformi sono disponibili per la spedizione."
                else:
                    if dati.get("contabilizzato") != "1":
                        raise s.SerbatoiError("Confermare che i pezzi KO sono già stati contabilizzati.")
                    cq.contabilizza_ko(controllo, _current_username())
                    messaggio = "Contabilizzazione registrata e giacenza KO azzerata."
            db.session.commit()
            flash(messaggio, "success")
        except s.SerbatoiError as exc:
            db.session.rollback()
            flash(str(exc), "danger")
        except (IntegrityError, OperationalError):
            db.session.rollback()
            current_app.logger.exception("Concorrenza nel controllo qualità Serbatoi")
            flash("Dati modificati da un’altra operazione. Ricaricare la pagina.", "warning")
        return redirect(_url_cq(id=id_selezionato, piano=codice_piano))

    righe = db.session.query(cq.Controllo, s.Cliente, s.Lotto).join(
        s.Cliente, s.Cliente.id == cq.Controllo.cliente_id
    ).join(s.Lotto, s.Lotto.id == cq.Controllo.lotto_id).filter(
        cq.Controllo.fase == "rientro"
    )
    if not can_piani:
        righe = righe.filter(cq.Controllo.data.is_(None), s.Lotto.stato == cq.STATO_CQ)
    righe = righe.order_by(cq.Controllo.data.isnot(None), cq.Controllo.id.desc()).all()
    selezione = next((r for r in righe if str(r[0].id) == request.args.get("id")), None)
    piano_selezionato = cq.piano_controllo_rientro(selezione[0], selezione[2]) if selezione else None
    giacenze = {r[0].id: db.session.get(s.Lotto, r[0].lotto_ko_id).quantita
                for r in righe if can_piani and r[0].lotto_ko_id and not r[0].contabilizzato_il}
    ddt = s.Movimento.query.filter_by(tipo="RIENTRO", destinazione_id=selezione[2].id).first() if selezione else None
    codice_piano = request.args.get("piano")
    piano_in_modifica = cq.piano_codice(codice_piano) if can_piani and codice_piano else None
    return render_template("serbatoi_qualita.j2", righe=righe, selezione=selezione, giacenze=giacenze,
                           piani=cq.Piano.query.order_by(cq.Piano.codice).all() if can_piani else (), can_piani=can_piani,
                           piano_in_modifica=piano_in_modifica, piano_selezionato=piano_selezionato,
                           can_contabilizza=can_contabilizza, csrf=session["serbatoi_csrf"], url_cq=_url_cq, ddt=ddt)


@main_bp.get("/serbatoi/cqc/<int:cliente_id>")
@require_active_any_perm(*SERBATOI_OPERATIVI)
def serbatoi_cqc(cliente_id):
    _accesso()
    stampa = request.args.get("stampa") == "1"
    if stampa and not active_policy().can("all_serbatoi"):
        abort(403)
    cliente = db.get_or_404(s.Cliente, cliente_id)
    try:
        fresature, rientri = cq.dati_cqc(cliente)
    except s.SerbatoiError as exc:
        flash(str(exc), "warning")
        return redirect(_url(tab="cqc") if stampa else _url(tab="clienti", id=cliente.id))
    articolo = s.record(s.Articolo, cliente.articolo_id)
    lotti = []
    for r in rientri:
        lotto = s.record(s.Lotto, r.lotto_id)
        movimento = s.Movimento.query.filter_by(tipo="RIENTRO", destinazione_id=lotto.id).first()
        lotti.append((r, lotto.riferimento, movimento.ddt if movimento else ""))
    odp = [(f, s.record(s.Produzione, f.produzione_id).riferimento) for f in fresature]
    return send_file(build_cqc_pdf(cliente, articolo, odp, lotti), mimetype="application/pdf",
                     as_attachment=not stampa, download_name=secure_filename(
                         f"CQC_{cliente.cliente}_{cliente.riferimento}_riga_{cliente.id_riga}.pdf"
                     ))


def _url(**kwargs):
    if kwargs.get("tab", "clienti") == request.args.get("tab", "clienti"):
        for key in ("q", "fase", "ubicazione", "fornitore"):
            if key not in kwargs and request.args.get(key):
                kwargs[key] = request.args[key]
    token = active_token()
    if token:
        kwargs["tab_session"] = token
    return url_for("main.serbatoi_page", **kwargs)


def _riassunto_cliente(c, tempistiche, oggi):
    q = s.copertura(c)
    status = "Da assegnare" if not q else "Coperto" if q >= c.quantita else "Parzialmente coperto"
    stati = s.stati_cliente(c)
    fasi = _fasi_da_stati(stati)
    fasi["da_assegnare"] = max(0, c.quantita - q)
    codice = s.record(s.Articolo, c.articolo_id).codice_oss
    scadenza = s.scadenza_cliente(c.consegna, codice, fasi, tempistiche, oggi) if fasi["spedito"] < c.quantita else None
    return {"id": c.id, "titolo": f"{c.riferimento} · riga {c.id_riga}", "codice": codice, "scadenza": scadenza,
            "info": f"{c.cliente} · {status}", "copertura": status,
            "stato": "; ".join(f"{label}: {qty}" for label, qty in stati.items()) or "In attesa di assegnazione",
            "quantita": f"{q} / {c.quantita}", "oggetto": c, "stati": stati, "fasi": fasi}


def _riga_barre(b, lotti, prezzo=None):
    received = sum(l.iniziale for l in s.Lotto.query.filter_by(barre_id=b.id, padre_id=None, tipo="Barra"))
    residuo_mm = sum(s.disponibilita_barra_mm(l, b.lunghezza_mm) for l in lotti
                     if l.barre_id == b.id and l.tipo == "Barra" and l.stato == "Disponibile")
    remaining = s.barre_equivalenti(residuo_mm, b.lunghezza_mm)
    return dict(id=b.id, titolo=b.riferimento, codice=b.codice, info=b.fornitore_nome,
                stato=f"{s.formatta_barre(received - remaining)} consumate · {s.formatta_barre(remaining)} disponibili", quantita=b.quantita,
                oggetto=b, ricevute=received, residue=remaining, fuori_parametri=False, prezzo=prezzo,
                scadenza={"colore": "blu", "testo": "Approvvigionamento"})


@main_bp.get("/serbatoi/mercato-alluminio")
@require_active_perm("all_serbatoi")
def serbatoi_mercato_alluminio():
    _accesso()
    return jsonify(mercato_alluminio(s._now_rome_dt().date()))


@main_bp.get("/serbatoi")
@require_active_any_perm(*SERBATOI_OPERATIVI)
def serbatoi_page():
    _accesso()
    tab = request.args.get("tab", "clienti")
    if tab not in TABS:
        abort(404)
    can_all = active_policy().can("all_serbatoi")
    tabs = TABS if can_all else TABS_PRODUZIONE
    if tab not in tabs:
        abort(403)
    if tab == "cqc":
        con_rientro = cq.Controllo.query.filter_by(fase="rientro").filter(
            cq.Controllo.cliente_id == s.Cliente.id
        ).exists()
        candidati = db.session.query(s.Cliente, s.Articolo.codice_oss).join(
            s.Articolo, s.Articolo.id == s.Cliente.articolo_id
        ).filter(con_rientro).order_by(s.Cliente.cliente, s.Cliente.riferimento, s.Cliente.id)
        certificati = []
        for cliente, codice in candidati:
            try:
                cq.dati_cqc(cliente)
            except s.SerbatoiError:
                continue
            certificati.append((cliente, codice))
        return render_template("serbatoi_stampa_cqc.j2", tab=tab, tabs=tabs, certificati=certificati,
                               serbatoi_url=_url)
    con_fasi = tab in ("clienti", "produzione", "magazzino", "storico")
    fasi_vista = FASI_ODP if tab == "produzione" else FASI_SPEDIZIONI if tab == "storico" else FASI
    if tab == "magazzino":
        fasi_vista = FASI_MAGAZZINO
    fase = request.args.get("fase", "") if con_fasi else ""
    ubicazione = request.args.get("ubicazione", "") if con_fasi and tab != "produzione" else ""
    if fase not in ("", "da_assegnare", *(f["codice"] for f in fasi_vista)) or ubicazione not in ("", "Bernardi", "Gaser", "Cliente"):
        abort(400)
    cfg = s.parametri()
    config_tempi = s.tempistiche() if con_fasi or tab == "articoli" else None
    descrizioni = s.descrizioni_articoli()
    articoli = s.Articolo.query.order_by(s.Articolo.codice_oss).all()
    clienti = s.Cliente.query.order_by(s.Cliente.consegna, s.Cliente.id).all()
    produzioni = s.Produzione.query.order_by(s.Produzione.id.desc()).all()
    clienti_by_id = {c.id: c for c in clienti}
    produzioni_by_id = {p.id: p for p in produzioni}
    odp_by_key = {(o.IdDocumento, o.IdRiga): o for o in InputOdp.query.all()}
    clienti_erp_by_key = {(o.IdDocumento,o.IdRigaDoc): o for o in AcqOrdineClienteAperto.query.all()}
    lotti = s.Lotto.query.filter((s.Lotto.quantita > 0) | (s.Lotto.residuo_barra_mm > 0)).order_by(s.Lotto.id.desc()).all()
    if tab == "spezzoni" and request.args.get("id"):
        try:
            lotto_storico = db.session.get(s.Lotto, s.intero(request.args["id"], "Lotto"))
        except s.SerbatoiError:
            lotto_storico = None
        if lotto_storico and lotto_storico.tipo == "Spezzone" and lotto_storico.stato == "Disponibile" and not lotto_storico.quantita:
            lotti.append(lotto_storico)
    barre = s.Barre.query.order_by(s.Barre.id.desc()).all()
    oggi = s._now_rome_dt().date()
    impegni_spezzoni = {i.produzione_id: i for i in s.ImpegnoSpezzone.query.all()} if tab in (
        "clienti", "produzione", "spezzoni", "magazzino"
    ) else {}
    impegnati_per_lotto = Counter()
    for impegno in impegni_spezzoni.values():
        impegnati_per_lotto[impegno.lotto_id] += impegno.quantita

    @cache
    def riepilogo_cliente(cliente_id):
        return _riassunto_cliente(clienti_by_id[cliente_id], config_tempi, oggi)

    scadenze_odp = {}
    stock_odp = set()
    clienti_per_impegno = {}
    if tab in ("produzione", "magazzino"):
        destinazioni = defaultdict(set)
        for impegno_id, produzione_id, cliente_id in db.session.query(s.Impegno.id, s.Impegno.produzione_id, s.Impegno.cliente_id).filter(
            s.Impegno.quantita > s.Impegno.realizzata
        ):
            destinazioni[produzione_id].add(cliente_id)
            clienti_per_impegno[impegno_id] = cliente_id
        for lotto in lotti:
            if lotto.tipo != "Materiale" or lotto.stato in ("Spedito", "Scarto", "Consumato") or not lotto.produzione_id:
                continue
            if lotto.cliente_id:
                destinazioni[lotto.produzione_id].add(lotto.cliente_id)
            else:
                stock_odp.add(lotto.produzione_id)
        for produzione_id, ids_cliente in destinazioni.items():
            scadenze = [riepilogo_cliente(c)["scadenza"] for c in sorted(ids_cliente) if c in clienti_by_id]
            scadenze_odp[produzione_id] = max(
                (scadenza for scadenza in scadenze if scadenza),
                key=lambda scadenza: PRIORITA_SCADENZA[scadenza["colore"]], default=None,
            )

    quote_lavorazione = {}
    rows = []
    prezzi_barre, serie_prezzi = {}, []
    riferimento_prezzo = scostamento_ultimo = None
    if tab == "clienti":
        rows = [riepilogo_cliente(c.id) for c in clienti]
    elif tab == "produzione":
        rettifiche = {r.produzione_id: r for r in SerbatoiRettificaVista.query.all()}
        for p in produzioni:
            ordine = odp_by_key.get((p.id_documento, p.id_riga))
            quantita_fasi, warning = _fasi_produzione(p, ordine)
            scadenza = scadenze_odp.get(p.id)
            if p.id not in scadenze_odp and (p.id in stock_odp or sum(quantita_fasi.values()) > quantita_fasi["lavorati"]):
                scadenza = STOCK_INTERNO
            rettifica = rettifiche.get(p.id)
            residuo_rettifica = s.capacita_produzione(p) if ordine is None else 0
            pezzi_rettificati = s.capacita_produzione(p) if rettifica else 0
            if pezzi_rettificati:
                for fase_rettificata in ("grezzo", "fresatura", "buratto"):
                    quantita_fasi[fase_rettificata] = 0
                quantita_fasi["lavorati"] += pezzi_rettificati
                warning = ""
            status = "; ".join(f'{f["nome"]}: {quantita_fasi[f["codice"]]}' for f in FASI_ODP if quantita_fasi[f["codice"]]) or "Da verificare"
            if pezzi_rettificati:
                status += " · Rettifica visiva"
            rows.append(dict(id=p.id, titolo=p.riferimento, codice=s.record(s.Articolo, p.articolo_id).codice_lav,
                             info=p.riferimento_stock, stato=status, quantita=p.quantita, oggetto=p,
                             fasi=quantita_fasi, avviso_fase=warning, scadenza=scadenza,
                             rettifica=rettifica, pezzi_rettificati=pezzi_rettificati,
                             residuo_rettifica=residuo_rettifica))
    elif tab == "barre":
        prezzi_barre = {p.barre_id: p for p in s.PrezzoBarre.query.all()}
        riferimento_prezzo = db.session.get(s.RiferimentoBarre, (
            cfg.fornitore_barre_tipo, cfg.fornitore_barre_codice, cfg.codice_barre
        ))
        for b in barre:
            if not s.corrisponde_barre(b,cfg):
                continue
            prezzo = prezzi_barre.get(b.id)
            rows.append(_riga_barre(b, lotti, prezzo))
            if prezzo:
                serie_prezzi.append(dict(data=prezzo.data_prezzo.isoformat(), ordine=b.riferimento,
                                         centesimi=prezzo.centesimi_kg, id=b.id))
        serie_prezzi.sort(key=lambda punto: (punto["data"], punto["id"]))
        if serie_prezzi and riferimento_prezzo:
            scostamento_ultimo = s.scostamento_prezzo_barre(prezzi_barre[serie_prezzi[-1]["id"]], riferimento_prezzo)
    elif tab in ("spezzoni", "magazzino", "storico"):
        for l in lotti:
            fase_spedizione = SPEDIZIONI_PER_STATO.get((l.stato, l.ubicazione)) if tab == "storico" else None
            visible = ((tab == "spezzoni" and l.tipo == "Spezzone" and l.stato == "Disponibile")
                       or (tab == "magazzino" and l.tipo == "Materiale" and l.stato not in ("Spedito", "Scarto", "Consumato"))
                       or (fase_spedizione and l.tipo == "Materiale" and (l.cliente_id in clienti_by_id or l.stato == "Spedito")))
            if visible:
                c, p = clienti_by_id.get(l.cliente_id), produzioni_by_id.get(l.produzione_id)
                titolo = l.riferimento
                stato = f"{l.stato if l.quantita else 'Esaurito'} · {l.ubicazione}"
                info = f"{c.riferimento} · {c.cliente}" if c else f"{p.riferimento} · Lavorazione ODP" if p and l.stato == "In lavorazione" else f"Stock · {p.riferimento}" if p else "Stock libero"
                if tab == "magazzino":
                    if c:
                        scadenza = riepilogo_cliente(c.id)["scadenza"]
                    elif l.stato == "In lavorazione" and l.produzione_id in scadenze_odp:
                        scadenza = scadenze_odp[l.produzione_id]
                    else:
                        scadenza = STOCK_INTERNO
                elif tab == "spezzoni":
                    scadenza = STOCK_INTERNO
                    if impegnati_per_lotto[l.id]:
                        stato = f"{impegnati_per_lotto[l.id]} impegnati · {l.quantita - impegnati_per_lotto[l.id]} liberi"
                else:
                    if c:
                        titolo = f"{c.riferimento} · riga {c.id_riga}"
                        info = f"{c.cliente} · {l.riferimento}"
                    scadenza = {"colore": "grigio", "testo": "Spedizione registrata"} if l.stato == "Spedito" else riepilogo_cliente(c.id)["scadenza"]
                    stato = "Da spedire" if l.stato == "Ossidato" else fase_spedizione["nome"]
                fasi_lotto = _fasi_da_stati({f"{l.stato} · {l.ubicazione}": l.quantita})
                assegnati = l.quantita if l.cliente_id else 0
                if tab == "magazzino":
                    if l.stato == cq.STATO_CQ:
                        fasi_lotto = {"cq": l.quantita}
                    if not l.cliente_id and p and l.stato == "In lavorazione":
                        if p.id not in quote_lavorazione:
                            quote_lavorazione[p.id] = sum(s.quote_future(p).values())
                        assegnati = min(l.quantita, quote_lavorazione[p.id])
                        quote_lavorazione[p.id] -= assegnati
                rows.append(dict(id=l.id, titolo=titolo, codice=l.codice,
                                 info=info, scadenza=scadenza,
                                 stato=stato, quantita=l.quantita, oggetto=l,
                                 fasi=fasi_lotto, assegnati=assegnati))
        if tab == "magazzino":
            for p in produzioni:
                ordine = odp_by_key.get((p.id_documento, p.id_riga))
                if p.avviata or ordine is None or not lct_sections([ordine])["fresa_da_eseguire"]:
                    continue
                quote = s.quote_future(p)
                for impegno_id, quantita in quote.items():
                    if not quantita:
                        continue
                    c = clienti_by_id[clienti_per_impegno[impegno_id]]
                    rows.append(dict(
                        id=f"grezzo-cliente-{impegno_id}", titolo=f"{c.riferimento} · riga {c.id_riga}",
                        codice=ordine.CodArt, descrizione=ordine.DesArt, info=f"{c.cliente} · ODP {p.riferimento}",
                        scadenza=riepilogo_cliente(c.id)["scadenza"], stato="Grezzo · Pianificato",
                        quantita=quantita, oggetto=p, fasi={"grezzo": quantita}, assegnati=quantita,
                        tab_dettaglio="clienti", id_dettaglio=c.id, fase_dettaglio="grezzo",
                    ))
                liberi = p.quantita - sum(quote.values())
                if liberi:
                    rows.append(dict(
                        id=f"grezzo-stock-{p.id}", titolo=f"Stock · {p.riferimento}",
                        codice=ordine.CodArt, descrizione=ordine.DesArt, info=p.riferimento_stock,
                        scadenza=STOCK_INTERNO, stato="Grezzo · Pianificato",
                        quantita=liberi, oggetto=p, fasi={"grezzo": liberi}, assegnati=0, da_produrre=liberi,
                        tab_dettaglio="produzione", id_dettaglio=p.id, fase_dettaglio="grezzo",
                    ))
    else:
        rows = [dict(id=a.id, titolo=a.codice_oss, codice=a.codice_lav, info=a.descrizione,
                     stato="Attivo" if a.attivo else "Escluso", quantita="—", oggetto=a) for a in articoli]
    for row in rows:
        obj = row["oggetto"]
        source = clienti_erp_by_key.get((obj.id_documento,obj.id_riga)) if tab == "clienti" else odp_by_key.get((obj.id_documento,obj.id_riga)) if tab == "produzione" else None
        row["descrizione"] = s._norm_text(row.get("descrizione") or getattr(source,"DesArt",None)) or descrizioni.get(row["codice"], "Descrizione non disponibile")
    filtro = request.args.get("q", "").strip().casefold()
    if filtro:
        rows = [r for r in rows if filtro in " ".join(str(r[k]) for k in ("titolo", "codice", "descrizione", "info", "stato")).casefold()]
    if ubicazione:
        rows = [r for r in rows if any(r.get("fasi", {}).get(f["codice"], 0) for f in fasi_vista if f["ubicazione"] == ubicazione)]
    totali_fasi = {f["codice"]: sum(r.get("fasi", {}).get(f["codice"], 0) for r in rows)
                   if not ubicazione or f["ubicazione"] == ubicazione else 0 for f in fasi_vista}
    ordini_per_fase = _ordini_per_fase(rows, tab, ubicazione, fasi_vista) if con_fasi and tab != "magazzino" else {}
    riepilogo_magazzino = {}
    scorte_materiali = {"Barra": Counter(), "Spezzone": Counter()}
    spezzoni_impegnati_magazzino = 0
    giacenze_spezzoni = {}
    if tab in ("magazzino", "spezzoni") and ubicazione in ("", "Bernardi"):
        codici = [codice for codice in cfg.codici_spezzoni
                  if not filtro or filtro in f"{codice} {descrizioni.get(codice, '')}".casefold()]
        giacenze = s.giacenze_spezzoni_erp(codici)
        giacenze_spezzoni = {codice: giacenze.get(codice) for codice in sorted(codici)}
    serbatoi_da_produrre = 0
    riepilogo_clienti = {}
    if tab == "magazzino":
        lunghezze_barre = {b.id: b.lunghezza_mm for b in barre}
        for lotto in lotti:
            if lotto.tipo not in scorte_materiali or lotto.stato != "Disponibile":
                continue
            if ubicazione and lotto.ubicazione != ubicazione:
                continue
            if filtro and filtro not in f"{lotto.codice} {descrizioni.get(lotto.codice, '')}".casefold():
                continue
            quantita = lotto.quantita
            if lotto.tipo == "Barra":
                lunghezza = lunghezze_barre[lotto.barre_id]
                quantita = s.barre_equivalenti(s.disponibilita_barra_mm(lotto, lunghezza), lunghezza)
            elif lotto.tipo == "Spezzone":
                impegnati = impegnati_per_lotto[lotto.id]
                quantita -= impegnati
                spezzoni_impegnati_magazzino += impegnati
            scorte_materiali[lotto.tipo][lotto.codice] += quantita
        serbatoi_da_produrre = sum(r.get("da_produrre", 0) for r in rows)
        for f in fasi_vista:
            codice = f["codice"]
            if ubicazione and f["ubicazione"] != ubicazione:
                continue
            assegnati = sum(r["assegnati"] for r in rows if r["fasi"].get(codice))
            riepilogo_magazzino[codice] = dict(assegnati=assegnati, stock=totali_fasi[codice] - assegnati)
    elif tab == "clienti":
        aperti = [r for r in rows if r["fasi"]["spedito"] < r["oggetto"].quantita]
        id_aperti = {r["id"] for r in aperti}
        id_per_fase = {
            codice: {ordine["id"] for ordine in ordini} & id_aperti
            for codice, ordini in ordini_per_fase.items()
        }
        scadenze = [r["scadenza"] or {} for r in aperti]
        lead = [scadenza["giorni"] for scadenza in scadenze if scadenza.get("giorni") is not None]
        riepilogo_clienti = dict(
            righe=len(aperti),
            grezzo=len(id_per_fase["grezzo"]),
            in_lavorazione=len(id_per_fase["in_lavorazione"]),
            lavorati=len(id_per_fase["lavorato"] | id_per_fase["gaser"]),
            ossidati=len(id_per_fase["ossidato"]),
            per_ubicazione={
                posto: len(set().union(*(id_per_fase[f["codice"]] for f in fasi_vista if f["ubicazione"] == posto)))
                for posto in ("Bernardi", "Gaser")
            },
            giorni_ritardo=sum(max(0, (oggi - scadenza["consegna"]).days)
                              for scadenza in scadenze if scadenza.get("consegna")),
            lead_totale=sum(lead) if lead or not aperti else None,
            lead_mancanti=len(aperti) - len(lead),
        )
    if fase:
        matches_place = not ubicazione or any(f["codice"] == fase and f["ubicazione"] == ubicazione for f in fasi_vista)
        rows = [r for r in rows if matches_place and r.get("fasi", {}).get(fase, 0) > 0]
    selected = next((r for r in rows if not r.get("tab_dettaglio") and str(r["id"]) == request.args.get("id")), None)
    if tab == "barre" and selected is None and request.args.get("id"):
        try:
            historical = db.session.get(s.Barre,s.intero(request.args["id"],"Riferimento"))
        except s.SerbatoiError:
            historical = None
        if historical and not s.corrisponde_barre(historical,cfg):
            selected = _riga_barre(historical, lotti, prezzi_barre.get(historical.id))
            selected.update(fuori_parametri=True,descrizione=descrizioni.get(historical.codice,"Descrizione non disponibile"))
    detail = selected["oggetto"] if selected else None
    cqc_presente = bool(tab == "clienti" and detail and cq.Controllo.query.filter_by(
        cliente_id=detail.id, fase="rientro"
    ).first())
    trace, docs, impegni, disponibilita, stati, avvisi = [], [], [], [], {}, []
    provenienza_ddt, odp_per_spezzone, prelievi_spezzone, ddt_ricezioni = [], [], [], {}
    produzioni_collegate = set()
    documenti_spezzone = None
    origine_barra = riferimento_dettaglio = scostamento_prezzo = None
    data_prezzo_proposta = oggi
    if tab == "clienti" and detail:
        impegni = s.Impegno.query.filter_by(cliente_id=detail.id).all()
        disponibilita = [l for l in lotti if l.cliente_id == detail.id and l.tipo == "Materiale"]
        stati = s.stati_cliente(detail)
        produzioni_collegate = {i.produzione_id for i in impegni}
        produzioni_collegate.update(l.produzione_id for l in disponibilita if l.produzione_id)
    elif tab == "produzione" and detail:
        impegni = s.Impegno.query.filter_by(produzione_id=detail.id).all()
        produzioni_collegate.add(detail.id)
    elif tab == "barre" and detail:
        disponibilita = [l for l in lotti if l.barre_id == detail.id and l.tipo == "Barra" and l.stato == "Disponibile"]
        ricezioni = db.session.query(s.Lotto.riferimento, s.Movimento.ddt, s.Lotto.id).join(
            s.Movimento, s.Movimento.destinazione_id == s.Lotto.id
        ).filter(s.Lotto.barre_id == detail.id, s.Movimento.tipo == "RICEZIONE_BARRE").order_by(s.Movimento.id).all()
        ddt_ricezioni = {l.id: l for l in ricezioni}
        origine_barra = s.BarreOrigine.query.filter_by(barre_id=detail.id).first()
        if origine_barra:
            origine_erp = db.session.get(AcqOrdineFornitoreAperto, (origine_barra.id_documento, origine_barra.id_riga))
            if origine_erp:
                data_prezzo_proposta = s._parse_acquisti_date(origine_erp.DataRegistrazione) or oggi
        riferimento_dettaglio = riferimento_prezzo if not selected["fuori_parametri"] else db.session.get(
            s.RiferimentoBarre, (detail.fornitore_tipo, detail.fornitore_codice, detail.codice)
        )
        if selected["prezzo"] and riferimento_dettaglio:
            scostamento_prezzo = s.scostamento_prezzo_barre(selected["prezzo"], riferimento_dettaglio)
    elif tab in ("magazzino", "spezzoni", "storico") and detail:
        trace, docs = s.tracciabilita(detail)
        if tab == "spezzoni":
            documenti_spezzone = s.documenti_spezzone(detail)
            prelievi_spezzone = s.Consumo.query.join(s.Lotto, s.Lotto.id == s.Consumo.lotto_id).filter(
                s.Lotto.padre_id == detail.id
            ).order_by(s.Consumo.id).all()
            if detail.quantita and detail.codice in cfg.codici_spezzoni:
                articoli_compatibili = {a.id for a in articoli if a.codice_spezzone == detail.codice}
                for p in produzioni:
                    if p.articolo_id not in articoli_compatibili or not s.odp_aperto_per_spezzoni(
                        odp_by_key.get((p.id_documento, p.id_riga))
                    ):
                        continue
                    previsti = s.disponibilita_prelievo_spezzoni(p)
                    impegno = impegni_spezzoni.get(p.id)
                    if previsti and not (impegno and impegno.lotto_id == detail.id) and (
                        previsti <= detail.quantita - impegnati_per_lotto[detail.id]
                    ):
                        odp_per_spezzone.append((p, previsti))
    if produzioni_collegate:
        prelievi = s.Consumo.query.filter(s.Consumo.produzione_id.in_(produzioni_collegate)).order_by(s.Consumo.id)
        provenienza_ddt = [
            dict(s.documenti_spezzone(s.record(s.Lotto, c.lotto_id)),
                 odp=produzioni_by_id[c.produzione_id], quantita=c.quantita, impegnato=False)
            for c in prelievi
        ]
        for produzione_id in sorted(produzioni_collegate & impegni_spezzoni.keys()):
            impegno = impegni_spezzoni[produzione_id]
            provenienza_ddt.append(dict(s.documenti_spezzone(s.record(s.Lotto, impegno.lotto_id)),
                                        odp=produzioni_by_id[produzione_id], quantita=impegno.quantita, impegnato=True))
    rientro_cq = bool(tab in ("magazzino", "storico") and detail
                      and detail.stato == "Lavorato" and detail.ubicazione == "Gaser")
    if tab == "articoli":
        known = {code for a in articoli if a.attivo for code in (a.codice_oss, a.codice_lav)}
        avvisi = sorted({s._norm_text(r.CodArt) for r in clienti_erp_by_key.values()
                         if s._norm_text(r.CodArt).upper() not in known})
    session.setdefault("serbatoi_csrf", secrets.token_urlsafe(32))
    lista_fornitori = s.fornitori()
    odp_assegnabili, odp_esclusi = [], []
    if tab == "clienti" and detail and not s.copertura(detail):
        for p in produzioni:
            if p.articolo_id != detail.articolo_id:
                continue
            try:
                liberi = s.verifica_assegnazione_produzione(detail, p)
            except s.SerbatoiError as exc:
                odp_esclusi.append(dict(riferimento=p.riferimento, motivo=p.anomalia or detail.anomalia or str(exc)))
                continue
            odp_assegnabili.append(dict(id=p.id, riferimento=p.riferimento, liberi=liberi))
        acquisiti = {(p.id_documento, p.id_riga) for p in produzioni}
        articolo = s.record(s.Articolo, detail.articolo_id)
        for o in odp_by_key.values():
            if (o.IdDocumento, o.IdRiga) not in acquisiti and s._norm_text(o.CodArt).upper() in (articolo.codice_lav, articolo.codice_oss):
                odp_esclusi.append(dict(riferimento=f"{o.RifRegistraz or o.IdDocumento} · riga {o.IdRiga}",
                                        motivo="ODP presente nella cache ma non ancora acquisito in Serbatoi. Aggiorna e verifica gli avvisi su articolo e fasi del ciclo."))
    return render_template("serbatoi.j2", tab=tab, tabs=tabs, rows=rows, selected=selected, detail=detail,
                           odp_assegnabili=odp_assegnabili, odp_esclusi=odp_esclusi,
                           articoli=articoli, clienti=clienti, produzioni=produzioni, lotti=lotti, barre=barre,
                           fornitori=lista_fornitori, trace=trace, docs=docs, impegni=impegni,
                           disponibilita=disponibilita, stati=stati, esclusi=avvisi,
                           provenienza_ddt=provenienza_ddt, documenti_spezzone=documenti_spezzone,
                           odp_per_spezzone=odp_per_spezzone, prelievi_spezzone=prelievi_spezzone,
                           impegni_spezzoni=impegni_spezzoni, impegnati_per_lotto=impegnati_per_lotto,
                           spezzoni_impegnati_magazzino=spezzoni_impegnati_magazzino,
                           ddt_ricezioni=ddt_ricezioni,
                           servizio=s, serbatoi_url=_url, csrf=session["serbatoi_csrf"], request_id=lambda: uuid4().hex,
                           fasi=fasi_vista, con_fasi=con_fasi, fase_filtro=fase, ubicazione_filtro=ubicazione,
                           totali_fasi=totali_fasi,
                           ordini_per_fase=ordini_per_fase, riepilogo_magazzino=riepilogo_magazzino,
                           riepilogo_clienti=riepilogo_clienti, scorte_materiali=scorte_materiali,
                           serbatoi_da_produrre=serbatoi_da_produrre, giacenze_spezzoni=giacenze_spezzoni,
                           parametri=cfg, descrizioni=descrizioni, tempistiche=config_tempi,
                           campi_tempistiche=s.TEMPI_SERBATOI, colori_scadenza=s.COLORI_SCADENZA,
                           ordini_barre_erp=[r for r in s.riferimenti_barre_erp(cfg) if db.session.get(s.BarreOrigine,(r.IdDocumento,r.IdRigaDoc)) is None] if tab == "barre" else (),
                           origine_barra=origine_barra,
                           serie_prezzi=serie_prezzi, riferimento_prezzo=riferimento_prezzo,
                           scostamento_ultimo=scostamento_ultimo,
                           riferimento_dettaglio=riferimento_dettaglio, scostamento_prezzo=scostamento_prezzo,
                           data_prezzo_proposta=data_prezzo_proposta, oggi=oggi,
                           barra_manuale=db.session.get(s.BarreManuale,detail.id) if tab == "barre" and detail else None,
                           forn_barre_label=next((f.RagioneSociale or f.CodCliFor for f in lista_fornitori if (f.TipoAnagrafica,f.CodCliFor)==(cfg.fornitore_barre_tipo,cfg.fornitore_barre_codice)),"Non configurato"),
                           forn_spezzoni_label=next((f.RagioneSociale or f.CodCliFor for f in lista_fornitori if (f.TipoAnagrafica,f.CodCliFor)==(cfg.fornitore_spezzoni_tipo,cfg.fornitore_spezzoni_codice)),"Non configurato"),
                           can_produzione=True, can_acquisti=can_all,
                           rientro_cq=rientro_cq, cqc_presente=cqc_presente,
                           can_rettifica_vista=active_policy().has_direct_admin_role)


@main_bp.post("/serbatoi/azione")
@require_active_any_perm(*SERBATOI_OPERATIVI)
def serbatoi_azione():
    _accesso()
    data = request.form
    if not session.get("serbatoi_csrf") or not secrets.compare_digest(data.get("csrf", ""), session["serbatoi_csrf"]):
        abort(400, "Sessione del modulo scaduta. Ricaricare la pagina.")
    action = data.get("azione", "")
    production_actions = {"sync", "avanza", "assegna_odp", "assegna_magazzino", "libera_impegno",
                          "libera_materiale", "consuma", "libera_lotto_spezzoni", "riconcilia", "riconcilia_cliente"}
    all_actions = {"articolo", "catalogo_precedente", "attivo", "fasi", "parametri", "tempistiche",
                   "barre", "ricevi", "taglia", "collega_barre_erp", "conferma_barre_manuali",
                   "prezzo_barre", "riferimento_prezzo_barre", "ddt_spezzone", "consuma_da_spezzoni"}
    azioni_rettifica = {"completa_rettifica", "ripristina_fase_odp"}
    can_all = active_policy().can("all_serbatoi")
    if action in all_actions and not can_all:
        abort(403)
    if action in azioni_rettifica and not active_policy().has_direct_admin_role:
        abort(403)
    if action not in production_actions | all_actions | azioni_rettifica:
        abort(400)
    ajax = request.headers.get("X-Requested-With") == "XMLHttpRequest"
    changed, notices = 0, []
    lotto_avanzato = None
    try:
        uid = s.testo(data.get("richiesta"), "Richiesta", 64)
        key = hashlib.sha256(f"{active_user().id}:{uid}".encode()).hexdigest()
        digest = hashlib.sha256(json.dumps(sorted((k, v) for k, v in data.items() if k not in ("csrf", "richiesta"))).encode()).hexdigest()
        old = db.session.get(SerbatoiRichiesta, key)
        if old:
            if old.dati != digest:
                raise s.SerbatoiError("Richiesta già utilizzata con dati diversi. Ricaricare la pagina.")
        else:
            db.session.add(SerbatoiRichiesta(chiave=key, dati=digest))
            db.session.flush()
            user = _current_username()
            if action == "sync":
                changed, notices = s.sincronizza(includi_barre=can_all)
            elif action == "parametri":
                s.salva_parametri(data,user)
            elif action == "tempistiche":
                s.salva_tempistiche(data, user)
            elif action == "articolo":
                s.configura_articolo(data)
            elif action == "catalogo_precedente":
                path = Path(__file__).resolve().parents[1] / "data" / "serbatoi_catalogo.json"
                for row in json.loads(path.read_text(encoding="utf-8")):
                    if not s.Articolo.query.filter_by(codice_oss=row["codice_oss"]).first():
                        s.configura_articolo(row)
                        db.session.flush()
            elif action == "attivo":
                a = s.record(s.Articolo, data.get("articolo_id"))
                a.attivo = not a.attivo
            elif action == "fasi":
                a = s.record(s.Articolo, data.get("articolo_id"))
                for key in ("fase_inizio", "fase_fine"):
                    raw = s._norm_text(data.get(key))
                    setattr(a, key, str(s.intero(raw, "Fase")) if raw else None)
            elif action == "assegna_odp":
                s.assegna_produzione(data.get("cliente_id"), data.get("produzione_id"), user)
            elif action == "assegna_magazzino":
                s.assegna_magazzino(data.get("cliente_id"), data.get("lotto_id"), data.get("quantita"), user)
            elif action == "libera_impegno":
                s.libera_impegno(data.get("impegno_id"), data.get("quantita"), user)
            elif action == "libera_materiale":
                s.libera_materiale(data.get("lotto_id"), data.get("quantita"), user)
            elif action in ("consuma", "consuma_da_spezzoni"):
                s.impegna_spezzoni(data.get("produzione_id"), data.get("lotto_id"), user,
                                   solo_odp_aperti=action == "consuma_da_spezzoni")
            elif action == "libera_lotto_spezzoni":
                p = s.blocca(s.record(s.Produzione, data.get("produzione_id")))
                impegno = db.session.get(s.ImpegnoSpezzone, p.id)
                if impegno is not None:
                    db.session.delete(impegno)
            elif action == "ddt_spezzone":
                s.collega_ddt_spezzone(data.get("lotto_id"), data.get("ddt"))
            elif action == "barre":
                barra = s.crea_ordine_barre(data, user)
                if s._norm_text(data.get("prezzo_kg")):
                    s.salva_prezzo_barre(barra, data, user)
            elif action == "prezzo_barre":
                s.salva_prezzo_barre(s.record(s.Barre, data.get("barre_id")), data, user)
            elif action == "riferimento_prezzo_barre":
                s.salva_riferimento_barre(data, user)
            elif action == "ricevi":
                s.ricevi_barre(data.get("barre_id"), data.get("quantita"), data.get("ddt"), user)
            elif action == "collega_barre_erp":
                try:
                    keys = json.loads(data.get("origine_erp","[]"))
                except (TypeError,ValueError):
                    keys = []
                if not isinstance(keys,list) or len(keys) != 2 or not all(isinstance(x,str) and 0 < len(x) <= 200 for x in keys):
                    raise s.SerbatoiError("Selezionare una riga ERP valida.")
                s.collega_barre_erp(data.get("barre_id"),*keys,user)
            elif action == "conferma_barre_manuali":
                s.conferma_barre_manuali(data.get("barre_id"),data.get("note"),user)
            elif action == "taglia":
                s.taglia_barre(data.get("lotto_id"), data.get("codice_spezzone"),
                               data.get("pezzi"), data.get("lunghezza_mm"), data.get("sfrido_mm",s.parametri().sfrido_mm), user,
                               fornitore_atteso=data.get("fornitore_spezzoni"), ddt=data.get("ddt"))
            elif action == "avanza":
                lotto = s.avanza_materiale(data.get("lotto_id"), data.get("quantita"), data.get("passaggio"),
                                           data.get("ddt"), data.get("scarti"), user)
                if data.get("tab") == "storico" or lotto.stato == cq.STATO_CQ:
                    lotto_avanzato = lotto
            elif action == "riconcilia":
                s.riconcilia_produzione(data.get("produzione_id"), data.get("quantita"), data.get("note"), user)
            elif action == "riconcilia_cliente":
                s.riconcilia_cliente(data.get("cliente_id"), data.get("quantita"), data.get("note"), user)
            elif action in azioni_rettifica:
                p = s.record(s.Produzione, data.get("produzione_id"))
                rettifica = db.session.get(SerbatoiRettificaVista, p.id)
                if action == "completa_rettifica":
                    if data.get("chiuso_erp") != "1":
                        raise s.SerbatoiError("Confermare che la riga ODP è già chiusa nel gestionale.")
                    s.rettifica_completamento(p, data.get("quantita_ok"), data.get("quantita_ko"), data.get("note"), user)
                    changed = 1
                if rettifica is not None:
                    db.session.delete(rettifica)
            db.session.commit()
        if ajax:
            return jsonify(ok=True, changed=changed, avvisi=notices)
        if action in azioni_rettifica:
            flash("Rettifica registrata nel magazzino Serbatoi." if action == "completa_rettifica" else "Rettifica visiva rimossa.", "success")
            return redirect(_url(tab="produzione", id=data.get("produzione_id"),
                                 fase="", q=""))
        flash("Operazione registrata." + (" " + " ".join(notices) if notices else ""), "success")
        if lotto_avanzato is not None:
            if lotto_avanzato.stato == cq.STATO_CQ:
                controllo = cq.Controllo.query.filter_by(lotto_id=lotto_avanzato.id).one()
                return redirect(_url_cq(id=controllo.id))
            fase_destinazione = SPEDIZIONI_PER_STATO[(lotto_avanzato.stato, lotto_avanzato.ubicazione)]
            return redirect(_url(tab="storico", id=lotto_avanzato.id, fase=fase_destinazione["codice"], ubicazione=""))
    except s.SerbatoiError as exc:
        db.session.rollback()
        if ajax:
            return jsonify(ok=False, error=str(exc)), 400
        flash(str(exc), "danger")
    except (IntegrityError, OperationalError):
        db.session.rollback()
        current_app.logger.exception("Concorrenza o vincolo Serbatoi")
        if ajax:
            return jsonify(ok=False, error="Dati modificati da un'altra operazione. Ricaricare la pagina."), 409
        flash("Dati modificati da un'altra operazione o riferimento già presente. Ricaricare la pagina.", "warning")
    return redirect(_url(tab=data.get("tab") if data.get("tab") in (TABS if can_all else TABS_PRODUZIONE) else "clienti", id=data.get("id", ""),
                         fase=data.get("fase", ""), ubicazione=data.get("ubicazione", ""), q=data.get("q", ""),
                         fornitore=data.get("fornitore_filtro", "")))
