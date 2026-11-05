import hashlib
import json
from pathlib import Path
import secrets
from uuid import uuid4

from flask import abort, current_app, flash, jsonify, redirect, render_template, request, session, url_for
from sqlalchemy.exc import IntegrityError, OperationalError

from app_odp.models import db, InputOdp, AcqOrdineClienteAperto
from app_odp.operator_session import active_policy, active_user, active_token
from app_odp.policy.decorator import require_active_any_perm
from app_odp.routes_blueprint import main_bp
from app_odp.serbatoi_models import SerbatoiRichiesta
from app_odp.services import serbatoi_service as s
from app_odp.services.session_helpers import _current_username


TABS = {"clienti": "Ordini cliente", "produzione": "ODP e stock", "barre": "Ordini barre",
        "spezzoni": "Spezzoni", "magazzino": "Magazzino materiale", "storico": "Spedizioni", "articoli": "Articoli gestiti"}


@main_bp.app_errorhandler(s.SerbatoiError)
def serbatoi_errore_lct(exc):
    db.session.rollback()
    return jsonify(ok=False, error=str(exc)), 409


def _accesso():
    if not getattr(active_user(), "active", False):
        abort(403)


def _url(**kwargs):
    token = active_token()
    if token:
        kwargs["tab_session"] = token
    return url_for("main.serbatoi_page", **kwargs)


def _riassunto_cliente(c):
    q = s.copertura(c)
    status = "Da assegnare" if not q else "Coperto" if q >= c.quantita else "Parzialmente coperto"
    stati = s.stati_cliente(c)
    return {"id": c.id, "titolo": f"{c.riferimento} · riga {c.id_riga}", "codice": s.record(s.Articolo, c.articolo_id).codice_oss,
            "info": f"{c.cliente} · {status}", "copertura": status,
            "stato": "; ".join(f"{label}: {qty}" for label, qty in stati.items()) or "In attesa di assegnazione",
            "quantita": f"{q} / {c.quantita}", "oggetto": c}


@main_bp.get("/serbatoi")
@require_active_any_perm("serbatoi_produzione", "serbatoi_acquisti")
def serbatoi_page():
    _accesso()
    tab = request.args.get("tab", "clienti")
    if tab not in TABS:
        abort(404)
    articoli = s.Articolo.query.order_by(s.Articolo.codice_oss).all()
    clienti = s.Cliente.query.order_by(s.Cliente.consegna, s.Cliente.id).all()
    produzioni = s.Produzione.query.order_by(s.Produzione.id.desc()).all()
    lotti = s.Lotto.query.filter(s.Lotto.quantita > 0).order_by(s.Lotto.id.desc()).all()
    barre = s.Barre.query.order_by(s.Barre.id.desc()).all()
    rows = []
    if tab == "clienti":
        rows = [_riassunto_cliente(c) for c in clienti]
    elif tab == "produzione":
        for p in produzioni:
            qty = s.capacita_produzione(p)
            status = "Grezzo" if not p.avviata else "In lavorazione" if qty else "Lavorazione completata"
            rows.append(dict(id=p.id, titolo=p.riferimento, codice=s.record(s.Articolo, p.articolo_id).codice_lav,
                             info=p.riferimento_stock, stato=status, quantita=p.quantita, oggetto=p))
    elif tab == "barre":
        for b in barre:
            if request.args.get("fornitore") and request.args["fornitore"] != f"{b.fornitore_tipo}|{b.fornitore_codice}":
                continue
            received = sum(l.iniziale for l in s.Lotto.query.filter_by(barre_id=b.id, padre_id=None, tipo="Barra"))
            remaining = sum(l.quantita for l in lotti if l.barre_id == b.id and l.tipo == "Barra" and l.stato == "Disponibile")
            rows.append(dict(id=b.id, titolo=b.riferimento, codice=b.codice, info=b.fornitore_nome,
                             stato=f"{received - remaining} consumate · {remaining} disponibili", quantita=b.quantita, oggetto=b,
                             ricevute=received, residue=remaining))
    elif tab in ("spezzoni", "magazzino", "storico"):
        for l in lotti:
            visible = ((tab == "spezzoni" and l.tipo == "Spezzone" and l.stato == "Disponibile")
                       or (tab == "magazzino" and l.tipo == "Materiale" and l.stato not in ("Spedito", "Scarto", "Consumato"))
                       or (tab == "storico" and l.stato == "Spedito"))
            if visible:
                rows.append(dict(id=l.id, titolo=l.riferimento, codice=l.codice,
                                 info="Assegnato a cliente" if l.cliente_id else "Lavorazione ODP" if l.stato == "In lavorazione" else "Stock libero",
                                 stato=f"{l.stato} · {l.ubicazione}", quantita=l.quantita, oggetto=l))
    else:
        rows = [dict(id=a.id, titolo=a.codice_oss, codice=a.codice_lav, info=a.descrizione,
                     stato="Attivo" if a.attivo else "Escluso", quantita="—", oggetto=a) for a in articoli]
    filtro = request.args.get("q", "").strip().casefold()
    if filtro:
        rows = [r for r in rows if filtro in " ".join(str(r[k]) for k in ("titolo", "codice", "info", "stato")).casefold()]
    selected = next((r for r in rows if str(r["id"]) == request.args.get("id")), None)
    detail = selected["oggetto"] if selected else None
    trace, docs, impegni, disponibilita, stati, avvisi = [], [], [], [], {}, []
    if tab == "clienti" and detail:
        impegni = s.Impegno.query.filter_by(cliente_id=detail.id).all()
        disponibilita = [l for l in lotti if l.cliente_id == detail.id and l.tipo == "Materiale"]
        stati = s.stati_cliente(detail)
    elif tab == "produzione" and detail:
        impegni = s.Impegno.query.filter_by(produzione_id=detail.id).all()
        disponibilita = s.Consumo.query.filter_by(produzione_id=detail.id).all()
    elif tab == "barre" and detail:
        disponibilita = [l for l in lotti if l.barre_id == detail.id and l.tipo == "Barra" and l.stato == "Disponibile"]
    elif tab in ("magazzino", "spezzoni", "storico") and detail:
        trace, docs = s.tracciabilita(detail)
    if tab == "articoli":
        known = {code for a in articoli if a.attivo for code in (a.codice_oss, a.codice_lav)}
        avvisi = sorted({s._norm_text(r.CodArt) for r in AcqOrdineClienteAperto.query.all()
                         if s._norm_text(r.CodArt).upper() not in known})
    session.setdefault("serbatoi_csrf", secrets.token_urlsafe(32))
    return render_template("serbatoi.j2", tab=tab, tabs=TABS, rows=rows, selected=selected, detail=detail,
                           articoli=articoli, clienti=clienti, produzioni=produzioni, lotti=lotti, barre=barre,
                           fornitori=s.fornitori(), trace=trace, docs=docs, impegni=impegni,
                           disponibilita=disponibilita, stati=stati, esclusi=avvisi,
                           servizio=s, serbatoi_url=_url, csrf=session["serbatoi_csrf"], request_id=lambda: uuid4().hex,
                           can_produzione=active_policy().can("serbatoi_produzione"),
                           can_acquisti=active_policy().can("serbatoi_acquisti"))


@main_bp.post("/serbatoi/azione")
@require_active_any_perm("serbatoi_produzione", "serbatoi_acquisti")
def serbatoi_azione():
    _accesso()
    data = request.form
    if not session.get("serbatoi_csrf") or not secrets.compare_digest(data.get("csrf", ""), session["serbatoi_csrf"]):
        abort(400, "Sessione del modulo scaduta. Ricaricare la pagina.")
    action = data.get("azione", "")
    production_actions = {"articolo", "catalogo_precedente", "attivo", "fasi", "assegna_odp", "assegna_magazzino", "libera_impegno", "libera_materiale", "consuma", "riconcilia", "riconcilia_cliente"}
    purchasing_actions = {"barre", "ricevi", "taglia"}
    if action in production_actions and not active_policy().can("serbatoi_produzione"):
        abort(403)
    if action in purchasing_actions and not active_policy().can("serbatoi_acquisti"):
        abort(403)
    if action not in production_actions | purchasing_actions | {"sync", "avanza"}:
        abort(400)
    ajax = request.headers.get("X-Requested-With") == "XMLHttpRequest"
    changed, notices = 0, []
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
                changed, notices = s.sincronizza()
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
                s.assegna_produzione(data.get("cliente_id"), data.get("produzione_id"), data.get("quantita"), user)
            elif action == "assegna_magazzino":
                s.assegna_magazzino(data.get("cliente_id"), data.get("lotto_id"), data.get("quantita"), user)
            elif action == "libera_impegno":
                s.libera_impegno(data.get("impegno_id"), data.get("quantita"), user)
            elif action == "libera_materiale":
                s.libera_materiale(data.get("lotto_id"), data.get("quantita"), user)
            elif action == "consuma":
                s.consuma_spezzoni(data.get("produzione_id"), data.get("lotto_id"), data.get("quantita"), user)
            elif action == "barre":
                s.crea_ordine_barre(data, user)
            elif action == "ricevi":
                s.ricevi_barre(data.get("barre_id"), data.get("quantita"), data.get("ddt"), user)
            elif action == "taglia":
                s.taglia_barre(data.get("lotto_id"), data.get("numero_barre"), data.get("codice_spezzone"),
                               data.get("pezzi"), data.get("lunghezza_mm"), data.get("sfrido_mm"), user)
            elif action == "avanza":
                s.avanza_materiale(data.get("lotto_id"), data.get("quantita"), data.get("passaggio"),
                                   data.get("ddt"), data.get("scarti"), user)
            elif action == "riconcilia":
                s.riconcilia_produzione(data.get("produzione_id"), data.get("quantita"), data.get("note"), user)
            elif action == "riconcilia_cliente":
                s.riconcilia_cliente(data.get("cliente_id"), data.get("quantita"), data.get("note"), user)
            db.session.commit()
        if ajax:
            return jsonify(ok=True, changed=changed, avvisi=notices)
        flash("Operazione registrata." + (" " + " ".join(notices) if notices else ""), "success")
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
    return redirect(_url(tab=data.get("tab") if data.get("tab") in TABS else "clienti", id=data.get("id", "")))
