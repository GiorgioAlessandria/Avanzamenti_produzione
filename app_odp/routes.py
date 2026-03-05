from datetime import datetime
from zoneinfo import ZoneInfo
import json
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from flask import Blueprint, render_template, request, url_for, abort, jsonify
from flask_login import login_required, current_user
from sqlalchemy import func, select

from app_odp.models import (
    InputOdp,
    StatoOdp,
    db,
    ChangeEvent,
    Causaliattivita,
)
from app_odp.policy.decorator import require_perm
from app_odp.policy.policy import RbacPolicy
from app_odp.models import InputOdpLog, StatoOdpLog, ChangeEventLog

try:
    from icecream import ic
finally:
    pass

main_bp = Blueprint("main", __name__)
ROME_TZ = ZoneInfo("Europe/Rome")

# region FUNZIONI

HOME_TABS = {
    "10": {
        "tab": "montaggio",
        "label_fallback": "Montaggio",
        "template": "partials/_home_montaggio.html",
    },
    "20": {
        "tab": "officina",
        "label_fallback": "Officina",
        "template": "partials/_home_officina.html",
    },
    "30": {
        "tab": "carpenteria",
        "label_fallback": "Carpenteria",
        "template": "partials/_home_carpenteria.html",
    },
    "40": {
        "tab": "Magazzino",
        "label_fallback": "Magazzino",
        "template": "partials/page_vuota.html",
    },
    "50": {
        "tab": "Fornitori",
        "label_fallback": "Fornitori",
        "template": "partials/page_vuota.html",
    },
    "60": {
        "tab": "Ufficio Tecnico",
        "label_fallback": "Ufficio Tecnico",
        "template": "partials/page_vuota.html",
    },
    "70": {
        "tab": "collaudo",
        "label_fallback": "Collaudo",
        "template": "partials/_home_collaudo.html",
    },
}

TAB_TO_TEMPLATE = {
    "montaggio": ("partials/_home_montaggio.html", {"reparto": "10", "perm": "home"}),
    "officina": ("partials/_home_officina.html", {"reparto": "20", "perm": "home"}),
    "carpenteria": (
        "partials/_home_carpenteria.html",
        {"reparto": "30", "perm": "home"},
    ),
    "collaudo": (
        "partials/_home_collaudo.html",
        {"reparto": "70", "perm": "home"},
    ),
}
BRIDGE_CONFIG = {
    "officina": {"reparto": "20", "perm": "home", "renderer": "officina"},
    "carpenteria": {"reparto": "30", "perm": "home", "renderer": "carpenteria"},
    "montaggio": {"reparto": "10", "perm": "home", "renderer": "montaggio"},
    "collaudo": {"reparto": "70", "perm": "home", "renderer": "collaudo"},
}


def _tab_scoped_odp(policy: RbacPolicy, reparto_code: str):
    q = InputOdp.query
    return policy.filter_input_odp_for_reparto(q, reparto_code)


def _last_change_event_id() -> int:
    return db.session.query(func.max(ChangeEvent.id)).scalar() or 0


def _parse_qty_decimal(value) -> Decimal:
    raw = _norm_text(value).replace(",", ".")
    if raw == "":
        return Decimal("0")
    try:
        return Decimal(raw)
    except InvalidOperation:
        raise ValueError(f"Quantità non valida: {value!r}")


@main_bp.context_processor
def inject_policy_and_nav():
    if not current_user.is_authenticated:
        return {}

    policy = RbacPolicy(current_user)
    items = []

    # voci reparto da DB + policy
    for cod, descr in policy.allowed_reparti_menu:
        cfg = HOME_TABS.get(str(cod))
        if not cfg:
            continue
        items.append(
            {
                "label": descr or cfg["label_fallback"],
                "url": url_for(".home", tab=cfg["tab"]),
                "tab": cfg["tab"],
            }
        )
    return {"policy": policy, "home_switch_items": items}


# region PERCORSI
@main_bp.route("/")
@login_required
@require_perm("home")
def home():
    policy = RbacPolicy(current_user)
    tab = request.args.get("tab")

    # default: prima tab consentita
    if not tab:
        for t, (_, req) in TAB_TO_TEMPLATE.items():
            if req.get("reparto") in policy.allowed_reparti and policy.can(req["perm"]):
                tab = t
                break

    cfg = TAB_TO_TEMPLATE.get(tab)
    if not cfg:
        abort(404)

    template, req = cfg
    if req.get("reparto") not in policy.allowed_reparti:
        abort(403)
    if not policy.can(req["perm"]):
        abort(403)

    q = _tab_scoped_odp(policy, req["reparto"])
    odp = list(q.all())

    causali = (
        db.session.execute(
            select(Causaliattivita.DesCausaleAttivita).order_by(
                Causaliattivita.DesCausaleAttivita
            )
        )
        .scalars()
        .all()
    )
    return render_template(
        "home.j2",
        active_partial=template,
        active_tab=tab,
        policy=policy,
        odp=odp,
        causali_attivita=causali,
        bridge_url=url_for("main.api_home_bridge", tab=tab),
        bridge_last_event_id=_last_change_event_id(),
    )


def _query_for_tab(policy, reparto_code):
    q = InputOdp.query
    q = policy.filter_input_odp_for_reparto(q, reparto_code)
    return q


def _render_bridge_officina(odp):
    return {
        "tbody_ordini_da_eseguire": render_template(
            "partials/_home_officina_rows_da_eseguire.html", odp=odp
        ),
        "tbody_ordini_in_corso": render_template(
            "partials/_home_officina_rows_in_corso.html", odp=odp
        ),
    }


def _render_bridge_carpenteria(odp):
    return {
        "tbody_ordini_da_eseguire": render_template(
            "partials/_home_carpenteria_rows_da_eseguire.html", odp=odp
        ),
        "tbody_ordini_in_corso": render_template(
            "partials/_home_carpenteria_rows_in_corso.html", odp=odp
        ),
    }


def _render_bridge_montaggio(odp):
    return {
        "tbody_ordini_da_eseguire_sl": render_template(
            "partials/_home_montaggio_sl_rows_da_eseguire.html", odp=odp
        ),
        "tbody_ordini_in_corso_sl": render_template(
            "partials/_home_montaggio_sl_rows_in_corso.html", odp=odp
        ),
        "tbody_ordini_da_eseguire_m": render_template(
            "partials/_home_montaggio_m_rows_da_eseguire.html", odp=odp
        ),
        "tbody_ordini_in_corso_m": render_template(
            "partials/_home_montaggio_m_rows_in_corso.html", odp=odp
        ),
    }


def _render_bridge_collaudo(odp):
    return {
        "tbody_tbl_da_eseguire_sl": render_template(
            "partials/_home_montaggio_sl_rows_da_eseguire.html", odp=odp
        ),
        "tbody_ordini_in_corso_sl": render_template(
            "partials/_home_montaggio_sl_rows_in_corso.html", odp=odp
        ),
        "tbody_tbl_da_eseguire_m": render_template(
            "partials/_home_collaudo_m_rows_da_eseguire.html", odp=odp
        ),
        "tbody_ordini_in_corso_m": render_template(
            "partials/_home_collaudo_m_rows_in_corso.html", odp=odp
        ),
    }


RENDERERS = {
    "officina": _render_bridge_officina,
    "carpenteria": _render_bridge_carpenteria,
    "montaggio": _render_bridge_montaggio,
    "collaudo": _render_bridge_collaudo,
}


@main_bp.get("/api/home/<tab>/bridge")
@login_required
@require_perm("home")
def api_home_bridge(tab):
    cfg = BRIDGE_CONFIG.get(tab)
    if not cfg:
        abort(404)

    policy = RbacPolicy(current_user)

    if cfg["reparto"] not in policy.allowed_reparti:
        abort(403)
    if not policy.can(cfg["perm"]):
        abort(403)

    after = request.args.get("after", type=int, default=0)
    last_event_id = _last_change_event_id()

    if after and last_event_id <= after:
        return {"changed": False, "last_event_id": last_event_id}

    odp = list(_query_for_tab(policy, cfg["reparto"]).all())
    fragments = RENDERERS[tab](odp)
    return {
        "changed": True,
        "last_event_id": last_event_id,
        "fragments": fragments,
    }


def _row_key(id_documento: str, id_riga: str) -> str:
    return f"{id_documento}|{id_riga}"


def _norm_text(value) -> str:
    return str(value or "").strip()


def _now_rome_dt() -> datetime:
    return datetime.now(ROME_TZ)


def _parse_iso_dt(value) -> datetime | None:
    raw = _norm_text(value)
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ROME_TZ)
    return dt


def _tempo_to_seconds(value) -> int:
    raw = _norm_text(value).replace(",", ".")
    if not raw:
        return 0
    try:
        hours = Decimal(raw)
    except InvalidOperation:
        return 0
    return int((hours * Decimal("3600")).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _seconds_to_tempo_text(seconds: int) -> str:
    if seconds <= 0:
        return "0"
    hours = (Decimal(seconds) / Decimal("3600")).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )
    text = format(hours, "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def _ensure_stato_attivo(
    ordine,
    stato,
    username: str,
    when_dt: datetime,
    fase_corrente: str,
):
    now_iso = when_dt.isoformat(timespec="seconds")

    if stato is None:
        stato = StatoOdp(
            IdDocumento=ordine.IdDocumento,
            IdRiga=ordine.IdRiga,
            RifRegistraz=ordine.RifRegistraz,
            Stato_odp="Attivo",
            Data_in_carico=now_iso,
            Tempo_funzionamento="0",
            Utente_operazione=username,
            Fase=fase_corrente,
            data_ultima_attivazione=now_iso,
        )
        db.session.add(stato)
        return stato

    stato.Stato_odp = "Attivo"
    stato.Utente_operazione = username
    if fase_corrente:
        stato.Fase = fase_corrente
    if not _norm_text(stato.Data_in_carico):
        stato.Data_in_carico = now_iso
    if not _norm_text(stato.Tempo_funzionamento):
        stato.Tempo_funzionamento = "0"

    stato.data_ultima_attivazione = now_iso
    return stato


def _accumulate_runtime_until(stato, end_dt: datetime) -> int:
    if stato is None:
        return 0

    start_dt = _parse_iso_dt(stato.data_ultima_attivazione)
    if start_dt is None:
        stato.data_ultima_attivazione = None
        if not _norm_text(stato.Tempo_funzionamento):
            stato.Tempo_funzionamento = "0"
        return 0

    elapsed_seconds = max(0, int((end_dt - start_dt).total_seconds()))
    total_seconds = _tempo_to_seconds(stato.Tempo_funzionamento) + elapsed_seconds

    stato.Tempo_funzionamento = _seconds_to_tempo_text(total_seconds)
    stato.data_ultima_attivazione = None
    return elapsed_seconds


def _extract_codes_from_cell(value) -> list[str]:
    """
    Normalizza celle che possono contenere:
    - "10"
    - ["10"]
    - [["10"]]
    - {"key": "10"}
    """
    if value in (None, ""):
        return []

    def walk(node):
        if isinstance(node, dict):
            for v in node.values():
                yield from walk(v)
        elif isinstance(node, (list, tuple, set)):
            for item in node:
                yield from walk(item)
        else:
            s = str(node).strip()
            if s:
                yield s

    if isinstance(value, (dict, list, tuple, set)):
        return list(dict.fromkeys(walk(value)))

    raw = str(value).strip()
    if not raw:
        return []

    try:
        parsed = json.loads(raw)
    except Exception:
        return [raw]

    return list(dict.fromkeys(walk(parsed)))


def _get_visible_odp_by_key(
    policy: RbacPolicy, id_documento: str, id_riga: str
) -> InputOdp:
    """
    Cerca l'ordine nel perimetro RBAC dell'utente.
    Se esiste ma è fuori perimetro -> 403
    Se non esiste -> 404
    """
    ordine = (
        policy.filter_input_odp(InputOdp.query)
        .filter_by(IdDocumento=id_documento, IdRiga=id_riga)
        .first()
    )
    if ordine:
        return ordine

    exists_anyway = InputOdp.query.filter_by(
        IdDocumento=id_documento,
        IdRiga=id_riga,
    ).first()

    if exists_anyway is None:
        abort(404)

    abort(403)


def _tab_from_ordine(ordine: InputOdp) -> str | None:
    reparto_codes = set(_extract_codes_from_cell(ordine.CodReparto))
    for tab, cfg in BRIDGE_CONFIG.items():
        if cfg["reparto"] in reparto_codes:
            return tab
    return None


def _fragments_for_ordine_tab(
    policy: RbacPolicy, ordine: InputOdp
) -> tuple[str | None, dict]:
    tab = _tab_from_ordine(ordine)
    if not tab:
        return None, {}

    reparto_code = BRIDGE_CONFIG[tab]["reparto"]
    odp = list(_query_for_tab(policy, reparto_code).all())
    fragments = RENDERERS[tab](odp)
    return tab, fragments


def _push_change_event(
    topic: str, ordine: InputOdp, extra_payload: dict | None = None
) -> ChangeEvent:
    reparto_codes = _extract_codes_from_cell(ordine.CodReparto)
    payload = {
        "id_documento": ordine.IdDocumento,
        "id_riga": ordine.IdRiga,
        "row_key": _row_key(ordine.IdDocumento, ordine.IdRiga),
        "rif_registraz": ordine.RifRegistraz,
        "cod_reparto": ordine.CodReparto,
        "stato_ordine": ordine.StatoOrdine,
    }
    if extra_payload:
        payload.update(extra_payload)

    evt = ChangeEvent(
        topic=topic,
        scope=reparto_codes[0] if reparto_codes else str(ordine.CodReparto or ""),
        payload_json=json.dumps(payload),
    )
    db.session.add(evt)
    return evt


@main_bp.post("/api/ordini/presa")
@login_required
@require_perm("home")
def api_prendi_ordine():
    data = request.get_json(silent=True) or {}

    id_documento = _norm_text(data.get("id_documento"))
    id_riga = _norm_text(data.get("id_riga"))

    if not id_documento or not id_riga:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "IdDocumento e IdRiga sono obbligatori",
                }
            ),
            400,
        )

    policy = RbacPolicy(current_user)
    ordine = _get_visible_odp_by_key(policy, id_documento, id_riga)

    stato_attuale = _norm_text(ordine.StatoOrdine)
    stato_norm = stato_attuale.lower()
    changed = False
    message = None

    if stato_norm == "pianificata":
        now_dt = _now_rome_dt()

        ordine.StatoOrdine = "Attivo"

        stato = StatoOdp.query.filter_by(
            IdDocumento=ordine.IdDocumento,
            IdRiga=ordine.IdRiga,
        ).first()

        fase_corrente = _norm_text(ordine.FaseAttiva) or _norm_text(ordine.NumFase)

        stato = _ensure_stato_attivo(
            ordine=ordine,
            stato=stato,
            username=current_user.username,
            when_dt=now_dt,
            fase_corrente=fase_corrente,
        )

        _push_change_event(
            topic="ordine_preso",
            ordine=ordine,
            extra_payload={
                "azione": "presa_in_carico",
                "utente": current_user.username,
                "data_ultima_attivazione": stato.data_ultima_attivazione,
                "tempo_funzionamento": stato.Tempo_funzionamento,
            },
        )

        db.session.commit()
        changed = True
        message = "Ordine preso in carico"

    tab, fragments = _fragments_for_ordine_tab(policy, ordine)

    return (
        jsonify(
            {
                "ok": True,
                "changed": changed,
                "message": message,
                "id_documento": ordine.IdDocumento,
                "id_riga": ordine.IdRiga,
                "row_key": _row_key(ordine.IdDocumento, ordine.IdRiga),
                "rif_registraz": ordine.RifRegistraz,
                "stato_ordine": ordine.StatoOrdine,
                "active_tab": tab,
                "last_event_id": _last_change_event_id(),
                "fragments": fragments,
            }
        ),
        200,
    )


@main_bp.post("/api/ordini/sospendi")
@login_required
@require_perm("home")
def api_sospendi_ordine():
    data = request.get_json(silent=True) or {}

    id_documento = _norm_text(data.get("id_documento"))
    id_riga = _norm_text(data.get("id_riga"))
    causale = _norm_text(data.get("causale"))

    if not id_documento or not id_riga:
        return (
            jsonify({"ok": False, "error": "IdDocumento e IdRiga sono obbligatori"}),
            400,
        )

    policy = RbacPolicy(current_user)
    ordine = _get_visible_odp_by_key(policy, id_documento, id_riga)

    stato_attuale = _norm_text(ordine.StatoOrdine)
    stato_norm = stato_attuale.lower()

    changed = False
    message = None
    elapsed_seconds = 0
    tempo_funzionamento = "0"

    if stato_norm == "attivo":
        now_dt = _now_rome_dt()
        ordine.StatoOrdine = "In Sospeso"

        stato = StatoOdp.query.filter_by(
            IdDocumento=ordine.IdDocumento,
            IdRiga=ordine.IdRiga,
        ).first()

        # Non andare avanti in silenzio: se manca il record di runtime,
        # la sospensione non può calcolare correttamente il tempo.
        if stato is None:
            db.session.rollback()
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": (
                            "Record odp_in_carico non trovato per questo ordine. "
                            "La sospensione non può aggiornare Tempo_funzionamento."
                        ),
                        "id_documento": ordine.IdDocumento,
                        "id_riga": ordine.IdRiga,
                    }
                ),
                409,
            )

        stato.Stato_odp = "In Sospeso"
        stato.Utente_operazione = current_user.username

        elapsed_seconds = _accumulate_runtime_until(stato, now_dt)
        stato.Stato_odp = "In Sospeso"
        tempo_funzionamento = _norm_text(stato.Tempo_funzionamento) or "0"

        _push_change_event(
            topic="ordine_sospeso",
            ordine=ordine,
            extra_payload={
                "azione": "sospensione",
                "utente": current_user.username,
                "causale": causale,
                "elapsed_seconds": elapsed_seconds,
                "tempo_funzionamento": tempo_funzionamento,
                "data_ultima_attivazione": stato.data_ultima_attivazione,
            },
        )

        db.session.commit()
        changed = True
        message = "Ordine sospeso"

    elif stato_norm == "in sospeso":
        message = "Ordine già in sospeso"

    else:
        message = f"Ordine non sospendibile: stato attuale '{stato_attuale}'"

    tab, fragments = _fragments_for_ordine_tab(policy, ordine)

    return (
        jsonify(
            {
                "ok": True,
                "changed": changed,
                "message": message,
                "id_documento": ordine.IdDocumento,
                "id_riga": ordine.IdRiga,
                "row_key": _row_key(ordine.IdDocumento, ordine.IdRiga),
                "rif_registraz": ordine.RifRegistraz,
                "stato_ordine": ordine.StatoOrdine,
                "tempo_funzionamento": tempo_funzionamento,
                "elapsed_seconds": elapsed_seconds,
                "active_tab": tab,
                "last_event_id": _last_change_event_id(),
                "fragments": fragments,
            }
        ),
        200,
    )


@main_bp.post("/api/ordini/montaggio/macchina/sospendi")
@login_required
@require_perm("home")
def api_sospendi_ordine_montaggio_macchina():
    data = request.get_json(silent=True) or {}

    id_documento = _norm_text(data.get("id_documento"))
    id_riga = _norm_text(data.get("id_riga"))
    causale = _norm_text(data.get("causale"))
    matricola = _norm_text(data.get("matricola"))
    fase = _norm_text(data.get("fase"))

    if not id_documento or not id_riga:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "IdDocumento e IdRiga sono obbligatori",
                }
            ),
            400,
        )

    policy = RbacPolicy(current_user)
    ordine = _get_visible_odp_by_key(policy, id_documento, id_riga)

    if _tab_from_ordine(ordine) != "montaggio":
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Ordine non appartenente alla vista montaggio",
                }
            ),
            400,
        )

    if _norm_text(ordine.GestioneMatricola).lower() != "si":
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Questa modalità è riservata agli ordini macchina",
                }
            ),
            400,
        )

    stato_attuale = _norm_text(ordine.StatoOrdine)
    stato_norm = stato_attuale.lower()

    changed = False
    message = None
    elapsed_seconds = 0
    tempo_funzionamento = "0"

    if stato_norm == "attivo":
        now_dt = _now_rome_dt()
        ordine.StatoOrdine = "In Sospeso"

        stato = StatoOdp.query.filter_by(
            IdDocumento=ordine.IdDocumento,
            IdRiga=ordine.IdRiga,
        ).first()

        if stato is None:
            db.session.rollback()
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": (
                            "Record odp_in_carico non trovato per questo ordine macchina. "
                            "La sospensione non può aggiornare Tempo_funzionamento."
                        ),
                        "id_documento": ordine.IdDocumento,
                        "id_riga": ordine.IdRiga,
                    }
                ),
                409,
            )

        stato.Stato_odp = "In Sospeso"
        stato.Utente_operazione = current_user.username

        elapsed_seconds = _accumulate_runtime_until(stato, now_dt)
        stato.Stato_odp = "In Sospeso"
        tempo_funzionamento = _norm_text(stato.Tempo_funzionamento) or "0"

        _push_change_event(
            topic="ordine_sospeso_montaggio_macchina",
            ordine=ordine,
            extra_payload={
                "azione": "sospensione_macchina",
                "utente": current_user.username,
                "causale": causale,
                "matricola": matricola,
                "fase": fase,
                "elapsed_seconds": elapsed_seconds,
                "tempo_funzionamento": tempo_funzionamento,
                "data_ultima_attivazione": stato.data_ultima_attivazione,
            },
        )

        db.session.commit()
        changed = True
        message = "Ordine macchina sospeso"

    elif stato_norm == "in sospeso":
        message = "Ordine macchina già in sospeso"

    else:
        message = f"Ordine macchina non sospendibile: stato attuale '{stato_attuale}'"

    tab, fragments = _fragments_for_ordine_tab(policy, ordine)

    return (
        jsonify(
            {
                "ok": True,
                "changed": changed,
                "message": message,
                "id_documento": ordine.IdDocumento,
                "id_riga": ordine.IdRiga,
                "row_key": _row_key(ordine.IdDocumento, ordine.IdRiga),
                "rif_registraz": ordine.RifRegistraz,
                "stato_ordine": ordine.StatoOrdine,
                "tempo_funzionamento": tempo_funzionamento,
                "elapsed_seconds": elapsed_seconds,
                "active_tab": tab,
                "last_event_id": _last_change_event_id(),
                "fragments": fragments,
            }
        ),
        200,
    )


@main_bp.post("/api/ordini/riattiva")
@login_required
@require_perm("home")
def api_riattiva_ordine():
    data = request.get_json(silent=True) or {}

    id_documento = _norm_text(data.get("id_documento"))
    id_riga = _norm_text(data.get("id_riga"))

    if not id_documento or not id_riga:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "IdDocumento e IdRiga sono obbligatori",
                }
            ),
            400,
        )

    policy = RbacPolicy(current_user)
    ordine = _get_visible_odp_by_key(policy, id_documento, id_riga)

    stato_attuale = _norm_text(ordine.StatoOrdine)
    stato_norm = stato_attuale.lower()
    changed = False
    message = None

    if stato_norm == "in sospeso":
        now_dt = _now_rome_dt()
        fase_corrente = _norm_text(ordine.FaseAttiva) or _norm_text(ordine.NumFase)

        ordine.StatoOrdine = "Attivo"

        stato = StatoOdp.query.filter_by(
            IdDocumento=ordine.IdDocumento,
            IdRiga=ordine.IdRiga,
        ).first()

        stato = _ensure_stato_attivo(
            ordine=ordine,
            stato=stato,
            username=current_user.username,
            when_dt=now_dt,
            fase_corrente=fase_corrente,
        )

        _push_change_event(
            topic="ordine_riattivato",
            ordine=ordine,
            extra_payload={
                "azione": "riattivazione",
                "utente": current_user.username,
                "data_ultima_attivazione": stato.data_ultima_attivazione,
                "tempo_funzionamento": stato.Tempo_funzionamento,
            },
        )

        db.session.commit()
        changed = True
        message = "Ordine riattivato"

    elif stato_norm == "attivo":
        message = "Ordine già attivo"

    else:
        message = f"Ordine non riattivabile: stato attuale '{stato_attuale}'"

    tab, fragments = _fragments_for_ordine_tab(policy, ordine)

    return (
        jsonify(
            {
                "ok": True,
                "changed": changed,
                "message": message,
                "id_documento": ordine.IdDocumento,
                "id_riga": ordine.IdRiga,
                "row_key": _row_key(ordine.IdDocumento, ordine.IdRiga),
                "rif_registraz": ordine.RifRegistraz,
                "stato_ordine": ordine.StatoOrdine,
                "active_tab": tab,
                "last_event_id": _last_change_event_id(),
                "fragments": fragments,
            }
        ),
        200,
    )


@main_bp.post("/api/ordini/montaggio/macchina/riattiva")
@login_required
@require_perm("home")
def api_riattiva_ordine_montaggio_macchina():
    data = request.get_json(silent=True) or {}

    id_documento = _norm_text(data.get("id_documento"))
    id_riga = _norm_text(data.get("id_riga"))
    matricola = _norm_text(data.get("matricola"))
    fase = _norm_text(data.get("fase"))

    if not id_documento or not id_riga:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "IdDocumento e IdRiga sono obbligatori",
                }
            ),
            400,
        )

    policy = RbacPolicy(current_user)
    ordine = _get_visible_odp_by_key(policy, id_documento, id_riga)

    if _tab_from_ordine(ordine) != "montaggio":
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Ordine non appartenente alla vista montaggio",
                }
            ),
            400,
        )

    if _norm_text(ordine.GestioneMatricola).lower() != "si":
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Questa modalità è riservata agli ordini macchina",
                }
            ),
            400,
        )

    stato_attuale = _norm_text(ordine.StatoOrdine)
    stato_norm = stato_attuale.lower()
    changed = False
    message = None

    if stato_norm == "in sospeso":
        now_dt = _now_rome_dt()
        fase_corrente = (
            fase or _norm_text(ordine.FaseAttiva) or _norm_text(ordine.NumFase)
        )

        ordine.StatoOrdine = "Attivo"

        stato = StatoOdp.query.filter_by(
            IdDocumento=ordine.IdDocumento,
            IdRiga=ordine.IdRiga,
        ).first()

        stato = _ensure_stato_attivo(
            ordine=ordine,
            stato=stato,
            username=current_user.username,
            when_dt=now_dt,
            fase_corrente=fase_corrente,
        )

        _push_change_event(
            topic="ordine_riattivato_montaggio_macchina",
            ordine=ordine,
            extra_payload={
                "azione": "riattivazione_macchina",
                "utente": current_user.username,
                "matricola": matricola,
                "fase": fase_corrente,
                "data_ultima_attivazione": stato.data_ultima_attivazione,
                "tempo_funzionamento": stato.Tempo_funzionamento,
            },
        )

        db.session.commit()
        changed = True
        message = "Ordine macchina riattivato"
    elif stato_norm == "attivo":
        message = "Ordine macchina già attivo"

    else:
        message = f"Ordine macchina non riattivabile: stato attuale '{stato_attuale}'"

    tab, fragments = _fragments_for_ordine_tab(policy, ordine)

    return (
        jsonify(
            {
                "ok": True,
                "changed": changed,
                "message": message,
                "id_documento": ordine.IdDocumento,
                "id_riga": ordine.IdRiga,
                "row_key": _row_key(ordine.IdDocumento, ordine.IdRiga),
                "rif_registraz": ordine.RifRegistraz,
                "stato_ordine": ordine.StatoOrdine,
                "active_tab": tab,
                "last_event_id": _last_change_event_id(),
                "fragments": fragments,
            }
        ),
        200,
    )


@main_bp.post("/api/ordini/chiudi")
@login_required
@require_perm("home")
def api_chiudi_ordine():
    data = request.get_json(silent=True) or {}

    id_documento = _norm_text(data.get("id_documento"))
    id_riga = _norm_text(data.get("id_riga"))
    q_ok_raw = data.get("quantita_conforme")
    q_nok_raw = data.get("quantita_non_conforme")
    note = _norm_text(data.get("note"))

    if not id_documento or not id_riga:
        return jsonify(
            {"ok": False, "error": "IdDocumento e IdRiga sono obbligatori"}
        ), 400

    policy = RbacPolicy(current_user)
    ordine = _get_visible_odp_by_key(policy, id_documento, id_riga)

    stato_attuale = _norm_text(ordine.StatoOrdine).lower()
    if stato_attuale == "pianificata":
        return jsonify(
            {"ok": False, "error": "Ordine non chiudibile: è ancora Pianificata"}
        ), 409

    # parse quantità ordine
    try:
        q_tot = _parse_qty_decimal(ordine.Quantita)
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400

    # quantità input (se non passate, default: tutto conforme)
    try:
        q_ok = _parse_qty_decimal(q_ok_raw) if q_ok_raw is not None else q_tot
        q_nok = _parse_qty_decimal(q_nok_raw) if q_nok_raw is not None else Decimal("0")
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400

    if q_ok < 0 or q_nok < 0:
        return jsonify(
            {"ok": False, "error": "Le quantità non possono essere negative"}
        ), 400
    if (q_ok + q_nok) > q_tot:
        return jsonify(
            {
                "ok": False,
                "error": "Quantità conforme + non conforme supera la quantità ordine",
            }
        ), 400

    now_dt = _now_rome_dt()
    now_iso = now_dt.isoformat(timespec="seconds")

    stato = StatoOdp.query.filter_by(
        IdDocumento=ordine.IdDocumento, IdRiga=ordine.IdRiga
    ).first()

    # finalizza runtime al momento della chiusura
    if stato is not None:
        # se vuoi essere più robusto, usa lo stato in odp_in_carico (non quello in input_odp)
        if _norm_text(stato.Stato_odp).lower().startswith("attiv"):
            _accumulate_runtime_until(
                stato, now_dt
            )  # <-- aggiorna Tempo_funzionamento sommando il delta

        tempo_finale = _norm_text(stato.Tempo_funzionamento) or "0"

        db.session.add(
            StatoOdpLog(
                IdDocumento=stato.IdDocumento,
                IdRiga=stato.IdRiga,
                RifRegistraz=stato.RifRegistraz,
                Stato_odp=stato.Stato_odp,
                Data_in_carico=stato.Data_in_carico,
                Tempo_funzionamento=tempo_finale,  # <-- qui finisce il valore “già sommato”
                Utente_operazione=stato.Utente_operazione,
                Fase=stato.Fase,
                data_ultima_attivazione=stato.data_ultima_attivazione,
                ClosedBy=current_user.username,
                ClosedAt=now_iso,
            )
        )
    db.session.flush()  # assicura che ChangeEvent abbia un id

    # --- LOG: 1) input_odp snapshot ---
    db.session.add(
        InputOdpLog(
            IdDocumento=ordine.IdDocumento,
            IdRiga=ordine.IdRiga,
            RifRegistraz=ordine.RifRegistraz,
            CodArt=ordine.CodArt,
            DesArt=ordine.DesArt,
            Quantita=ordine.Quantita,
            NumFase=ordine.NumFase,
            CodLavorazione=ordine.CodLavorazione,
            CodRisorsaProd=ordine.CodRisorsaProd,
            DataInizioSched=ordine.DataInizioSched,
            DataFineSched=ordine.DataFineSched,
            GestioneLotto=ordine.GestioneLotto,
            GestioneMatricola=ordine.GestioneMatricola,
            DistintaMateriale=ordine.DistintaMateriale,
            CodMatricola=ordine.CodMatricola,
            StatoRiga=ordine.StatoRiga,
            CodFamiglia=ordine.CodFamiglia,
            CodMacrofamiglia=ordine.CodMacrofamiglia,
            CodMagPrincipale=ordine.CodMagPrincipale,
            CodReparto=ordine.CodReparto,
            TempoPrevistoLavoraz=ordine.TempoPrevistoLavoraz,
            StatoOrdine=ordine.StatoOrdine,
            CodClassifTecnica=ordine.CodClassifTecnica,
            CodTipoDoc=ordine.CodTipoDoc,
            FaseAttiva=ordine.FaseAttiva,
            Note=ordine.Note,
            QuantitaConforme=str(q_ok),
            QuantitaNonConforme=str(q_nok),
            NoteChiusura=note,
            ClosedBy=current_user.username,
            ClosedAt=now_iso,
        )
    )

    # --- LOG: 2) odp_in_carico snapshot ---
    if stato is not None:
        db.session.add(
            StatoOdpLog(
                IdDocumento=stato.IdDocumento,
                IdRiga=stato.IdRiga,
                RifRegistraz=stato.RifRegistraz,
                Stato_odp=stato.Stato_odp,
                Data_in_carico=stato.Data_in_carico,
                Tempo_funzionamento=stato.Tempo_funzionamento,
                Utente_operazione=stato.Utente_operazione,
                Fase=stato.Fase,
                data_ultima_attivazione=stato.data_ultima_attivazione,
                ClosedBy=current_user.username,
                ClosedAt=now_iso,
            )
        )

    # --- LOG: 3) change_event (tutte le righe dell’ordine) ---
    # Usa json_extract perché i riferimenti ordine sono nel payload_json generato da _push_change_event.
    ce_rows = (
        ChangeEvent.query.filter(ChangeEvent.payload_json.isnot(None))
        .filter(
            func.json_extract(ChangeEvent.payload_json, "$.id_documento")
            == ordine.IdDocumento
        )
        .filter(
            func.json_extract(ChangeEvent.payload_json, "$.id_riga") == ordine.IdRiga
        )
        .order_by(ChangeEvent.id)
        .all()
    )
    for ce in ce_rows:
        db.session.add(
            ChangeEventLog(
                src_id=ce.id,
                topic=ce.topic,
                scope=ce.scope,
                payload_json=ce.payload_json,
                created_at=ce.created_at,
                IdDocumento=ordine.IdDocumento,
                IdRiga=ordine.IdRiga,
            )
        )

    # --- DELETE dal DB padre (ordine + runtime) ---
    tab = _tab_from_ordine(ordine)

    # (consigliato) conserva l’evento "ordine_chiuso" per far aggiornare gli altri client via polling,
    # ma puoi eliminare gli eventi vecchi dell’ordine per non far crescere la tabella.
    (
        ChangeEvent.query.filter(ChangeEvent.payload_json.isnot(None))
        .filter(
            func.json_extract(ChangeEvent.payload_json, "$.id_documento")
            == ordine.IdDocumento
        )
        .filter(
            func.json_extract(ChangeEvent.payload_json, "$.id_riga") == ordine.IdRiga
        )
        .filter(ChangeEvent.topic != "ordine_chiuso")
        .delete(synchronize_session=False)
    )

    if stato is not None:
        db.session.delete(stato)
    db.session.delete(ordine)

    # fragments dopo delete (così l’ordine sparisce)
    fragments = {}
    if tab:
        reparto_code = BRIDGE_CONFIG[tab]["reparto"]
        odp = list(_query_for_tab(policy, reparto_code).all())
        fragments = RENDERERS[tab](odp)

    db.session.commit()

    return jsonify(
        {
            "ok": True,
            "changed": True,
            "message": "Ordine chiuso",
            "id_documento": id_documento,
            "id_riga": id_riga,
            "row_key": _row_key(id_documento, id_riga),
            "active_tab": tab,
            "last_event_id": _last_change_event_id(),
            "fragments": fragments,
        }
    ), 200
