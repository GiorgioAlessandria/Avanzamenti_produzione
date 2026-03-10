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
    GiacenzaLotti,
    LottiUsatiLog,
    ErpOutbox,
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


def _parse_bool_flag(value) -> bool:
    if isinstance(value, bool):
        return value
    raw = _norm_text(value).lower()
    return raw in {"1", "true", "si", "sì", "yes", "on"}


def _decimal_to_text(value: Decimal) -> str:
    if not isinstance(value, Decimal):
        value = Decimal(str(value))
    s = format(value.normalize(), "f") if value != 0 else "0"
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s or "0"


def _qty_da_lavorare_text(ordine) -> str:
    return _norm_text(getattr(ordine, "QtyDaLavorare", "")) or _norm_text(
        ordine.Quantita
    )


def _qty_da_lavorare_decimal(ordine) -> Decimal:
    return _parse_qty_decimal(_qty_da_lavorare_text(ordine))


def _get_blocking_outbox_for_phase(
    id_documento: str,
    id_riga: str,
    fase: str,
):
    fase = _norm_text(fase)
    if not fase:
        return None

    return (
        ErpOutbox.query.filter_by(
            IdDocumento=id_documento,
            IdRiga=id_riga,
            Fase=fase,
        )
        .filter(ErpOutbox.status.in_(["pending", "exported", "error"]))
        .order_by(ErpOutbox.outbox_id.desc())
        .first()
    )


def _parse_distinta_materiale(ordine) -> list[dict]:
    distinta = []
    if ordine.DistintaMateriale:
        try:
            distinta = json.loads(ordine.DistintaMateriale)
            if isinstance(distinta, str):
                distinta = json.loads(distinta)
        except (json.JSONDecodeError, TypeError):
            distinta = []
    return distinta if isinstance(distinta, list) else []


def _fase_attiva_int(ordine) -> int | None:
    try:
        return int(float(_norm_text(ordine.FaseAttiva)))
    except (ValueError, TypeError):
        return None


def _fase_to_int(value) -> int | None:
    s = _norm_text(value)
    if not s:
        return None
    try:
        return int(float(s))
    except (TypeError, ValueError):
        return None


def _phase_sequence_for_ordine(ordine) -> list[str]:
    totale_fasi = _fase_to_int(getattr(ordine, "NumFase", ""))

    if totale_fasi is None or totale_fasi <= 0:
        fase_corrente = _fase_to_int(getattr(ordine, "FaseAttiva", ""))
        if fase_corrente is not None and fase_corrente > 0:
            return [str(fase_corrente)]
        return []

    return [str(i) for i in range(1, totale_fasi + 1)]


def _get_phase_transition(ordine, fase_corrente: str) -> tuple[bool, str | None]:
    fasi = _phase_sequence_for_ordine(ordine)
    if not fasi:
        return True, None

    fase_corrente = _norm_text(fase_corrente)
    if fase_corrente not in fasi:
        return True, None

    idx = fasi.index(fase_corrente)
    is_last = idx >= len(fasi) - 1
    next_phase = None if is_last else fasi[idx + 1]
    return is_last, next_phase


def _reset_runtime_after_close(stato, username: str):
    if stato is None:
        return
    stato.Stato_odp = "Pianificata"
    stato.Utente_operazione = username
    stato.data_ultima_attivazione = None


def _advance_or_finalize_phase(
    *,
    ordine,
    stato,
    fase_corrente: str,
    q_ok: Decimal,
    q_nok: Decimal,
    qty_residua: Decimal,
    qty_residua_text: str,
    qty_lavorata_text: str,
    chiusura_parziale: bool,
    username: str,
):
    is_last_phase, next_phase = _get_phase_transition(ordine, fase_corrente)

    # Chiusura parziale: stessa fase, torna da eseguire
    if chiusura_parziale:
        ordine.FaseAttiva = fase_corrente
        ordine.StatoOrdine = "Pianificata"
        ordine.QtyDaLavorare = qty_residua_text
        _reset_runtime_after_close(stato, username)
        return {
            "tipo": "parziale_stessa_fase",
            "fase_corrente": fase_corrente,
            "fase_successiva": fase_corrente,
        }

    # Ultima fase: chiusura definitiva
    if is_last_phase:
        ordine.FaseAttiva = fase_corrente
        ordine.StatoOrdine = "Chiusa"
        ordine.QtyDaLavorare = "0"
        return {
            "tipo": "finale",
            "fase_corrente": fase_corrente,
            "fase_successiva": None,
        }

    # Fase intermedia completa: passa alla successiva
    ordine.FaseAttiva = next_phase
    ordine.StatoOrdine = "Pianificata"
    ordine.QtyDaLavorare = _decimal_to_text(q_ok)
    _reset_runtime_after_close(stato, username)

    return {
        "tipo": "avanzata",
        "fase_corrente": fase_corrente,
        "fase_successiva": next_phase,
    }


def _fase_corrente_for_export(ordine, stato=None, fase_override="") -> str:
    raw = (
        _norm_text(fase_override)
        or _norm_text(getattr(stato, "Fase", ""))
        or _norm_text(getattr(ordine, "FaseAttiva", ""))
    )

    fase_int = _fase_to_int(raw)
    if fase_int is not None and fase_int > 0:
        return str(fase_int)

    totale = _fase_to_int(getattr(ordine, "NumFase", ""))
    if totale == 1:
        return "1"

    return ""


def _componenti_lotto_per_ordine(
    ordine,
    include_senza_lotti: bool = False,
    **_unused,
) -> list[dict]:
    if _norm_text(ordine.GestioneLotto).lower() != "si":
        return []

    distinta = _parse_distinta_materiale(ordine)
    fase_attiva = _fase_attiva_int(ordine)

    componenti_lotto = []
    codici_visti = set()

    for comp in distinta:
        if not isinstance(comp, dict):
            continue

        if fase_attiva is not None:
            try:
                comp_fase = int(float(comp.get("NumFase", 0)))
            except (ValueError, TypeError):
                comp_fase = 0
            if comp_fase != fase_attiva:
                continue

        comp_gl = _norm_text(comp.get("GestioneLotto", "")).lower()
        if comp_gl != "si":
            continue

        cod_art = _norm_text(comp.get("CodArt", ""))
        if not cod_art or cod_art in codici_visti:
            continue

        codici_visti.add(cod_art)

        lotti_db = GiacenzaLotti.query.filter_by(CodArt=cod_art).all()
        lotti_list = []
        for lotto in lotti_db:
            try:
                giacenza_val = int(float(_norm_text(lotto.Giacenza)))
            except (ValueError, TypeError):
                giacenza_val = 0

            if giacenza_val <= 0:
                continue

            lotti_list.append(
                {
                    "RifLottoAlfa": lotto.RifLottoAlfa,
                    "Giacenza": giacenza_val,
                    "CodMag": lotto.CodMag,
                }
            )

        if include_senza_lotti or lotti_list:
            componenti_lotto.append(
                {
                    "CodArt": cod_art,
                    "DesArt": _norm_text(comp.get("DesArt", "")),
                    "Quantita": comp.get("Quantita", 0),
                    "NumFase": comp.get("NumFase", ""),
                    "GestioneLotto": "si",
                    "lotti": lotti_list,
                }
            )

    return componenti_lotto


def _same_decimal_qty(a: Decimal, b: Decimal, tol: Decimal = Decimal("0.0001")) -> bool:
    return abs(a - b) <= tol


def _scaled_component_qty(
    comp_qty,
    q_lavorata: Decimal,
    q_tot: Decimal,
) -> Decimal:
    try:
        base_qty = _parse_qty_decimal(comp_qty)
    except ValueError:
        return Decimal("0")

    if q_tot <= 0:
        return base_qty

    return (base_qty * q_lavorata / q_tot).quantize(
        Decimal("0.0001"),
        rounding=ROUND_HALF_UP,
    )


def _normalize_lotti_for_payload(lotti_input: list[dict]) -> list[dict]:
    rows = []
    for row in lotti_input or []:
        rows.append(
            {
                "CodArt": _norm_text(row.get("CodArt")),
                "RifLottoAlfa": _norm_text(row.get("RifLottoAlfa")),
                "Quantita": str(row.get("Quantita", 0)),
                "Esito": _norm_text(row.get("Esito", "ok")),
            }
        )
    return rows


def _build_phase_payload(
    ordine,
    fase_corrente: str,
    q_ok: Decimal,
    q_nok: Decimal,
    tempo_finale: str,
    lotti_input: list[dict],
    note: str,
    now_iso: str,
) -> dict:
    return {
        "kind": "consuntivo_fase",
        "id_documento": ordine.IdDocumento,
        "id_riga": ordine.IdRiga,
        "rif_registraz": ordine.RifRegistraz,
        "cod_art": ordine.CodArt,
        "descrizione": ordine.DesArt,
        "fase": fase_corrente,
        "cod_reparto": ordine.CodReparto,
        "quantita_ordine": _norm_text(ordine.Quantita),
        "quantita_da_lavorare": _qty_da_lavorare_text(ordine),
        "quantita_ok": str(q_ok),
        "quantita_ko": str(q_nok),
        "tempo_funzionamento": tempo_finale,
        "note": note,
        "lotti": _normalize_lotti_for_payload(lotti_input),
        "created_at": now_iso,
        "created_by": current_user.username,
        "chiusura_parziale": False,
    }


def _queue_phase_export(ordine, fase_corrente: str, payload: dict):
    outbox = ErpOutbox(
        kind="consuntivo_fase",
        status="pending",
        IdDocumento=ordine.IdDocumento,
        IdRiga=ordine.IdRiga,
        RifRegistraz=ordine.RifRegistraz,
        CodArt=ordine.CodArt,
        Fase=fase_corrente,
        CodReparto=_norm_text(ordine.CodReparto),
        payload_json=json.dumps(payload, ensure_ascii=False),
    )
    db.session.add(outbox)
    db.session.flush()
    return outbox


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

    fase_corrente = _fase_corrente_for_export(ordine)
    blocking_outbox = _get_blocking_outbox_for_phase(
        id_documento=ordine.IdDocumento,
        id_riga=ordine.IdRiga,
        fase=fase_corrente,
    )

    if blocking_outbox is not None:
        tab, fragments = _fragments_for_ordine_tab(policy, ordine)

        return (
            jsonify(
                {
                    "ok": False,
                    "changed": False,
                    "error": (
                        "Questa fase risulta già consuntivata ed è ancora in attesa "
                        "di sincronizzazione con il gestionale."
                    ),
                    "message": (
                        f"Presa in carico bloccata: fase {fase_corrente} con export "
                        f"in stato '{blocking_outbox.status}'."
                    ),
                    "id_documento": ordine.IdDocumento,
                    "id_riga": ordine.IdRiga,
                    "row_key": _row_key(ordine.IdDocumento, ordine.IdRiga),
                    "rif_registraz": ordine.RifRegistraz,
                    "stato_ordine": ordine.StatoOrdine,
                    "fase": fase_corrente,
                    "outbox_status": blocking_outbox.status,
                    "outbox_id": blocking_outbox.outbox_id,
                    "active_tab": tab,
                    "last_event_id": _last_change_event_id(),
                    "fragments": fragments,
                }
            ),
            409,
        )

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
                "fase": fase_corrente,
                "data_ultima_attivazione": stato.data_ultima_attivazione,
                "tempo_funzionamento": stato.Tempo_funzionamento,
            },
        )

        db.session.commit()
        changed = True
        message = "Ordine preso in carico"

    elif stato_norm == "attivo":
        message = "Ordine già attivo"

    elif stato_norm == "in sospeso":
        message = "Ordine in sospeso: usare la riattivazione"

    else:
        message = f"Ordine non prendibile: stato attuale '{stato_attuale}'"

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
                "fase": fase_corrente,
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
    fase_corrente = _fase_corrente_for_export(ordine)

    blocking_outbox = _get_blocking_outbox_for_phase(
        id_documento=ordine.IdDocumento,
        id_riga=ordine.IdRiga,
        fase=fase_corrente,
    )

    if blocking_outbox is not None:
        tab, fragments = _fragments_for_ordine_tab(policy, ordine)

        return (
            jsonify(
                {
                    "ok": False,
                    "changed": False,
                    "error": (
                        "Questa fase risulta già consuntivata ed è ancora in attesa "
                        "di sincronizzazione con il gestionale."
                    ),
                    "message": (
                        f"Riattivazione bloccata: fase {fase_corrente} con export "
                        f"in stato '{blocking_outbox.status}'."
                    ),
                    "id_documento": ordine.IdDocumento,
                    "id_riga": ordine.IdRiga,
                    "row_key": _row_key(ordine.IdDocumento, ordine.IdRiga),
                    "rif_registraz": ordine.RifRegistraz,
                    "stato_ordine": ordine.StatoOrdine,
                    "fase": fase_corrente,
                    "outbox_status": blocking_outbox.status,
                    "outbox_id": blocking_outbox.outbox_id,
                    "active_tab": tab,
                    "last_event_id": _last_change_event_id(),
                    "fragments": fragments,
                }
            ),
            409,
        )

    stato_attuale = _norm_text(ordine.StatoOrdine)
    stato_norm = stato_attuale.lower()
    changed = False
    message = None

    if stato_norm == "in sospeso":
        now_dt = _now_rome_dt()

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
                "fase": fase_corrente,
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
                "fase": fase_corrente,
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

    fase_corrente = _fase_corrente_for_export(ordine, fase_override=fase)
    blocking_outbox = _get_blocking_outbox_for_phase(
        id_documento=ordine.IdDocumento,
        id_riga=ordine.IdRiga,
        fase=fase_corrente,
    )

    if blocking_outbox is not None:
        tab, fragments = _fragments_for_ordine_tab(policy, ordine)

        return (
            jsonify(
                {
                    "ok": False,
                    "changed": False,
                    "error": (
                        "Questa fase risulta già consuntivata ed è ancora in attesa "
                        "di sincronizzazione con il gestionale."
                    ),
                    "message": (
                        f"Riattivazione bloccata: fase {fase_corrente} con export "
                        f"in stato '{blocking_outbox.status}'."
                    ),
                    "id_documento": ordine.IdDocumento,
                    "id_riga": ordine.IdRiga,
                    "row_key": _row_key(ordine.IdDocumento, ordine.IdRiga),
                    "rif_registraz": ordine.RifRegistraz,
                    "stato_ordine": ordine.StatoOrdine,
                    "fase": fase_corrente,
                    "outbox_status": blocking_outbox.status,
                    "outbox_id": blocking_outbox.outbox_id,
                    "active_tab": tab,
                    "last_event_id": _last_change_event_id(),
                    "fragments": fragments,
                }
            ),
            409,
        )

    stato_attuale = _norm_text(ordine.StatoOrdine)
    stato_norm = stato_attuale.lower()
    changed = False
    message = None

    if stato_norm == "in sospeso":
        now_dt = _now_rome_dt()

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
                "fase": fase_corrente,
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
    q_ok_raw = data.get("quantita_conforme") or data.get("quantita_prodotta")
    q_nok_raw = data.get("quantita_non_conforme") or data.get("quantita_scartata")
    note = _norm_text(data.get("note"))
    lotti_input = data.get("lotti") or []
    chiusura_parziale = _parse_bool_flag(data.get("chiusura_parziale"))

    if not id_documento or not id_riga:
        return (
            jsonify({"ok": False, "error": "IdDocumento e IdRiga sono obbligatori"}),
            400,
        )

    policy = RbacPolicy(current_user)
    ordine = _get_visible_odp_by_key(policy, id_documento, id_riga)

    fase_corrente = _fase_corrente_for_export(ordine)
    blocking_outbox = _get_blocking_outbox_for_phase(
        id_documento=ordine.IdDocumento,
        id_riga=ordine.IdRiga,
        fase=fase_corrente,
    )
    if blocking_outbox is not None:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": (
                        "Questa fase risulta già consuntivata ed è ancora in attesa "
                        "di sincronizzazione con il gestionale."
                    ),
                    "outbox_status": blocking_outbox.status,
                    "outbox_id": blocking_outbox.outbox_id,
                }
            ),
            409,
        )

    stato_attuale = _norm_text(ordine.StatoOrdine).lower()
    if stato_attuale == "pianificata":
        return (
            jsonify(
                {"ok": False, "error": "Ordine non chiudibile: è ancora Pianificata"}
            ),
            409,
        )

    try:
        q_tot = _qty_da_lavorare_decimal(ordine)
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400

    try:
        q_ok = _parse_qty_decimal(q_ok_raw) if q_ok_raw is not None else q_tot
        q_nok = _parse_qty_decimal(q_nok_raw) if q_nok_raw is not None else Decimal("0")
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400

    if q_ok < 0 or q_nok < 0:
        return (
            jsonify({"ok": False, "error": "Le quantità non possono essere negative"}),
            400,
        )

    q_lavorata = q_ok + q_nok
    qty_residua = q_tot - q_lavorata
    qty_residua_text = _decimal_to_text(qty_residua)
    qty_lavorata_text = _decimal_to_text(q_lavorata)
    qty_pre_text = _qty_da_lavorare_text(ordine)

    # Regole nuove:
    # - SL totale: nessun controllo min/max quantità
    # - SL parziale: quantità lavorata > 0 e strettamente minore del totale ordine
    if chiusura_parziale:
        if q_lavorata <= 0:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": "Per la chiusura parziale devi indicare una quantità lavorata > 0.",
                    }
                ),
                400,
            )

        if q_lavorata >= q_tot:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": "Nella chiusura parziale la quantità lavorata deve essere strettamente minore della quantità totale dell'ordine.",
                    }
                ),
                400,
            )

    componenti_richiesti_lotto = _componenti_lotto_per_ordine(
        ordine,
        include_senza_lotti=True,
    )
    if componenti_richiesti_lotto and not lotti_input:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Per questo ordine è obbligatoria l'assegnazione dei lotti materiale prima della chiusura.",
                }
            ),
            400,
        )

    if lotti_input:
        for lotto_row in lotti_input:
            cod_art = _norm_text(lotto_row.get("CodArt"))
            rif_lotto = _norm_text(lotto_row.get("RifLottoAlfa"))
            try:
                qty = _parse_qty_decimal(lotto_row.get("Quantita"))
            except ValueError as e:
                return (
                    jsonify({"ok": False, "error": f"Quantità lotto non valida: {e}"}),
                    400,
                )

            if not cod_art or not rif_lotto:
                return (
                    jsonify(
                        {
                            "ok": False,
                            "error": "Codice e lotto obbligatori per ogni riga.",
                        }
                    ),
                    400,
                )
            if qty <= 0:
                return (
                    jsonify(
                        {
                            "ok": False,
                            "error": f"{cod_art} lotto {rif_lotto}: quantità deve essere > 0.",
                        }
                    ),
                    400,
                )

            lotto_db = GiacenzaLotti.query.filter_by(
                CodArt=cod_art, RifLottoAlfa=rif_lotto
            ).first()
            if lotto_db is None:
                return (
                    jsonify(
                        {
                            "ok": False,
                            "error": f"Lotto {rif_lotto} non trovato per {cod_art}.",
                        }
                    ),
                    400,
                )
            try:
                giacenza = _parse_qty_decimal(lotto_db.Giacenza)
            except ValueError:
                giacenza = Decimal("0")
            if qty > giacenza:
                return (
                    jsonify(
                        {
                            "ok": False,
                            "error": f"{cod_art} lotto {rif_lotto}: qtà {qty} > giacenza {giacenza}.",
                        }
                    ),
                    400,
                )

    now_dt = _now_rome_dt()
    now_iso = now_dt.isoformat(timespec="seconds")

    stato = StatoOdp.query.filter_by(
        IdDocumento=ordine.IdDocumento, IdRiga=ordine.IdRiga
    ).first()

    tempo_finale = "0"
    if stato is not None:
        if _norm_text(stato.Stato_odp).lower().startswith("attiv"):
            _accumulate_runtime_until(stato, now_dt)
        tempo_finale = _norm_text(stato.Tempo_funzionamento) or "0"

    fase_corrente = _fase_corrente_for_export(ordine, stato=stato)
    outbox = None

    if chiusura_parziale:
        if stato is None:
            stato = StatoOdp(
                IdDocumento=ordine.IdDocumento,
                IdRiga=ordine.IdRiga,
                RifRegistraz=ordine.RifRegistraz,
                Stato_odp="In Sospeso",
                Data_in_carico=now_iso,
                Tempo_funzionamento=tempo_finale or "0",
                Utente_operazione=current_user.username,
                Fase=fase_corrente,
                data_ultima_attivazione=None,
            )
            db.session.add(stato)
        else:
            stato.Stato_odp = "In Sospeso"
            stato.Utente_operazione = current_user.username
            stato.Fase = fase_corrente
            stato.data_ultima_attivazione = None

        ordine.StatoOrdine = "In Sospeso"
        ordine.QtyDaLavorare = qty_residua_text

        _push_change_event(
            topic="fase_consuntivata_parziale",
            ordine=ordine,
            extra_payload={
                "azione": "consuntivo_fase_parziale",
                "utente": current_user.username,
                "fase": fase_corrente,
                "quantita_conforme": str(q_ok),
                "quantita_non_conforme": str(q_nok),
                "quantita_lavorata_step": qty_lavorata_text,
                "qty_da_lavorare_pre": qty_pre_text,
                "qty_da_lavorare_post": qty_residua_text,
                "tempo_funzionamento": tempo_finale,
                "lotti_count": len(lotti_input),
                "chiusura_parziale": True,
                "richiede_export_erp": True,
                "erp_export_kind": "consuntivo_fase_parziale",
                "erp_outbox_flag_only": True,
            },
        )
    else:
        payload = _build_phase_payload(
            ordine=ordine,
            fase_corrente=fase_corrente,
            q_ok=q_ok,
            q_nok=q_nok,
            tempo_finale=tempo_finale,
            lotti_input=lotti_input,
            note=note,
            now_iso=now_iso,
        )
        outbox = _queue_phase_export(
            ordine=ordine,
            fase_corrente=fase_corrente,
            payload=payload,
        )

        _push_change_event(
            topic="fase_consuntivata",
            ordine=ordine,
            extra_payload={
                "azione": "consuntivo_fase",
                "utente": current_user.username,
                "fase": fase_corrente,
                "quantita_conforme": str(q_ok),
                "quantita_non_conforme": str(q_nok),
                "tempo_funzionamento": tempo_finale,
                "lotti_count": len(lotti_input),
                "outbox_id": outbox.outbox_id,
                "export_status": outbox.status,
                "chiusura_parziale": False,
            },
        )

    db.session.flush()

    note_chiusura_log = note
    if chiusura_parziale:
        note_chiusura_log = (
            f"[PARZIALE] residuo={qty_residua_text}; {note}".strip().rstrip(";")
        )

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
            NoteChiusura=note_chiusura_log,
            ClosedBy=current_user.username,
            ClosedAt=now_iso,
        )
    )

    if stato is not None:
        db.session.add(
            StatoOdpLog(
                IdDocumento=stato.IdDocumento,
                IdRiga=stato.IdRiga,
                RifRegistraz=stato.RifRegistraz,
                Stato_odp=stato.Stato_odp,
                Data_in_carico=stato.Data_in_carico,
                Tempo_funzionamento=tempo_finale,
                Utente_operazione=stato.Utente_operazione,
                Fase=stato.Fase,
                data_ultima_attivazione=stato.data_ultima_attivazione,
                ClosedBy=current_user.username,
                ClosedAt=now_iso,
            )
        )

    if lotti_input:
        for lotto_row in lotti_input:
            lotto_log = LottiUsatiLog(
                IdDocumento=ordine.IdDocumento,
                IdRiga=ordine.IdRiga,
                RifRegistraz=ordine.RifRegistraz,
                CodArt=_norm_text(lotto_row.get("CodArt")),
                RifLottoAlfa=_norm_text(lotto_row.get("RifLottoAlfa")),
                Quantita=str(lotto_row.get("Quantita", 0)),
                Esito=_norm_text(lotto_row.get("Esito", "ok")),
                ClosedBy=current_user.username,
                ClosedAt=now_iso,
            )
            if hasattr(LottiUsatiLog, "Fase"):
                lotto_log.Fase = fase_corrente
            db.session.add(lotto_log)

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
    transition = _advance_or_finalize_phase(
        ordine=ordine,
        stato=stato,
        fase_corrente=fase_corrente,
        q_ok=q_ok,
        q_nok=q_nok,
        qty_residua=qty_residua,
        qty_residua_text=qty_residua_text,
        qty_lavorata_text=qty_lavorata_text,
        chiusura_parziale=chiusura_parziale,
        username=current_user.username,
    )

    tab = _tab_from_ordine(ordine)

    if stato is not None:
        db.session.delete(stato)

    fragments = {}
    if tab:
        reparto_code = BRIDGE_CONFIG[tab]["reparto"]
        odp = list(_query_for_tab(policy, reparto_code).all())
        fragments = RENDERERS[tab](odp)

    db.session.commit()

    if transition["tipo"] == "finale":
        message = "Ordine chiuso definitivamente"
    elif transition["tipo"] == "avanzata":
        message = (
            f"Fase {transition['fase_corrente']} consuntivata. "
            f"Ordine riportato in pianificata sulla fase {transition['fase_successiva']}."
        )
    else:
        message = (
            f"Fase {transition['fase_corrente']} chiusa parzialmente. "
            f"Ordine riportato in pianificata sulla stessa fase."
        )

    return (
        jsonify(
            {
                "ok": True,
                "changed": True,
                "message": message,
                "id_documento": id_documento,
                "id_riga": id_riga,
                "row_key": _row_key(id_documento, id_riga),
                "fase": transition["fase_corrente"],
                "fase_successiva": transition["fase_successiva"],
                "stato_ordine": ordine.StatoOrdine,
                "chiusura_parziale": chiusura_parziale,
                "qty_da_lavorare": _norm_text(ordine.QtyDaLavorare),
                "outbox_id": outbox.outbox_id if outbox else None,
                "outbox_status": outbox.status if outbox else None,
                "active_tab": tab,
                "last_event_id": _last_change_event_id(),
                "fragments": fragments,
            }
        ),
        200,
    )


@main_bp.post("/api/ordini/montaggio/macchina/chiudi")
@login_required
@require_perm("home")
def api_chiudi_ordine_montaggio_macchina():
    data = request.get_json(silent=True) or {}

    id_documento = _norm_text(data.get("id_documento"))
    id_riga = _norm_text(data.get("id_riga"))
    matricola = _norm_text(data.get("matricola"))
    fase = _norm_text(data.get("fase"))
    note = _norm_text(data.get("note"))
    lotti_input = data.get("lotti") or []

    if not id_documento or not id_riga:
        return (
            jsonify({"ok": False, "error": "IdDocumento e IdRiga sono obbligatori"}),
            400,
        )

    policy = RbacPolicy(current_user)
    ordine = _get_visible_odp_by_key(policy, id_documento, id_riga)

    if _tab_from_ordine(ordine) != "montaggio":
        return (
            jsonify(
                {"ok": False, "error": "Ordine non appartenente alla vista montaggio"}
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

    fase_corrente = _fase_corrente_for_export(ordine, fase_override=fase)
    blocking_outbox = _get_blocking_outbox_for_phase(
        id_documento=ordine.IdDocumento,
        id_riga=ordine.IdRiga,
        fase=fase_corrente,
    )
    if blocking_outbox is not None:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": (
                        "Questa fase risulta già consuntivata ed è ancora in attesa "
                        "di sincronizzazione con il gestionale."
                    ),
                    "outbox_status": blocking_outbox.status,
                    "outbox_id": blocking_outbox.outbox_id,
                }
            ),
            409,
        )

    stato_attuale = _norm_text(ordine.StatoOrdine).lower()
    if stato_attuale == "pianificata":
        return (
            jsonify(
                {"ok": False, "error": "Ordine non chiudibile: è ancora Pianificata"}
            ),
            409,
        )

    componenti_richiesti_lotto = _componenti_lotto_per_ordine(
        ordine,
        include_senza_lotti=True,
    )
    if componenti_richiesti_lotto and not lotti_input:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Per questo ordine è obbligatoria l'assegnazione dei lotti materiale prima della chiusura.",
                }
            ),
            400,
        )

    if lotti_input:
        for lotto_row in lotti_input:
            cod_art = _norm_text(lotto_row.get("CodArt"))
            rif_lotto = _norm_text(lotto_row.get("RifLottoAlfa"))
            try:
                qty = _parse_qty_decimal(lotto_row.get("Quantita"))
            except ValueError as e:
                return jsonify(
                    {"ok": False, "error": f"Quantità lotto non valida: {e}"}
                ), 400

            if not cod_art or not rif_lotto:
                return jsonify(
                    {
                        "ok": False,
                        "error": "Codice e lotto sono obbligatori per ogni riga lotti.",
                    }
                ), 400

            if qty <= 0:
                return jsonify(
                    {
                        "ok": False,
                        "error": f"{cod_art} lotto {rif_lotto}: quantità deve essere > 0.",
                    }
                ), 400

            lotto_db = GiacenzaLotti.query.filter_by(
                CodArt=cod_art, RifLottoAlfa=rif_lotto
            ).first()

            if lotto_db is None:
                return jsonify(
                    {
                        "ok": False,
                        "error": f"Lotto {rif_lotto} non trovato per {cod_art}.",
                    }
                ), 400

            try:
                giacenza = _parse_qty_decimal(lotto_db.Giacenza)
            except ValueError:
                giacenza = Decimal("0")

            if qty > giacenza:
                return jsonify(
                    {
                        "ok": False,
                        "error": f"{cod_art} lotto {rif_lotto}: quantità {qty} supera giacenza {giacenza}.",
                    }
                ), 400

    try:
        q_tot = _qty_da_lavorare_decimal(ordine)
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400

    q_ok = q_tot
    q_nok = Decimal("0")
    qty_residua = Decimal("0")
    qty_residua_text = "0"
    qty_lavorata_text = _decimal_to_text(q_tot)
    chiusura_parziale = False

    now_dt = _now_rome_dt()
    now_iso = now_dt.isoformat(timespec="seconds")

    stato = StatoOdp.query.filter_by(
        IdDocumento=ordine.IdDocumento, IdRiga=ordine.IdRiga
    ).first()

    tempo_finale = "0"
    if stato is not None:
        if _norm_text(stato.Stato_odp).lower().startswith("attiv"):
            _accumulate_runtime_until(stato, now_dt)
        tempo_finale = _norm_text(stato.Tempo_funzionamento) or "0"

    fase_corrente = _fase_corrente_for_export(ordine, stato=stato, fase_override=fase)
    payload = _build_phase_payload(
        ordine=ordine,
        fase_corrente=fase_corrente,
        q_ok=q_ok,
        q_nok=q_nok,
        tempo_finale=tempo_finale,
        lotti_input=lotti_input,
        note=note,
        now_iso=now_iso,
    )
    outbox = _queue_phase_export(
        ordine=ordine,
        fase_corrente=fase_corrente,
        payload=payload,
    )

    _push_change_event(
        topic="fase_consuntivata_montaggio_macchina",
        ordine=ordine,
        extra_payload={
            "azione": "consuntivo_fase_macchina",
            "utente": current_user.username,
            "matricola": matricola,
            "fase": fase_corrente,
            "quantita_conforme": str(q_ok),
            "quantita_non_conforme": str(q_nok),
            "tempo_funzionamento": tempo_finale,
            "lotti_count": len(lotti_input),
            "outbox_id": outbox.outbox_id,
            "export_status": outbox.status,
        },
    )
    db.session.flush()

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

    if stato is not None:
        db.session.add(
            StatoOdpLog(
                IdDocumento=stato.IdDocumento,
                IdRiga=stato.IdRiga,
                RifRegistraz=stato.RifRegistraz,
                Stato_odp=stato.Stato_odp,
                Data_in_carico=stato.Data_in_carico,
                Tempo_funzionamento=tempo_finale,
                Utente_operazione=stato.Utente_operazione,
                Fase=stato.Fase,
                data_ultima_attivazione=stato.data_ultima_attivazione,
                ClosedBy=current_user.username,
                ClosedAt=now_iso,
            )
        )

    for lotto_row in lotti_input:
        lotto_log = LottiUsatiLog(
            IdDocumento=ordine.IdDocumento,
            IdRiga=ordine.IdRiga,
            RifRegistraz=ordine.RifRegistraz,
            CodArt=_norm_text(lotto_row.get("CodArt")),
            RifLottoAlfa=_norm_text(lotto_row.get("RifLottoAlfa")),
            Quantita=str(lotto_row.get("Quantita", 0)),
            Esito=_norm_text(lotto_row.get("Esito", "ok")),
            ClosedBy=current_user.username,
            ClosedAt=now_iso,
        )
        if hasattr(LottiUsatiLog, "Fase"):
            lotto_log.Fase = fase_corrente
        db.session.add(lotto_log)

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
    transition = _advance_or_finalize_phase(
        ordine=ordine,
        stato=stato,
        fase_corrente=fase_corrente,
        q_ok=q_ok,
        q_nok=q_nok,
        qty_residua=qty_residua,
        qty_residua_text=qty_residua_text,
        qty_lavorata_text=qty_lavorata_text,
        chiusura_parziale=chiusura_parziale,
        username=current_user.username,
    )

    tab = _tab_from_ordine(ordine)

    if stato is not None:
        db.session.delete(stato)

    fragments = {}
    if tab:
        reparto_code = BRIDGE_CONFIG[tab]["reparto"]
        odp = list(_query_for_tab(policy, reparto_code).all())
        fragments = RENDERERS[tab](odp)

    db.session.commit()

    if transition["tipo"] == "finale":
        message = "Ordine macchina chiuso definitivamente"
    else:
        message = (
            f"Fase macchina {transition['fase_corrente']} consuntivata. "
            f"Ordine riportato in pianificata sulla fase {transition['fase_successiva']}."
        )

    return (
        jsonify(
            {
                "ok": True,
                "changed": True,
                "message": message,
                "id_documento": id_documento,
                "id_riga": id_riga,
                "row_key": _row_key(id_documento, id_riga),
                "fase": transition["fase_corrente"],
                "fase_successiva": transition["fase_successiva"],
                "stato_ordine": ordine.StatoOrdine,
                "qty_da_lavorare": _norm_text(ordine.QtyDaLavorare),
                "outbox_id": outbox.outbox_id,
                "outbox_status": outbox.status,
                "active_tab": tab,
                "last_event_id": _last_change_event_id(),
                "fragments": fragments,
            }
        ),
        200,
    )


@main_bp.post("/api/ordini/lotti-componenti")
@login_required
@require_perm("home")
def api_lotti_componenti():
    data = request.get_json(silent=True) or {}
    id_documento = _norm_text(data.get("id_documento"))
    id_riga = _norm_text(data.get("id_riga"))
    modalita = _norm_text(data.get("modalita")).lower()
    is_macchina = modalita == "m"

    if not id_documento or not id_riga:
        return jsonify({"ok": False, "error": "IdDocumento e IdRiga obbligatori"}), 400

    policy = RbacPolicy(current_user)
    ordine = _get_visible_odp_by_key(policy, id_documento, id_riga)

    if is_macchina:
        if _norm_text(ordine.GestioneLotto).lower() != "si":
            return jsonify(
                {
                    "ok": True,
                    "gestioneLotto": False,
                    "haComponentiLotto": False,
                    "componenti": [],
                }
            )

        componenti_lotto = _componenti_lotto_per_ordine(
            ordine,
            include_senza_lotti=True,
        )

        return jsonify(
            {
                "ok": True,
                "gestioneLotto": True,
                "force_show_section": len(componenti_lotto) > 0,
                "haComponentiLotto": any(
                    isinstance(c.get("lotti"), list) and len(c["lotti"]) > 0
                    for c in componenti_lotto
                ),
                "componenti": componenti_lotto,
            }
        )

    ordine_gestione_lotto = _norm_text(ordine.GestioneLotto).lower() == "si"

    if not ordine_gestione_lotto:
        return jsonify(
            {
                "ok": True,
                "gestioneLotto": False,
                "haComponentiLotto": False,
                "componenti": [],
            }
        )

    componenti_lotto = _componenti_lotto_per_ordine(
        ordine,
        include_senza_lotti=True,
    )

    return jsonify(
        {
            "ok": True,
            "gestioneLotto": True,
            "force_show_section": len(componenti_lotto) > 0,
            "haComponentiLotto": any(
                isinstance(c.get("lotti"), list) and len(c["lotti"]) > 0
                for c in componenti_lotto
            ),
            "componenti": componenti_lotto,
        }
    )
