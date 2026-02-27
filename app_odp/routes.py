from flask import render_template, Blueprint, request, url_for, abort
from flask_login import login_required, current_user
from sqlalchemy import func, select

from app_odp.models import InputOdp, db, ChangeEvent, Causaliattivita
from app_odp.policy.decorator import require_perm
from app_odp.policy.policy import RbacPolicy

try:
    from icecream import ic
finally:
    pass
main_bp = Blueprint("main", __name__)

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
        "tbody_tbl_da_eseguire_sl": render_template(
            "partials/_home_montaggio_sl_rows_da_eseguire.html", odp=odp
        ),
        "tbody_ordini_in_corso_sl": render_template(
            "partials/_home_montaggio_sl_rows_in_corso.html", odp=odp
        ),
        "tbody_tbl_da_eseguire_m": render_template(
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
