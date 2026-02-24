# region IMPORT
# LIBRERIE ESTERNE
from flask import render_template, Blueprint, request, url_for, abort
from flask_login import login_required, current_user

# LIBRERIE INTERNE
from app_odp.models import InputOdp, db, Roles
from app_odp.RBAC.decorator import require_perm
from app_odp.RBAC.policy import RbacPolicy

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
        {"reparto": "10", "perm": "controllo_qualita"},
    ),
}


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

    q = InputOdp.query
    q = policy.filter_input_odp(q)
    odp = list(q.all())
    return render_template(
        "home.j2", active_partial=template, active_tab=tab, policy=policy, odp=odp
    )
