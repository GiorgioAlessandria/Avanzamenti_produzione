from flask import request, url_for, g
from app_odp.routes_blueprint import main_bp
from app_odp.ordine_ref import format_ordine_ref_display_from_ordine
from app_odp.operator_session import (
    active_user,
    active_policy,
    active_token,
)
from app_odp.services.order_helpers import (
    _norm_text,
)
from app_odp.services.home_service import (
    _home_reparto_code,
    _home_reparto_label,
    _allowed_home_reparto_configs,
)


@main_bp.context_processor
def inject_policy_and_nav():
    user = active_user()

    if not getattr(user, "is_authenticated", False):
        return {}

    policy = active_policy()

    operator_token = _norm_text(request.args.get("tab_session")) or active_token()

    items = []

    for cfg in _allowed_home_reparto_configs(policy):
        url_kwargs = {"tab": cfg.tab_code}
        if operator_token:
            url_kwargs["tab_session"] = operator_token

        items.append(
            {
                "label": _home_reparto_label(cfg),
                "url": url_for(".home", **url_kwargs),
                "tab": cfg.tab_code,
                "reparto": _home_reparto_code(cfg),
            }
        )

    area_switch_items = []

    if policy.can("home_acquisti"):
        acq_kwargs = {}
        if operator_token:
            acq_kwargs["tab_session"] = operator_token

        area_switch_items.append(
            {
                "label": "Acquisti",
                "url": url_for(".home_acquisti", **acq_kwargs),
                "area": "acquisti",
            }
        )

    if policy.can("home"):
        first_production_tab = None

        for it in items:
            first_production_tab = it["tab"]
            break

        if first_production_tab:
            prod_kwargs = {"tab": first_production_tab}
            if operator_token:
                prod_kwargs["tab_session"] = operator_token

            area_switch_items.append(
                {
                    "label": "Produzione",
                    "url": url_for(".home", **prod_kwargs),
                    "area": "produzione",
                }
            )

    tarature_alerts = None
    if policy.can("tarature"):
        from app_odp.services.tarature_service import alerts_summary

        tarature_alerts = alerts_summary()

    return {
        "policy": policy,
        "operator_user": getattr(g, "operator_user", None),
        "operator_policy": getattr(g, "operator_policy", None),
        "tab_session": operator_token,
        "home_switch_items": items,
        "area_switch_items": area_switch_items,
        "tarature_alerts": tarature_alerts,
    }


@main_bp.context_processor
def inject_order_ref_formatters():
    return {
        "ordine_ref_display": format_ordine_ref_display_from_ordine,
    }


from app_odp.routes_modules import (
    acquisti,
    priorita,
    dashboard,
    etichette,
    impostazioni,
    logistica,
    ordini,
    erp,
    documenti,
    home,
    report_settimanale,
    storico_ordini,
    manutenzioni,
    rifiuti,
    tarature,
    vendite,
)
