# app_odp/routes_modules/home.py

from flask import abort, current_app, jsonify, render_template, request, url_for
from sqlalchemy import select

from app_odp.models import db, Causaliattivita
from app_odp.operator_session import (
    active_token,
    active_policy,
    active_user,
    operator_perm_required,
)
from app_odp.services.documenti_service import _build_metodo_lookup
from app_odp.services.order_helpers import _norm_text
from app_odp.services.common import _last_log_token
from app_odp.services.home_service import (
    _home_method_settings_for_user,
    _home_ui_texts_for_user,
    _first_allowed_home_reparto_config,
    _home_reparto_config_by_tab,
    _policy_can_access_home_config,
    _render_fragments_for_home_config,
    _home_rows_for_config,
    _packaging_note_signature,
)
from app_odp.services.manutenzioni_eventi_service import (
    build_scadenziario_manutenzioni,
    sync_all_active_plans,
    today_rome,
)
from app_odp.services.manutenzioni_service import (
    filter_eventi_per_operatore,
)
from app_odp.routes_blueprint import main_bp
from app_odp.vendite_models import (
    VenditeNotaImballoLettura,
    VenditeOrdineClienteRiga,
    _rome_iso_now,
)


@main_bp.get("/api/home/<tab>/bridge")
@operator_perm_required("home")
def api_home_bridge(tab):
    config = _home_reparto_config_by_tab(tab)
    if config is None:
        abort(404)

    policy = active_policy()

    if not _policy_can_access_home_config(policy, config):
        abort(403)

    client_last_event_id = _norm_text(request.args.get("last_event_id"))
    server_last_event_id = _last_log_token()

    if (
        client_last_event_id
        and client_last_event_id == server_last_event_id
        and not policy.can("utente_imballi")
    ):
        return jsonify(
            {
                "ok": True,
                "changed": False,
                "last_event_id": server_last_event_id,
                "fragments": {},
            }
        )

    odp = _home_rows_for_config(policy, config, apply_priorita=True, sort_priorita=True)

    fragments = _render_fragments_for_home_config(config, odp)

    return jsonify(
        {
            "ok": True,
            "changed": True,
            "active_tab": config.tab_code,
            "last_event_id": server_last_event_id,
            "fragments": fragments,
        }
    )


@main_bp.post("/api/home/note-imballo/<int:riga_id>/leggi")
@operator_perm_required("utente_imballi")
def api_home_nota_imballo_leggi(riga_id):
    user = active_user()
    if user is None:
        abort(403)

    customer_row = db.session.get(VenditeOrdineClienteRiga, riga_id)
    note = _norm_text(getattr(customer_row, "note_spedizione", ""))
    if customer_row is None or not note:
        return jsonify(
            {"ok": False, "error": "Le note per imballo non sono più disponibili."}
        ), 404

    signature = _packaging_note_signature(note)
    reading = db.session.get(VenditeNotaImballoLettura, (user.id, riga_id))
    if reading is None:
        reading = VenditeNotaImballoLettura(
            operatore_id=user.id,
            ordine_cliente_riga_id=riga_id,
            nota_firma=signature,
            letta_il=_rome_iso_now(),
        )
        db.session.add(reading)
    else:
        reading.nota_firma = signature
        reading.letta_il = _rome_iso_now()

    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        current_app.logger.exception(
            "Errore durante la conferma di lettura delle note per imballo."
        )
        return jsonify(
            {"ok": False, "error": "Impossibile registrare la lettura della nota."}
        ), 500

    return jsonify({"ok": True, "note": note, "letta_il": reading.letta_il})


@main_bp.get("/")
@operator_perm_required("home")
def home():
    policy = active_policy()
    user = active_user()

    tab_raw = request.args.get("tab")
    config = _home_reparto_config_by_tab(tab_raw) if tab_raw else None

    # Default: prima home reparto consentita dal DB.
    if config is None and not tab_raw:
        config = _first_allowed_home_reparto_config(policy)

    if config is None:
        abort(404)

    if not _policy_can_access_home_config(policy, config):
        abort(403)

    active_tab = config.tab_code
    template = config.template

    odp = _home_rows_for_config(policy, config, apply_priorita=True, sort_priorita=True)

    oggi_manutenzioni = today_rome()
    manutenzioni_da_eseguire = []

    if policy.can("manutenzioni_visualizza") or policy.can(
        "manutenzioni_amministrazione"
    ):
        try:
            sync_all_active_plans(data_dal=oggi_manutenzioni)
            scadenziario = build_scadenziario_manutenzioni(
                policy,
                data_fino=oggi_manutenzioni,
                stato="APERTI",
            )
            manutenzioni_da_eseguire = [
                row
                for row in scadenziario["rows"]
                if row["macchinario_attivo"]
            ]
            manutenzioni_da_eseguire = filter_eventi_per_operatore(
                manutenzioni_da_eseguire,
                user,
            )
        except Exception:
            db.session.rollback()
            current_app.logger.exception(
                "Errore durante il caricamento delle manutenzioni in scadenza."
            )

    causali = (
        db.session.execute(
            select(Causaliattivita.DesCausaleAttivita).order_by(
                Causaliattivita.DesCausaleAttivita
            )
        )
        .scalars()
        .all()
    )
    method_settings = _home_method_settings_for_user(config, policy, user)
    home_ui_texts = _home_ui_texts_for_user(config, policy, user)

    return render_template(
        "home.j2",
        active_partial=template,
        active_tab=active_tab,
        home_config=config,
        home_ui_texts=home_ui_texts,
        policy=policy,
        odp=odp,
        can_view_packaging_notes=policy.can("utente_imballi"),
        causali_attivita=causali,
        bridge_url=url_for(
            "main.api_home_bridge",
            tab=active_tab,
            tab_session=active_token(),
        ),
        bridge_last_event_id=_last_log_token(),
        method_settings=method_settings,
        metodo_lookup=_build_metodo_lookup(
            odp,
            path_key=method_settings["path_key"],
            prefisso=method_settings["prefisso"],
        ),
        metodo_documentale_prefisso=method_settings["prefisso"],
        metodo_documentale_tipo=method_settings["tipo"],
        operator_user=user,
        operator_policy=policy,
        tab_session=active_token(),
        manutenzioni_da_eseguire=manutenzioni_da_eseguire,
        oggi_manutenzioni=oggi_manutenzioni.isoformat(),
    )
