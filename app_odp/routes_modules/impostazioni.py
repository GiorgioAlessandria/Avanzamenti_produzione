# app_odp/routes_modules/impostazioni.py
from flask import current_app, jsonify, request, render_template
from app_odp.models import (
    HomeRepartoConfig,
    HomeVisibilityRule,
    db,
    User,
    Roles,
    Reparti,
    InputOdpRuntime,
    user_roles,
    users_lavorazioni,
    users_risorse,
    users_famiglia,
    roles_permission,
    roles_reparti,
    roles_risorse,
    roles_lavorazioni,
    roles_magazzini,
    roles_famiglia,
    roles_macrofamiglia,
    roles_ineritance,
    roles_manageable_roles,
    ProductionCapacityCalendar,
)
from app_odp.manutenzioni_models import MacchinarioOperatore
from app_odp.operator_session import (
    active_policy,
    active_user,
    revoke_operator_sessions_for_user,
)
from app_odp.policy.decorator import require_active_perm
from app_odp.policy.policy import PROTECTED_ROLE_NAMES
from app_odp.routes_blueprint import main_bp
from app_odp.services.session_helpers import _current_username
from app_odp.services.capacity_service import (
    _capacity_float,
    _capacity_scope_code_is_valid,
    _capacity_settings_payload,
)
from sqlalchemy import func, select, delete
from sqlalchemy.exc import IntegrityError
import re
from app_odp.services.order_helpers import (
    _norm_text,
    _now_rome_dt,
    _parse_bool_flag,
)
from app_odp.services.home_service import _normalize_home_tab_code
from app_odp.services.impostazioni_service import (
    _login_code_error_response,
    ROLE_LINK_CONFIG,
    LOGIN_CODE_DUPLICATO_MSG,
    _build_public_id_from_full_name,
    _is_login_code_integrity_error,
    _normalize_id_list,
    _normalize_role_creation_links,
    _normalize_user_registry_payload,
    _prepare_login_code_or_response,
    _role_config_items_for_creation,
    _valid_role_creation_ids,
    HOME_CONFIG_METODO_OPTIONS,
    HOME_CONFIG_RENDERER_OPTIONS,
    HOME_CONFIG_TEMPLATE_OPTIONS,
    HOME_RULE_APPLY_TO_OPTIONS,
    HOME_RULE_PHASE_MODE_OPTIONS,
    _build_home_config_settings_payload,
    _home_config_audit,
    _home_config_bool,
    _home_config_int,
    _home_config_role_is_manageable,
    _home_config_text,
    _home_config_user_is_manageable,
    _home_reparto_config_to_dict,
    _home_visibility_rule_to_dict,
    _parse_home_rule_phase_values,
)


@main_bp.route("/impostazioni")
@require_active_perm("impostazioni_utente")
def impostazioni():
    user = active_user()
    policy = active_policy()
    show_home_config_section = policy.can_view_home_config_section
    home_config_payload = (
        _build_home_config_settings_payload(policy) if show_home_config_section else {}
    )

    show_role_assignment_section = policy.can_view_role_assignment_section
    show_user_abac_section = policy.can_view_user_abac_section
    show_capacity_config_section = policy.can("kpi_config")

    ruolo_options = []
    utenti_per_ruolo = {}
    ruolo_details = {}
    user_abac_details = {}

    manageable_users = []
    manageable_roles = []

    show_role_links_section = policy.can_view_role_links_section

    role_link_tables = []
    role_link_details = {}
    role_link_role_options = []

    ruoli_link_gestibili = policy.role_link_manageable_roles()

    show_role_creation_section = policy.can_view_role_creation_section
    role_creation_tables = []
    role_creation_options = {}

    show_role_delete_section = policy.can_view_role_delete_section
    deletable_role_options = []
    deletable_role_details = {}

    show_user_registry_section = policy.can_view_role_assignment_section
    can_edit_user_registry = bool(policy.has_direct_admin_role)

    registry_role_options = []
    registry_users = []

    registry_reparti_options = []

    if show_role_creation_section:
        role_creation_tables = [
            {"key": key, "label": cfg["label"]} for key, cfg in ROLE_LINK_CONFIG.items()
        ]

        for key, cfg in ROLE_LINK_CONFIG.items():
            role_creation_options[key] = {
                "label": cfg["label"],
                "items": _role_config_items_for_creation(policy, cfg),
            }

    if show_role_assignment_section:
        assignable_users = (
            policy.role_assignment_users_query().order_by(User.username.asc()).all()
        )

        assignable_roles = (
            policy.role_assignment_roles_query()
            .order_by(
                func.lower(func.coalesce(Roles.description, Roles.name)),
                func.lower(Roles.name),
            )
            .all()
        )

        manageable_users = []
        for utente in assignable_users:
            ruolo_corrente = utente.roles[0] if utente.roles else None

            manageable_users.append(
                {
                    "id": utente.id,
                    "username": utente.username or "",
                    "current_role_id": ruolo_corrente.id if ruolo_corrente else None,
                    "current_role_name": ruolo_corrente.name if ruolo_corrente else "",
                    "current_role_description": (
                        ruolo_corrente.description or ruolo_corrente.name
                    )
                    if ruolo_corrente
                    else "",
                }
            )

        manageable_roles = [
            {
                "id": ruolo.id,
                "name": ruolo.name or "",
                "description": ruolo.description or ruolo.name or "",
            }
            for ruolo in assignable_roles
        ]

    if show_role_links_section:
        role_link_role_options = ruoli_link_gestibili

        role_link_tables = [
            {"key": key, "label": cfg["label"]} for key, cfg in ROLE_LINK_CONFIG.items()
        ]

        for ruolo in ruoli_link_gestibili:
            role_link_details[str(ruolo.id)] = {
                "id": ruolo.id,
                "name": ruolo.name or "",
                "description": ruolo.description or "",
                "tables": {},
            }

            for key, cfg in ROLE_LINK_CONFIG.items():
                model = cfg["model"]
                code_attr = cfg["code_attr"]
                desc_attr = cfg["desc_attr"]

                if model is Roles:
                    all_items = [
                        item
                        for item in policy.role_link_manageable_roles()
                        if int(item.id) != int(ruolo.id)
                    ]
                else:
                    all_items = model.query.order_by(
                        func.lower(
                            func.coalesce(
                                getattr(model, desc_attr),
                                getattr(model, code_attr),
                            )
                        ),
                        func.lower(getattr(model, code_attr)),
                    ).all()

                selected_ids = set()
                assoc_table = cfg["assoc_table"]
                left_col = getattr(assoc_table.c, cfg["left_fk"])
                right_col = getattr(assoc_table.c, cfg["right_fk"])

                stmt = select(right_col).where(left_col == ruolo.id)
                selected_ids = set(db.session.execute(stmt).scalars().all())

                role_link_details[str(ruolo.id)]["tables"][key] = {
                    "label": cfg["label"],
                    "items": [
                        {
                            "id": getattr(item, cfg["model_id"]),
                            "codice": getattr(item, code_attr, "") or "",
                            "descrizione": getattr(item, desc_attr, "") or "",
                            "checked": getattr(item, cfg["model_id"]) in selected_ids,
                        }
                        for item in all_items
                    ],
                }

    if show_user_abac_section:
        ruoli_gestibili = policy.abac_manageable_roles()

        for ruolo in ruoli_gestibili:
            utenti_ruolo = (
                ruolo.users.filter(User.active.is_(True))
                .order_by(User.username.asc())
                .all()
            )

            if not utenti_ruolo:
                continue

            ruolo_options.append(ruolo)
            utenti_per_ruolo[ruolo.id] = utenti_ruolo

            lavorazioni = sorted(
                ruolo.effective_lavorazioni,
                key=lambda x: ((x.Codice or "").lower(), (x.Descrizione or "").lower()),
            )
            risorse = sorted(
                ruolo.effective_risorse,
                key=lambda x: ((x.Codice or "").lower(), (x.Descrizione or "").lower()),
            )

            famiglie = sorted(
                ruolo.effective_famiglia,
                key=lambda x: ((x.Codice or "").lower(), (x.Descrizione or "").lower()),
            )
            ruolo_lavorazioni_ids = {x.id for x in lavorazioni}
            ruolo_risorse_ids = {x.id for x in risorse}
            ruolo_famiglia_ids = {x.id for x in famiglie}

            ruolo_details[str(ruolo.id)] = {
                "id": ruolo.id,
                "name": ruolo.name or "",
                "description": ruolo.description or "",
                "lavorazioni": [
                    {
                        "id": x.id,
                        "codice": x.Codice or "",
                        "descrizione": x.Descrizione or "",
                    }
                    for x in lavorazioni
                ],
                "risorse": [
                    {
                        "id": x.id,
                        "codice": x.Codice or "",
                        "descrizione": x.Descrizione or "",
                    }
                    for x in risorse
                ],
                "famiglia": [
                    {
                        "id": x.id,
                        "codice": x.Codice or "",
                        "descrizione": x.Descrizione or "",
                    }
                    for x in famiglie
                ],
            }

            user_abac_details[str(ruolo.id)] = {}

            for utente in utenti_ruolo:
                user_abac_details[str(ruolo.id)][str(utente.id)] = {
                    "id": utente.id,
                    "username": utente.username or "",
                    "lavorazioni_ids": sorted(
                        x.id
                        for x in (utente.lavorazioni or [])
                        if x.id in ruolo_lavorazioni_ids
                    ),
                    "risorse_ids": sorted(
                        x.id
                        for x in (utente.risorse or [])
                        if x.id in ruolo_risorse_ids
                    ),
                    "famiglia_ids": sorted(
                        int(famiglia.id)
                        for famiglia in (getattr(utente, "famiglie", []) or [])
                        if famiglia.id in ruolo_famiglia_ids
                    ),
                    "has_filtro_macchine": bool(
                        utente.has_permission("filtro_macchine")
                    ),
                }
    if show_role_delete_section:
        ruoli_eliminabili = policy.role_delete_manageable_roles()
        deletable_role_options = ruoli_eliminabili

        for ruolo in ruoli_eliminabili:
            utenti_collegati_ids = (
                db.session.execute(
                    select(user_roles.c.user_id).where(user_roles.c.role_id == ruolo.id)
                )
                .scalars()
                .all()
            )

            deletable_role_details[str(ruolo.id)] = {
                "id": ruolo.id,
                "name": ruolo.name or "",
                "description": ruolo.description or "",
                "users_count": len(set(int(x) for x in utenti_collegati_ids)),
            }

    if show_user_registry_section:
        registry_role_options = manageable_roles

        manageable_role_ids = set(policy.role_assignment_manageable_role_ids)

        if manageable_role_ids:
            utenti_anagrafica = (
                User.query.join(user_roles, user_roles.c.user_id == User.id)
                .filter(
                    User.id != user.id,
                    user_roles.c.role_id.in_(sorted(manageable_role_ids)),
                )
                .distinct()
                .order_by(User.username.asc())
                .all()
            )
            registry_reparti_options = [
                {
                    "codice": rep.Codice or "",
                    "descrizione": rep.Descrizione or "",
                }
                for rep in Reparti.query.order_by(
                    func.lower(func.coalesce(Reparti.Descrizione, Reparti.Codice)),
                    func.lower(Reparti.Codice),
                ).all()
            ]

            registry_users = [
                {
                    "id": utente.id,
                    "username": utente.username or "",
                    "public_id": utente.public_id or "",
                    "active": bool(utente.active),
                    "genere": utente.genere or "",
                    "reparto_princ": utente.RepartoPrinc or "",
                    "current_role_id": utente.roles[0].id if utente.roles else None,
                    "current_role_name": utente.roles[0].name if utente.roles else "",
                    "current_role_description": (
                        utente.roles[0].description or utente.roles[0].name
                    )
                    if utente.roles
                    else "",
                }
                for utente in utenti_anagrafica
            ]
    return render_template(
        "impostazioni.j2",
        ruolo_options=ruolo_options,
        utenti_per_ruolo=utenti_per_ruolo,
        ruolo_details=ruolo_details,
        user_abac_details=user_abac_details,
        manageable_users=manageable_users,
        manageable_roles=manageable_roles,
        show_role_assignment_section=show_role_assignment_section,
        show_user_abac_section=show_user_abac_section,
        show_role_creation_section=show_role_creation_section,
        role_creation_tables=role_creation_tables,
        role_creation_options=role_creation_options,
        role_link_tables=role_link_tables,
        role_link_details=role_link_details,
        show_role_links_section=show_role_links_section,
        role_link_role_options=role_link_role_options,
        show_role_delete_section=show_role_delete_section,
        deletable_role_options=deletable_role_options,
        deletable_role_details=deletable_role_details,
        show_user_registry_section=show_user_registry_section,
        registry_role_options=registry_role_options,
        registry_users=registry_users,
        registry_reparti_options=registry_reparti_options,
        can_edit_user_registry=can_edit_user_registry,
        show_home_config_section=show_home_config_section,
        home_config_payload=home_config_payload,
        show_capacity_config_section=show_capacity_config_section,
    )


@main_bp.post("/api/impostazioni/crea-utente")
@require_active_perm("impostazioni_utente")
def api_crea_utente():
    policy = active_policy()

    if not policy.can_view_role_assignment_section:
        return jsonify({"ok": False, "error": "Permesso insufficiente."}), 403

    data = request.get_json(silent=True) or {}

    username = _norm_text(data.get("username"))
    genere = _norm_text(data.get("genere"))
    reparto_princ = _norm_text(data.get("reparto_princ"))
    active = _parse_bool_flag(data.get("active", True))
    role_id_raw = data.get("role_id")
    public_id_source = _norm_text(data.get("public_id"))
    public_id = _build_public_id_from_full_name(public_id_source)
    login_code_raw = data.get("login_code")

    try:
        role_id = int(role_id_raw)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Ruolo non valido."}), 400

    if not username:
        return jsonify({"ok": False, "error": "Username obbligatorio."}), 400

    if len(username) < 3:
        return jsonify({"ok": False, "error": "Username troppo corto."}), 400

    ruolo = Roles.query.get(role_id)
    if ruolo is None:
        return jsonify({"ok": False, "error": "Ruolo non trovato."}), 404

    if not policy.can_assign_target_role(ruolo):
        return jsonify({"ok": False, "error": "Ruolo non assegnabile."}), 403

    existing = User.query.filter(func.lower(User.username) == username.lower()).first()
    if existing is not None:
        return jsonify(
            {"ok": False, "error": "Esiste già un utente con questo username."}
        ), 409

    if not public_id:
        return jsonify(
            {"ok": False, "error": "Il public_id non può essere vuoto."}
        ), 400

    existing_public_id = User.query.filter(User.public_id == public_id).first()
    if existing_public_id is not None:
        return jsonify(
            {"ok": False, "error": "Esiste già un utente con questo public_id."}
        ), 409

    login_code, error_response = _prepare_login_code_or_response(login_code_raw)
    if error_response:
        return error_response

    if reparto_princ:
        reparto_exists = Reparti.query.filter(Reparti.Codice == reparto_princ).first()
        if reparto_exists is None:
            return jsonify(
                {"ok": False, "error": "Reparto principale non valido."}
            ), 400

    try:
        utente = User(
            username=username,
            public_id=public_id or None,
            active=active,
            genere=genere or None,
            RepartoPrinc=reparto_princ or None,
        )
        utente.set_login_code(login_code)

        db.session.add(utente)
        db.session.flush()

        db.session.execute(
            user_roles.insert().values(
                user_id=utente.id,
                role_id=ruolo.id,
            )
        )

        db.session.commit()

    except IntegrityError as exc:
        db.session.rollback()

        if _is_login_code_integrity_error(exc):
            return _login_code_error_response(LOGIN_CODE_DUPLICATO_MSG, 409)

        current_app.logger.exception("Errore integrità durante la creazione utente")
        return jsonify(
            {
                "ok": False,
                "error": "Errore creazione utente.",
            }
        ), 500

    except Exception as exc:
        db.session.rollback()
        current_app.logger.exception("Errore creazione utente")
        return jsonify(
            {
                "ok": False,
                "error": f"Errore creazione utente: {exc}",
            }
        ), 500

    return jsonify(
        {
            "ok": True,
            "message": "Utente creato correttamente.",
            "user": {
                "id": utente.id,
                "public_id": utente.public_id or "",
                "username": utente.username or "",
                "active": bool(utente.active),
                "genere": utente.genere or "",
                "reparto_princ": utente.RepartoPrinc or "",
                "current_role_id": ruolo.id,
                "current_role_name": ruolo.name or "",
                "current_role_description": ruolo.description or ruolo.name or "",
            },
        }
    ), 201


@main_bp.get("/api/impostazioni/production-capacity")
@require_active_perm("kpi_config")
def api_production_capacity_data():
    return jsonify(
        {
            "ok": True,
            "data": _capacity_settings_payload(),
        }
    ), 200


@main_bp.post("/api/impostazioni/production-capacity")
@require_active_perm("kpi_config")
def api_save_production_capacity():
    data = request.get_json(silent=True) or {}

    scope_type = _norm_text(data.get("scope_type")).lower() or "operatore"

    if scope_type != "operatore":
        return jsonify(
            {
                "ok": False,
                "error": "La capacità produttiva può essere configurata solo per operatore.",
            }
        ), 400

    scope_code = _norm_text(data.get("scope_code"))

    if not scope_code:
        return jsonify({"ok": False, "error": "Codice scope obbligatorio."}), 400

    if not _capacity_scope_code_is_valid(scope_type, scope_code):
        return jsonify({"ok": False, "error": "Scope non valido o non esistente."}), 400

    rows = data.get("rows") or []

    if not isinstance(rows, list):
        return jsonify({"ok": False, "error": "Formato righe non valido."}), 400

    if not rows:
        return jsonify({"ok": False, "error": "Nessuna riga capacità ricevuta."}), 400

    now = _now_rome_dt().isoformat(timespec="seconds")
    username = _current_username()

    try:
        for item in rows:
            weekday = _home_config_int(item.get("weekday"), -1)

            if weekday < 0 or weekday > 6:
                return jsonify(
                    {"ok": False, "error": "Giorno settimana non valido."}
                ), 400

            hours = _capacity_float(item.get("hours_capacity"), 0.0)
            active = _parse_bool_flag(item.get("active", True))

            row = ProductionCapacityCalendar.query.filter_by(
                scope_type=scope_type,
                scope_code=scope_code,
                weekday=weekday,
            ).first()

            if row is None:
                row = ProductionCapacityCalendar(
                    scope_type=scope_type,
                    scope_code=scope_code,
                    weekday=weekday,
                )
                db.session.add(row)

            row.hours_capacity = hours
            row.active = active
            row.updated_at = now
            row.updated_by = username

        db.session.commit()

    except Exception as exc:
        db.session.rollback()
        current_app.logger.exception("Errore salvataggio production_capacity_calendar")
        return jsonify({"ok": False, "error": f"Errore salvataggio: {exc}"}), 500

    return jsonify(
        {
            "ok": True,
            "message": "Capacità produttiva salvata.",
            "data": _capacity_settings_payload(),
        }
    ), 200


@main_bp.get("/api/impostazioni/home-config")
@require_active_perm("configurazione_home")
def api_home_config_data():
    policy = active_policy()

    if not policy.can_view_home_config_section:
        return jsonify({"ok": False, "error": "Permesso insufficiente."}), 403

    return jsonify(
        {
            "ok": True,
            "data": _build_home_config_settings_payload(policy),
        }
    ), 200


@main_bp.post("/api/impostazioni/home-reparto-config")
@require_active_perm("configurazione_home")
def api_save_home_reparto_config():
    policy = active_policy()

    if not policy.can_view_home_config_section:
        return jsonify({"ok": False, "error": "Permesso insufficiente."}), 403

    data = request.get_json(silent=True) or {}

    config_id = _home_config_int(data.get("id"), 0)
    reparto_id = _home_config_int(data.get("reparto_id"), 0)

    reparto = Reparti.query.get(reparto_id)
    if reparto is None:
        return jsonify({"ok": False, "error": "Reparto non valido."}), 400

    tab_code = _normalize_home_tab_code(data.get("tab_code"))
    if not tab_code:
        return jsonify({"ok": False, "error": "Tab code obbligatorio."}), 400

    label = _home_config_text(data.get("label"))
    if not label:
        return jsonify({"ok": False, "error": "Label menu obbligatoria."}), 400

    template = _home_config_text(data.get("template"))
    renderer = _home_config_text(data.get("renderer"))
    metodo_tipo = _home_config_text(data.get("metodo_documentale_tipo")) or "nessuno"

    if template not in HOME_CONFIG_TEMPLATE_OPTIONS:
        return jsonify({"ok": False, "error": "Template non valido."}), 400

    if renderer not in HOME_CONFIG_RENDERER_OPTIONS:
        return jsonify({"ok": False, "error": "Renderer non valido."}), 400

    if metodo_tipo not in HOME_CONFIG_METODO_OPTIONS:
        return jsonify(
            {"ok": False, "error": "Tipo metodo documentale non valido."}
        ), 400

    duplicate = HomeRepartoConfig.query.filter(
        func.lower(HomeRepartoConfig.tab_code) == tab_code
    )

    if config_id:
        duplicate = duplicate.filter(HomeRepartoConfig.id != config_id)

    if duplicate.first() is not None:
        return jsonify({"ok": False, "error": "Tab code già utilizzato."}), 409

    if config_id:
        row = HomeRepartoConfig.query.get(config_id)
        if row is None:
            return jsonify({"ok": False, "error": "Configurazione non trovata."}), 404
        action = "update"
        old_payload = _home_reparto_config_to_dict(row)
    else:
        row = HomeRepartoConfig()
        db.session.add(row)
        action = "create"
        old_payload = None

    row.reparto_id = reparto.id
    row.tab_code = tab_code
    row.label = label
    row.template = template
    row.renderer = renderer
    row.permesso = _home_config_text(data.get("permesso")) or "home"
    row.ordine_menu = _home_config_int(data.get("ordine_menu"), 100)
    row.attivo = _home_config_bool(data.get("attivo"))

    row.titolo_macchine_da_eseguire = (
        _home_config_text(data.get("titolo_macchine_da_eseguire")) or None
    )
    row.titolo_macchine_attive = (
        _home_config_text(data.get("titolo_macchine_attive")) or None
    )
    row.titolo_semilavorati_da_eseguire = (
        _home_config_text(data.get("titolo_semilavorati_da_eseguire")) or None
    )
    row.titolo_semilavorati_attivi = (
        _home_config_text(data.get("titolo_semilavorati_attivi")) or None
    )

    row.testo_presa_macchina = (
        _home_config_text(data.get("testo_presa_macchina")) or None
    )
    row.testo_sospendi_macchina = (
        _home_config_text(data.get("testo_sospendi_macchina")) or None
    )
    row.testo_riattiva_macchina = (
        _home_config_text(data.get("testo_riattiva_macchina")) or None
    )
    row.testo_chiudi_macchina = (
        _home_config_text(data.get("testo_chiudi_macchina")) or None
    )

    row.metodo_documentale_tipo = metodo_tipo
    row.metodo_documentale_prefisso = (
        _home_config_text(data.get("metodo_documentale_prefisso")) or None
    )
    row.metodo_documentale_path_key = (
        _home_config_text(data.get("metodo_documentale_path_key")) or None
    )

    row.updated_at = _now_rome_dt().isoformat(timespec="seconds")
    row.updated_by = _current_username()

    try:
        db.session.flush()
        new_payload = _home_reparto_config_to_dict(row)

        _home_config_audit(
            entity_type="home_reparto_config",
            entity_id=row.id,
            action=action,
            old_payload=old_payload,
            new_payload=new_payload,
            note="Salvataggio configurazione home reparto da impostazioni.",
        )

        db.session.commit()

    except Exception as exc:
        db.session.rollback()
        current_app.logger.exception("Errore salvataggio home_reparto_config")
        return jsonify({"ok": False, "error": f"Errore salvataggio: {exc}"}), 500

    return jsonify(
        {
            "ok": True,
            "message": "Configurazione home reparto salvata.",
            "row": _home_reparto_config_to_dict(row),
        }
    ), 200


@main_bp.post("/api/impostazioni/home-visibility-rule")
@require_active_perm("configurazione_home")
def api_save_home_visibility_rule():
    policy = active_policy()

    if not policy.can_view_home_config_section:
        return jsonify({"ok": False, "error": "Permesso insufficiente."}), 403

    data = request.get_json(silent=True) or {}

    rule_id = _home_config_int(data.get("id"), 0)
    reparto_id = _home_config_int(data.get("reparto_id"), 0)

    reparto = Reparti.query.get(reparto_id)
    if reparto is None:
        return jsonify({"ok": False, "error": "Reparto non valido."}), 400

    scope_type = _home_config_text(data.get("scope_type")).lower()
    role_id = _home_config_int(data.get("role_id"), 0)
    user_id = _home_config_int(data.get("user_id"), 0)

    role = None
    utente = None

    if scope_type == "role":
        if not role_id:
            return jsonify({"ok": False, "error": "Ruolo obbligatorio."}), 400

        role = Roles.query.get(role_id)
        if not _home_config_role_is_manageable(policy, role):
            return jsonify({"ok": False, "error": "Ruolo non gestibile."}), 403

        user_id = None

    elif scope_type == "user":
        if not user_id:
            return jsonify({"ok": False, "error": "Utente obbligatorio."}), 400

        utente = User.query.get(user_id)
        if not _home_config_user_is_manageable(policy, utente):
            return jsonify({"ok": False, "error": "Utente non gestibile."}), 403

        role_id = None

    else:
        return jsonify({"ok": False, "error": "Tipo regola non valido."}), 400

    apply_to = _home_config_text(data.get("apply_to")).lower() or "macchine"
    phase_mode = _home_config_text(data.get("phase_mode")).lower() or "all"

    if apply_to not in HOME_RULE_APPLY_TO_OPTIONS:
        return jsonify({"ok": False, "error": "Campo 'applica a' non valido."}), 400

    if phase_mode not in HOME_RULE_PHASE_MODE_OPTIONS:
        return jsonify({"ok": False, "error": "Modalità fase non valida."}), 400

    try:
        phase_values = _parse_home_rule_phase_values(
            data.get("phase_values"),
            phase_mode,
        )
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400

    if rule_id:
        row = HomeVisibilityRule.query.get(rule_id)
        if row is None:
            return jsonify({"ok": False, "error": "Regola non trovata."}), 404
        action = "update"
        old_payload = _home_visibility_rule_to_dict(row)
    else:
        row = HomeVisibilityRule()
        db.session.add(row)
        action = "create"
        old_payload = None

    row.reparto_id = reparto.id
    row.role_id = role_id or None
    row.user_id = user_id or None
    row.apply_to = apply_to
    row.phase_mode = phase_mode
    row.phase_values = phase_values
    row.attivo = _home_config_bool(data.get("attivo"))

    row.titolo_macchine_da_eseguire = (
        _home_config_text(data.get("titolo_macchine_da_eseguire")) or None
    )
    row.titolo_macchine_attive = (
        _home_config_text(data.get("titolo_macchine_attive")) or None
    )

    row.testo_presa_macchina = (
        _home_config_text(data.get("testo_presa_macchina")) or None
    )
    row.testo_sospendi_macchina = (
        _home_config_text(data.get("testo_sospendi_macchina")) or None
    )
    row.testo_riattiva_macchina = (
        _home_config_text(data.get("testo_riattiva_macchina")) or None
    )
    row.testo_chiudi_macchina = (
        _home_config_text(data.get("testo_chiudi_macchina")) or None
    )

    metodo_tipo = _home_config_text(data.get("metodo_documentale_tipo"))
    if metodo_tipo and metodo_tipo not in HOME_CONFIG_METODO_OPTIONS:
        return jsonify(
            {"ok": False, "error": "Tipo metodo documentale non valido."}
        ), 400

    row.metodo_documentale_tipo = metodo_tipo or None
    row.metodo_documentale_prefisso = (
        _home_config_text(data.get("metodo_documentale_prefisso")) or None
    )
    row.metodo_documentale_path_key = (
        _home_config_text(data.get("metodo_documentale_path_key")) or None
    )

    row.updated_at = _now_rome_dt().isoformat(timespec="seconds")
    row.updated_by = _current_username()

    try:
        db.session.flush()
        new_payload = _home_visibility_rule_to_dict(row)

        _home_config_audit(
            entity_type="home_visibility_rules",
            entity_id=row.id,
            action=action,
            old_payload=old_payload,
            new_payload=new_payload,
            note="Salvataggio regola visibilità home da impostazioni.",
        )

        db.session.commit()

    except Exception as exc:
        db.session.rollback()
        current_app.logger.exception("Errore salvataggio home_visibility_rules")
        return jsonify({"ok": False, "error": f"Errore salvataggio: {exc}"}), 500

    return jsonify(
        {
            "ok": True,
            "message": "Regola visibilità home salvata.",
            "row": _home_visibility_rule_to_dict(row),
        }
    ), 200


@main_bp.post("/api/impostazioni/home-visibility-rule/<int:rule_id>/toggle")
@require_active_perm("configurazione_home")
def api_toggle_home_visibility_rule(rule_id: int):
    policy = active_policy()

    if not policy.can_view_home_config_section:
        return jsonify({"ok": False, "error": "Permesso insufficiente."}), 403

    row = HomeVisibilityRule.query.get(rule_id)
    if row is None:
        return jsonify({"ok": False, "error": "Regola non trovata."}), 404

    old_payload = _home_visibility_rule_to_dict(row)

    row.attivo = not bool(row.attivo)
    row.updated_at = _now_rome_dt().isoformat(timespec="seconds")
    row.updated_by = _current_username()

    action = "enable" if row.attivo else "disable"

    try:
        db.session.flush()

        _home_config_audit(
            entity_type="home_visibility_rules",
            entity_id=row.id,
            action=action,
            old_payload=old_payload,
            new_payload=_home_visibility_rule_to_dict(row),
            note="Cambio stato regola visibilità home.",
        )

        db.session.commit()

    except Exception as exc:
        db.session.rollback()
        return jsonify({"ok": False, "error": f"Errore aggiornamento: {exc}"}), 500

    return jsonify(
        {
            "ok": True,
            "message": "Stato regola aggiornato.",
            "row": _home_visibility_rule_to_dict(row),
        }
    ), 200


@main_bp.post("/api/impostazioni/reset-login-code")
@require_active_perm("impostazioni_utente")
def api_reset_login_code():
    user = active_user()
    policy = active_policy()

    if not policy.can_view_role_assignment_section:
        return jsonify({"ok": False, "error": "Permesso insufficiente."}), 403

    data = request.get_json(silent=True) or {}

    user_id_raw = data.get("user_id")
    login_code_raw = data.get("login_code")

    try:
        user_id = int(user_id_raw)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Utente non valido."}), 400

    utente = User.query.get(user_id)
    if utente is None:
        return jsonify({"ok": False, "error": "Utente non trovato."}), 404

    if int(utente.id) == int(user.id):
        return jsonify(
            {
                "ok": False,
                "error": "Non puoi resettare il tuo login_code da questa funzione.",
            }
        ), 403

    if not policy.can_manage_target_user(utente):
        return jsonify({"ok": False, "error": "Utente non gestibile."}), 403

    login_code, error_response = _prepare_login_code_or_response(
        login_code_raw,
        exclude_user_id=utente.id,
    )
    if error_response:
        return error_response

    try:
        utente.set_login_code(login_code)
        db.session.flush()
        revoke_operator_sessions_for_user(utente.id, commit=False)
        db.session.commit()

    except IntegrityError as exc:
        db.session.rollback()

        if _is_login_code_integrity_error(exc):
            return _login_code_error_response(LOGIN_CODE_DUPLICATO_MSG, 409)

        current_app.logger.exception("Errore integrità durante il reset login code")
        return jsonify(
            {
                "ok": False,
                "error": "Errore reset login_code.",
            }
        ), 500

    except Exception as exc:
        db.session.rollback()
        current_app.logger.exception("Errore reset login_code")
        return jsonify(
            {
                "ok": False,
                "error": f"Errore reset login_code: {exc}",
            }
        ), 500

    return jsonify(
        {
            "ok": True,
            "message": "Login code aggiornato correttamente.",
            "user_id": utente.id,
        }
    ), 200


@main_bp.post("/api/impostazioni/modifica-utente")
@require_active_perm("impostazioni_utente")
def api_modifica_utente():
    user = active_user()
    policy = active_policy()

    if not policy.has_direct_admin_role:
        return jsonify({"ok": False, "error": "Permesso insufficiente."}), 403

    data = request.get_json(silent=True) or {}

    try:
        user_id = int(data.get("user_id"))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Utente non valido."}), 400

    utente = User.query.get(user_id)
    if utente is None:
        return jsonify({"ok": False, "error": "Utente non trovato."}), 404

    if int(utente.id) == int(user.id):
        return jsonify(
            {"ok": False, "error": "Non puoi modificare la tua anagrafica."}
        ), 403

    if not policy.can_manage_target_user(utente):
        return jsonify({"ok": False, "error": "Utente non gestibile."}), 403

    try:
        values = _normalize_user_registry_payload(data)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400

    username_in_uso = User.query.filter(
        func.lower(User.username) == values["username"].lower(),
        User.id != utente.id,
    ).first()
    if username_in_uso is not None:
        return jsonify(
            {"ok": False, "error": "Esiste già un utente con questo username."}
        ), 409

    public_id_in_uso = User.query.filter(
        func.lower(User.public_id) == values["public_id"].lower(),
        User.id != utente.id,
    ).first()
    if public_id_in_uso is not None:
        return jsonify(
            {"ok": False, "error": "Esiste già un utente con questo Public ID."}
        ), 409

    if values["reparto_princ"]:
        reparto_exists = Reparti.query.filter(
            Reparti.Codice == values["reparto_princ"]
        ).first()
        if reparto_exists is None:
            return jsonify(
                {"ok": False, "error": "Reparto principale non valido."}
            ), 400

    old_username = utente.username
    old_public_id = utente.public_id

    try:
        utente.username = values["username"]
        utente.public_id = values["public_id"]
        utente.genere = values["genere"] or None
        utente.RepartoPrinc = values["reparto_princ"] or None

        if old_username != values["username"]:
            InputOdpRuntime.query.filter(
                InputOdpRuntime.Utente_operazione == old_username,
                InputOdpRuntime.Stato_odp.in_(["Attivo", "In Sospeso"]),
            ).update(
                {InputOdpRuntime.Utente_operazione: values["username"]},
                synchronize_session=False,
            )
            revoke_operator_sessions_for_user(utente.id, commit=False)

        if (
            old_username != values["username"]
            or old_public_id != values["public_id"]
        ):
            MacchinarioOperatore.query.filter(
                MacchinarioOperatore.operatore_public_id == old_public_id
            ).update(
                {
                    MacchinarioOperatore.operatore_public_id: values["public_id"],
                    MacchinarioOperatore.operatore_username: values["username"],
                },
                synchronize_session=False,
            )

        db.session.commit()
    except IntegrityError:
        db.session.rollback()
        return jsonify(
            {
                "ok": False,
                "error": "Username o Public ID già utilizzato da un altro utente.",
            }
        ), 409
    except Exception as exc:
        db.session.rollback()
        current_app.logger.exception("Errore modifica anagrafica utente")
        return jsonify(
            {
                "ok": False,
                "error": f"Errore modifica anagrafica utente: {exc}",
            }
        ), 500

    ruolo = utente.roles[0] if utente.roles else None
    return jsonify(
        {
            "ok": True,
            "message": "Anagrafica utente aggiornata correttamente.",
            "user": {
                "id": utente.id,
                "username": utente.username or "",
                "public_id": utente.public_id or "",
                "active": bool(utente.active),
                "genere": utente.genere or "",
                "reparto_princ": utente.RepartoPrinc or "",
                "current_role_id": ruolo.id if ruolo else None,
                "current_role_name": ruolo.name if ruolo else "",
                "current_role_description": (
                    ruolo.description or ruolo.name
                )
                if ruolo
                else "",
            },
        }
    ), 200


@main_bp.post("/api/impostazioni/utente-attivo")
@require_active_perm("impostazioni_utente")
def api_set_utente_attivo():
    policy = active_policy()

    if not policy.can_view_role_assignment_section:
        return jsonify({"ok": False, "error": "Permesso insufficiente."}), 403

    data = request.get_json(silent=True) or {}

    user_id_raw = data.get("user_id")
    active_raw = data.get("active")

    try:
        user_id = int(user_id_raw)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Utente non valido."}), 400

    utente = User.query.get(user_id)
    if utente is None:
        return jsonify({"ok": False, "error": "Utente non trovato."}), 404

    user = active_user()

    if int(utente.id) == int(user.id):
        return jsonify(
            {"ok": False, "error": "Non puoi modificare il tuo stato attivo."}
        ), 403

    if not policy.can_manage_target_user(utente):
        return jsonify({"ok": False, "error": "Utente non gestibile."}), 403

    nuovo_stato = _parse_bool_flag(active_raw)

    if not nuovo_stato:
        ordini_aperti = InputOdpRuntime.query.filter(
            InputOdpRuntime.Utente_operazione == utente.username,
            InputOdpRuntime.Stato_odp.in_(["Attivo", "In Sospeso"]),
        ).count()
        if ordini_aperti > 0:
            return jsonify(
                {
                    "ok": False,
                    "error": "Impossibile mettere inattivo l'utente: ha ordini ancora attivi o sospesi.",
                }
            ), 409

    try:
        utente.active = nuovo_stato
        db.session.commit()
    except Exception as exc:
        db.session.rollback()
        return jsonify(
            {
                "ok": False,
                "error": f"Errore aggiornamento stato utente: {exc}",
            }
        ), 500

    return jsonify(
        {
            "ok": True,
            "message": "Stato utente aggiornato correttamente.",
            "user_id": utente.id,
            "active": bool(utente.active),
        }
    ), 200


@main_bp.post("/api/impostazioni/elimina-ruolo")
@require_active_perm("impostazioni_utente")
def api_elimina_ruolo():
    policy = active_policy()

    if not policy.can_view_role_delete_section:
        return jsonify({"ok": False, "error": "Permesso insufficiente."}), 403

    data = request.get_json(silent=True) or {}
    role_id_raw = data.get("role_id")

    try:
        role_id = int(role_id_raw)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Parametro role_id non valido."}), 400

    ruolo = Roles.query.get(role_id)
    if ruolo is None:
        return jsonify({"ok": False, "error": "Ruolo non trovato."}), 404

    if not policy.can_manage_target_role(ruolo):
        return jsonify({"ok": False, "error": "Ruolo non eliminabile."}), 403

    if (ruolo.name or "").strip().lower() in PROTECTED_ROLE_NAMES:
        return jsonify(
            {
                "ok": False,
                "error": "Questo ruolo è protetto e non può essere eliminato.",
            }
        ), 403

    try:
        impacted_user_ids = set(
            int(x)
            for x in db.session.execute(
                select(user_roles.c.user_id).where(user_roles.c.role_id == ruolo.id)
            )
            .scalars()
            .all()
        )

        # rimuove assegnazioni utente -> ruolo
        db.session.execute(delete(user_roles).where(user_roles.c.role_id == ruolo.id))

        # pulizia override ABAC utenti che avevano quel ruolo
        if impacted_user_ids:
            db.session.execute(
                delete(users_lavorazioni).where(
                    users_lavorazioni.c.user_id.in_(sorted(impacted_user_ids))
                )
            )
            db.session.execute(
                delete(users_risorse).where(
                    users_risorse.c.user_id.in_(sorted(impacted_user_ids))
                )
            )
            db.session.execute(
                delete(users_famiglia).where(
                    users_famiglia.c.user_id.in_(sorted(impacted_user_ids))
                )
            )

        # pulizia link role -> entità
        db.session.execute(
            delete(roles_permission).where(roles_permission.c.role_id == ruolo.id)
        )
        db.session.execute(
            delete(roles_reparti).where(roles_reparti.c.roles_id == ruolo.id)
        )
        db.session.execute(
            delete(roles_risorse).where(roles_risorse.c.roles_id == ruolo.id)
        )
        db.session.execute(
            delete(roles_lavorazioni).where(roles_lavorazioni.c.roles_id == ruolo.id)
        )
        db.session.execute(
            delete(roles_magazzini).where(roles_magazzini.c.roles_id == ruolo.id)
        )
        db.session.execute(
            delete(roles_famiglia).where(roles_famiglia.c.roles_id == ruolo.id)
        )
        db.session.execute(
            delete(roles_macrofamiglia).where(
                roles_macrofamiglia.c.roles_id == ruolo.id
            )
        )

        # pulizia relazioni tra ruoli
        db.session.execute(
            delete(roles_ineritance).where(
                (roles_ineritance.c.role_id == ruolo.id)
                | (roles_ineritance.c.included_role == ruolo.id)
            )
        )

        db.session.execute(
            delete(roles_manageable_roles).where(
                (roles_manageable_roles.c.manager_role_id == ruolo.id)
                | (roles_manageable_roles.c.managed_role_id == ruolo.id)
            )
        )

        db.session.execute(delete(Roles).where(Roles.id == ruolo.id))

        db.session.commit()

    except Exception as exc:
        db.session.rollback()
        return jsonify(
            {
                "ok": False,
                "error": f"Errore eliminazione ruolo: {exc}",
            }
        ), 500

    return jsonify(
        {
            "ok": True,
            "message": "Ruolo eliminato correttamente.",
            "role_id": role_id,
            "impacted_users": sorted(impacted_user_ids),
        }
    ), 200


@main_bp.post("/api/impostazioni/assegna-ruolo")
@require_active_perm("impostazioni_utente")
def api_assegna_ruolo():
    policy = active_policy()

    if not policy.can_view_role_assignment_section:
        return jsonify({"ok": False, "error": "Permesso insufficiente."}), 403

    data = request.get_json(silent=True) or {}

    user_id_raw = data.get("user_id")
    role_id_raw = data.get("role_id")

    try:
        user_id = int(user_id_raw)
        role_id = int(role_id_raw)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Parametri non validi."}), 400

    utente = User.query.get(user_id)
    if utente is None:
        return jsonify({"ok": False, "error": "Utente non trovato."}), 404

    ruolo = Roles.query.get(role_id)
    if ruolo is None:
        return jsonify({"ok": False, "error": "Ruolo non trovato."}), 404

    if not policy.can_manage_target_user(utente):
        return jsonify({"ok": False, "error": "Utente non gestibile."}), 403

    if not policy.can_assign_target_role(ruolo):
        return jsonify({"ok": False, "error": "Ruolo non assegnabile."}), 403

    try:
        db.session.execute(delete(user_roles).where(user_roles.c.user_id == utente.id))

        db.session.execute(
            user_roles.insert().values(
                user_id=utente.id,
                role_id=ruolo.id,
            )
        )
        db.session.execute(
            delete(users_lavorazioni).where(users_lavorazioni.c.user_id == utente.id)
        )

        db.session.execute(
            delete(users_risorse).where(users_risorse.c.user_id == utente.id)
        )
        db.session.execute(
            delete(users_famiglia).where(users_famiglia.c.user_id == utente.id)
        )

        db.session.commit()

    except Exception as exc:
        db.session.rollback()
        return jsonify(
            {
                "ok": False,
                "error": f"Errore assegnazione ruolo: {exc}",
            }
        ), 500

    return jsonify(
        {
            "ok": True,
            "message": "Ruolo assegnato correttamente.",
            "user_id": utente.id,
            "role_id": ruolo.id,
            "role_name": ruolo.name or "",
            "role_description": ruolo.description or ruolo.name or "",
        }
    ), 200


@main_bp.post("/api/impostazioni/crea-ruolo")
@require_active_perm("impostazioni_utente")
def api_crea_ruolo():
    policy = active_policy()

    if not policy.can_view_role_creation_section:
        return jsonify({"ok": False, "error": "Permesso insufficiente."}), 403

    data = request.get_json(silent=True) or {}

    name = _norm_text(data.get("name"))
    description = _norm_text(data.get("description"))

    try:
        links = _normalize_role_creation_links(data.get("links") or {})
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400

    if not name:
        return jsonify({"ok": False, "error": "Il nome ruolo è obbligatorio."}), 400

    if not description:
        return jsonify(
            {"ok": False, "error": "La descrizione ruolo è obbligatoria."}
        ), 400

    if not re.fullmatch(r"[A-Za-z0-9_]+", name):
        return jsonify(
            {
                "ok": False,
                "error": "Il nome ruolo può contenere solo lettere, numeri e underscore.",
            }
        ), 400

    normalized_name = name.strip()
    normalized_name_lower = normalized_name.lower()

    if normalized_name_lower in PROTECTED_ROLE_NAMES:
        return jsonify(
            {
                "ok": False,
                "error": "Non è consentito creare questo ruolo.",
            }
        ), 403

    existing_role = Roles.query.filter(
        func.lower(Roles.name) == normalized_name_lower
    ).first()
    if existing_role is not None:
        return jsonify(
            {
                "ok": False,
                "error": "Esiste già un ruolo con questo nome.",
            }
        ), 409

    validated_links: dict[str, set[int]] = {}

    for table_key, selected_ids in links.items():
        cfg = ROLE_LINK_CONFIG[table_key]
        valid_ids = _valid_role_creation_ids(policy, cfg)

        invalid_ids = selected_ids - valid_ids
        if invalid_ids:
            return jsonify(
                {
                    "ok": False,
                    "error": f"Il payload contiene id non validi per '{table_key}'.",
                    "table_key": table_key,
                    "invalid_ids": sorted(invalid_ids),
                }
            ), 400

        validated_links[table_key] = set(selected_ids)

    try:
        nuovo_ruolo = Roles(
            name=normalized_name,
            description=description,
        )
        db.session.add(nuovo_ruolo)
        db.session.flush()

        # Il ruolo appena creato diventa automaticamente subordinato ai ruoli diretti del creatore.
        parent_links = [
            {
                "manager_role_id": int(parent_role.id),
                "managed_role_id": int(nuovo_ruolo.id),
            }
            for parent_role in policy.direct_assigned_roles
        ]
        if parent_links:
            db.session.execute(roles_manageable_roles.insert(), parent_links)

        for table_key, selected_ids in validated_links.items():
            if not selected_ids:
                continue

            cfg = ROLE_LINK_CONFIG[table_key]
            assoc_table = cfg["assoc_table"]

            db.session.execute(
                assoc_table.insert(),
                [
                    {
                        cfg["left_fk"]: int(nuovo_ruolo.id),
                        cfg["right_fk"]: int(item_id),
                    }
                    for item_id in sorted(selected_ids)
                ],
            )

        db.session.commit()

    except Exception as exc:
        db.session.rollback()
        return jsonify(
            {
                "ok": False,
                "error": f"Errore creazione ruolo: {exc}",
            }
        ), 500

    return jsonify(
        {
            "ok": True,
            "message": "Ruolo creato correttamente.",
            "role": {
                "id": int(nuovo_ruolo.id),
                "name": nuovo_ruolo.name or "",
                "description": nuovo_ruolo.description or "",
            },
            "links": {key: sorted(value) for key, value in validated_links.items()},
        }
    ), 201


@main_bp.post("/api/impostazioni/utente-abac")
@require_active_perm("impostazioni_utente")
def api_save_user_abac():
    policy = active_policy()
    data = request.get_json(silent=True) or {}

    role_id_raw = data.get("role_id")
    user_id_raw = data.get("user_id")

    required_keys = {
        "role_id",
        "user_id",
        "lavorazioni_ids",
        "risorse_ids",
        "famiglia_ids",
    }

    missing_keys = sorted(key for key in required_keys if key not in data)
    if missing_keys:
        return jsonify(
            {
                "ok": False,
                "error": "Payload incompleto: salvataggio annullato per evitare cancellazioni involontarie.",
                "missing_keys": missing_keys,
            }
        ), 400

    lavorazioni_ids_raw = data.get("lavorazioni_ids")
    risorse_ids_raw = data.get("risorse_ids")
    famiglia_ids_raw = data.get("famiglia_ids")

    if not isinstance(lavorazioni_ids_raw, list):
        return jsonify(
            {"ok": False, "error": "lavorazioni_ids deve essere una lista."}
        ), 400

    if not isinstance(risorse_ids_raw, list):
        return jsonify(
            {"ok": False, "error": "risorse_ids deve essere una lista."}
        ), 400

    if not isinstance(famiglia_ids_raw, list):
        return jsonify(
            {"ok": False, "error": "famiglia_ids deve essere una lista."}
        ), 400

    try:
        role_id = int(role_id_raw)
        user_id = int(user_id_raw)
        lavorazioni_ids = {int(x) for x in lavorazioni_ids_raw}
        risorse_ids = {int(x) for x in risorse_ids_raw}
        famiglia_ids = _normalize_id_list(famiglia_ids_raw)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Parametri non validi."}), 400

    ruolo = Roles.query.get(role_id)
    if ruolo is None:
        return jsonify({"ok": False, "error": "Ruolo non trovato."}), 404

    manageable_role_ids = {int(role.id) for role in policy.abac_manageable_roles()}
    if int(ruolo.id) not in manageable_role_ids:
        return jsonify({"ok": False, "error": "Ruolo non gestibile."}), 403

    utente = User.query.get(user_id)
    if utente is None:
        return jsonify({"ok": False, "error": "Utente non trovato."}), 404

    if not any(r.id == ruolo.id for r in (utente.roles or [])):
        return jsonify(
            {"ok": False, "error": "L'utente non appartiene al ruolo selezionato."}
        ), 400

    allowed_lavorazioni_ids = {x.id for x in ruolo.effective_lavorazioni}
    allowed_risorse_ids = {x.id for x in ruolo.effective_risorse}
    allowed_famiglia_ids = {x.id for x in ruolo.effective_famiglia}

    invalid_lavorazioni = lavorazioni_ids - allowed_lavorazioni_ids
    invalid_risorse = risorse_ids - allowed_risorse_ids
    invalid_famiglia = set(famiglia_ids) - allowed_famiglia_ids

    if invalid_lavorazioni or invalid_risorse or invalid_famiglia:
        return jsonify(
            {
                "ok": False,
                "error": "Il payload contiene assegnazioni fuori dal perimetro RBAC del ruolo.",
                "invalid_lavorazioni": sorted(invalid_lavorazioni),
                "invalid_risorse": sorted(invalid_risorse),
                "invalid_famiglia": sorted(invalid_famiglia),
            }
        ), 400

    target_has_filtro_macchine = bool(utente.has_permission("filtro_macchine"))

    if not target_has_filtro_macchine:
        famiglia_ids = []

    current_lavorazioni_ids = {x.id for x in (utente.lavorazioni or [])}
    current_risorse_ids = {x.id for x in (utente.risorse or [])}

    current_lavorazioni_in_scope = current_lavorazioni_ids & allowed_lavorazioni_ids
    current_risorse_in_scope = current_risorse_ids & allowed_risorse_ids

    lavorazioni_to_add = lavorazioni_ids - current_lavorazioni_in_scope
    lavorazioni_to_remove = current_lavorazioni_in_scope - lavorazioni_ids
    clear_all_requested = not lavorazioni_ids and not risorse_ids and not famiglia_ids

    has_existing_assignments = (
        bool(current_lavorazioni_in_scope)
        or bool(current_risorse_in_scope)
        or bool(getattr(utente, "famiglie", []))
    )

    if (
        clear_all_requested
        and has_existing_assignments
        and not data.get("confirm_clear_all")
    ):
        return jsonify(
            {
                "ok": False,
                "error": (
                    "Il salvataggio rimuoverebbe tutte le spunte. "
                    "Operazione annullata per sicurezza."
                ),
                "requires_confirm_clear_all": True,
            }
        ), 409

    risorse_to_add = risorse_ids - current_risorse_in_scope
    risorse_to_remove = current_risorse_in_scope - risorse_ids

    try:
        if lavorazioni_to_add:
            db.session.execute(
                users_lavorazioni.insert(),
                [
                    {"user_id": utente.id, "lavorazioni_id": item_id}
                    for item_id in sorted(lavorazioni_to_add)
                ],
            )

        if lavorazioni_to_remove:
            db.session.execute(
                delete(users_lavorazioni).where(
                    users_lavorazioni.c.user_id == utente.id,
                    users_lavorazioni.c.lavorazioni_id.in_(
                        sorted(lavorazioni_to_remove)
                    ),
                )
            )

        if risorse_to_add:
            db.session.execute(
                users_risorse.insert(),
                [
                    {"user_id": utente.id, "risorse_id": item_id}
                    for item_id in sorted(risorse_to_add)
                ],
            )

        if risorse_to_remove:
            db.session.execute(
                delete(users_risorse).where(
                    users_risorse.c.user_id == utente.id,
                    users_risorse.c.risorse_id.in_(sorted(risorse_to_remove)),
                )
            )

        db.session.execute(
            delete(users_famiglia).where(users_famiglia.c.user_id == utente.id)
        )

        if target_has_filtro_macchine:
            for famiglia_id in famiglia_ids:
                db.session.execute(
                    users_famiglia.insert().values(
                        user_id=utente.id,
                        famiglia_id=famiglia_id,
                    )
                )

        db.session.commit()

    except Exception as exc:
        db.session.rollback()
        return jsonify(
            {
                "ok": False,
                "error": f"Errore salvataggio impostazioni ABAC: {exc}",
            }
        ), 500

    return jsonify(
        {
            "ok": True,
            "message": "Impostazioni ABAC salvate correttamente.",
            "role_id": ruolo.id,
            "user_id": utente.id,
            "lavorazioni_ids": sorted(lavorazioni_ids),
            "risorse_ids": sorted(risorse_ids),
            "famiglia_ids": famiglia_ids if target_has_filtro_macchine else [],
            "has_filtro_macchine": target_has_filtro_macchine,
            "delta": {
                "lavorazioni": {
                    "added": sorted(lavorazioni_to_add),
                    "removed": sorted(lavorazioni_to_remove),
                },
                "risorse": {
                    "added": sorted(risorse_to_add),
                    "removed": sorted(risorse_to_remove),
                },
                "famiglia": {
                    "selected": famiglia_ids if target_has_filtro_macchine else [],
                },
            },
        }
    ), 200


@main_bp.post("/api/impostazioni/ruolo-link")
@require_active_perm("impostazioni_utente")
def api_save_role_links():
    policy = active_policy()

    if not policy.can_view_role_links_section:
        return jsonify({"ok": False, "error": "Permesso insufficiente."}), 403

    data = request.get_json(silent=True) or {}

    role_id_raw = data.get("role_id")
    table_key = (data.get("table_key") or "").strip()
    selected_ids_raw = data.get("selected_ids") or []

    try:
        role_id = int(role_id_raw)
        selected_ids = {int(x) for x in selected_ids_raw}
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Parametri non validi."}), 400

    if table_key not in ROLE_LINK_CONFIG:
        return jsonify({"ok": False, "error": "Tabella non valida."}), 400

    ruolo = Roles.query.get(role_id)
    if ruolo is None:
        return jsonify({"ok": False, "error": "Ruolo non trovato."}), 404
    cfg = ROLE_LINK_CONFIG[table_key]
    assoc_table = cfg["assoc_table"]
    model = cfg["model"]

    manageable_role_ids = {int(r.id) for r in policy.role_link_manageable_roles()}
    if role_id not in manageable_role_ids:
        return jsonify({"ok": False, "error": "Ruolo non gestibile."}), 403

    if model is Roles:
        allowed_role_ids = set(manageable_role_ids)

        allowed_role_ids.discard(int(ruolo.id))

        invalid_role_ids = selected_ids - allowed_role_ids
        if invalid_role_ids:
            return jsonify(
                {
                    "ok": False,
                    "error": "Il payload contiene ruoli non consentiti o di livello uguale/superiore.",
                    "invalid_ids": sorted(invalid_role_ids),
                }
            ), 400

    if table_key in {"ruoli_ereditati", "ruoli_gestibili"}:
        if role_id in selected_ids:
            return jsonify(
                {"ok": False, "error": "Un ruolo non può essere collegato a sé stesso."}
            ), 400

        invalid_target_ids = selected_ids - manageable_role_ids
        if invalid_target_ids:
            return jsonify(
                {
                    "ok": False,
                    "error": "Il payload contiene ruoli non gestibili.",
                    "invalid_ids": sorted(invalid_target_ids),
                }
            ), 400

    valid_ids: set[int] = set()

    for item in model.query.all():
        raw_id = getattr(item, cfg["model_id"], None)

        if raw_id is None:
            continue

        try:
            valid_ids.add(int(raw_id))
        except (TypeError, ValueError):
            continue

    invalid_ids = selected_ids - valid_ids
    if invalid_ids:
        return jsonify(
            {
                "ok": False,
                "error": "Il payload contiene id non validi.",
                "invalid_ids": sorted(invalid_ids),
            }
        ), 400

    left_col = getattr(assoc_table.c, cfg["left_fk"])
    right_col = getattr(assoc_table.c, cfg["right_fk"])

    current_ids = set(
        db.session.execute(select(right_col).where(left_col == ruolo.id))
        .scalars()
        .all()
    )

    to_add = selected_ids - current_ids
    to_remove = current_ids - selected_ids

    try:
        if to_add:
            db.session.execute(
                assoc_table.insert(),
                [
                    {
                        cfg["left_fk"]: ruolo.id,
                        cfg["right_fk"]: item_id,
                    }
                    for item_id in sorted(to_add)
                ],
            )

        if to_remove:
            db.session.execute(
                delete(assoc_table).where(
                    left_col == ruolo.id,
                    right_col.in_(sorted(to_remove)),
                )
            )

        db.session.commit()

    except Exception as exc:
        db.session.rollback()
        return jsonify(
            {
                "ok": False,
                "error": f"Errore salvataggio connessioni ruolo: {exc}",
            }
        ), 500

    return jsonify(
        {
            "ok": True,
            "message": "Connessioni ruolo salvate correttamente.",
            "role_id": ruolo.id,
            "table_key": table_key,
            "selected_ids": sorted(selected_ids),
            "delta": {
                "added": sorted(to_add),
                "removed": sorted(to_remove),
            },
        }
    ), 200
