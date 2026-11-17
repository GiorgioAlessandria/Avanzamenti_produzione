import re
import unicodedata
from flask import jsonify
from sqlalchemy import func, select, and_, exists
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import selectinload
import json
from app_odp.operator_session import active_user
from app_odp.services.session_helpers import _current_user_id
from app_odp.models import (
    db,
    User,
    Roles,
    Reparti,
    Permissions,
    Risorse,
    Lavorazioni,
    Magazzini,
    Famiglia,
    Macrofamiglia,
    roles_permission,
    roles_reparti,
    roles_risorse,
    roles_lavorazioni,
    roles_magazzini,
    roles_famiglia,
    roles_macrofamiglia,
    roles_ineritance,
    roles_manageable_roles,
    ConfigAuditLog,
    HomeRepartoConfig,
    HomeVisibilityRule,
    user_roles,
)
from app_odp.policy.policy import RbacPolicy
from app_odp.services.order_helpers import (
    _parse_bool_flag,
    _norm_text,
    _now_rome_dt,
    _json_safe,
)

ROLE_LINK_CONFIG = {
    "permissions": {
        "label": "Permessi",
        "assoc_table": roles_permission,
        "left_fk": "role_id",
        "right_fk": "permission_id",
        "model": Permissions,
        "model_id": "id",
        "code_attr": "Codice",
        "desc_attr": "Descrizione",
    },
    "reparti": {
        "label": "Reparti",
        "assoc_table": roles_reparti,
        "left_fk": "roles_id",
        "right_fk": "reparto_id",
        "model": Reparti,
        "model_id": "id",
        "code_attr": "Codice",
        "desc_attr": "Descrizione",
    },
    "risorse": {
        "label": "Risorse",
        "assoc_table": roles_risorse,
        "left_fk": "roles_id",
        "right_fk": "risorse_id",
        "model": Risorse,
        "model_id": "id",
        "code_attr": "Codice",
        "desc_attr": "Descrizione",
    },
    "lavorazioni": {
        "label": "Lavorazioni",
        "assoc_table": roles_lavorazioni,
        "left_fk": "roles_id",
        "right_fk": "lavorazioni_id",
        "model": Lavorazioni,
        "model_id": "id",
        "code_attr": "Codice",
        "desc_attr": "Descrizione",
    },
    "magazzini": {
        "label": "Magazzini",
        "assoc_table": roles_magazzini,
        "left_fk": "roles_id",
        "right_fk": "magazzini_id",
        "model": Magazzini,
        "model_id": "id",
        "code_attr": "Codice",
        "desc_attr": "Descrizione",
    },
    "famiglia": {
        "label": "Famiglia",
        "assoc_table": roles_famiglia,
        "left_fk": "roles_id",
        "right_fk": "famiglia_id",
        "model": Famiglia,
        "model_id": "id",
        "code_attr": "Codice",
        "desc_attr": "Descrizione",
    },
    "macrofamiglia": {
        "label": "Macrofamiglia",
        "assoc_table": roles_macrofamiglia,
        "left_fk": "roles_id",
        "right_fk": "macrofamiglia_id",
        "model": Macrofamiglia,
        "model_id": "id",
        "code_attr": "Codice",
        "desc_attr": "Descrizione",
    },
    "ruoli_ereditati": {
        "label": "Ruoli ereditati",
        "assoc_table": roles_ineritance,
        "left_fk": "role_id",
        "right_fk": "included_role",
        "model": Roles,
        "model_id": "id",
        "code_attr": "name",
        "desc_attr": "description",
    },
    "ruoli_gestibili": {
        "label": "Ruoli gestibili",
        "assoc_table": roles_manageable_roles,
        "left_fk": "manager_role_id",
        "right_fk": "managed_role_id",
        "model": Roles,
        "model_id": "id",
        "code_attr": "name",
        "desc_attr": "description",
    },
}


def _role_config_items_for_creation(policy: RbacPolicy, cfg: dict) -> list[dict]:
    model = cfg["model"]
    code_attr = cfg["code_attr"]
    desc_attr = cfg["desc_attr"]

    if model is Roles:
        rows = policy.role_creation_manageable_roles()
    else:
        rows = model.query.order_by(
            func.lower(
                func.coalesce(
                    getattr(model, desc_attr),
                    getattr(model, code_attr),
                )
            ),
            func.lower(getattr(model, code_attr)),
        ).all()

    return [
        {
            "id": getattr(row, cfg["model_id"]),
            "codice": getattr(row, code_attr, "") or "",
            "descrizione": getattr(row, desc_attr, "") or "",
        }
        for row in rows
    ]


def _valid_role_creation_ids(policy: RbacPolicy, cfg: dict) -> set[int]:
    model = cfg["model"]

    if model is Roles:
        return {int(role.id) for role in policy.role_creation_manageable_roles()}

    model_id_attr = getattr(model, cfg["model_id"])
    stmt = select(model_id_attr)
    return {int(x) for x in db.session.execute(stmt).scalars().all()}


def _normalize_role_creation_links(raw_links) -> dict[str, set[int]]:
    if raw_links in (None, ""):
        return {}

    if not isinstance(raw_links, dict):
        raise ValueError("Il payload 'links' deve essere un oggetto JSON.")

    normalized = {}

    for key, raw_values in raw_links.items():
        if key not in ROLE_LINK_CONFIG:
            raise ValueError(f"Tabella non valida nel payload: {key}")

        if raw_values in (None, ""):
            normalized[key] = set()
            continue

        if not isinstance(raw_values, (list, tuple, set)):
            raise ValueError(f"I valori per '{key}' devono essere una lista di id.")

        try:
            normalized[key] = {int(x) for x in raw_values}
        except (TypeError, ValueError):
            raise ValueError(f"Gli id per '{key}' non sono validi.")

    return normalized


def _normalize_id_list(raw_values) -> list[int]:
    if raw_values in (None, ""):
        return []

    if not isinstance(raw_values, (list, tuple, set)):
        raise ValueError("I valori devono essere una lista di id.")

    out = []
    for value in raw_values:
        try:
            item_id = int(value)
        except (TypeError, ValueError):
            raise ValueError("Id non valido.")

        if item_id > 0 and item_id not in out:
            out.append(item_id)

    return out


def _build_public_id_from_full_name(value: str) -> str:
    raw = _norm_text(value)
    if not raw:
        return ""

    normalized = unicodedata.normalize("NFKD", raw)
    normalized = normalized.encode("ascii", "ignore").decode("ascii")
    normalized = normalized.lower().strip()

    normalized = re.sub(r"\s+", "_", normalized)
    normalized = re.sub(r"[^a-z0-9_]", "", normalized)
    normalized = re.sub(r"_+", "_", normalized).strip("_")

    return normalized


def _normalize_user_registry_payload(data) -> dict:
    data = data or {}
    if not isinstance(data, dict):
        raise ValueError("Formato dati non valido.")

    username = _norm_text(data.get("username"))
    public_id = _norm_text(data.get("public_id"))
    genere = _norm_text(data.get("genere")).lower()
    reparto_princ = _norm_text(data.get("reparto_princ"))

    if not username:
        raise ValueError("Username obbligatorio.")
    if len(username) < 3:
        raise ValueError("Username troppo corto.")
    if len(username) > 100:
        raise ValueError("Username troppo lungo.")

    if not public_id:
        raise ValueError("Public ID obbligatorio.")
    if len(public_id) > 100:
        raise ValueError("Public ID troppo lungo.")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", public_id):
        raise ValueError(
            "Il Public ID può contenere solo lettere, numeri, punto, trattino e underscore."
        )

    if genere not in {"", "m", "f"}:
        raise ValueError("Genere non valido: usare m, f oppure lasciare vuoto.")

    return {
        "username": username,
        "public_id": public_id,
        "genere": genere,
        "reparto_princ": reparto_princ,
    }


def _login_code_error_response(message: str, status_code: int = 400):
    return jsonify(
        {
            "ok": False,
            "error": message,
        }
    ), status_code


def _is_login_code_integrity_error(exc: IntegrityError) -> bool:
    msg = str(getattr(exc, "orig", exc)).lower()
    return "unique constraint failed" in msg and "login_code_lookup" in msg


LOGIN_CODE_DUPLICATO_MSG = "Il codice di login è già utilizzato"


def _prepare_login_code_or_response(
    raw_code,
    exclude_user_id: int | None = None,
):
    """
    Valida il login code e verifica che non sia già assegnato.

    exclude_user_id serve nel reset/modifica:
    - permette allo stesso utente di mantenere lo stesso codice;
    - blocca il codice se appartiene a un altro utente.
    """
    try:
        code = User.validate_login_code(raw_code)
    except ValueError as exc:
        return None, _login_code_error_response(str(exc), 400)

    lookup = User.login_code_lookup_for(code)

    query = User.query.filter(User.login_code_lookup == lookup)

    if exclude_user_id is not None:
        query = query.filter(User.id != int(exclude_user_id))

    if query.first() is not None:
        return None, _login_code_error_response(LOGIN_CODE_DUPLICATO_MSG, 409)

    return code, None


HOME_CONFIG_TEMPLATE_OPTIONS = {
    "partials/_home_montaggio.j2": "Layout montaggio/macchine",
    "partials/_home_standard.j2": "Layout standard",
    "partials/page_vuota.html": "Pagina vuota",
}

HOME_CONFIG_RENDERER_OPTIONS = {
    "montaggio": "Montaggio/macchine",
    "standard": "Standard",
    "empty": "Vuoto",
}

HOME_CONFIG_METODO_OPTIONS = {
    "montaggio": "Metodo montaggio",
    "collaudo": "Metodo collaudo",
    "nessuno": "Nessuno",
}

HOME_RULE_APPLY_TO_OPTIONS = {
    "macchine": "Macchine",
    "semilavorati": "Semilavorati",
    "all": "Tutto",
}

HOME_RULE_PHASE_MODE_OPTIONS = {
    "all": "Tutte",
    "exact": "Esatta",
    "last": "Ultima",
    "not_first": "Dopo la prima",
    "list": "Lista",
}


def _home_config_bool(value) -> bool:
    return _parse_bool_flag(value)


def _home_config_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _home_config_text(value) -> str:
    return _norm_text(value)


def _home_config_json_payload(obj) -> str:
    return json.dumps(_json_safe(obj), ensure_ascii=False, sort_keys=True)


def _home_config_audit(
    *,
    entity_type: str,
    entity_id: str,
    action: str,
    old_payload=None,
    new_payload=None,
    note: str = "",
):
    user = active_user()

    db.session.add(
        ConfigAuditLog(
            entity_type=entity_type,
            entity_id=str(entity_id),
            action=action,
            old_payload=_home_config_json_payload(old_payload)
            if old_payload is not None
            else None,
            new_payload=_home_config_json_payload(new_payload)
            if new_payload is not None
            else None,
            changed_by_user_id=getattr(user, "id", None),
            changed_by_username=getattr(user, "username", None) or "",
            changed_at=_now_rome_dt().isoformat(timespec="seconds"),
            note=note,
        )
    )


def _home_reparto_config_to_dict(row: HomeRepartoConfig) -> dict:
    return {
        "id": row.id,
        "reparto_id": row.reparto_id,
        "reparto_codice": row.reparto.Codice if row.reparto else "",
        "reparto_descrizione": row.reparto.Descrizione if row.reparto else "",
        "tab_code": row.tab_code or "",
        "label": row.label or "",
        "template": row.template or "",
        "renderer": row.renderer or "",
        "permesso": row.permesso or "home",
        "ordine_menu": int(row.ordine_menu or 0),
        "attivo": bool(row.attivo),
        "titolo_macchine_da_eseguire": row.titolo_macchine_da_eseguire or "",
        "titolo_macchine_attive": row.titolo_macchine_attive or "",
        "titolo_semilavorati_da_eseguire": row.titolo_semilavorati_da_eseguire or "",
        "titolo_semilavorati_attivi": row.titolo_semilavorati_attivi or "",
        "testo_presa_macchina": row.testo_presa_macchina or "",
        "testo_sospendi_macchina": row.testo_sospendi_macchina or "",
        "testo_riattiva_macchina": row.testo_riattiva_macchina or "",
        "testo_chiudi_macchina": row.testo_chiudi_macchina or "",
        "metodo_documentale_tipo": row.metodo_documentale_tipo or "nessuno",
        "metodo_documentale_prefisso": row.metodo_documentale_prefisso or "",
        "metodo_documentale_path_key": row.metodo_documentale_path_key or "",
    }


def _home_visibility_rule_to_dict(row: HomeVisibilityRule) -> dict:
    return {
        "id": row.id,
        "reparto_id": row.reparto_id,
        "reparto_codice": row.reparto.Codice if row.reparto else "",
        "reparto_descrizione": row.reparto.Descrizione if row.reparto else "",
        "role_id": row.role_id,
        "role_name": row.role.name if row.role else "",
        "role_description": row.role.description if row.role else "",
        "user_id": row.user_id,
        "username": row.user.username if row.user else "",
        "apply_to": row.apply_to or "macchine",
        "phase_mode": row.phase_mode or "all",
        "phase_values": row.phase_values or "",
        "attivo": bool(row.attivo),
        "titolo_macchine_da_eseguire": row.titolo_macchine_da_eseguire or "",
        "titolo_macchine_attive": row.titolo_macchine_attive or "",
        "testo_presa_macchina": row.testo_presa_macchina or "",
        "testo_sospendi_macchina": row.testo_sospendi_macchina or "",
        "testo_riattiva_macchina": row.testo_riattiva_macchina or "",
        "testo_chiudi_macchina": row.testo_chiudi_macchina or "",
        "metodo_documentale_tipo": row.metodo_documentale_tipo or "",
        "metodo_documentale_prefisso": row.metodo_documentale_prefisso or "",
        "metodo_documentale_path_key": row.metodo_documentale_path_key or "",
    }


def _home_config_manageable_users(policy: RbacPolicy) -> list[User]:
    manageable_role_ids = set(policy.descendant_manageable_role_ids)

    if not manageable_role_ids:
        return []

    ur_allowed = user_roles.alias("home_cfg_ur_allowed")
    ur_forbidden = user_roles.alias("home_cfg_ur_forbidden")

    allowed_exists = exists(
        select(1)
        .select_from(ur_allowed)
        .where(
            and_(
                ur_allowed.c.user_id == User.id,
                ur_allowed.c.role_id.in_(manageable_role_ids),
            )
        )
    )

    forbidden_exists = exists(
        select(1)
        .select_from(ur_forbidden)
        .where(
            and_(
                ur_forbidden.c.user_id == User.id,
                ~ur_forbidden.c.role_id.in_(manageable_role_ids),
            )
        )
    )

    return (
        User.query.filter(User.active.is_(True))
        .filter(User.id != _current_user_id())
        .filter(allowed_exists)
        .filter(~forbidden_exists)
        .order_by(func.lower(User.username))
        .all()
    )


def _home_config_user_is_manageable(policy: RbacPolicy, user: User | None) -> bool:
    if user is None:
        return False

    manageable_role_ids = set(policy.descendant_manageable_role_ids)
    if not manageable_role_ids:
        return False

    target_roles = list(user.roles or [])
    if not target_roles:
        return False

    return all(int(role.id) in manageable_role_ids for role in target_roles)


def _home_config_role_is_manageable(policy: RbacPolicy, role: Roles | None) -> bool:
    if role is None:
        return False

    return int(role.id) in set(policy.descendant_manageable_role_ids)


def _build_home_config_settings_payload(policy: RbacPolicy) -> dict:
    reparto_rows = Reparti.query.order_by(
        func.lower(func.coalesce(Reparti.Descrizione, Reparti.Codice)),
        func.lower(Reparti.Codice),
    ).all()

    config_rows = (
        HomeRepartoConfig.query.options(selectinload(HomeRepartoConfig.reparto))
        .order_by(HomeRepartoConfig.ordine_menu.asc(), HomeRepartoConfig.id.asc())
        .all()
    )

    rule_rows = (
        HomeVisibilityRule.query.options(
            selectinload(HomeVisibilityRule.reparto),
            selectinload(HomeVisibilityRule.role),
            selectinload(HomeVisibilityRule.user),
        )
        .order_by(HomeVisibilityRule.reparto_id.asc(), HomeVisibilityRule.id.asc())
        .all()
    )

    manageable_roles = sorted(
        list(policy.descendant_manageable_roles),
        key=lambda r: ((r.description or r.name or "").lower(), (r.name or "").lower()),
    )

    manageable_users = _home_config_manageable_users(policy)

    return {
        "reparti": [
            {
                "id": r.id,
                "codice": r.Codice or "",
                "descrizione": r.Descrizione or r.Codice or "",
            }
            for r in reparto_rows
        ],
        "roles": [
            {
                "id": r.id,
                "name": r.name or "",
                "description": r.description or r.name or "",
            }
            for r in manageable_roles
        ],
        "users": [
            {
                "id": u.id,
                "username": u.username or "",
            }
            for u in manageable_users
        ],
        "home_configs": [_home_reparto_config_to_dict(row) for row in config_rows],
        "visibility_rules": [_home_visibility_rule_to_dict(row) for row in rule_rows],
        "template_options": HOME_CONFIG_TEMPLATE_OPTIONS,
        "renderer_options": HOME_CONFIG_RENDERER_OPTIONS,
        "metodo_options": HOME_CONFIG_METODO_OPTIONS,
        "apply_to_options": HOME_RULE_APPLY_TO_OPTIONS,
        "phase_mode_options": HOME_RULE_PHASE_MODE_OPTIONS,
    }


def _parse_home_rule_phase_values(raw_values, phase_mode: str) -> str | None:
    phase_mode = _home_config_text(phase_mode).lower()

    if phase_mode in {"all", "last", "not_first"}:
        return None

    if isinstance(raw_values, str):
        raw_values = raw_values.strip()

        if raw_values.startswith("["):
            try:
                parsed = json.loads(raw_values)
            except json.JSONDecodeError:
                parsed = [raw_values]
        else:
            parsed = [x.strip() for x in raw_values.split(",")]
    elif isinstance(raw_values, (list, tuple, set)):
        parsed = list(raw_values)
    else:
        parsed = []

    values = []
    for item in parsed:
        value = _home_config_text(item)
        if not value:
            continue

        try:
            phase_int = int(float(value))
            if phase_int <= 0:
                raise ValueError
            value = str(phase_int)
        except (TypeError, ValueError):
            raise ValueError("I valori fase devono essere numerici.")

        if value not in values:
            values.append(value)

    if phase_mode in {"exact", "list"} and not values:
        raise ValueError("Per fase esatta/lista devi indicare almeno un valore fase.")

    return json.dumps(values, ensure_ascii=False)
