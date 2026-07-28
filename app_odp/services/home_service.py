import re
import unicodedata
from sqlalchemy import func, or_
from sqlalchemy.orm import selectinload
from app_odp.models import HomeRepartoConfig, InputOdp, HomeVisibilityRule
from app_odp.policy.policy import RbacPolicy
from app_odp.services.order_helpers import _extract_codes_from_cell, _norm_text
from app_odp.services.ordini_query_service import _base_odp_query
from flask import abort, current_app, render_template
from app_odp.operator_session import active_policy, active_user
from app_odp.services.session_helpers import _current_user_id
from app_odp.services.priorita_service import _apply_priorita_to_ordini


def filter_input_odp_for_home_config(
    query,
    config: HomeRepartoConfig,
    policy: RbacPolicy,
    user=None,
):
    return policy.filter_input_odp_for_home_config(
        query,
        config,
        user=user,
    )


def _normalize_home_tab_code(value) -> str:
    raw = _norm_text(value)
    if not raw:
        return ""

    raw = unicodedata.normalize("NFKD", raw)
    raw = raw.encode("ascii", "ignore").decode("ascii")
    raw = raw.strip().lower()
    raw = re.sub(r"[\s\-]+", "_", raw)
    raw = re.sub(r"[^a-z0-9_]+", "", raw)
    raw = re.sub(r"_+", "_", raw).strip("_")

    return raw


def _home_reparto_configs_query():
    return (
        HomeRepartoConfig.query.options(
            selectinload(HomeRepartoConfig.reparto),
        )
        .filter(HomeRepartoConfig.attivo.is_(True))
        .order_by(
            HomeRepartoConfig.ordine_menu.asc(),
            func.lower(HomeRepartoConfig.label).asc(),
            HomeRepartoConfig.id.asc(),
        )
    )


def _home_reparto_config_by_tab(tab_code: str):
    tab_norm = _normalize_home_tab_code(tab_code)
    if not tab_norm:
        return None

    return (
        HomeRepartoConfig.query.options(
            selectinload(HomeRepartoConfig.reparto),
        )
        .filter(HomeRepartoConfig.attivo.is_(True))
        .filter(func.lower(HomeRepartoConfig.tab_code) == tab_norm)
        .first()
    )


def _home_reparto_code(config: HomeRepartoConfig) -> str:
    if config is None or config.reparto is None:
        return ""

    return _norm_text(config.reparto.Codice)


def _home_reparto_label(config: HomeRepartoConfig) -> str:
    if config is None:
        return ""

    return (
        _norm_text(config.label)
        or _norm_text(getattr(config.reparto, "Descrizione", ""))
        or _norm_text(getattr(config.reparto, "Codice", ""))
        or _norm_text(config.tab_code)
    )


def _policy_can_access_home_config(
    policy: RbacPolicy, config: HomeRepartoConfig
) -> bool:
    if policy is None or config is None:
        return False

    reparto_code = _home_reparto_code(config)
    permesso = _norm_text(config.permesso) or "home"

    if not reparto_code:
        return False

    if getattr(policy, "has_direct_admin_role", False):
        return True

    if reparto_code not in policy.allowed_reparti:
        return False

    if not policy.can(permesso):
        return False

    return True


def _allowed_home_reparto_configs(policy: RbacPolicy) -> list[HomeRepartoConfig]:
    configs = _home_reparto_configs_query().all()

    return [cfg for cfg in configs if _policy_can_access_home_config(policy, cfg)]


def _first_allowed_home_reparto_config(policy: RbacPolicy):
    rows = _allowed_home_reparto_configs(policy)
    return rows[0] if rows else None


def _home_reparto_config_for_ordine(ordine: InputOdp):
    reparto_codes = set(_extract_codes_from_cell(getattr(ordine, "CodReparto", "")))
    if not reparto_codes:
        return None

    configs = _home_reparto_configs_query().all()

    for cfg in configs:
        if _home_reparto_code(cfg) in reparto_codes:
            return cfg

    return None


def _home_ui_texts_for_user(
    config: HomeRepartoConfig,
    policy: RbacPolicy,
    user=None,
) -> dict:
    user = user or active_user()

    texts = {
        "titolo_macchine_da_eseguire": "Ordini macchina da eseguire",
        "titolo_macchine_attive": "Ordini macchina attivi",
        "titolo_semilavorati_da_eseguire": "Ordini semilavorati da eseguire",
        "titolo_semilavorati_attivi": "Ordini semilavorati attivi",
        "testo_presa_macchina": "Attiva ordine macchina",
        "testo_sospendi_macchina": "Sospendi ordine",
        "testo_riattiva_macchina": "Riattiva ordine",
        "testo_chiudi_macchina": "Chiudi ordine",
        "toast_presa_macchina": "Presa in carico ordine macchina",
        "toast_sospendi_macchina": "Sospensione ordine macchina",
        "toast_riattiva_macchina": "Riattivazione ordine macchina",
        "toast_chiudi_macchina": "Chiusura ordine macchina",
    }

    config_map = {
        "titolo_macchine_da_eseguire": getattr(
            config, "titolo_macchine_da_eseguire", None
        ),
        "titolo_macchine_attive": getattr(config, "titolo_macchine_attive", None),
        "titolo_semilavorati_da_eseguire": getattr(
            config, "titolo_semilavorati_da_eseguire", None
        ),
        "titolo_semilavorati_attivi": getattr(
            config, "titolo_semilavorati_attivi", None
        ),
        "testo_presa_macchina": getattr(config, "testo_presa_macchina", None),
        "testo_sospendi_macchina": getattr(config, "testo_sospendi_macchina", None),
        "testo_riattiva_macchina": getattr(config, "testo_riattiva_macchina", None),
        "testo_chiudi_macchina": getattr(config, "testo_chiudi_macchina", None),
    }

    for key, value in config_map.items():
        value = _norm_text(value)
        if value:
            texts[key] = value

    rules = (
        HomeVisibilityRule.query.filter(
            HomeVisibilityRule.attivo.is_(True),
            HomeVisibilityRule.reparto_id == config.reparto_id,
        )
        .filter(
            or_(
                HomeVisibilityRule.role_id.in_(policy.role_ids),
                HomeVisibilityRule.user_id == getattr(user, "id", None),
            )
        )
        .order_by(
            HomeVisibilityRule.user_id.isnot(None).asc(),
            HomeVisibilityRule.id.asc(),
        )
        .all()
    )

    for rule in rules:
        rule_map = {
            "titolo_macchine_da_eseguire": rule.titolo_macchine_da_eseguire,
            "titolo_macchine_attive": rule.titolo_macchine_attive,
            "testo_presa_macchina": rule.testo_presa_macchina,
            "testo_sospendi_macchina": rule.testo_sospendi_macchina,
            "testo_riattiva_macchina": rule.testo_riattiva_macchina,
            "testo_chiudi_macchina": rule.testo_chiudi_macchina,
        }

        for key, value in rule_map.items():
            value = _norm_text(value)
            if value:
                texts[key] = value

    # Toast: se non hai un campo dedicato, riuso gli stessi testi azione.
    texts["toast_presa_macchina"] = (
        texts.get("testo_presa_macchina") or texts["toast_presa_macchina"]
    )
    texts["toast_sospendi_macchina"] = (
        texts.get("testo_sospendi_macchina") or texts["toast_sospendi_macchina"]
    )
    texts["toast_riattiva_macchina"] = (
        texts.get("testo_riattiva_macchina") or texts["toast_riattiva_macchina"]
    )
    texts["toast_chiudi_macchina"] = (
        texts.get("testo_chiudi_macchina") or texts["toast_chiudi_macchina"]
    )

    return texts


def _home_method_settings_for_user(
    config: HomeRepartoConfig,
    policy: RbacPolicy,
    user=None,
) -> dict:
    user = user or active_user()

    settings = {
        "tipo": _norm_text(getattr(config, "metodo_documentale_tipo", ""))
        or "montaggio",
        "prefisso": _norm_text(getattr(config, "metodo_documentale_prefisso", "")),
        "path_key": _norm_text(getattr(config, "metodo_documentale_path_key", ""))
        or "MONTAGGIO_PDF_DIR",
    }

    rules = (
        HomeVisibilityRule.query.filter(
            HomeVisibilityRule.attivo.is_(True),
            HomeVisibilityRule.reparto_id == config.reparto_id,
        )
        .filter(
            or_(
                HomeVisibilityRule.role_id.in_(policy.role_ids),
                HomeVisibilityRule.user_id == getattr(user, "id", None),
            )
        )
        .order_by(
            HomeVisibilityRule.user_id.isnot(None).asc(),
            HomeVisibilityRule.id.asc(),
        )
        .all()
    )

    for rule in rules:
        if _norm_text(rule.metodo_documentale_tipo):
            settings["tipo"] = _norm_text(rule.metodo_documentale_tipo)
        if _norm_text(rule.metodo_documentale_prefisso):
            settings["prefisso"] = _norm_text(rule.metodo_documentale_prefisso)
        if _norm_text(rule.metodo_documentale_path_key):
            settings["path_key"] = _norm_text(rule.metodo_documentale_path_key)

    return settings


def _tab_from_ordine(ordine: InputOdp) -> str | None:
    config = _home_reparto_config_for_ordine(ordine)
    return config.tab_code if config else None


def _get_visible_odp_by_key(
    policy: RbacPolicy,
    id_documento: str,
    id_riga: str,
) -> InputOdp:
    exists_anyway = (
        _base_odp_query()
        .filter_by(
            IdDocumento=id_documento,
            IdRiga=id_riga,
        )
        .first()
    )

    if exists_anyway is None:
        abort(404)

    config = _home_reparto_config_for_ordine(exists_anyway)

    if config is None:
        abort(403)

    if not _policy_can_access_home_config(policy, config):
        abort(403)

    ordine = (
        filter_input_odp_for_home_config(
            _base_odp_query(),
            config,
            policy,
            active_user(),
        )
        .filter(
            InputOdp.IdDocumento == id_documento,
            InputOdp.IdRiga == id_riga,
        )
        .first()
    )

    if ordine is None:
        abort(403)

    return ordine


def _render_bridge_standard(odp):
    from app_odp.services.documenti_service import _build_metodo_montaggio_lookup

    metodo_montaggio_lookup = _build_metodo_montaggio_lookup(odp)

    return {
        "tbody_ordini_da_eseguire": render_template(
            "partials/_home_standard_rows_da_eseguire.j2",
            odp=odp,
        ),
        "tbody_ordini_in_corso": render_template(
            "partials/_home_standard_rows_in_corso.j2",
            odp=odp,
            metodo_montaggio_lookup=metodo_montaggio_lookup,
        ),
    }


def _render_bridge_montaggio(
    odp,
    *,
    metodo_path_key: str = "MONTAGGIO_PDF_DIR",
    metodo_prefisso: str = "",
):
    from app_odp.services.documenti_service import _build_metodo_lookup

    metodo_lookup = _build_metodo_lookup(
        odp,
        path_key=metodo_path_key,
        prefisso=metodo_prefisso,
    )

    ctx = {
        "odp": odp,
        "metodo_lookup": metodo_lookup,
        "metodo_documentale_prefisso": metodo_prefisso,
    }

    return {
        "tbody_ordini_da_eseguire_sl": render_template(
            "partials/_home_montaggio_sl_rows_da_eseguire.j2",
            odp=odp,
        ),
        "tbody_ordini_in_corso_sl": render_template(
            "partials/_home_montaggio_sl_rows_in_corso.j2",
            **ctx,
        ),
        "tbody_ordini_da_eseguire_m": render_template(
            "partials/_home_montaggio_m_rows_da_eseguire.j2",
            odp=odp,
        ),
        "tbody_ordini_in_corso_m": render_template(
            "partials/_home_montaggio_m_rows_in_corso.j2",
            **ctx,
        ),
    }


def _render_bridge_empty(odp):
    return {}


RENDERERS = {
    "standard": _render_bridge_standard,
    "montaggio": _render_bridge_montaggio,
    "empty": _render_bridge_empty,
}


def _render_fragments_for_home_config(
    config: HomeRepartoConfig,
    odp: list[InputOdp],
) -> dict:
    renderer_key = _norm_text(getattr(config, "renderer", "")) or "empty"

    if renderer_key == "montaggio":
        method_settings = _home_method_settings_for_user(
            config,
            active_policy(),
            active_user(),
        )

        return _render_bridge_montaggio(
            odp,
            metodo_path_key=method_settings["path_key"],
            metodo_prefisso=method_settings["prefisso"],
        )

    renderer = RENDERERS.get(renderer_key)
    if renderer is None:
        current_app.logger.warning(
            "Renderer home non valido: tab_code=%s renderer=%s",
            getattr(config, "tab_code", ""),
            renderer_key,
        )
        return {}

    return renderer(odp)


def _query_for_home_config(policy: RbacPolicy, config: HomeRepartoConfig):
    return filter_input_odp_for_home_config(
        _base_odp_query(),
        config,
        policy,
        active_user(),
    )


def _home_rows_for_config(
    policy: RbacPolicy,
    config: HomeRepartoConfig,
    *,
    apply_priorita: bool = True,
    sort_priorita: bool = True,
) -> list:
    from app_odp.services.ordini_gruppi_service import (
        _collapse_work_group_rows_for_home,
    )

    odp = list(_query_for_home_config(policy, config).all())

    if apply_priorita:
        odp = _apply_priorita_to_ordini(
            list(odp),
            _current_user_id(),
            sort_result=sort_priorita,
        )

    return _collapse_work_group_rows_for_home(odp)


def _fragments_for_ordine_tab(
    policy: RbacPolicy,
    ordine: InputOdp,
) -> tuple[str | None, dict]:
    config = _home_reparto_config_for_ordine(ordine)
    if config is None:
        return None, {}

    if not _policy_can_access_home_config(policy, config):
        return None, {}

    odp = _home_rows_for_config(policy, config, apply_priorita=True, sort_priorita=True)
    fragments = _render_fragments_for_home_config(config, odp)

    return config.tab_code, fragments
