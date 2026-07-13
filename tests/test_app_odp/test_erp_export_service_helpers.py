from types import SimpleNamespace

from app_odp.services import erp_export_service as service


def test_safe_txt_suffix_and_prefix_replace_unsafe_chars_and_use_fallbacks():
    assert service._safe_txt_suffix(" ordine / prova 01 ") == "ordine___prova_01"
    assert service._safe_txt_suffix("___") == "export"
    assert service._safe_txt_suffix("", fallback="fallback") == "fallback"

    assert service._safe_txt_prefix(" AVP/B ") == "AVP_B"
    assert service._safe_txt_prefix("***", fallback="TES") == "TES"


def test_json_loads_safe_returns_default_on_invalid_json():
    assert service._json_loads_safe('{"a": 1}', {}) == {"a": 1}
    assert service._json_loads_safe("{bad", {"fallback": True}) == {"fallback": True}


def test_get_outbox_payload_returns_dict_only():
    assert service._get_outbox_payload(SimpleNamespace(payload_json='{"a": 1}')) == {"a": 1}
    assert service._get_outbox_payload(SimpleNamespace(payload_json="[1, 2]")) == {}
    assert service._get_outbox_payload(SimpleNamespace(payload_json="{bad")) == {}
    assert service._get_outbox_payload(SimpleNamespace(payload_json=None)) == {}


def test_build_operation_group_id_uses_timestamp_and_safe_order_tokens():
    ordine = SimpleNamespace(IdDocumento=" DOC/1 ", IdRiga=" 10 ")

    assert service._build_operation_group_id(
        ordine,
        action="presa in carico",
        when_iso="2026-07-09T14:30:10",
    ) == "20260709143010_DOC_1_10_presa_in_carico"
