from decimal import Decimal
import json
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


def test_build_export_distinta_keeps_global_component_progressive_between_phases(
    monkeypatch,
):
    distinta = [
        {"NumFase": 1, "CodArt": "C1", "Quantita": "1"},
        {"NumFase": 1, "CodArt": "C2", "Quantita": "1"},
        {"NumFase": 1, "CodArt": "C3", "Quantita": "1"},
        {"NumFase": 1, "CodArt": "C4", "Quantita": "1"},
        {"NumFase": 2, "CodArt": "C5", "Quantita": "1"},
        {"NumFase": 2, "CodArt": "C6", "Quantita": "1"},
    ]
    monkeypatch.setattr(service, "_parse_distinta_materiale", lambda ordine: distinta)

    phase_two = json.loads(
        service._build_export_distinta_base(
            ordine=SimpleNamespace(),
            fase_corrente="2",
            q_lavorata=Decimal("1"),
            q_tot=Decimal("1"),
        )
    )

    assert [row["CodArt"] for row in phase_two] == ["C5", "C6"]
    assert [row["ProgressivoRiga"] for row in phase_two] == ["5", "6"]
