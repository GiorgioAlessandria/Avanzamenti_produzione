import json
import re

import pytest

from app_odp.services import impostazioni_service as service


def test_normalize_role_creation_links_accepts_known_tables_and_id_lists():
    assert service._normalize_role_creation_links(
        {"permissions": ["1", 2, 2], "reparti": None}
    ) == {"permissions": {1, 2}, "reparti": set()}


@pytest.mark.parametrize("value", [None, "", [], ()])
def test_normalize_id_list_returns_empty_for_empty_values(value):
    assert service._normalize_id_list(value) == []


def test_normalize_id_list_keeps_positive_unique_ids_in_order():
    assert service._normalize_id_list(["2", 1, 2, 0, -1]) == [2, 1]


@pytest.mark.parametrize("value", ["bad", 5, {"bad": [1]}])
def test_role_and_id_normalizers_raise_for_invalid_payloads(value):
    if isinstance(value, dict):
        with pytest.raises(ValueError):
            service._normalize_role_creation_links(value)
    else:
        with pytest.raises(ValueError):
            service._normalize_id_list(value)


def test_build_public_id_from_full_name_normalizes_ascii_identifier():
    assert service._build_public_id_from_full_name(" Mario   Rossi-01 ") == "mario_rossi01"
    assert service._build_public_id_from_full_name("") == ""


def test_normalize_user_registry_payload_trims_and_normalizes_fields():
    assert service._normalize_user_registry_payload(
        {
            "username": " Mario Rossi ",
            "public_id": "mario.rossi-01",
            "genere": " F ",
            "reparto_princ": " MON ",
        }
    ) == {
        "username": "Mario Rossi",
        "public_id": "mario.rossi-01",
        "genere": "f",
        "reparto_princ": "MON",
    }


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ([], "Formato dati non valido."),
        ({"username": "", "public_id": "mario"}, "Username obbligatorio."),
        ({"username": "ab", "public_id": "mario"}, "Username troppo corto."),
        ({"username": "mario", "public_id": ""}, "Public ID obbligatorio."),
        (
            {"username": "mario", "public_id": "mario rossi"},
            "Il Public ID può contenere solo lettere, numeri, punto, trattino e underscore.",
        ),
        (
            {"username": "mario", "public_id": "mario", "genere": "x"},
            "Genere non valido: usare m, f oppure lasciare vuoto.",
        ),
    ],
)
def test_normalize_user_registry_payload_rejects_invalid_values(payload, message):
    with pytest.raises(ValueError, match=re.escape(message)):
        service._normalize_user_registry_payload(payload)


def test_home_config_scalar_helpers_normalize_common_values():
    assert service._home_config_bool("true") is True
    assert service._home_config_bool("0") is False
    assert service._home_config_int("12", default=0) == 12
    assert service._home_config_int("bad", default=7) == 7
    assert service._home_config_text(" abc ") == "abc"


def test_home_config_json_payload_is_sorted_json():
    assert service._home_config_json_payload({"b": 2, "a": 1}) == '{"a": 1, "b": 2}'


def test_parse_home_rule_phase_values_handles_modes_lists_and_csv():
    assert service._parse_home_rule_phase_values("1, 2.0, 2", "list") == '["1", "2"]'
    assert json.loads(service._parse_home_rule_phase_values(["3", "4.0"], "exact")) == [
        "3",
        "4",
    ]
    assert service._parse_home_rule_phase_values("1,2", "all") is None


def test_parse_home_rule_phase_values_requires_numeric_values_for_exact_modes():
    with pytest.raises(ValueError):
        service._parse_home_rule_phase_values("abc", "exact")

    with pytest.raises(ValueError):
        service._parse_home_rule_phase_values("", "list")
