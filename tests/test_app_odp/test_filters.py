import importlib
import sys
from types import SimpleNamespace

import pytest
from flask import Flask

MODULE_PATH = "app_odp.filters"

# Indice test
# 1. test_register_filters_registers_expected_filter_names:
#    verifica che register_filters registri db_json, db_list_display e db_date nell'ambiente Jinja.
# 2. test_db_json_returns_none_for_none_blank_and_invalid_json:
#    verifica i casi None, stringa vuota e JSON non parseabile.
# 3. test_db_json_returns_python_object_for_valid_json_string:
#    verifica il parsing di stringhe JSON valide verso oggetti Python.
# 4. test_db_json_returns_same_object_for_list_and_dict_inputs:
#    verifica che list e dict già Python vengano restituiti invariati.
# 5. test_db_json_returns_none_for_unsupported_scalar_inputs:
#    verifica che tipi scalari non supportati restituiscano None.
# 6. test_db_list_display_formats_scalar_lists_from_json_or_python:
#    verifica la resa testuale di liste di scalari sia da JSON che da lista Python.
# 7. test_db_list_display_returns_empty_for_non_list_or_nested_values:
#    verifica il ritorno stringa vuota per dict, liste annidate e valori non supportati.
# 8. test_db_date_returns_empty_for_none_or_blank:
#    verifica che db_date restituisca stringa vuota con input None o vuoto.
# 9. test_db_date_formats_valid_string_input:
#    verifica la formattazione di una data stringa valida.
# 10. test_db_date_returns_empty_for_invalid_string_input:
#     verifica il fallback a stringa vuota con stringa non coerente col formato atteso.
# 11. test_db_date_formats_json_list_with_single_string_date:
#     verifica che una lista JSON con una sola data venga prima ridotta da db_list_display e poi formattata.
# 12. test_db_date_returns_empty_for_python_date_and_datetime_objects_current_behavior:
#     verifica il comportamento attuale per date/datetime Python, che oggi producono stringa vuota.
# 13. test_db_date_returns_empty_for_json_list_with_multiple_dates:
#     verifica che più date in lista generino stringa vuota perché la stringa "a, b" non è parseabile.


@pytest.fixture()
def mod(monkeypatch):
    monkeypatch.setitem(
        sys.modules,
        "icecream",
        SimpleNamespace(ic=lambda *args, **kwargs: None),
    )
    sys.modules.pop(MODULE_PATH, None)
    return importlib.import_module(MODULE_PATH)


@pytest.fixture()
def app(mod):
    app = Flask(__name__)
    mod.register_filters(app)
    return app


@pytest.fixture()
def filters(app):
    return app.jinja_env.filters


def test_register_filters_registers_expected_filter_names(filters):
    assert "db_json" in filters
    assert "db_list_display" in filters
    assert "db_date" in filters


@pytest.mark.parametrize("value", [None, "", "   ", "not-json", "["])
def test_db_json_returns_none_for_none_blank_and_invalid_json(filters, value):
    assert filters["db_json"](value) is None


def test_db_json_returns_python_object_for_valid_json_string(filters):
    assert filters["db_json"]('["10", "20"]') == ["10", "20"]
    assert filters["db_json"]('{"key": 10}') == {"key": 10}


def test_db_json_returns_same_object_for_list_and_dict_inputs(filters):
    lst = ["10", 20]
    dct = {"key": "value"}

    assert filters["db_json"](lst) is lst
    assert filters["db_json"](dct) is dct


@pytest.mark.parametrize("value", [5, 2.5, True, object()])
def test_db_json_returns_none_for_unsupported_scalar_inputs(filters, value):
    assert filters["db_json"](value) is None


def test_db_list_display_formats_scalar_lists_from_json_or_python(filters):
    db_list_display = filters["db_list_display"]

    assert db_list_display('["10", "20"]') == "10, 20"
    assert db_list_display(["10", 20, 2.5]) == "10, 20, 2.5"
    assert db_list_display("[1]") == "1"


@pytest.mark.parametrize(
    "value",
    [
        None,
        "",
        "{}",
        {"a": 1},
        '[{"a": 1}]',
        [["10"]],
        '["10", ["20"]]',
    ],
)
def test_db_list_display_returns_empty_for_non_list_or_nested_values(filters, value):
    assert filters["db_list_display"](value) == ""


@pytest.mark.parametrize("value", [None, ""])
def test_db_date_returns_empty_for_none_or_blank(filters, value):
    assert filters["db_date"](value, "%Y-%m-%d", "%d/%m/%Y") == ""


def test_db_date_returns_empty_for_plain_string_date_current_behavior(filters):
    assert filters["db_date"]("2026-03-20", "%Y-%m-%d", "%d/%m/%Y") == ""


@pytest.mark.parametrize("value", ["20-03-2026", "bad", "2026/03/20"])
def test_db_date_returns_empty_for_invalid_string_input(filters, value):
    assert filters["db_date"](value, "%Y-%m-%d", "%d/%m/%Y") == ""


def test_db_date_formats_json_list_with_single_string_date(filters):
    assert filters["db_date"]('["2026-03-20"]', "%Y-%m-%d", "%d/%m/%Y") == "20/03/2026"


def test_db_date_returns_empty_for_python_date_and_datetime_objects_current_behavior(
    filters, mod
):
    assert filters["db_date"](mod.date(2026, 3, 20), "%Y-%m-%d", "%d/%m/%Y") == ""
    assert (
        filters["db_date"](
            mod.datetime(2026, 3, 20, 11, 30, 0),
            "%Y-%m-%d",
            "%d/%m/%Y",
        )
        == ""
    )


def test_db_date_returns_empty_for_json_list_with_multiple_dates(filters):
    assert (
        filters["db_date"](
            '["2026-03-20", "2026-03-21"]',
            "%Y-%m-%d",
            "%d/%m/%Y",
        )
        == ""
    )
