from datetime import date

import pytest

from app_odp.services.order_helpers import (
    _decimal_input_text,
    _extract_codes_from_cell,
    _parse_minuti_non_funzionamento,
    _parse_registration_date_input,
)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("", None),
        (None, None),
        ("2026-07-09", date(2026, 7, 9)),
        ("09/07/2026", date(2026, 7, 9)),
    ],
)
def test_parse_registration_date_input_accepts_blank_iso_and_italian_dates(
    value,
    expected,
):
    assert _parse_registration_date_input(value) == expected


def test_parse_registration_date_input_rejects_unknown_format():
    with pytest.raises(ValueError, match="Data registrazione non valida"):
        _parse_registration_date_input("07-09-2026")


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("", 0),
        (None, 0),
        ("15", 15),
        ("003", 3),
    ],
)
def test_parse_minuti_non_funzionamento_accepts_blank_and_integer_minutes(
    value,
    expected,
):
    assert _parse_minuti_non_funzionamento(value, "Minuti") == expected


@pytest.mark.parametrize("value", ["-1", "1.5", "abc"])
def test_parse_minuti_non_funzionamento_rejects_non_integer_values(value):
    with pytest.raises(ValueError, match="Minuti deve essere un numero intero >= 0"):
        _parse_minuti_non_funzionamento(value, "Minuti")


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, []),
        ("", []),
        ("10", ["10"]),
        ('["10", ["20", "10"], {"x": "30"}]', ["10", "20", "30"]),
        ({"a": ["10", {"b": "20"}], "c": "10"}, ["10", "20"]),
    ],
)
def test_extract_codes_from_cell_flattens_jsonish_values_and_deduplicates(
    value,
    expected,
):
    assert _extract_codes_from_cell(value) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("12,5", "12.5"),
        ("1.234,50", "1234.50"),
        ("1,234.50", "1234.50"),
        (" 12 345 ", "12345"),
    ],
)
def test_decimal_input_text_normalizes_decimal_separators(value, expected):
    assert _decimal_input_text(value) == expected

