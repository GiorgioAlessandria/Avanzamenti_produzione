from decimal import Decimal

import pytest

from sync import sync_input


def test_safe_token_keeps_safe_chars_and_replaces_other_runs():
    assert sync_input._safe_token(" DOC 1/A ") == "DOC-1-A"
    assert sync_input._safe_token("", default="fallback") == "fallback"
    assert sync_input._safe_token("///", default="fallback") == "fallback"


def test_build_sync_operation_group_id_uses_timestamp_and_safe_tokens():
    assert sync_input._build_sync_operation_group_id(
        id_documento=" DOC/1 ",
        id_riga=" 10 ",
        action=" nuovo ordine ",
        when_iso="2026-07-09T14:30:10",
    ) == "20260709143010_DOC-1_10_nuovo-ordine"


def test_pk_key_strips_document_and_row_ids():
    assert sync_input._pk_key(" DOC ", " 10 ") == ("DOC", "10")


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (" pz. ", "PZ"),
        (" n ", "N"),
        ("kg", "KG"),
        ("", ""),
    ],
)
def test_normalize_lotto_udm(value, expected):
    assert sync_input._normalize_lotto_udm(value) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("PZ.", True),
        ("N", True),
        ("KG", False),
    ],
)
def test_lotto_requires_integer_udm(value, expected):
    assert sync_input._lotto_requires_integer_udm(value) is expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("1.234,50", Decimal("1234.50")),
        ("1,25", Decimal("1.25")),
        ("", None),
        ("bad", None),
    ],
)
def test_parse_lotto_giacenza_accepts_erp_decimal_formats(value, expected):
    assert sync_input._parse_lotto_giacenza(value) == expected


def test_decimal_lotto_text_removes_trailing_zeroes():
    assert sync_input._decimal_lotto_text(Decimal("2.500")) == "2.5"
    assert sync_input._decimal_lotto_text(Decimal("3.000")) == "3"


@pytest.mark.parametrize(
    ("value", "udm", "expected"),
    [
        ("2,5", "KG", "2.5"),
        ("2", "PZ", "2"),
        ("2,5", "PZ", None),
        ("0", "KG", None),
        ("bad", "KG", None),
    ],
)
def test_normalizza_giacenza_lotto_rejects_invalid_or_non_positive_quantities(
    value, udm, expected
):
    assert sync_input._normalizza_giacenza_lotto(value, udm) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("Terminata", True),
        ("terminato", True),
        ("Chiusa", False),
        ("", False),
    ],
)
def test_is_stato_ordine_terminato_matches_erp_terminated_states(value, expected):
    assert sync_input._is_stato_ordine_terminato(value) is expected
