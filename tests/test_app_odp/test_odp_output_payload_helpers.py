import pytest

from app_odp.odp_output import (
    _bool_from_payload,
    _is_multiphase_payload,
    _jsonish_list,
    _phase_sequence_from_payload,
    _should_emit_product_line,
    _to_int,
)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (True, True),
        (False, False),
        ("si", True),
        ("on", True),
        ("no", False),
        ("off", False),
        ("altro", None),
        ("", None),
    ],
)
def test_bool_from_payload_parses_common_boolean_values(value, expected):
    assert _bool_from_payload(value) is expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("", None),
        (None, None),
        ("2", 2),
        ("2.7", 2),
        ("abc", None),
    ],
)
def test_to_int_returns_integer_or_none(value, expected):
    assert _to_int(value) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (["1", "", "2"], ["1", "2"]),
        ('["1", "", "2"]', ["1", "2"]),
        ('"fase"', ["fase"]),
        ("raw", ["raw"]),
        ("", []),
    ],
)
def test_jsonish_list_accepts_python_json_and_raw_values(value, expected):
    assert _jsonish_list(value) == expected


def test_phase_sequence_from_payload_prefers_explicit_lists_over_total_count():
    assert _phase_sequence_from_payload(
        {"phase_sequence": ["2", "4"], "totale_fasi": "3"}
    ) == ["2", "4"]


def test_phase_sequence_from_payload_builds_sequence_from_total_count():
    assert _phase_sequence_from_payload({"totale_fasi": "3"}) == ["1", "2", "3"]


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ({"emit_product_line": "1", "fase": "1", "phase_sequence": ["1", "2"]}, True),
        ({"emit_product_line": "0", "fase": "2", "phase_sequence": ["1", "2"]}, False),
        ({"is_last_phase": "1", "fase": "1", "phase_sequence": ["1", "2"]}, True),
        ({"fase": "1", "phase_sequence": ["1", "2"]}, False),
        ({"fase": "2", "phase_sequence": ["1", "2"]}, True),
        ({}, True),
    ],
)
def test_should_emit_product_line_uses_explicit_flag_then_last_phase(payload, expected):
    assert _should_emit_product_line(payload) is expected


def test_is_multiphase_payload_detects_multiple_phases():
    assert _is_multiphase_payload({"phase_sequence": ["1", "2"]}) is True
    assert _is_multiphase_payload({"phase_sequence": ["1"]}) is False

