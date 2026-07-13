import pytest

from app_odp.services.report_settimanale_service import (
    _is_unknown_phase,
    _phase_label_from_key,
    _phase_sort_key,
)


@pytest.mark.parametrize(
    ("phase_key", "expected"),
    [
        (("DOC", "1", ""), "-"),
        (("DOC", "1", "0"), "-"),
        (("DOC", "1", "2.0"), "2"),
        (("DOC", "1", "COLLAUDO"), "COLLAUDO"),
    ],
)
def test_phase_label_from_key_formats_blank_numeric_and_text_phases(phase_key, expected):
    assert _phase_label_from_key(phase_key) == expected


def test_phase_sort_key_orders_numeric_phases_before_text_phases():
    assert _phase_sort_key(("DOC", "1", "2")) == (0, "0002")
    assert _phase_sort_key(("DOC", "1", "COLLAUDO")) == (1, "COLLAUDO")


@pytest.mark.parametrize(
    ("phase_key", "expected"),
    [
        (None, True),
        (("DOC", "1", ""), True),
        (("DOC", "1", "0"), True),
        (("DOC", "1", "1"), False),
        (("DOC", "1", "COLLAUDO"), False),
    ],
)
def test_is_unknown_phase_detects_missing_or_zero_phase(phase_key, expected):
    assert _is_unknown_phase(phase_key) is expected

