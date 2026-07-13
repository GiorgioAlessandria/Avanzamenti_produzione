import pytest

from app_odp.services.report_settimanale_service import _phase_number_from_value


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        ("", None),
        ("0", None),
        ("-1", None),
        ("2", 2),
        ("2.0", 2),
        ("2,7", 2),
        ("fase-a", None),
    ],
)
def test_phase_number_from_value_returns_positive_integer_or_none(value, expected):
    assert _phase_number_from_value(value) == expected

