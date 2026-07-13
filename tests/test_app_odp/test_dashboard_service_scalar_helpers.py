import pytest

from app_odp.services.dashboard_service import _dashboard_text_filter, _home_config_int


@pytest.mark.parametrize(
    ("value", "default", "expected"),
    [
        ("12", 0, 12),
        (5, 0, 5),
        ("", 7, 7),
        (None, 9, 9),
        ("bad", -1, -1),
    ],
)
def test_home_config_int_returns_integer_or_default(value, default, expected):
    assert _home_config_int(value, default=default) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (" Reparto ", "reparto"),
        ("", ""),
        (None, ""),
    ],
)
def test_dashboard_text_filter_strips_and_lowercases(value, expected):
    assert _dashboard_text_filter(value) == expected

