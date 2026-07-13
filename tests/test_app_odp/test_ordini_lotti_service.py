from types import SimpleNamespace

import pytest

from app_odp.services.ordini_lotti_service import _fase_attiva_int


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("2", 2),
        ("2.0", 2),
        ("", None),
        (None, None),
        ("bad", None),
    ],
)
def test_fase_attiva_int_parses_numeric_phase_or_none(value, expected):
    assert _fase_attiva_int(SimpleNamespace(FaseAttiva=value)) == expected
