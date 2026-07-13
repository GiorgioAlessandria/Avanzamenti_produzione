from types import SimpleNamespace

import pytest

from app_odp.services.capacity_service import _capacity_float, _capacity_row_to_dict


@pytest.mark.parametrize(
    ("value", "default", "expected"),
    [
        ("", 7.5, 7.5),
        (None, 4.0, 4.0),
        ("2,5", 0.0, 2.5),
        ("3.25", 0.0, 3.25),
        ("bad", 1.0, 1.0),
        ("-2", 0.0, 0.0),
    ],
)
def test_capacity_float_parses_values_and_clamps_negative_numbers(value, default, expected):
    assert _capacity_float(value, default=default) == expected


def test_capacity_row_to_dict_serializes_calendar_row():
    row = SimpleNamespace(
        id=10,
        scope_type="operatore",
        scope_code="42",
        weekday=0,
        hours_capacity=7.5,
        active=True,
        updated_at=None,
        updated_by="mario",
    )

    assert _capacity_row_to_dict(row) == {
        "id": 10,
        "scope_type": "operatore",
        "scope_code": "42",
        "weekday": 0,
        "weekday_label": "Lunedì",
        "hours_capacity": 7.5,
        "active": True,
        "updated_at": "",
        "updated_by": "mario",
    }

