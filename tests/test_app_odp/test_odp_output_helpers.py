import pytest

from app_odp.odp_output import (
    _load_distinta_base,
    _normal_phase_suffix,
    _phase_ref_for_export,
)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ([{"CodArt": "A"}, "skip"], [{"CodArt": "A"}]),
        ('[{"CodArt": "A"}, "skip"]', [{"CodArt": "A"}]),
        ("", []),
        ("not-json", []),
        ('{"CodArt": "A"}', []),
    ],
)
def test_load_distinta_base_accepts_only_list_of_dicts(value, expected):
    assert _load_distinta_base(value) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("", ""),
        (None, ""),
        ("1", "1"),
        ("1.0", "1"),
        ("fase-a", "fase-a"),
    ],
)
def test_normal_phase_suffix_normalizes_numeric_suffixes(value, expected):
    assert _normal_phase_suffix(value) == expected


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ({"phase_sequence": ["1"], "fase": "1"}, "2026.1.252.5,00"),
        ({"phase_sequence": ["1", "2"], "fase": "2"}, "2026.1.252.5,00.2,00"),
        ({"totale_fasi": "2", "fase": ""}, "2026.1.252.5,00"),
    ],
)
def test_phase_ref_for_export_adds_phase_only_for_multiphase_payload(payload, expected):
    assert _phase_ref_for_export("2026.1.252.5,00", payload) == expected

