import pytest

from app_odp.ordine_ref import (
    format_erp_decimal_ref_part,
    format_ordine_ref_display,
    format_ordine_ref_export,
)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("", ""),
        (None, ""),
        ("5", "5,00"),
        ("5.0", "5,00"),
        ("5,0", "5,00"),
        ("5.005", "5,01"),
        ("ABC", "ABC"),
    ],
)
def test_format_erp_decimal_ref_part_normalizes_numeric_values(value, expected):
    assert format_erp_decimal_ref_part(value) == expected


def test_format_ordine_ref_display_uses_first_available_row_part():
    assert format_ordine_ref_display("2026.1.252", "", "5") == "2026.1.252 5,00"


@pytest.mark.parametrize(
    ("rif_registraz", "num_progr_riga", "id_riga", "expected"),
    [
        ("2026.1.252", "", "", "2026.1.252"),
        ("", "3", "", "3,00"),
        ("", "", "", ""),
    ],
)
def test_format_ordine_ref_display_handles_missing_parts(
    rif_registraz,
    num_progr_riga,
    id_riga,
    expected,
):
    assert format_ordine_ref_display(rif_registraz, num_progr_riga, id_riga) == expected


def test_format_ordine_ref_export_formats_decimal_row_and_phase():
    assert (
        format_ordine_ref_export("2026.1.252", num_progr_riga="5.5", fase="1")
        == "2026.1.252.5,50.1,00"
    )

