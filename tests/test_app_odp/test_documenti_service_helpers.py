import pytest

from app_odp.services import documenti_service as service


@pytest.mark.parametrize("value", ["", None, "-", "X", "nan", "None", "NULL"])
def test_normalize_indice_modifica_for_pdf_clears_empty_sentinel_values(value):
    assert service._normalize_indice_modifica_for_pdf(value) == ""


def test_build_montaggio_pdf_key_adds_revision_only_when_present():
    assert service._build_montaggio_pdf_key(" ART-1 ", " A ") == "ART-1.A"
    assert service._build_montaggio_pdf_key(" ART-1 ", " X ") == "ART-1"
    assert service._build_montaggio_pdf_key("", "A") == ""


def test_build_metodo_pdf_key_applies_prefix_and_revision():
    assert service._build_metodo_pdf_key(" ART-1 ", " A ", prefisso="M_") == "M_ART-1.A"
    assert service._build_metodo_pdf_key(" ART-1 ", "", prefisso="M_") == "M_ART-1"
    assert service._build_metodo_pdf_key("", "A", prefisso="M_") == ""


def test_build_materiale_image_key_handles_variant_and_revision_parts():
    assert service._build_materiale_image_key(" ART-1 ", " B ", " C ") == "ART-1.B.C"
    assert service._build_materiale_image_key(" ART-1 ", " B ", " X ") == "ART-1.B"
    assert service._build_materiale_image_key(" ART-1 ", " X ", " C ") == "ART-1..C"
    assert service._build_materiale_image_key(" ART-1 ", "", "") == "ART-1"
    assert service._build_materiale_image_key("", "B", "C") == ""


def test_norm_articolo_search_value_strips_and_uppercases():
    assert service._norm_articolo_search_value(" art-1 ") == "ART-1"
    assert service._norm_articolo_search_value(None) == ""


@pytest.mark.parametrize("value", ["", None, "X", "-", "none", "NULL", "nan"])
def test_norm_articolo_revisione_clears_empty_sentinel_values(value):
    assert service._norm_articolo_revisione(value) == ""


def test_same_articolo_helpers_compare_normalized_values():
    assert service._same_articolo_variante(" a ", "A") is True
    assert service._same_articolo_variante(" a ", "B") is False
    assert service._same_articolo_revisione(" X ", "") is True
    assert service._same_articolo_revisione(" A ", "a") is True


@pytest.mark.parametrize(
    ("stato", "expected"),
    [
        ("Chiusa", False),
        ("Pianificata", True),
        ("Attiva", True),
        ("Sospesa", True),
        ("Annullata", False),
    ],
)
def test_is_articolo_search_state_matches_searchable_order_states(stato, expected):
    assert service._is_articolo_search_state(stato) is expected
