from pathlib import Path


TEMPLATE = (
    Path(__file__).resolve().parents[2]
    / "app_odp"
    / "templates"
    / "partials"
    / "_home_montaggio.j2"
)


def test_machine_close_toast_filters_missing_components_and_lots():
    source = TEMPLATE.read_text(encoding="utf-8")

    assert 'id="toast-chiudi-m-escludi-componenti"' in source
    assert 'id="toast-chiudi-m-cerca-mancanti"' in source
    assert 'id="distinta-parziale-section-m" style="display: none;' in source
    assert "function filterMissingRows()" in source
    assert "function setMissingSectionVisible(visible)" in source
    assert "_lottiAll.filter(comp => !missing.has(keyForComponent(comp)))" in source


def test_suspended_machine_shows_missing_components_in_row_and_reactivation_toast():
    source = TEMPLATE.read_text(encoding="utf-8")
    rows_template = (
        TEMPLATE.parent / "_home_montaggio_m_rows_in_corso.j2"
    ).read_text(encoding="utf-8")

    assert "Componenti mancanti" in source
    assert 'id="toast-riatt-m-componenti-mancanti"' in source
    assert 'id="toast-riatt-m-mancanti-section"' in source
    assert "function renderMissingComponents()" in source
    assert 'data-componenti-mancanti="' in rows_template
    assert 'data-col="componenti-mancanti"' in rows_template
    assert "componenti_mancanti|length" in rows_template
