from pathlib import Path


def test_get_filter_preserves_operator_tab_token():
    template = (
        Path(__file__).parents[2]
        / "app_odp/templates/manutenzioni/dashboard.j2"
    ).read_text(encoding="utf-8")
    form_start = template.index('id="manutenzioni-filter-form"')
    form_end = template.index("</form>", form_start)

    assert 'name="tab_session"' in template[form_start:form_end]
