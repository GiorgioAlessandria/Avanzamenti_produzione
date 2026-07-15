from app_odp.services.erp_export_service import _build_export_txt_path


def test_build_export_txt_path_uses_only_existing_output_dir(tmp_path):
    path = _build_export_txt_path(
        prefix="AVPB",
        suffix="ricreato_1",
        output_dir=tmp_path,
    )
    assert path.parent == tmp_path.resolve()
    assert path.name.startswith("AVPB_ricreato_1_")

    try:
        _build_export_txt_path(output_dir=tmp_path / "inesistente")
    except ValueError as exc:
        assert "non esiste" in str(exc)
