from app_odp.odp_output import txt_generator


def _base_payload():
    return {
        "registrazione_data": "09/07/2026",
        "id_documento": "47366",
        "id_riga": "1",
        "rif_registraz": "2026.1.403",
        "fase": "1",
        "num_progr_riga": "1",
        "cod_art": "BE06-004-0010",
        "variante": "",
        "lotto_prodotto": "LOT-PF",
        "magazzino": "0",
        "risorsa": "ASSEMBLAGGIO",
        "salda_riga": "0",
        "quantita_ok": "1",
        "quantita_ko": "0",
        "tempo_funzionamento": "4.58",
        "distinta_base": [
            {
                "CodArt": "BE06-002-0002",
                "VarianteArt": "",
                "Quantita": "1",
                "IdRigacomponente": "2",
            },
            {
                "CodArt": "BE10-004-0200",
                "VarianteArt": "",
                "Quantita": "1",
                "IdRigacomponente": "400",
            },
        ],
    }


def test_txt_generator_numbers_703_components_from_one_ignoring_source_row_ids():
    lines = txt_generator([{"payload": _base_payload()}], include_time_line=False)

    refs_703 = [line.split(";")[5] for line in lines if line.split(";")[4] == "703"]

    assert refs_703 == [
        "2026.1.403.1,00.1,00",
        "2026.1.403.1,00.2,00",
    ]
    assert all(".400,00" not in ref for ref in refs_703)