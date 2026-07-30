from app_odp.odp_output import txt_generator


def test_703_component_refs_keep_distinta_progressive_without_changing_709_phase():
    payload = {
        "registrazione_data": "15/07/2026",
        "id_documento": "47449",
        "id_riga": "1",
        "rif_registraz": "2026.1.410.1,00",
        "fase": "7",
        "phase_sequence": ["7", "8"],
        "num_progr_riga": "",
        "cod_art": "PRODOTTO",
        "variante": "",
        "lotto_prodotto": "",
        "magazzino": "0",
        "risorsa": "ASSEMBLAGGIO",
        "salda_riga": 1,
        "quantita_ok": "1",
        "quantita_ko": "0",
        "tempo_funzionamento": "0.05",
        "lotti": [],
        "distinta_base": [
            {
                "IdRigacomponente": 2,
                "ProgressivoRiga": 5,
                "NumFase": 7,
                "CodArt": "COMPONENTE-1",
                "VarianteArt": "",
                "Quantita": "1",
            },
            {
                "IdRigacomponente": 3,
                "ProgressivoRiga": 6,
                "NumFase": 7,
                "CodArt": "COMPONENTE-2",
                "VarianteArt": "",
                "Quantita": "1",
            },
        ],
    }

    lines = txt_generator([{"payload": payload}], include_time_line=True)
    rows = [line.split(";") for line in lines]
    phase_refs = [row[5] for row in rows if row[4] == "709"]
    component_refs = [row[5] for row in rows if row[4] == "703"]
    phase_hours = [row[17] for row in rows if row[4] == "709"]

    assert len(phase_refs) == 1
    assert phase_refs[0].endswith(".7,00")
    assert phase_hours == ["0.05"]
    assert component_refs == [
        "2026.1.410.1,00.5,00",
        "2026.1.410.1,00.6,00",
    ]

    payload["tempo_avanzamento_ore"] = "0.17"
    forced_rows = [
        line.split(";")
        for line in txt_generator([{"payload": payload}], include_time_line=True)
    ]

    assert [row[17] for row in forced_rows if row[4] == "709"] == ["0.17"]
    assert [row[17] for row in forced_rows if row[4] == "703"] == ["0.05", "0.05"]
