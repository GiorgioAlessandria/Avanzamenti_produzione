from app_odp.odp_output import txt_generator


def test_multiphase_first_order_row_shifts_703_progressives_only():
    payload = {
        "registrazione_data": "15/07/2026",
        "id_documento": "47449",
        "id_riga": "1",
        "rif_registraz": "2026.1.410",
        "fase": "7",
        "phase_sequence": ["7", "8"],
        "num_progr_riga": "1",
        "cod_art": "PRODOTTO",
        "variante": "",
        "lotto_prodotto": "LOT-PF",
        "magazzino": "0",
        "risorsa": "ASSEMBLAGGIO",
        "salda_riga": 1,
        "quantita_ok": "1",
        "quantita_ko": "0",
        "tempo_funzionamento": "0.05",
        "lotti": [
            {
                "CodArt": "COMPONENTE-1",
                "VarianteArt": "",
                "RifLottoAlfa": "LOT-COMPONENTE",
                "Quantita": "1",
                "CodMag": "0",
            }
        ],
        "distinta_base": [
            {
                "IdRigacomponente": 2,
                "ProgressivoRiga": 2,
                "NumFase": 7,
                "CodArt": "COMPONENTE-1",
                "VarianteArt": "",
                "Quantita": "1",
            },
            {
                "IdRigacomponente": 3,
                "ProgressivoRiga": 3,
                "NumFase": 7,
                "CodArt": "COMPONENTE-2",
                "VarianteArt": "",
                "Quantita": "1",
            },
        ],
    }

    lines = txt_generator([{"payload": payload}])
    rows = [line.split(";") for line in lines]
    phase_refs = [row[5] for row in rows if row[4] == "709"]
    component_refs = [row[5] for row in rows if row[4] == "703"]
    phase_hours = [row[17] for row in rows if row[4] == "709"]

    assert len(phase_refs) == 1
    assert phase_refs[0].endswith(".7,00")
    assert phase_hours == ["0.05"]
    assert component_refs == [
        "2026.1.410.1,00.1,00",
        "2026.1.410.1,00.2,00",
    ]

    payload["tempo_avanzamento_ore"] = "0.17"
    forced_rows = [
        line.split(";")
        for line in txt_generator([{"payload": payload}])
    ]

    assert [row[17] for row in forced_rows if row[4] == "709"] == ["0.17"]
    assert [row[17] for row in forced_rows if row[4] == "703"] == ["0.05", "0.05"]

    payload["num_progr_riga"] = "2"
    payload["distinta_base"][0]["ProgressivoRiga"] = 80
    payload["distinta_base"][1]["ProgressivoRiga"] = 95
    second_machine_rows = [
        line.split(";")
        for line in txt_generator([{"payload": payload}])
    ]

    assert [
        row[5]
        for row in second_machine_rows
        if row[4] == "703"
    ] == [
        "2026.1.410.2,00.80,00",
        "2026.1.410.2,00.95,00",
    ]


def test_generated_lot_omits_all_703_progressives_for_singlephase():
    payload = {
        "registrazione_data": "31/07/2026",
        "id_documento": "48273",
        "id_riga": "1",
        "rif_registraz": "2026.1.446.1,00",
        "fase": "1",
        "phase_sequence": ["1"],
        "num_progr_riga": "",
        "cod_art": "PRODOTTO",
        "variante": "",
        "lotto_prodotto": "20260731",
        "magazzino": "0",
        "risorsa": "ASSEMBLAGGIO",
        "salda_riga": 1,
        "quantita_ok": "2",
        "quantita_ko": "0",
        "tempo_funzionamento": "0",
        "lotti": [
            {
                "CodArt": "BE03-001-0200",
                "VarianteArt": "",
                "RifLottoAlfa": "20260729",
                "Quantita": "2",
                "CodMag": "0",
            }
        ],
        "distinta_base": [
            {
                "ProgressivoRiga": 2,
                "NumFase": 1,
                "CodArt": "BE03-001-0101",
                "VarianteArt": "",
                "Quantita": "2",
            },
            {
                "ProgressivoRiga": 6,
                "NumFase": 1,
                "CodArt": "BE03-001-0200",
                "VarianteArt": "",
                "Quantita": "2",
            }
        ],
    }

    rows = [
        line.split(";")
        for line in txt_generator([{"payload": payload}])
    ]
    component_rows = [row for row in rows if row[4] == "703"]

    assert [row[5] for row in component_rows] == [
        "2026.1.446.1,00",
        "2026.1.446.1,00",
    ]
    assert [row[12] for row in component_rows] == ["", "20260729"]
