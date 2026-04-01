from pprint import pprint
from app_odp.odp_output import txt_generator

export_rows = [
    {
        "outbox": None,
        "payload": {
            "cod_art": "BE03-005-0300",
            "created_at": "2026-04-01T12:12:38+02:00",
            "created_by": "Margaria Stefano",
            "descrizione": "Assieme bracci 6",
            "distinta_base": '[{"CodArt": "BE03-004-0106", "DesArt": "Rondella perno centrale piccola", "Quantita": 15.0, "NumFase": 1.0, "TecniciUm": "N.", "GestioneLotto": "no"}, {"CodArt": "BE03-004-0301", "DesArt": "Perno centrale ø30", "Quantita": 15.0, "NumFase": 1.0, "TecniciUm": "N.", "GestioneLotto": "no"}, {"CodArt": "BE03-004-0302", "DesArt": "Boccola centrale M24x24", "Quantita": 15.0, "NumFase": 1.0, "TecniciUm": "N.", "GestioneLotto": "no"}, {"CodArt": "BE03-004-0304", "DesArt": "Boccola M24x40 condotta", "Quantita": 15.0, "NumFase": 1.0, "TecniciUm": "N.", "GestioneLotto": "no"}, {"CodArt": "BE03-004-0303", "DesArt": "Boccola M24x40", "Quantita": 15.0, "NumFase": 1.0, "TecniciUm": "N.", "GestioneLotto": "no"}, {"CodArt": "BE03-005-0400", "DesArt": "Braccio sinistro 6 pallinato", "Quantita": 15.0, "NumFase": 1.0, "TecniciUm": "N.", "GestioneLotto": "si"}, {"CodArt": "BE03-005-0500", "DesArt": "Braccio destro 6 pallinato", "Quantita": 15.0, "NumFase": 1.0, "TecniciUm": "N.", "GestioneLotto": "si"}, {"CodArt": "CB01-000-3013", "DesArt": "Vite TS EI M8x16 INOX - ISO 10642", "Quantita": 15.0, "NumFase": 1.0, "TecniciUm": "N.", "GestioneLotto": "no"}, {"CodArt": "CB08-000-0001", "DesArt": "Boccola di plastica flangiata A350FM Di16 De18 L17", "Quantita": 30.0, "NumFase": 1.0, "TecniciUm": "N.", "GestioneLotto": "no"}, {"CodArt": "CB08-000-0002", "DesArt": "Boccola di plastica flangiata A350FM Di16 De18 L12", "Quantita": 30.0, "NumFase": 1.0, "TecniciUm": "N.", "GestioneLotto": "no"}, {"CodArt": "CB08-000-0008", "DesArt": "Boccola di plastica flangiata A350FM Di15 De17 L17", "Quantita": 30.0, "NumFase": 1.0, "TecniciUm": "N.", "GestioneLotto": "no"}]',
            "fase": "1",
            "id_documento": "40559",
            "id_riga": "13",
            "kind": "consuntivo_fase",
            "lotti": [
                {
                    "CodArt": "BE03-005-0400",
                    "Esito": "ok",
                    "Quantita": "15",
                    "RifLottoAlfa": "20260129",
                },
                {
                    "CodArt": "BE03-005-0500",
                    "Esito": "ok",
                    "Quantita": "15",
                    "RifLottoAlfa": "20260320",
                },
            ],
            "lotto_prodotto": "20260401",
            "magazzino": "0",
            "note": "",
            "quantita_ko": "0",
            "quantita_ok": "15",
            "rif_registraz": "2026.1.79",
            "risorsa": "ASSEMBLAGGIO",
            "salda_riga": 1,
            "tempo_funzionamento": "0.02",
            "tipo_documento": "701",
        },
        "source_row": None,
    }
]


def main():
    result = txt_generator(export_rows)
    pprint(result)

    print("\n--- RIGHE TXT ---")

    print(result)


if __name__ == "__main__":
    main()


# 1:tipo record;10: tipo documento;20: registrazione data;30: registrazione numero;40: registrazione appendice digitata;80: tipo operazione avanzamento;90: riferimento ordine produzione;100: Codice articolo;140: Quantità principale;150: Quantità scarti prima scelta;160: Quantità scarti seconda scelta;290: Riga saldata;340: Riferimento lotto pf:codice alfanumerico;210: magazzino principale;300: Codice risorsa;310: Causale prestazione;322: ore lavorate risorsa 1
# TES;40559;01/04/2026 12:12;13;"";"";"";"";"";"";"";"";"";"";"";"";""
# RIG;40559;01/04/2026 12:12;13;"";701;2026.1.79.1,00;BE03-005-0300;15;0;0;1;20260401;0;ASSEMBLAGGIO;"";0.02
# RIG;40559;01/04/2026 12:12;13;"";701;2026.1.79.1,00;BE03-004-0106;15.0;"";"";1;"";0;ASSEMBLAGGIO;"";0.02
# RIG;40559;01/04/2026 12:12;13;"";701;2026.1.79.1,00;BE03-004-0301;15.0;"";"";1;"";0;ASSEMBLAGGIO;"";0.02
# RIG;40559;01/04/2026 12:12;13;"";701;2026.1.79.1,00;BE03-004-0302;15.0;"";"";1;"";0;ASSEMBLAGGIO;"";0.02
# RIG;40559;01/04/2026 12:12;13;"";701;2026.1.79.1,00;BE03-004-0304;15.0;"";"";1;"";0;ASSEMBLAGGIO;"";0.02
# RIG;40559;01/04/2026 12:12;13;"";701;2026.1.79.1,00;BE03-004-0303;15.0;"";"";1;"";0;ASSEMBLAGGIO;"";0.02
# RIG;40559;01/04/2026 12:12;13;"";701;2026.1.79.1,00;BE03-005-0400;15.0;"";"";1;20260129;0;ASSEMBLAGGIO;"";0.02
# RIG;40559;01/04/2026 12:12;13;"";701;2026.1.79.1,00;BE03-005-0500;15.0;"";"";1;20260320;0;ASSEMBLAGGIO;"";0.02
# RIG;40559;01/04/2026 12:12;13;"";701;2026.1.79.1,00;CB01-000-3013;15.0;"";"";1;"";0;ASSEMBLAGGIO;"";0.02
# RIG;40559;01/04/2026 12:12;13;"";701;2026.1.79.1,00;CB08-000-0001;30.0;"";"";1;"";0;ASSEMBLAGGIO;"";0.02
# RIG;40559;01/04/2026 12:12;13;"";701;2026.1.79.1,00;CB08-000-0002;30.0;"";"";1;"";0;ASSEMBLAGGIO;"";0.02
# RIG;40559;01/04/2026 12:12;13;"";701;2026.1.79.1,00;CB08-000-0008;30.0;"";"";1;"";0;ASSEMBLAGGIO;"";0.02
