from functools import cache
from io import BytesIO
import json
import re

from openpyxl import Workbook
from openpyxl.cell import WriteOnlyCell
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
from openpyxl.styles import Font, PatternFill
from openpyxl.utils import get_column_letter

from app_odp.models import db, InputOdpLog
from app_odp.services import storico_ordini_service as storico


COMMON = [("matricola", "Matricola richiesta"), ("ordine", "Ordine"),
          ("id_documento", "Documento"), ("num_progr_riga", "Riga ERP"), ("id_riga", "ID riga")]
SHEETS = {
    "Riepilogo": COMMON + [("esito", "Esito ricerca"), ("articoli", "Articoli"),
        ("passaggi", "Numero eventi"), ("primo", "Primo evento (Roma)"), ("ultimo", "Ultimo evento (Roma)"), ("gruppi", "Gruppi")],
    "Passaggi": COMMON + [("data", "Data e ora (Roma)"), ("azione_label", "Evento"), ("fase", "Fase"),
        ("fase_successiva", "Fase successiva"), ("articolo", "Articolo"), ("utente", "Autore evento"),
        ("operatore_in_carico", "Operatore in carico"), ("risorsa", "Risorsa"), ("stato_pre", "Stato prima"),
        ("stato_post", "Stato dopo"), ("q_ok", "Quantità OK"), ("q_ko", "Quantità KO"),
        ("quantita_residua", "Quantità residua"), ("tempo_lavorazione_ore", "Lavorazione (ore)"),
        ("tempo_non_funzionamento_minuti", "Non funzionamento (min)"), ("causale", "Causale"), ("note", "Note"),
        ("descrizione", "Descrizione evento"), ("modalita", "Modalità"), ("group_uid", "Gruppo"),
        ("tecnico", "Registrazione tecnica"), ("orario_incoerente", "Orario incoerente"),
        ("operation_group_id", "ID operazione"), ("log_id", "ID log"), ("source", "Fonte"), ("dati", "Dati completi evento (JSON)")],
    "Avanzamenti": COMMON + [("data", "Data e ora (Roma)"), ("articolo", "Articolo"), ("descrizione", "Descrizione articolo"),
        ("fase", "Fase consuntivata"), ("fase_attiva", "Fase attiva"), ("risorsa", "Risorsa"),
        ("utente", "Operatore"), ("matricola_documento", "Matricola documento"), ("q_ok", "Quantità OK"), ("q_ko", "Quantità KO"),
        ("tempo_finale", "Lavorazione (ore)"), ("tempo_non_funzionamento_minuti", "Non funzionamento (min)"),
        ("chiusura_parziale", "Chiusura parziale"), ("note", "Note"), ("operation_group_id", "ID operazione"), ("log_id", "ID log")],
    "Lotti usati": COMMON + [("data", "Data e ora (Roma)"), ("articolo", "Articolo"), ("lotto", "Lotto / matricola"),
        ("quantita", "Quantità"), ("fase", "Fase"), ("esito", "Esito"), ("utente", "Operatore"), ("log_id", "ID log")],
    "Lotti generati": COMMON + [("data", "Data e ora (Roma)"), ("articolo", "Articolo"), ("lotto", "Lotto / matricola"),
        ("quantita", "Quantità"), ("fase", "Fase"), ("utente", "Operatore"), ("label", "Etichetta"), ("log_id", "ID log")],
}
NUMBERS = {"passaggi", "q_ok", "q_ko", "quantita", "quantita_residua", "tempo_lavorazione_ore", "tempo_finale", "tempo_non_funzionamento_minuti"}
RECORD_TYPES = {"Riepilogo": "Riepilogo ordine", "Passaggi": "Passaggio", "Avanzamenti": "Avanzamento",
                "Lotti usati": "Lotto usato", "Lotti generati": "Lotto generato", "Testi estesi": "Testo esteso"}


def parse_matricole(text):
    if not isinstance(text, str) or not text.strip():
        raise ValueError("Inserire almeno una matricola.")
    result, seen = [], set()
    for value in re.split(r"[\r\n;,\t]+", text):
        value = value.strip()
        if value and value.lower() not in seen:
            result.append(value)
            seen.add(value.lower())
    if not result:
        raise ValueError("Inserire almeno una matricola.")
    return result


def _date(value):
    parsed = storico._parse_dt(value)
    if parsed:
        return parsed.astimezone(storico.ROME_TZ).replace(tzinfo=None)
    return value or None


def build_matricole_excel(text):
    matricole = parse_matricole(text)
    book = Workbook(write_only=True)
    sheet = book.create_sheet("Storico matricole")
    sheet.freeze_panes = "G2"
    columns = {"tipo_record": "Tipo record"}
    for fields in SHEETS.values():
        for key, label in fields:
            columns.setdefault(key, label)
    columns["esito"] = "Esito"
    columns["descrizione"] = "Descrizione"
    columns["tempo_finale"] = "Lavorazione avanzamento (ore)"
    columns.update(riferimento="Riferimento testo esteso", parte="Parte testo esteso", testo="Testo esteso")
    headers = []
    for i, label in enumerate(columns.values(), 1):
        sheet.column_dimensions[get_column_letter(i)].width = max(16, min(len(label) + 3, 42))
        cell = WriteOnlyCell(sheet, label)
        cell.font = Font(bold=True, color="FFFFFF")
        cell.fill = PatternFill("solid", fgColor="17364D")
        headers.append(cell)
    sheet.append(headers)
    count = 1

    def append(name, values):
        nonlocal count
        cells, chunks = [], []
        for key in columns:
            value = RECORD_TYPES[name] if key == "tipo_record" else values.get(key)
            if key == "tipo_record" and values.get("esito") == "Non presente nello storico":
                value = "Matricola non trovata"
            if key in NUMBERS and value not in (None, ""):
                try:
                    number = storico._parse_qty_decimal(value)
                    if number.is_finite():
                        value = int(number) if number == number.to_integral_value() else float(number)
                except ValueError:
                    pass
            if isinstance(value, str):
                value = ILLEGAL_CHARACTERS_RE.sub(lambda m: f"\\x{ord(m[0]):02x}", value)
                if len(value) > 32767:
                    ref = f"{sheet.title}!{get_column_letter(len(cells) + 1)}{count + 1}"
                    for index, offset in enumerate(range(0, len(value), 32000), 1):
                        chunks.append({**{key: values.get(key) for key, _ in COMMON},
                                       "riferimento": ref, "parte": index, "testo": value[offset:offset + 32000]})
                    value = f"Testo completo nelle righe di tipo Testo esteso: {ref}"
            cell = WriteOnlyCell(sheet, value)
            if isinstance(value, str):
                cell.data_type = "s"
            if cell.is_date:
                cell.number_format = "DD/MM/YYYY HH:MM:SS"
            cells.append(cell)
        if count >= 1048576:
            raise ValueError("Lo storico supera il limite di righe di Excel. Dividere l'elenco di matricole.")
        sheet.append(cells)
        count += 1
        for chunk in chunks:
            append("Testi estesi", chunk)

    @cache
    def detail(doc, row):
        return storico.build_storico_ordini_detail({"id_documento": doc, "id_riga": row})

    for serial in matricole:
        keys = sorted(db.session.execute(storico._ordini_matricola(serial), bind_arguments={"mapper": InputOdpLog}).all())
        if not keys:
            append("Riepilogo", {"matricola": serial, "esito": "Non presente nello storico", "passaggi": 0})
        for doc, row in keys:
            data = detail(doc, row)
            header, events = data["header"], data["timeline"]
            common = dict(matricola=serial, ordine=header["title"], id_documento=doc,
                          id_riga=row, num_progr_riga=header.get("num_progr_riga", ""))
            append("Riepilogo", dict(common, esito="Trovata", passaggi=len(events),
                articoli=", ".join(sorted({e["articolo"] for e in events if e["articolo"]})),
                primo=_date(events[0]["event_at"]) if events else None,
                ultimo=_date(events[-1]["event_at"]) if events else None, gruppi=", ".join(header.get("groups", []))))
            for event in events:
                values = {**common, **event, "matricola": serial, "data": _date(event["event_at"]),
                    "risorsa": event["payload"].get("risorsa") or ", ".join(sorted({d["risorsa"] for d in event["documenti"] if d["risorsa"]})),
                    "dati": json.dumps(event, ensure_ascii=False)}
                append("Passaggi", values)
            for name, key in (("Avanzamenti", "input_logs"), ("Lotti usati", "lotti_usati"), ("Lotti generati", "lotti_generati")):
                for item in data[key]:
                    values = {**common, **item, "matricola": serial, "matricola_documento": item.get("matricola", ""),
                              "data": _date(item.get("closed_at")) or item.get("closed_at_display")}
                    append(name, values)
    sheet.auto_filter.ref = f"A1:{get_column_letter(len(columns))}{count}"
    output = BytesIO()
    book.save(output)
    output.seek(0)
    return output
