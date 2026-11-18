from datetime import datetime
from io import BytesIO
from xml.sax.saxutils import escape

from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer


def build_manutenzione_pdf(evento, macchinario) -> BytesIO:
    stream = BytesIO()
    title = f"Report manutenzione n. {evento['id']}"
    document = SimpleDocTemplate(stream, pagesize=A4, title=title)
    styles = getSampleStyleSheet()
    reparto = " - ".join(
        filter(None, (macchinario["reparto_codice"], macchinario["reparto_descrizione"]))
    )
    fields = (
        ("Macchinario", f"{macchinario['codice']} - {macchinario['descrizione']}"),
        ("Reparto", reparto),
        ("Matricola", macchinario["matricola"]),
        ("Piano", evento["piano_codice"]),
        (
            "Data di esecuzione",
            datetime.fromisoformat(evento["data_esecuzione"]).strftime("%d/%m/%Y")
            if evento["data_esecuzione"]
            else None,
        ),
        ("Eseguita da", evento["eseguito_da_username"]),
        ("Registrata da", evento["registrato_da_username"]),
        ("Durata (minuti)", evento["durata_minuti"]),
        ("Esito", (evento["esito"] or "").replace("_", " ").capitalize()),
        ("Intervento straordinario collegato", evento["straordinaria_id"]),
        ("Descrizione prevista", evento["descrizione"]),
        ("Intervento eseguito", evento["descrizione_intervento"]),
        ("Note", evento["note"]),
    )
    story = [
        Paragraph(title, styles["Heading1"]),
        Paragraph(escape(evento["titolo"]), styles["Heading2"]),
        Spacer(1, 8),
    ]
    for label, value in fields:
        if value is None or value == "":
            continue
        body = escape(str(value)).replace("\n", "<br/>")
        story.append(Paragraph(f"<b>{label}</b><br/>{body}", styles["BodyText"]))
        story.append(Spacer(1, 6))

    document.build(story)
    stream.seek(0)
    return stream
