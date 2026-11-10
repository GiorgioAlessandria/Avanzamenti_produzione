from datetime import datetime
from io import BytesIO
from pathlib import Path
from xml.sax.saxutils import escape

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import Image, LongTable, Paragraph, SimpleDocTemplate, Spacer, TableStyle


def build_cqc_pdf(cliente, articolo, fresature, rientri):
    stream = BytesIO()
    pdf = SimpleDocTemplate(stream, pagesize=A4, leftMargin=16 * mm, rightMargin=16 * mm,
                            topMargin=17 * mm, bottomMargin=16 * mm, title="CQC", author="Bernardi s.r.l.")
    stili = getSampleStyleSheet()
    piccolo = ParagraphStyle("Controllo", parent=stili["BodyText"], fontSize=8, leading=11)
    blu = colors.HexColor("#163653")
    stili["Title"].textColor = blu

    def testo(valore, stile="BodyText"):
        return Paragraph(escape(str(valore or "-")).replace("\n", "<br/>"), stili[stile])

    logo_path = Path(__file__).resolve().parents[1] / "static" / "assets" / "img" / "logo_completo.jpg"
    intestazione = testo("BERNARDI s.r.l.", "Heading3")
    if logo_path.is_file():
        logo = Image(str(logo_path), hAlign="LEFT")
        logo.drawHeight *= (32 * mm) / logo.drawWidth
        logo.drawWidth = 32 * mm
        intestazione = logo

    riferimento_cliente = f"Ordine cliente {cliente.riferimento} | Riga {cliente.id_riga}"
    storia = [
        intestazione,
        testo("CQC - Certificato di controllo qualità", "Title"),
        testo(f"Cliente {cliente.cliente} | Ordine {cliente.riferimento} | Riga {cliente.id_riga}", "Heading3"),
        testo(f"Articolo cliente {articolo.codice_oss} - {articolo.descrizione}"),
        testo(f"Quantità ordine cliente: {cliente.quantita} pz"),
        Spacer(1, 3 * mm),
    ]

    for nome, righe in (
        ("Fresatura", [(c, f"ODP {ref}") for c, ref in fresature]),
        ("Rientro Gaser", [(c, f"Lotto {ref} | DDT {ddt or '-'}") for c, ref, ddt in rientri]),
    ):
        storia.append(testo(nome, "Heading2"))
        if nome == "Fresatura":
            storia.append(testo("I risultati di fresatura si riferiscono ai lotti ODP di origine."))
        for c, riferimento in righe:
            data = datetime.fromisoformat(c.data).strftime("%d/%m/%Y %H:%M")
            revisione = f" rev. {c.piano['revisione']}" if c.piano["revisione"] else ""
            storia.extend([
                testo(f"{riferimento} | {riferimento_cliente}", "Heading3"),
                testo(f"{data} | Operatore {c.operatore} | Piano {c.piano['codice']}{revisione}"),
                testo(f"Quantità lotto: {c.quantita} pz | Conformi: {c.quantita - c.ko} pz | KO: {c.ko} pz"),
                Spacer(1, 2 * mm),
            ])
            righe_tabella = [["Controllo", "Metodo / frequenza", "Verificati", "KO"]]
            for controllo, esito in zip(c.piano["controlli"], c.risultati):
                note = esito["note"]
                descrizione = controllo["descrizione"] + (f"\nEsito: {note}" if note else "")
                metodo = " / ".join(filter(None, [controllo["strumento"], controllo["frequenza"], controllo["valore"]]))
                righe_tabella.append([
                    Paragraph(escape(descrizione).replace("\n", "<br/>"), piccolo),
                    Paragraph(escape(metodo), piccolo), str(esito["verificati"]), str(esito["ko"]),
                ])
            tabella = LongTable(righe_tabella, colWidths=[95 * mm, 43 * mm, 24 * mm, 16 * mm], repeatRows=1)
            tabella.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), blu),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.HexColor("#edf3f8"), colors.white]),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("ALIGN", (2, 0), (-1, -1), "CENTER"),
                ("FONTSIZE", (0, 0), (-1, -1), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ]))
            storia.extend([tabella, Spacer(1, 3 * mm)])

    def pagina(canvas, documento):
        canvas.saveState()
        canvas.setStrokeColor(blu)
        canvas.line(16 * mm, 13 * mm, A4[0] - 16 * mm, 13 * mm)
        canvas.setFont("Helvetica", 8)
        canvas.drawString(16 * mm, 9 * mm, "Bernardi s.r.l. | CQC")
        canvas.drawRightString(A4[0] - 16 * mm, 9 * mm, f"Pagina {documento.page}")
        canvas.restoreState()

    pdf.build(storia, onFirstPage=pagina, onLaterPages=pagina)
    stream.seek(0)
    return stream
