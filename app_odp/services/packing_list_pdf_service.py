from __future__ import annotations

from decimal import Decimal
from io import BytesIO
from pathlib import Path
from xml.sax.saxutils import escape

from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    Image,
    KeepTogether,
    LongTable,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)


COMPANY_LINES = (
    "BERNARDI s.r.l.",
    "Via Pasquale Bottero, 16",
    "CN - ITALY",
    "tel.: +39-0171-902352",
    "e-mail: info@bernardi.it",
)


def _font_names(font_path: str | Path | None) -> tuple[str, str]:
    if not font_path:
        return "Helvetica", "Helvetica-Bold"

    regular_path = Path(font_path)
    if not regular_path.is_file():
        return "Helvetica", "Helvetica-Bold"

    regular_name = "PackingListArial"
    bold_name = "PackingListArialBold"
    registered = set(pdfmetrics.getRegisteredFontNames())

    if regular_name not in registered:
        pdfmetrics.registerFont(TTFont(regular_name, str(regular_path)))

    bold_path = regular_path.with_name("arialbd.ttf")
    if bold_name not in registered:
        pdfmetrics.registerFont(
            TTFont(bold_name, str(bold_path if bold_path.is_file() else regular_path))
        )

    return regular_name, bold_name


def _text(value) -> str:
    normalized = "" if value is None else str(value).strip()
    return escape(normalized).replace("\n", "<br/>") or "-"


def _number(value) -> str:
    if value is None:
        return "-"
    normalized = format(Decimal(str(value)), "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    return normalized or "0"


def _logo(path: str | Path | None):
    if not path or not Path(path).is_file():
        return Spacer(1, 18 * mm)

    image = Image(str(path))
    scale = min((65 * mm) / image.drawWidth, (20 * mm) / image.drawHeight)
    image.drawWidth *= scale
    image.drawHeight *= scale
    return image


def build_packing_list_pdf(
    packing_list,
    *,
    logo_path: str | Path | None = None,
    font_path: str | Path | None = None,
) -> BytesIO:
    regular_font, bold_font = _font_names(font_path)
    stream = BytesIO()
    document = SimpleDocTemplate(
        stream,
        pagesize=A4,
        rightMargin=14 * mm,
        leftMargin=14 * mm,
        topMargin=12 * mm,
        bottomMargin=16 * mm,
        title=f"Packing List {packing_list.id}",
        author="BERNARDI s.r.l.",
    )

    base_styles = getSampleStyleSheet()
    body = ParagraphStyle(
        "PackingBody",
        parent=base_styles["BodyText"],
        fontName=regular_font,
        fontSize=8.5,
        leading=11,
        textColor=colors.HexColor("#202020"),
    )
    label = ParagraphStyle(
        "PackingLabel",
        parent=body,
        fontName=bold_font,
        fontSize=8,
    )
    quantity = ParagraphStyle(
        "PackingQuantity",
        parent=body,
        alignment=TA_RIGHT,
    )
    table_header = ParagraphStyle(
        "PackingTableHeader",
        parent=label,
        textColor=colors.white,
    )
    company_name = ParagraphStyle(
        "PackingCompanyName",
        parent=body,
        fontName=bold_font,
        fontSize=10,
        leading=12,
        alignment=TA_RIGHT,
    )
    company = ParagraphStyle(
        "PackingCompany",
        parent=body,
        fontSize=9,
        leading=11,
        alignment=TA_RIGHT,
    )
    title = ParagraphStyle(
        "PackingTitle",
        parent=base_styles["Title"],
        fontName=bold_font,
        fontSize=17,
        leading=20,
        alignment=TA_LEFT,
        textColor=colors.HexColor("#bc8425"),
        spaceAfter=5 * mm,
    )

    def paragraph(value, style=body):
        return Paragraph(_text(value), style)

    border = colors.HexColor("#b7b7b7")
    label_background = colors.HexColor("#f0f0f0")
    header_background = colors.HexColor("#5b5b5b")

    def field_table(rows, widths):
        table = Table(
            [[paragraph(name, label), paragraph(value)] for name, value in rows],
            colWidths=widths,
            hAlign="LEFT",
        )
        table.setStyle(
            TableStyle(
                [
                    ("GRID", (0, 0), (-1, -1), 0.5, border),
                    ("BACKGROUND", (0, 0), (0, -1), label_background),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("LEFTPADDING", (0, 0), (-1, -1), 5),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                    ("TOPPADDING", (0, 0), (-1, -1), 4),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ]
            )
        )
        return table

    company_flowables = [
        Paragraph(_text(COMPANY_LINES[0]), company_name),
        Paragraph("<br/>".join(_text(line) for line in COMPANY_LINES[1:]), company),
    ]
    header = Table(
        [[_logo(logo_path), company_flowables]],
        colWidths=[91 * mm, 91 * mm],
        hAlign="LEFT",
    )
    header.setStyle(
        TableStyle(
            [
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("ALIGN", (1, 0), (1, 0), "RIGHT"),
                ("LEFTPADDING", (0, 0), (-1, -1), 0),
                ("RIGHTPADDING", (0, 0), (-1, -1), 0),
                ("TOPPADDING", (0, 0), (-1, -1), 0),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
            ]
        )
    )

    cliente = packing_list.cliente
    customer_table = field_table(
        [
            ("Customer", cliente.nome),
            ("Address", cliente.indirizzo),
            ("Province", cliente.provincia),
            ("Country", cliente.paese),
        ],
        [28 * mm, 59 * mm],
    )
    details_table = field_table(
        [
            ("Transport document", packing_list.transport_document),
            ("Invoice number", packing_list.invoice_number),
            ("Invoice date", packing_list.invoice_date.strftime("%d/%m/%Y")),
            ("Total Nr. of pallets", packing_list.total_pallets),
            ("Total net weight (Kg.)", _number(packing_list.total_net_weight)),
            ("Total gross weight (Kg.)", _number(packing_list.total_gross_weight)),
        ],
        [39 * mm, 50 * mm],
    )
    overview = Table(
        [[customer_table, details_table]],
        colWidths=[89 * mm, 91 * mm],
        hAlign="LEFT",
    )
    overview.setStyle(
        TableStyle(
            [
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (0, 0), 0),
                ("RIGHTPADDING", (0, 0), (0, 0), 2 * mm),
                ("LEFTPADDING", (1, 0), (1, 0), 0),
                ("RIGHTPADDING", (1, 0), (1, 0), 0),
                ("TOPPADDING", (0, 0), (-1, -1), 0),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
            ]
        )
    )

    comments = field_table(
        [("Comments", packing_list.comments or "-")],
        [39 * mm, 141 * mm],
    )

    item_rows = [
        [
            paragraph("Code", table_header),
            paragraph("Description", table_header),
            paragraph("Quantity", table_header),
        ]
    ]
    item_rows.extend(
        [
            paragraph(row.codice),
            paragraph(row.descrizione),
            Paragraph(_text(_number(row.quantita)), quantity),
        ]
        for row in packing_list.righe
    )
    items = LongTable(
        item_rows,
        colWidths=[38 * mm, 112 * mm, 30 * mm],
        repeatRows=1,
        hAlign="LEFT",
    )
    items.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), header_background),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("GRID", (0, 0), (-1, -1), 0.5, border),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("ALIGN", (2, 1), (2, -1), "RIGHT"),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8f8f8")]),
                ("LEFTPADDING", (0, 0), (-1, -1), 5),
                ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ]
        )
    )

    final_fields = field_table(
        [
            ("Delivery terms", packing_list.delivery_terms),
            ("Forwarder", packing_list.forwarder),
        ],
        [39 * mm, 141 * mm],
    )

    story = [
        header,
        Spacer(1, 6 * mm),
        Paragraph("PACKING LIST", title),
        overview,
        Spacer(1, 3 * mm),
        comments,
        Spacer(1, 5 * mm),
        items,
        KeepTogether([Spacer(1, 5 * mm), final_fields]),
    ]

    def footer(canvas, _document):
        canvas.saveState()
        canvas.setStrokeColor(colors.HexColor("#d0d0d0"))
        canvas.line(14 * mm, 12 * mm, A4[0] - 14 * mm, 12 * mm)
        canvas.setFillColor(colors.HexColor("#666666"))
        canvas.setFont(regular_font, 7.5)
        canvas.drawString(14 * mm, 8 * mm, f"Packing list #{packing_list.id}")
        canvas.drawRightString(
            A4[0] - 14 * mm,
            8 * mm,
            f"Page {canvas.getPageNumber()}",
        )
        canvas.restoreState()

    document.build(story, onFirstPage=footer, onLaterPages=footer)
    stream.seek(0)
    return stream
