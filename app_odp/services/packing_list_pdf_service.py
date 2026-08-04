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
        title="Packing List",
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

    def titled_field_table(title_text, rows, widths):
        heading = Table(
            [[paragraph(title_text, table_header)]],
            colWidths=[sum(widths)],
            hAlign="LEFT",
        )
        heading.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, -1), header_background),
                    ("GRID", (0, 0), (-1, -1), 0.5, border),
                    ("LEFTPADDING", (0, 0), (-1, -1), 5),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                    ("TOPPADDING", (0, 0), (-1, -1), 4),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ]
            )
        )
        return [heading, field_table(rows, widths)]

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
    delivery = packing_list.delivery
    addresses = Table(
        [
            [
                titled_field_table(
                    "Consignee name",
                    [
                        ("Customer", cliente.nome),
                        ("Address", cliente.indirizzo),
                        ("Province", cliente.provincia),
                        ("Country", cliente.paese),
                    ],
                    [28 * mm, 61 * mm],
                ),
                "",
                titled_field_table(
                    "Delivery address",
                    [
                        ("Customer", delivery.nome),
                        ("Address", delivery.indirizzo),
                        ("Province", delivery.provincia),
                        ("Country", delivery.paese),
                    ],
                    [28 * mm, 61 * mm],
                ),
            ]
        ],
        colWidths=[89 * mm, 2 * mm, 89 * mm],
        hAlign="LEFT",
    )
    addresses.setStyle(
        TableStyle(
            [
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 0),
                ("RIGHTPADDING", (0, 0), (-1, -1), 0),
                ("TOPPADDING", (0, 0), (-1, -1), 0),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
            ]
        )
    )
    shipment_details = field_table(
        [
            ("Transport document", packing_list.transport_document),
            ("Invoice number", packing_list.invoice_number),
            ("Invoice date", packing_list.invoice_date.strftime("%d/%m/%Y")),
            ("Total Nr. of pallets", packing_list.total_pallets),
            ("Total net weight (Kg.)", _number(packing_list.total_net_weight)),
            ("Total gross weight (Kg.)", _number(packing_list.total_gross_weight)),
            ("Comments", packing_list.comments or "-"),
        ],
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
        addresses,
        Spacer(1, 3 * mm),
        shipment_details,
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
        canvas.drawString(14 * mm, 8 * mm, "Packing list")
        canvas.drawRightString(
            A4[0] - 14 * mm,
            8 * mm,
            f"Page {canvas.getPageNumber()}",
        )
        canvas.restoreState()

    document.build(story, onFirstPage=footer, onLaterPages=footer)
    stream.seek(0)
    return stream
