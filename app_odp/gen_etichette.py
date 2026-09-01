#!/usr/bin/env python3

# -*- coding: utf-8 -*-
import qrcode
import qrcode.constants
from PIL import Image, ImageDraw, ImageFont
import os


def mm_to_px(mm: float, dpi: int) -> int:
    return int(round(float(mm) / 25.4 * int(dpi)))


# -------------------------- font --------------------------
def load_font(preferred: str, size: int) -> ImageFont.FreeTypeFont:
    if preferred and os.path.isfile(preferred):
        try:
            return ImageFont.truetype(preferred, size=size)
        except Exception:
            pass
    return ImageFont.load_default()


# -------------------------- QR --------------------------


def make_qr(data: str, size_px: int) -> Image.Image:
    qr = qrcode.QRCode(
        version=4,
        error_correction=qrcode.constants.ERROR_CORRECT_H,  # High error correction
        box_size=5,
        border=4,
    )
    qr.add_data(data)
    qr.make(fit=True)
    qr_img = qr.make_image(fill_color="black", back_color="white").convert("L")
    qr_img = qr_img.resize((size_px, size_px), resample=Image.NEAREST)
    return qr_img


# -------------------------- layout --------------------------


def invio_automatico(draw, text: str, font, max_width: int) -> str:
    words = str(text).split()
    if not words:
        return ""
    lines = []
    current = words[0]
    for word in words[1:]:
        test_line = f"{current} {word}"
        if draw.textlength(test_line, font=font) <= max_width:
            current = test_line
        else:
            lines.append(current)
            current = word
    lines.append(current)
    return "\n".join(lines)


def gen_etichette(
    codice: str,
    descrizione: str,
    lotto: str,
    qty: str,
    label_dimensions: list[float],
    dpi,
    font_path,
) -> Image.Image:

    codice = str(codice)
    descrizione = str(descrizione)
    lotto = str(lotto)

    w_px = mm_to_px(label_dimensions[0], dpi)
    h_px = mm_to_px(label_dimensions[1], dpi)
    img = Image.new("L", (w_px, h_px), 255)
    d = ImageDraw.Draw(img)

    font_med = load_font(font_path, size=max(22, int(h_px * 0.09)))
    font_small = load_font(font_path, size=max(20, int(h_px * 0.08)))
    font_lotto_size = max(36, int(h_px * 0.18))
    font_lotto = load_font(font_path, size=font_lotto_size)
    padding = max(4, mm_to_px(1, dpi))
    gap = max(4, mm_to_px(1.5, dpi))
    line_step = max(24, int(h_px * 0.12))
    qr_size = h_px - (padding * 2)
    text_x = padding + qr_size + gap
    text_width = max(1, w_px - text_x - padding)

    d.rectangle([(1, 1), (w_px - 3, h_px - 3)], outline=0, width=1)

    qr = make_qr(lotto, qr_size)
    img.paste(qr, (padding, padding))

    text_y = padding
    d.text((text_x, text_y), f"Codice: {codice}", font=font_med, fill=0)
    text_y += line_step
    d.text((text_x, text_y), "Descrizione:", font=font_med, fill=0)
    text_y += line_step
    description_lines = invio_automatico(
        draw=d,
        text=descrizione,
        font=font_small,
        max_width=text_width,
    ).splitlines()
    if len(description_lines) > 2:
        description_lines = description_lines[:2]
        description_lines[-1] = description_lines[-1].rstrip(".") + "..."
    d.multiline_text(
        (text_x, text_y),
        "\n".join(description_lines),
        font=font_small,
        fill=0,
    )
    text_y += line_step * max(1, len(description_lines))
    d.text((text_x, text_y), f"Quantità: {qty}", font=font_med, fill=0)
    lotto_y = h_px - padding - font_lotto_size
    d.text((text_x, lotto_y), f"LOTTO {lotto}", font=font_lotto, fill=0)
    return img
