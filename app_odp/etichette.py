#!/usr/bin/env python3

# -*- coding: utf-8 -*-
import qrcode
import qrcode.constants
from PIL import Image, ImageDraw, ImageFont
import numpy as np
import os

try:
    from icecream import ic
except:
    pass


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


def pattern_grid(hash_hex: str, grid: int, scale: int) -> Image.Image:
    bits_needed = grid * grid
    bits = bin(int(hash_hex, 16))[2:]
    if len(bits) < bits_needed:
        reps = (bits_needed + len(bits) - 1) // len(bits)
        bits = (bits * reps)[:bits_needed]
    else:
        bits = bits[:bits_needed]
    arr = np.array([255 if b == "1" else 0 for b in bits], dtype=np.uint8).reshape(
        (grid, grid)
    )
    img_small = Image.fromarray(arr, mode="L")
    return img_small.resize((grid * scale, grid * scale), resample=Image.NEAREST)


def gcd(a, b):
    while b:
        a, b = b, a % b
    return a


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


def compose_label_normal(
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

    font_med = load_font(font_path, size=max(30, int(h_px * 0.065)))
    font_small = load_font(font_path, size=max(25, int(h_px * 0.058)))
    font_med_size = max(30, int(h_px * 0.065))
    font_small_size = max(25, int(h_px * 0.058))
    x = w_px - 3
    y = h_px - 3
    d.rectangle([(2, 2), (x, y)], outline=0, width=1)

    margin_x = 8
    text_margin = 20
    margin_y = 9
    middle_x = int((x / 2) + margin_x)
    sesti_x = int((x / 6) + margin_x)
    terzo_y = int((y / 3) + margin_y)
    middle_y = int((y / 2) + margin_y)
    d.text((margin_x, margin_y), "Codice del componente", font=font_med, fill=0)
    d.multiline_text(
        (margin_x + text_margin, margin_y + font_small_size * 2),
        codice,
        font=font_small,
        fill=0,
    )
    d.text((margin_x, terzo_y), "Descrizione", font=font_med, fill=0)
    stringa_invio_automatico = invio_automatico(
        draw=d,
        text=descrizione,
        font=font_small,
        max_width=(w_px - (terzo_y * 3) + font_small_size * 2),
    )
    d.multiline_text(
        (margin_x + text_margin, terzo_y + font_small_size * 2),
        stringa_invio_automatico,
        font=font_small,
        fill=0,
    )
    d.text((middle_x, margin_y), "Lotto", font=font_med, fill=0)
    d.multiline_text(
        (middle_x + text_margin, margin_y + font_small_size * 2),
        lotto,
        font=font_small,
        fill=0,
    )
    d.text((margin_x, terzo_y * 2), "Quantità", font=font_med, fill=0)
    d.multiline_text(
        (margin_x + text_margin, terzo_y * 2 + font_small_size * 2),
        qty,
        font=font_small,
        fill=0,
    )

    qr = make_qr(lotto, 250)
    img.paste(qr, (sesti_x * 3, terzo_y))

    return img


def gen_etichette(
    codice: str,
    descrizione: str,
    lotto: str,
    label_dimension: list[float],
    dpi: int,
    font_path: str,
) -> Image.Image:
    codice = "BE02-004-0200"
    descrizione = "Piastra di supporto cuscinetti con boccola"
    lotto = "123123"
    qty = "50"
    label_dimension = [80.0, 50.0]
    dpi = 250
    font_path = r"C:\Windows\Fonts\arial.ttf"
    image_normal = compose_label_normal(
        codice, descrizione, lotto, qty, label_dimension, dpi, font_path
    )
    image_normal.show()
    return
