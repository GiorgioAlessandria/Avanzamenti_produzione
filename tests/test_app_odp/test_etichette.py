# INDICE TEST
# 1. Verifica la conversione da millimetri a pixel con arrotondamento corretto.
# 2. Verifica che load_font usi truetype quando il file font esiste.
# 3. Verifica che load_font usi il font di default se il file non esiste.
# 4. Verifica che load_font faccia fallback al font di default se truetype fallisce.
# 5. Verifica che make_qr generi un'immagine quadrata in scala di grigi della dimensione richiesta.
# 6. Verifica che pattern_grid ripeta i bit se l'hash è troppo corto e ridimensioni correttamente l'immagine.
# 7. Verifica che pattern_grid tronchi i bit se l'hash è più lungo del necessario.
# 8. Verifica il calcolo del massimo comune divisore con casi standard e bordo.
# 9. Verifica che invio_automatico restituisca stringa vuota per testo vuoto o solo spazi.
# 10. Verifica che invio_automatico mandi a capo correttamente in base alla larghezza massima.
# 11. Verifica che gen_etichette costruisca il layout, inserisca il QR e mostri l'immagine.

import importlib
import pytest

MODULE_PATH = (
    "app_odp.etichette"  # nel progetto puoi sostituire con "app_odp.etichette"
)


@pytest.fixture()
def mod():
    return importlib.import_module(MODULE_PATH)


import numpy as np
from types import SimpleNamespace


class FakeCanvas:
    def __init__(self, mode="L", size=(0, 0), color=255):
        self.mode = mode
        self.size = size
        self.color = color
        self.pasted = []
        self.show_called = False

    def paste(self, image, position):
        self.pasted.append((image, position))

    def show(self):
        self.show_called = True


class FakeDraw:
    def __init__(self):
        self.rectangles = []
        self.text_calls = []
        self.multiline_text_calls = []

    def rectangle(self, coords, outline=None, width=None):
        self.rectangles.append({"coords": coords, "outline": outline, "width": width})

    def text(self, position, text, font=None, fill=None):
        self.text_calls.append(
            {"position": position, "text": text, "font": font, "fill": fill}
        )

    def multiline_text(self, position, text, font=None, fill=None):
        self.multiline_text_calls.append(
            {"position": position, "text": text, "font": font, "fill": fill}
        )

    def textlength(self, text, font=None):
        return len(str(text))


@pytest.mark.parametrize(
    ("mm", "dpi", "expected"),
    [
        (25.4, 300, 300),
        (50, 300, 591),
        (0, 300, 0),
        (12.7, 200, 100),
    ],
)
def test_mm_to_px_converts_with_rounding(mod, mm, dpi, expected):
    assert mod.mm_to_px(mm, dpi) == expected


def test_load_font_uses_truetype_when_preferred_file_exists(mod, monkeypatch, tmp_path):
    font_path = tmp_path / "font.ttf"
    font_path.write_bytes(b"fake-font")
    sentinel_font = object()

    monkeypatch.setattr(mod.os.path, "isfile", lambda path: str(path) == str(font_path))
    monkeypatch.setattr(
        mod.ImageFont,
        "truetype",
        lambda preferred, size: (
            sentinel_font if preferred == str(font_path) and size == 18 else None
        ),
    )

    got = mod.load_font(str(font_path), 18)
    assert got is sentinel_font


def test_load_font_falls_back_to_default_when_file_is_missing(mod, monkeypatch):
    sentinel_default = object()

    monkeypatch.setattr(mod.os.path, "isfile", lambda path: False)
    monkeypatch.setattr(mod.ImageFont, "load_default", lambda: sentinel_default)

    got = mod.load_font("missing.ttf", 20)
    assert got is sentinel_default


def test_load_font_falls_back_to_default_when_truetype_raises(
    mod, monkeypatch, tmp_path
):
    font_path = tmp_path / "broken.ttf"
    font_path.write_bytes(b"broken-font")
    sentinel_default = object()

    monkeypatch.setattr(mod.os.path, "isfile", lambda path: str(path) == str(font_path))

    def raise_truetype(*args, **kwargs):
        raise OSError("font non valido")

    monkeypatch.setattr(mod.ImageFont, "truetype", raise_truetype)
    monkeypatch.setattr(mod.ImageFont, "load_default", lambda: sentinel_default)

    got = mod.load_font(str(font_path), 22)
    assert got is sentinel_default


def test_make_qr_returns_square_grayscale_image_with_requested_size(mod):
    qr = mod.make_qr("LOTTO-001", 120)

    assert qr.size == (120, 120)
    assert qr.mode == "L"


def test_pattern_grid_repeats_bits_when_hash_is_short(mod):
    img = mod.pattern_grid("f", grid=4, scale=3)
    arr = np.array(img)

    assert img.size == (12, 12)
    assert img.mode == "L"
    assert set(np.unique(arr)).issubset({0, 255})


def test_pattern_grid_truncates_bits_when_hash_is_long(mod):
    img = mod.pattern_grid("abcdef1234567890", grid=3, scale=2)
    arr = np.array(img)

    assert img.size == (6, 6)
    assert img.mode == "L"
    assert set(np.unique(arr)).issubset({0, 255})


@pytest.mark.parametrize(
    ("a", "b", "expected"),
    [
        (54, 24, 6),
        (24, 54, 6),
        (10, 0, 10),
        (0, 10, 10),
        (7, 7, 7),
    ],
)
def test_gcd_handles_standard_and_edge_cases(mod, a, b, expected):
    assert mod.gcd(a, b) == expected


@pytest.mark.parametrize("text", ["", "   "])
def test_invio_automatico_returns_empty_string_for_blank_text(mod, text):
    draw = FakeDraw()

    assert mod.invio_automatico(draw=draw, text=text, font=None, max_width=10) == ""


def test_invio_automatico_converts_none_to_string_none(mod):
    draw = FakeDraw()

    assert mod.invio_automatico(draw=draw, text=None, font=None, max_width=10) == "None"


def test_invio_automatico_wraps_lines_based_on_max_width(mod):
    draw = FakeDraw()

    result = mod.invio_automatico(
        draw=draw,
        text="uno due tre quattro",
        font=None,
        max_width=7,
    )

    assert result == "uno due\ntre\nquattro"


def test_gen_etichette_builds_layout_pastes_qr_and_shows_image(mod, monkeypatch):
    fake_canvas = FakeCanvas()
    fake_draw = FakeDraw()
    fake_qr = SimpleNamespace(name="fake-qr")
    load_font_calls = []
    invio_calls = []

    def fake_new(mode, size, color):
        fake_canvas.mode = mode
        fake_canvas.size = size
        fake_canvas.color = color
        return fake_canvas

    def fake_draw_factory(img):
        assert img is fake_canvas
        return fake_draw

    def fake_load_font(font_path, size):
        load_font_calls.append((font_path, size))
        return f"font-{size}"

    def fake_invio(draw, text, font, max_width):
        invio_calls.append(
            {
                "draw": draw,
                "text": text,
                "font": font,
                "max_width": max_width,
            }
        )
        return "descrizione\nspezzata"

    monkeypatch.setattr(mod.Image, "new", fake_new)
    monkeypatch.setattr(mod.ImageDraw, "Draw", fake_draw_factory)
    monkeypatch.setattr(mod, "load_font", fake_load_font)
    monkeypatch.setattr(mod, "make_qr", lambda data, size_px: fake_qr)
    monkeypatch.setattr(mod, "invio_automatico", fake_invio)

    result = mod.gen_etichette(
        codice=123,
        descrizione="descrizione molto lunga di prova",
        lotto=456,
        qty=7,
        label_dimensions=[50, 80],
        dpi=300,
        font_path="fake-font.ttf",
    )

    w_px = mod.mm_to_px(50, 300)
    h_px = mod.mm_to_px(80, 300)
    x = w_px - 3
    y = h_px - 3
    sesti_x = int((x / 6) + 8)
    terzo_y = int((y / 3) + 9)
    expected_paste_position = (sesti_x * 3, terzo_y)

    assert result is None
    assert fake_canvas.mode == "L"
    assert fake_canvas.size == (w_px, h_px)
    assert fake_canvas.color == 255
    assert fake_canvas.show_called is True
    assert fake_canvas.pasted == [(fake_qr, expected_paste_position)]

    assert fake_draw.rectangles == [
        {"coords": [(2, 2), (x, y)], "outline": 0, "width": 1}
    ]

    assert [call[0] for call in load_font_calls] == ["fake-font.ttf", "fake-font.ttf"]
    assert len(load_font_calls) == 2
    assert load_font_calls[0][1] == max(30, int(h_px * 0.065))
    assert load_font_calls[1][1] == max(25, int(h_px * 0.058))

    assert [call["text"] for call in fake_draw.text_calls] == [
        "Codice del componente",
        "Descrizione",
        "Lotto",
        "Quantità",
    ]

    assert [call["text"] for call in fake_draw.multiline_text_calls] == [
        "123",
        "descrizione\nspezzata",
        "456",
        7,
    ]

    assert len(invio_calls) == 1
    assert invio_calls[0]["draw"] is fake_draw
    assert invio_calls[0]["text"] == "descrizione molto lunga di prova"
    assert invio_calls[0]["font"] == f"font-{max(25, int(h_px * 0.058))}"


def test_load_font_returns_default_when_preferred_is_none_or_empty(mod, monkeypatch):
    sentinel_default = object()
    isfile_calls = []

    def fake_isfile(path):
        isfile_calls.append(path)
        return True

    monkeypatch.setattr(mod.os.path, "isfile", fake_isfile)
    monkeypatch.setattr(mod.ImageFont, "load_default", lambda: sentinel_default)

    got_none = mod.load_font(None, 16)
    got_empty = mod.load_font("", 16)

    assert got_none is sentinel_default
    assert got_empty is sentinel_default
    assert isfile_calls == []


def test_invio_automatico_keeps_line_when_width_is_exact_boundary(mod):
    draw = FakeDraw()

    result = mod.invio_automatico(
        draw=draw,
        text="uno due tre",
        font=None,
        max_width=7,
    )

    assert result == "uno due\ntre"


def test_invio_automatico_does_not_split_single_long_word(mod):
    draw = FakeDraw()

    result = mod.invio_automatico(
        draw=draw,
        text="lunghissimaparola",
        font=None,
        max_width=3,
    )

    assert result == "lunghissimaparola"


def test_pattern_grid_raises_on_invalid_hex_string(mod):
    with pytest.raises(ValueError):
        mod.pattern_grid("zz-not-hex", grid=3, scale=2)


def test_pattern_grid_scales_source_cells_with_nearest_blocks(mod):
    img = mod.pattern_grid("8", grid=2, scale=2)
    arr = np.array(img)
    expected = np.array(
        [
            [255, 255, 0, 0],
            [255, 255, 0, 0],
            [0, 0, 0, 0],
            [0, 0, 0, 0],
        ],
        dtype=np.uint8,
    )

    assert np.array_equal(arr, expected)


def test_gen_etichette_calls_make_qr_with_str_lotto_and_size_250(mod, monkeypatch):
    fake_canvas = FakeCanvas()
    fake_draw = FakeDraw()
    fake_qr = SimpleNamespace(name="fake-qr")
    qr_calls = []

    monkeypatch.setattr(mod.Image, "new", lambda mode, size, color: fake_canvas)
    monkeypatch.setattr(mod.ImageDraw, "Draw", lambda img: fake_draw)
    monkeypatch.setattr(mod, "load_font", lambda font_path, size: f"font-{size}")
    monkeypatch.setattr(
        mod, "invio_automatico", lambda draw, text, font, max_width: text
    )

    def fake_make_qr(data, size_px):
        qr_calls.append((data, size_px))
        return fake_qr

    monkeypatch.setattr(mod, "make_qr", fake_make_qr)

    result = mod.gen_etichette(
        codice="COD-01",
        descrizione="descrizione",
        lotto=456,
        qty="7",
        label_dimensions=[50, 80],
        dpi=300,
        font_path="fake-font.ttf",
    )

    assert result is None
    assert qr_calls == [("456", 250)]
    assert fake_canvas.pasted and fake_canvas.pasted[0][0] is fake_qr


def test_gen_etichette_writes_expected_static_labels(mod, monkeypatch):
    fake_canvas = FakeCanvas()
    fake_draw = FakeDraw()

    monkeypatch.setattr(mod.Image, "new", lambda mode, size, color: fake_canvas)
    monkeypatch.setattr(mod.ImageDraw, "Draw", lambda img: fake_draw)
    monkeypatch.setattr(mod, "load_font", lambda font_path, size: f"font-{size}")
    monkeypatch.setattr(
        mod, "make_qr", lambda data, size_px: SimpleNamespace(name="fake-qr")
    )
    monkeypatch.setattr(
        mod, "invio_automatico", lambda draw, text, font, max_width: text
    )

    result = mod.gen_etichette(
        codice="COD-01",
        descrizione="descrizione",
        lotto="LOT-01",
        qty="7",
        label_dimensions=[50, 80],
        dpi=300,
        font_path="fake-font.ttf",
    )

    assert result is None
    assert [call["text"] for call in fake_draw.text_calls] == [
        "Codice del componente",
        "Descrizione",
        "Lotto",
        "Quantità",
    ]
