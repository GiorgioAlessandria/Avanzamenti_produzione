from pathlib import Path
from app_odp.services.order_helpers import _norm_text
import re
from uuid import uuid4
from app_odp.gen_etichette import gen_etichette
from flask import current_app
import win32con
import win32ui
from PIL import Image, ImageOps, ImageWin
from app_odp.services.order_helpers import _now_rome_dt


def _resolve_label_file_path(filename: str) -> Path | None:
    filename = _norm_text(filename)
    if not filename:
        return None

    base_dir = Path(current_app.config["ETICHETTE_OUTPUT_DIR"]).expanduser()

    try:
        base_dir = base_dir.resolve()
        file_path = (base_dir / filename).resolve()
        file_path.relative_to(base_dir)
    except Exception:
        return None

    return file_path


def _apply_label_image_offset(img, offset_x_mm: float, offset_y_mm: float, dpi: int):
    """
    Applica offset al PNG mantenendo invariata la dimensione finale.

    Offset positivo X = sposta a destra.
    Offset negativo X = sposta a sinistra.
    Offset positivo Y = sposta in basso.
    Offset negativo Y = sposta in alto.
    """
    offset_x_px = int(round(float(offset_x_mm) / 25.4 * int(dpi)))
    offset_y_px = int(round(float(offset_y_mm) / 25.4 * int(dpi)))

    if offset_x_px == 0 and offset_y_px == 0:
        return img

    canvas = Image.new("RGB", img.size, "white")

    src_left = max(0, -offset_x_px)
    src_top = max(0, -offset_y_px)
    src_right = (
        min(img.width, img.width - offset_x_px) if offset_x_px > 0 else img.width
    )
    src_bottom = (
        min(img.height, img.height - offset_y_px) if offset_y_px > 0 else img.height
    )

    dst_left = max(0, offset_x_px)
    dst_top = max(0, offset_y_px)

    if src_right <= src_left or src_bottom <= src_top:
        return img

    cropped = img.crop((src_left, src_top, src_right, src_bottom))
    canvas.paste(cropped, (dst_left, dst_top))

    return canvas


def _mm_to_printer_px(mm: float, dpi: int) -> int:
    return int(round(float(mm) / 25.4 * int(dpi)))


def _get_label_print_settings() -> dict:
    dimensioni = current_app.config.get("DIMENSIONI") or [80.0, 30.0]

    return {
        "printer_name": current_app.config.get("LABEL_PRINTER_NAME") or "",
        "width_mm": float(dimensioni[0]),
        "height_mm": float(dimensioni[1]),
        "dpi": int(current_app.config.get("DPI") or 300),
        "rotation": int(current_app.config.get("LABEL_PRINT_ROTATION", 0) or 0),
        "offset_x_mm": float(
            current_app.config.get("LABEL_PRINT_OFFSET_X_MM", 0.0) or 0.0
        ),
        "offset_y_mm": float(
            current_app.config.get("LABEL_PRINT_OFFSET_Y_MM", 0.0) or 0.0
        ),
        "scale": float(current_app.config.get("LABEL_PRINT_SCALE", 1.0) or 1.0),
    }


def _create_label_printer_dc(printer_name: str, width_mm: float, height_mm: float):
    """
    Crea il Device Context della stampante etichette.

    Non forza PaperWidth/PaperLength da Python perché alcuni ambienti pywin32
    espongono win32gui.CreateDC con soli 3 argomenti.
    Il formato 80x30 deve essere configurato nel driver Windows della CAB.
    """
    printer_dc = win32ui.CreateDC()
    printer_dc.CreatePrinterDC(printer_name)
    return printer_dc


def _print_label_png_to_windows_printer(file_path: Path) -> None:
    settings = _get_label_print_settings()

    printer_name = settings["printer_name"]
    if not printer_name:
        raise RuntimeError("Nome stampante etichette non configurato.")

    width_mm = settings["width_mm"]
    height_mm = settings["height_mm"]
    dpi = settings["dpi"]
    rotation = settings["rotation"]
    offset_x_mm = settings["offset_x_mm"]
    offset_y_mm = settings["offset_y_mm"]
    scale = settings["scale"]

    if not file_path or not Path(file_path).is_file():
        raise FileNotFoundError(f"File etichetta non trovato: {file_path}")

    img = Image.open(file_path)
    img = ImageOps.exif_transpose(img).convert("RGB")
    img = _apply_label_image_offset(
        img,
        offset_x_mm=offset_x_mm,
        offset_y_mm=offset_y_mm,
        dpi=dpi,
    )

    if rotation:
        # PIL ruota in senso antiorario.
        img = img.rotate(rotation, expand=True)

    # Dimensione fisica voluta: 80x30 mm a 300 dpi.
    target_w_px = _mm_to_printer_px(width_mm * scale, dpi)
    target_h_px = _mm_to_printer_px(height_mm * scale, dpi)

    offset_x_px = _mm_to_printer_px(offset_x_mm, dpi)
    offset_y_px = _mm_to_printer_px(offset_y_mm, dpi)

    printer_dc = _create_label_printer_dc(printer_name, width_mm, height_mm)

    started_doc = False
    started_page = False

    try:
        printable_w = printer_dc.GetDeviceCaps(win32con.HORZRES)
        printable_h = printer_dc.GetDeviceCaps(win32con.VERTRES)
        expected_w = target_w_px
        expected_h = target_h_px

        max_w = int(expected_w * 1.35)
        max_h = int(expected_h * 1.35)

        if printable_w > max_w or printable_h > max_h:
            raise RuntimeError(
                "Formato pagina driver non coerente con etichetta. "
                f"Atteso circa {expected_w}x{expected_h}px, "
                f"driver restituisce {printable_w}x{printable_h}px. "
                "Configura nel driver Windows della cab EOS1/300 un formato 80x30 mm."
            )

        current_app.logger.info(
            "Stampa etichetta: file=%s printer=%s img=%sx%s target=%sx%s printable=%sx%s dpi=%s rotation=%s offset=%s,%s",
            file_path,
            printer_name,
            img.width,
            img.height,
            target_w_px,
            target_h_px,
            printable_w,
            printable_h,
            dpi,
            rotation,
            offset_x_px,
            offset_y_px,
        )

        # Se il driver restituisce un'area stampabile leggermente diversa,
        # evitiamo di uscire dal formato etichetta.
        draw_w = min(target_w_px, printable_w)
        draw_h = min(target_h_px, printable_h)

        x1 = 0
        y1 = 0
        x2 = draw_w
        y2 = draw_h

        dib = ImageWin.Dib(img)

        printer_dc.StartDoc(str(file_path.name))
        started_doc = True

        printer_dc.StartPage()
        started_page = True

        # Stampa una singola immagine in una singola area 80x30.
        dib.draw(printer_dc.GetHandleOutput(), (x1, y1, x2, y2))

        printer_dc.EndPage()
        started_page = False

        printer_dc.EndDoc()
        started_doc = False

    except Exception:
        if started_page:
            try:
                printer_dc.EndPage()
            except Exception:
                pass

        if started_doc:
            try:
                printer_dc.AbortDoc()
            except Exception:
                pass

        raise

    finally:
        printer_dc.DeleteDC()


def _safe_filename(value: str) -> str:
    value = str(value or "").strip()
    value = re.sub(r"[^A-Za-z0-9._-]+", "_", value)
    return value or "etichetta"


def _genera_etichetta_lotto(
    *,
    codice: str,
    descrizione: str,
    lotto: str,
    quantita: str,
) -> Image.Image:
    return gen_etichette(
        codice=codice,
        descrizione=descrizione,
        lotto=lotto,
        qty=quantita,
        label_dimensions=current_app.config["DIMENSIONI"],
        dpi=current_app.config["DPI"],
        font_path=current_app.config["FONT_PATH"],
    )


def _genera_e_salva_etichetta_lotto(
    *,
    codice: str,
    descrizione: str,
    lotto: str,
    quantita: str,
) -> str:
    """Compatibilità con i flussi storici che richiedono ancora un file PNG."""
    img = _genera_etichetta_lotto(
        codice=codice,
        descrizione=descrizione,
        lotto=lotto,
        quantita=quantita,
    )

    output_dir = Path(current_app.config["ETICHETTE_OUTPUT_DIR"])
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = _now_rome_dt().strftime("%Y%m%d_%H%M%S")
    unique_suffix = uuid4().hex[:8]
    filename = f"etichetta_{_safe_filename(lotto)}_{timestamp}_{unique_suffix}.png"
    file_path = output_dir / filename

    img.save(file_path, format="PNG")

    return filename


def generazione_lotti(dt=None) -> str:
    dt = dt or _now_rome_dt()
    return dt.strftime("%Y%m%d")
