from __future__ import annotations

import argparse
import inspect
import sys
from datetime import datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))


def load_flask_app():
    """
    Carica l'app Flask usando la tua app factory.

    Se nel tuo progetto create_app è in un altro punto,
    modifica solo questa funzione.
    """
    try:
        from app import create_app
    except ImportError:
        from app_odp.app import create_app

    return create_app()


def call_gen_etichette(gen_etichette, app, args):
    """
    Chiama gen_etichette passando solo gli argomenti realmente supportati
    dalla funzione, così lo script resta compatibile anche se hai cambiato
    leggermente la firma della funzione.
    """
    kwargs = {
        "codice": args.codice,
        "descrizione": args.descrizione,
        "lotto": args.lotto,
        "qty": args.qty,
        "label_dimensions": app.config.get("DIMENSIONI", [80, 50]),
        "dpi": app.config.get("DPI", 300),
        "font_path": app.config.get("FONT_PATH"),
    }

    signature = inspect.signature(gen_etichette)
    allowed_kwargs = {
        key: value for key, value in kwargs.items() if key in signature.parameters
    }

    return gen_etichette(**allowed_kwargs)


def main():
    parser = argparse.ArgumentParser(
        description="Genera e stampa una etichetta di test senza aprire ordini."
    )

    parser.add_argument("--codice", default="TEST-ART-001")
    parser.add_argument(
        "--descrizione",
        default="Etichetta di test per verifica stampa CAB EOS1/300",
    )
    parser.add_argument("--lotto", default="LOTTO-TEST-001")
    parser.add_argument("--qty", default="1")

    parser.add_argument(
        "--print",
        action="store_true",
        dest="do_print",
        help="Invia la PNG generata alla stampante.",
    )

    parser.add_argument(
        "--printer",
        default="",
        help="Nome stampante da forzare. Se vuoto usa la config Flask.",
    )

    parser.add_argument(
        "--output-dir",
        default="",
        help="Cartella output. Se vuota usa ETICHETTE_OUTPUT_DIR o instance/test_etichette.",
    )

    args = parser.parse_args()

    app = load_flask_app()

    with app.app_context():
        from app_odp.gen_etichette import gen_etichette

        if args.printer:
            app.config["LABEL_PRINTER_NAME"] = args.printer

        output_dir = (
            Path(args.output_dir)
            if args.output_dir
            else Path(
                app.config.get(
                    "ETICHETTE_OUTPUT_DIR",
                    PROJECT_ROOT / "instance" / "test_etichette",
                )
            )
        )

        output_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = output_dir / f"TEST_ETICHETTA_{timestamp}.png"

        img = call_gen_etichette(gen_etichette, app, args)
        img.save(output_file)

        print(f"PNG generata: {output_file}")

        if args.do_print:
            from app_odp.routes import _print_label_png_to_windows_printer

            print("Invio alla stampante...")
            _print_label_png_to_windows_printer(output_file)
            print("Stampa inviata.")

        else:
            print("Stampa non eseguita. Usa --print per inviare alla stampante.")


if __name__ == "__main__":
    main()
