import csv
from datetime import date
from functools import lru_cache
from io import BytesIO, StringIO
import logging
from math import isfinite
from pathlib import Path
import re
from statistics import linear_regression
from threading import Lock
from time import monotonic
from urllib.request import urlopen
from xml.etree import ElementTree
from zipfile import BadZipFile

from openpyxl import load_workbook
from openpyxl.utils.exceptions import InvalidFileException


PINK_SHEET = "https://www.worldbank.org/en/research/commodity-markets"
CAMBIO_CSV = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=EXUSEU"
CAMBIO_BCE = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist-90d.xml"
# Cambio di riferimento BCE del 06/11/2026, USD per 1 EUR.
CAMBIO_BCE_LOCALE = (date(2026, 11, 6), 1.1165)
SNAPSHOT = Path(__file__).resolve().parents[1] / "data" / "serbatoi_alluminio_snapshot.csv"
_lock = Lock()
_cache = None
_scadenza = 0.0


def _scarica(url):
    limite = 5_000_000
    with urlopen(url, timeout=4) as risposta:
        dati = risposta.read(limite + 1)
    if len(dati) > limite:
        raise ValueError("Fonte di mercato oltre il limite previsto")
    return dati


def _quotazioni_xlsx(dati):
    workbook = load_workbook(BytesIO(dati), read_only=True, data_only=True)
    try:
        righe = workbook["Monthly Prices"].iter_rows(values_only=True)
        for riga in righe:
            if "Aluminum" in riga:
                colonna = riga.index("Aluminum")
                break
        else:
            raise ValueError("Colonna Aluminum assente dalla Pink Sheet")
        prezzi = {}
        for riga in righe:
            mese = re.fullmatch(r"(\d{4})M(0[1-9]|1[0-2])", str(riga[0]))
            if mese and isinstance(riga[colonna], (int, float)):
                prezzi[f"{mese[1]}-{mese[2]}-01"] = riga[colonna]
        return prezzi
    finally:
        workbook.close()


def _punti(quotazioni, cambi):
    return [
        {"data": mese, "usd_t": quotazioni[mese], "euro_kg": round(quotazioni[mese] / cambi[mese] / 1000, 4)}
        for mese in sorted(quotazioni.keys() & cambi.keys())
        if isfinite(quotazioni[mese]) and isfinite(cambi[mese]) and quotazioni[mese] > 0 and cambi[mese] > 0
    ][-120:]


def _serie_online():
    pagina = _scarica(PINK_SHEET).decode("utf-8", errors="replace")
    file = re.search(r'https://thedocs\.worldbank\.org/en/doc/[\w-]+/related/CMO-Historical-Data-Monthly\.xlsx', pagina)
    if file is None:
        raise ValueError("Collegamento alla Pink Sheet mensile assente")
    quotazioni = _quotazioni_xlsx(_scarica(file[0]))
    cambi = {}
    for riga in csv.DictReader(StringIO(_scarica(CAMBIO_CSV).decode("utf-8-sig"))):
        valore = riga["EXUSEU"]
        if valore and valore != ".":
            mese = date.fromisoformat(riga["observation_date"])
            cambi[mese.isoformat()] = float(valore)
    punti = _punti(quotazioni, cambi)
    if not punti:
        raise ValueError("Nessun mese comune tra alluminio e cambio")
    return punti


def _serie_locale():
    # World Bank Pink Sheet (PDF mensili, valori USD/t arrotondati dalla fonte)
    # e Federal Reserve Board, serie EXUSEU via FRED.
    with SNAPSHOT.open(encoding="utf-8", newline="") as file:
        righe = list(csv.DictReader(file))
    return _punti({r["data"]: float(r["usd_t"]) for r in righe}, {r["data"]: float(r["usd_eur"]) for r in righe})


@lru_cache(maxsize=2)
def _cambi_bce(ora):
    try:
        documento = ElementTree.fromstring(_scarica(CAMBIO_BCE))
        cambi = []
        for riga in documento.iter():
            if "time" not in riga.attrib:
                continue
            usd = next((valuta for valuta in riga if valuta.get("currency") == "USD"), None)
            if usd is not None:
                cambio = float(usd.attrib["rate"])
                if isfinite(cambio) and cambio > 0:
                    cambi.append((date.fromisoformat(riga.attrib["time"]), cambio))
        return tuple(cambi)
    except (OSError, ValueError, KeyError, ElementTree.ParseError) as exc:
        logging.getLogger(__name__).warning("Aggiornamento cambio BCE: %s", exc)
        return ()


def _ultimo_cambio_bce(oggi):
    candidati = (*_cambi_bce(int(monotonic() // 3600)), CAMBIO_BCE_LOCALE)
    return max((c for c in candidati if oggi.replace(day=1) <= c[0] <= oggi), default=None)


def _stima(punti, oggi, cambio=None):
    if not punti:
        return None
    ultimi = punti[-6:]
    mesi = [int(p["data"][:4]) * 12 + int(p["data"][5:7]) - 1 for p in ultimi]
    corrente = oggi.replace(day=1)
    indice_corrente = corrente.year * 12 + corrente.month - 1
    tendenza = mesi == list(range(indice_corrente - 6, indice_corrente))
    risultato = {"data": corrente.isoformat()}
    if tendenza:
        campo = "usd_t" if cambio else "euro_kg"
        coefficiente, intercetta = linear_regression(range(6), [p[campo] for p in ultimi])
        valore = intercetta + 6 * coefficiente
    elif cambio:
        valore = ultimi[-1]["usd_t"]
        risultato["metodo"] = "ultima_quotazione"
    else:
        return None
    if cambio:
        giorno, usd_eur = cambio
        valore /= usd_eur * 1000
        risultato["cambio_data"] = giorno.isoformat()
    risultato["euro_kg"] = round(valore, 4)
    return risultato if risultato["euro_kg"] > 0 else None


def mercato_alluminio(oggi):
    global _cache, _scadenza
    with _lock:
        if monotonic() >= _scadenza:
            try:
                punti = _serie_online()
                if _cache is None:
                    _cache = _serie_locale(), True
                if punti[-1]["data"] < _cache[0][-1]["data"]:
                    raise ValueError("La fonte online è meno recente della serie locale")
                _cache = punti, False
                _scadenza = monotonic() + 86400
            except (OSError, ValueError, KeyError, BadZipFile, InvalidFileException) as exc:
                logging.getLogger(__name__).warning("Aggiornamento quotazione alluminio: %s", exc)
                _cache = (_cache[0] if _cache else _serie_locale()), True
                _scadenza = monotonic() + 900
        punti, copia_locale = _cache
    punti = [p for p in punti if p["data"] < oggi.replace(day=1).isoformat()]
    cambio = _ultimo_cambio_bce(oggi) if punti else None
    return {"punti": punti, "copia_locale": copia_locale, "stima": _stima(punti, oggi, cambio)}
