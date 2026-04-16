# region LIBRERIE
"""
Programma per l'acquisizione dei dati acquisti:
- anagrafica articoli
- giacenze
- fabbisogno materiali da ODP aperti
- riepilogo acquisti
"""

import logging
import statistics
from typing import Optional, Literal
import json
import math
import tomllib
from pathlib import Path
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from zoneinfo import ZoneInfo
from datetime import datetime, time, timedelta, date
import time as time_mod
import urllib.parse
import pathlib

try:
    from icecream import ic
except Exception:
    pass
# endregion

# region COSTANTI
CONFIG = None
sqlite_engine_app = None
sqlite_engine_acq = None
sqlserver_engine_app = None
ALLOWED_WEEKDAYS = None
START_H = None
END_H = None
TIMEZONE = None
POLL_SECONDS_DEFAULT = None
ELEMENTI_ESCLUSI = None
ELEMENTI_SELEZIONATI = None
_INITIALIZED = False
# endregion

# region DB E CONFIG
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "app_odp" / "static" / "config.toml"


def init(config_path: str | pathlib.Path = None, *, force: bool = False):
    global CONFIG, sqlite_engine_app, sqlite_engine_acq, sqlserver_engine_app
    global ALLOWED_WEEKDAYS, START_H, END_H, TIMEZONE, POLL_SECONDS_DEFAULT
    global ELEMENTI_ESCLUSI, ELEMENTI_SELEZIONATI, _INITIALIZED

    if _INITIALIZED and not force:
        return

    if config_path is None:
        config_path = DEFAULT_CONFIG_PATH

    CONFIG = load_config(config_path)

    percorsi = CONFIG["Percorsi"]
    sync_cfg = CONFIG["sync_config"]
    ELEMENTI_ESCLUSI = CONFIG["Elementi_esclusi"]
    ELEMENTI_SELEZIONATI = CONFIG["Elementi_selezionati"]

    ALLOWED_WEEKDAYS = set(sync_cfg["giorni_settimanali"])
    START_H = int(sync_cfg["ora_inizio"])
    END_H = int(sync_cfg["ora_fine"])
    TIMEZONE = sync_cfg.get("time_zone", "Europe/Rome")
    POLL_SECONDS_DEFAULT = float(sync_cfg.get("tempo_polling", 5))

    percorso_db = percorsi.get("percorso_db")
    if not percorso_db:
        raise KeyError("Percorsi.percorso_db mancante in config.toml")

    percorso_db_acq = percorsi.get("percorso_db_acq")
    if not percorso_db_acq:
        raise KeyError("Percorsi.percorso_db_acq mancante in config.toml")

    sql_params = sync_cfg.get("sql_params")
    if not sql_params:
        raise KeyError("sync_config.sql_params mancante in config.toml")

    params = urllib.parse.quote_plus(sql_params)

    sqlite_engine_app = create_engine(f"sqlite:///{percorso_db}")
    sqlite_engine_acq = create_engine(f"sqlite:///{percorso_db_acq}")
    sqlserver_engine_app = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

    logging.info("sqlite_engine_app=%s", sqlite_engine_app.url)
    logging.info("sqlite_engine_acq=%s", sqlite_engine_acq.url)
    logging.info("sqlserver_engine_app=%s", sqlserver_engine_app.url)

    _INITIALIZED = True


def ensure_init():
    if not _INITIALIZED:
        init()


def load_config(config: Path) -> dict:
    with config.open("rb") as f:
        return tomllib.load(f)


# endregion

# region LETTURA ERP / DB


def leggi_view(
    table: Literal[
        "vwESGiacenza",
        "vwESGiacenzaLotti",
        "vwESArticoli",
    ],
    colonna_filtro_esclusi: Optional[str] = "",
    colonna_filtro_stato: Optional[str] = "",
) -> pd.DataFrame:
    ensure_init()

    query = f"SELECT * FROM BernardiProd.dbo.{table}"
    df = pd.read_sql(query, sqlserver_engine_app)

    if colonna_filtro_esclusi != "":
        esclusi = ELEMENTI_ESCLUSI.get(colonna_filtro_esclusi, [])
        if esclusi:
            df = df[~df[colonna_filtro_esclusi].isin(esclusi)]
        df = df.dropna(subset=[colonna_filtro_esclusi], how="any")

    if colonna_filtro_stato != "":
        selezionato = ELEMENTI_SELEZIONATI.get(colonna_filtro_stato)
        if selezionato is not None:
            df = df[df[colonna_filtro_stato] == selezionato]
        df = df.dropna(subset=[colonna_filtro_stato], how="any")

    df = df.reset_index(drop=True)
    return df


def leggi_input_odp_aperti() -> pd.DataFrame:
    """
    Legge gli ODP da RBAC.db escludendo gli ordini chiusi.
    Si appoggia a input_odp e usa StatoOrdine come stato ERP disponibile.
    """
    ensure_init()

    query = """
            SELECT *
            FROM input_odp
            WHERE COALESCE(StatoOrdine, '') <> 'Chiusa' \
            """
    df = pd.read_sql(query, sqlite_engine_app)
    return df.reset_index(drop=True)


# endregion

# region HELPERS GENERALI


def _norm_text(value) -> str:
    return str(value or "").strip()


def _now_local_date() -> date:
    tz_name = TIMEZONE or "Europe/Rome"
    return datetime.now(ZoneInfo(tz_name)).date()


def _safe_float(value, default=0.0) -> float:
    if value is None:
        return default
    try:
        if isinstance(value, str):
            value = value.strip().replace(".", "").replace(",", ".")
        out = float(value)
        if math.isnan(out):
            return default
        return out
    except (TypeError, ValueError):
        return default


def _safe_int(value, default=0) -> int:
    return int(round(_safe_float(value, default=default)))


def add_workdays(start_date: date, days: int) -> date:
    """
    Aggiunge giorni lavorativi escludendo sabato e domenica.
    """
    if days <= 0:
        return start_date

    current = start_date
    added = 0
    while added < days:
        current += timedelta(days=1)
        if current.weekday() < 5:
            added += 1
    return current


# endregion

# region SCHEMA TABELLE CACHE


def ensure_schema():
    ensure_init()

    ddl = [
        """
        CREATE TABLE IF NOT EXISTS acq_articoli (
                                                    CodArt TEXT PRIMARY KEY,
                                                    DesArt TEXT,
                                                    LottoRiordino REAL,
                                                    PuntoRiordino REAL,
                                                    PianTempoApprovFisso INTEGER,
                                                    DataPrevistaApprovvigionamento TEXT,
                                                    synced_at TEXT,
                                                    MagUM TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS acq_giacenze (
                                                    CodArt TEXT NOT NULL,
                                                    CodMag TEXT NOT NULL,
                                                    Giacenza REAL,
                                                    synced_at TEXT,
                                                    PRIMARY KEY (CodArt, CodMag)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS acq_fabbisogno_odp (
                                                          IdDocumento TEXT NOT NULL,
                                                          IdRiga TEXT NOT NULL,
                                                          NumFase TEXT NOT NULL,
                                                          CodArt TEXT NOT NULL,
                                                          VarianteArt TEXT,
                                                          QuantitaNecessaria REAL,
                                                          synced_at TEXT,
                                                          PRIMARY KEY (IdDocumento, IdRiga, NumFase, CodArt, VarianteArt)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS acq_riepilogo_materiali (
                                                               IdDocumento TEXT NOT NULL,
                                                               IdRiga TEXT NOT NULL,
                                                               NumFase TEXT NOT NULL,
                                                               CodArt TEXT NOT NULL,
                                                               VarianteArt TEXT,
                                                               QuantitaNecessaria REAL,
                                                               GiacenzaTotale REAL,
                                                               LottoRiordino REAL,
                                                               PuntoRiordino REAL,
                                                               PianTempoApprovFisso INTEGER,
                                                               DataPrevistaApprovigionamento TEXT,
                                                               synced_at TEXT,
                                                               PRIMARY KEY (IdDocumento, IdRiga, NumFase, CodArt, VarianteArt)
        )
        """,
    ]

    with sqlite_engine_acq.begin() as conn:
        cols = conn.execute(sa.text("PRAGMA table_info(acq_articoli)")).fetchall()
        col_names = {row[1] for row in cols}
        if "MagUM" not in col_names:
            conn.execute(sa.text("ALTER TABLE acq_articoli ADD COLUMN MagUM TEXT"))


# endregion

# region COSTRUZIONE CACHE


def build_acq_articoli(df_articoli: pd.DataFrame) -> pd.DataFrame:
    today = _now_local_date()
    synced_at = datetime.now(ZoneInfo(TIMEZONE or "Europe/Rome")).isoformat(
        timespec="seconds"
    )
    needed_cols = [
        "CodArt",
        "DesArt",
        "MagUM",
        "LottoRiordino",
        "PuntoRiordino",
        "PianTempoApprovFisso",
    ]
    for col in needed_cols:
        if col not in df_articoli.columns:
            df_articoli[col] = None
    df = df_articoli[needed_cols].copy()
    df = df.dropna(subset=["CodArt"], how="any")
    df["CodArt"] = df["CodArt"].astype(str).str.strip()
    df = df[df["CodArt"] != ""]
    df["MagUM"] = df["MagUM"].fillna("").astype(str).str.strip()
    df["LottoRiordino"] = df["LottoRiordino"].apply(_safe_float)
    df["PuntoRiordino"] = df["PuntoRiordino"].apply(_safe_float)
    df["PianTempoApprovFisso"] = df["PianTempoApprovFisso"].apply(_safe_int)
    df["DataPrevistaApprovvigionamento"] = df["PianTempoApprovFisso"].apply(
        lambda x: add_workdays(today, x).isoformat()
    )
    df["synced_at"] = synced_at
    df = df.drop_duplicates(subset=["CodArt"], keep="last")

    return df[
        [
            "CodArt",
            "DesArt",
            "MagUM",
            "LottoRiordino",
            "PuntoRiordino",
            "PianTempoApprovFisso",
            "DataPrevistaApprovvigionamento",
            "synced_at",
        ]
    ].copy()


def build_acq_giacenze(df_giacenza: pd.DataFrame) -> pd.DataFrame:
    synced_at = datetime.now(ZoneInfo(TIMEZONE or "Europe/Rome")).isoformat(
        timespec="seconds"
    )

    needed_cols = ["CodArt", "CodMag", "Giacenza"]
    for col in needed_cols:
        if col not in df_giacenza.columns:
            df_giacenza[col] = None

    df = df_giacenza[needed_cols].copy()
    df = df.dropna(subset=["CodArt", "CodMag"], how="any")
    df["CodArt"] = df["CodArt"].astype(str).str.strip()
    df["CodMag"] = df["CodMag"].astype(str).str.strip()
    df = df[(df["CodArt"] != "") & (df["CodMag"] != "")]
    df["Giacenza"] = df["Giacenza"].apply(_safe_float)
    df["synced_at"] = synced_at

    df = (
        df.groupby(["CodArt", "CodMag"], as_index=False, dropna=False)["Giacenza"]
        .sum()
        .merge(
            df[["CodArt", "CodMag", "synced_at"]].drop_duplicates(),
            on=["CodArt", "CodMag"],
            how="left",
        )
    )
    return df


def _parse_distinta_materiale(value) -> list[dict]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return []
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []


def build_acq_fabbisogno_odp(df_input_odp: pd.DataFrame) -> pd.DataFrame:
    synced_at = datetime.now(ZoneInfo(TIMEZONE or "Europe/Rome")).isoformat(
        timespec="seconds"
    )

    if df_input_odp.empty:
        return pd.DataFrame(
            columns=[
                "IdDocumento",
                "IdRiga",
                "NumFase",
                "CodArt",
                "VarianteArt",
                "QuantitaNecessaria",
                "synced_at",
            ]
        )

    righe = []

    for _, row in df_input_odp.iterrows():
        id_documento = _norm_text(row.get("IdDocumento"))
        id_riga = _norm_text(row.get("IdRiga"))
        distinta = _parse_distinta_materiale(row.get("DistintaMateriale"))

        for comp in distinta:
            cod_art = _norm_text(comp.get("CodArt"))
            num_fase = _norm_text(comp.get("NumFase"))
            variante_art = _norm_text(comp.get("VarianteArt"))
            quantita = _safe_float(comp.get("Quantita"), 0.0)

            if not cod_art or not num_fase:
                continue

            # stessi esclusi del sync_input.py
            esclusi_codart = ELEMENTI_ESCLUSI.get("CodArt", [])
            if cod_art in esclusi_codart:
                continue

            righe.append(
                {
                    "IdDocumento": id_documento,
                    "IdRiga": id_riga,
                    "NumFase": num_fase,
                    "CodArt": cod_art,
                    "VarianteArt": variante_art or None,
                    "QuantitaNecessaria": quantita,
                    "synced_at": synced_at,
                }
            )

    if not righe:
        return pd.DataFrame(
            columns=[
                "IdDocumento",
                "IdRiga",
                "NumFase",
                "CodArt",
                "VarianteArt",
                "QuantitaNecessaria",
                "synced_at",
            ]
        )

    df = pd.DataFrame(righe)

    df = df.groupby(
        ["IdDocumento", "IdRiga", "NumFase", "CodArt", "VarianteArt"],
        as_index=False,
        dropna=False,
    )["QuantitaNecessaria"].sum()
    df["synced_at"] = synced_at
    return df


def build_acq_riepilogo_materiali(
    df_fabbisogno_odp: pd.DataFrame,
    df_articoli_cache: pd.DataFrame,
    df_giacenze_cache: pd.DataFrame,
) -> pd.DataFrame:
    synced_at = datetime.now(ZoneInfo(TIMEZONE or "Europe/Rome")).isoformat(
        timespec="seconds"
    )

    if df_fabbisogno_odp.empty:
        return pd.DataFrame(
            columns=[
                "IdDocumento",
                "IdRiga",
                "NumFase",
                "CodArt",
                "VarianteArt",
                "QuantitaNecessaria",
                "GiacenzaTotale",
                "LottoRiordino",
                "PuntoRiordino",
                "PianTempoApprovFisso",
                "DataPrevistaApprovvigionamento",
                "synced_at",
            ]
        )

    giac_tot = (
        df_giacenze_cache.groupby(["CodArt"], as_index=False, dropna=False)["Giacenza"]
        .sum()
        .rename(columns={"Giacenza": "GiacenzaTotale"})
    )

    df = df_fabbisogno_odp.merge(
        giac_tot,
        on="CodArt",
        how="left",
    ).merge(
        df_articoli_cache[
            [
                "CodArt",
                "LottoRiordino",
                "PuntoRiordino",
                "PianTempoApprovFisso",
                "DataPrevistaApprovvigionamento",
            ]
        ],
        on="CodArt",
        how="left",
    )

    df["GiacenzaTotale"] = df["GiacenzaTotale"].fillna(0.0)
    df["LottoRiordino"] = df["LottoRiordino"].fillna(0.0)
    df["PuntoRiordino"] = df["PuntoRiordino"].fillna(0.0)
    df["PianTempoApprovFisso"] = df["PianTempoApprovFisso"].fillna(0).astype(int)
    df["synced_at"] = synced_at

    return df[
        [
            "IdDocumento",
            "IdRiga",
            "NumFase",
            "CodArt",
            "VarianteArt",
            "QuantitaNecessaria",
            "GiacenzaTotale",
            "LottoRiordino",
            "PuntoRiordino",
            "PianTempoApprovFisso",
            "DataPrevistaApprovvigionamento",
            "synced_at",
        ]
    ].copy()


# endregion

# region WRITE CACHE


def _replace_table(engine, table_name: str, df: pd.DataFrame):
    with engine.begin() as conn:
        conn.execute(sa.text(f"DELETE FROM {table_name}"))
    if not df.empty:
        df.to_sql(name=table_name, con=engine, if_exists="append", index=False)


def elaborazione_dati_acq():
    ensure_init()
    ensure_schema()

    df_articoli = leggi_view("vwESArticoli", colonna_filtro_esclusi="CodArt")
    df_giacenza = leggi_view("vwESGiacenza", colonna_filtro_esclusi="CodArt")
    df_input_odp_aperti = leggi_input_odp_aperti()

    df_acq_articoli = build_acq_articoli(df_articoli)
    df_acq_giacenze = build_acq_giacenze(df_giacenza)
    df_acq_fabbisogno_odp = build_acq_fabbisogno_odp(df_input_odp_aperti)
    df_acq_riepilogo = build_acq_riepilogo_materiali(
        df_acq_fabbisogno_odp,
        df_acq_articoli,
        df_acq_giacenze,
    )

    _replace_table(sqlite_engine_acq, "acq_articoli", df_acq_articoli)
    _replace_table(sqlite_engine_acq, "acq_giacenze", df_acq_giacenze)
    _replace_table(sqlite_engine_acq, "acq_fabbisogno_odp", df_acq_fabbisogno_odp)
    _replace_table(sqlite_engine_acq, "acq_riepilogo_materiali", df_acq_riepilogo)

    logging.info(
        "Sync acquisti completato | articoli=%s | giacenze=%s | fabbisogno_odp=%s | riepilogo=%s",
        len(df_acq_articoli),
        len(df_acq_giacenze),
        len(df_acq_fabbisogno_odp),
        len(df_acq_riepilogo),
    )


# endregion

# region SCHEDULAZIONE


def _in_time_window(now_t: time, start: time, end: time) -> bool:
    if start == end:
        return True
    if start < end:
        return start <= now_t < end
    return (now_t >= start) or (now_t < end)


def _is_allowed_datetime(
    now: datetime, start: time, end: time, allowed_weekdays: set[int]
) -> bool:
    if start < end or start == end:
        return (now.weekday() in allowed_weekdays) and _in_time_window(
            now.timetz().replace(tzinfo=None), start, end
        )

    now_t = now.timetz().replace(tzinfo=None)
    if now_t >= start:
        start_day = now.weekday()
    else:
        start_day = (now.weekday() - 1) % 7

    return (start_day in allowed_weekdays) and _in_time_window(now_t, start, end)


def seconds_until_next_allowed(
    start_h: int,
    end_h: int,
    allowed_weekdays: set[int],
    tz: ZoneInfo = ZoneInfo("Europe/Rome"),
    step_minutes: int = 1,
) -> int:
    if step_minutes <= 0:
        logging.warning("step_minutes=%s non valido: imposto a 1", step_minutes)
        step_minutes = 1

    start = time(start_h, 0)
    end = time(end_h, 0)

    now = datetime.now(tz)
    if _is_allowed_datetime(now, start, end, allowed_weekdays):
        return 0

    probe = now.replace(second=0, microsecond=0)
    limit = probe + timedelta(days=8)
    step = timedelta(minutes=step_minutes)

    while probe < limit:
        probe += step
        if _is_allowed_datetime(probe, start, end, allowed_weekdays):
            return int((probe - now).total_seconds())

    raise RuntimeError(
        "Impossibile trovare una prossima finestra: controlla parametri schedule."
    )


def wait_if_not_allowed(start_h: int, end_h: int, allowed_weekdays: set[int]) -> None:
    s = seconds_until_next_allowed(start_h, end_h, allowed_weekdays)
    if s > 0:
        logging.info(
            "Fuori schedule. Sleep ~%d min (fino a prossima finestra).", s // 60
        )
        time_mod.sleep(s)


def read_cycle() -> None:
    ensure_init()
    counter = 0
    logging.info("Inizio programma sync_acq")
    list_elapsed = []
    list_sleep = []

    try:
        while True:
            wait_if_not_allowed(START_H, END_H, ALLOWED_WEEKDAYS)
            start = time_mod.time()

            try:
                elaborazione_dati_acq()
            except Exception:
                logging.exception("Errore generico sync_acq")

            elapsed = time_mod.time() - start
            sleep_for = max(0.0, float(POLL_SECONDS_DEFAULT) - elapsed)
            logging.info(
                "Ciclo %i completato in %.2f s. Sleep %.2fs",
                counter,
                elapsed,
                sleep_for,
            )

            list_elapsed.append(elapsed)
            list_sleep.append(sleep_for)
            time_mod.sleep(sleep_for)
            counter += 1

    except KeyboardInterrupt:
        if list_elapsed:
            logging.info("Media tempo ciclo %.5f", statistics.mean(list_elapsed))
        if list_sleep:
            logging.info("Media tempo riposo %.5f", statistics.mean(list_sleep))


if __name__ == "__main__":
    start_all = time_mod.time()
    read_cycle()
    end_all = time_mod.time()
    logging.info("Tempo di funzionamento %4.1f m", ((end_all - start_all) / 60))
# endregion
