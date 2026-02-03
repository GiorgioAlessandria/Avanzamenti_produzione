import shutil
import sqlite3
from pathlib import Path
import pytest

@pytest.fixture()
def rbac_db(tmp_path):
    # metti RBAC.db in tests/assets/RBAC.db (consigliato)
    src = Path(__file__).resolve().parent / "assets" / "RBAC.db"
    dst = tmp_path / "RBAC.db"
    shutil.copy(src, dst)

    # pulizia tabelle che toccherai nei test
    con = sqlite3.connect(dst)
    con.execute("DELETE FROM input_odp")
    con.execute("DELETE FROM change_event")
    con.commit()
    con.close()

    return dst