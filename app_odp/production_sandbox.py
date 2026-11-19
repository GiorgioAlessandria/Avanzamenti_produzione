"""Sessioni di prova della produzione con database SQLite isolati."""

from contextlib import closing
from datetime import datetime
from functools import lru_cache
import hashlib
import json
from pathlib import Path
import re
import shutil
import sqlite3
from tempfile import gettempdir
from time import time
from uuid import uuid4

from flask import Response, abort, jsonify, redirect, request, url_for
from werkzeug.wsgi import ClosingIterator

from app_odp.models import BrowserTabSession, User, db
from app_odp.operator_session import (
    OPERATOR_SESSION_TIMEOUT,
    create_operator_session,
    get_operator_token,
    hash_token,
    resolve_operator_session,
)
from app_odp.policy.policy import RbacPolicy


_TTL_SECONDS = int(OPERATOR_SESSION_TIMEOUT.total_seconds())
_ID = re.compile(r"[0-9a-f]{32}")
_ALLOWED = {
    "main.home",
    "main.api_home_bridge",
    "main.api_ordini_gruppi_disponibili",
    "main.api_apri_gruppo_multiplo",
    "main.api_apri_gruppo_misto",
    "main.api_apri_gruppo_mascherato",
    "main.api_sospendi_gruppo_ordini_payload",
    "main.api_sospendi_gruppo_ordini",
    "main.api_riattiva_gruppo_ordini_payload",
    "main.api_riattiva_gruppo_ordini",
    "main.api_sciogli_gruppo_per_chiusura_singola",
    "main.api_chiudi_membro_gruppo",
    "main.api_chiudi_gruppo_ordini",
    "main.api_prendi_ordine",
    "main.api_sospendi_ordine",
    "main.api_sospendi_ordine_montaggio_macchina",
    "main.api_riattiva_ordine",
    "main.api_riattiva_ordine_montaggio_macchina",
    "main.api_chiudi_ordine",
    "main.api_chiudi_ordine_montaggio_macchina",
    "main.api_piani_cq_ordini",
    "main.api_lotti_componenti",
    "production_sandbox_stop",
}


def _live_admin(token_hash):
    row = BrowserTabSession.query.filter_by(token_hash=token_hash, revoked_at=None).first()
    cutoff = datetime.now() - OPERATOR_SESSION_TIMEOUT
    if row and row.last_seen_at >= cutoff and row.user and row.user.active:
        if RbacPolicy(row.user).has_direct_admin_role:
            return row.user
    return None


def _backup_database(source: Path, destination: Path):
    with closing(sqlite3.connect(source.resolve().as_uri() + "?mode=ro", uri=True)) as live:
        with closing(sqlite3.connect(destination)) as snapshot:
            live.backup(snapshot)


def _prune_expired(root: Path):
    now = time()
    for directory in root.iterdir():
        if _ID.fullmatch(directory.name) and directory.is_dir() and not directory.is_symlink():
            try:
                if directory.stat().st_mtime < now - _TTL_SECONDS:
                    shutil.rmtree(directory, ignore_errors=True)
            except FileNotFoundError:
                continue


def _protect_sandbox(app, metadata):
    @app.before_request
    def authorize_sandbox():
        if hash_token(get_operator_token()) != metadata["token"] or resolve_operator_session() is None:
            abort(403)
        if request.endpoint not in _ALLOWED:
            abort(403, "Funzione non disponibile nella sandbox di produzione.")

    @app.after_request
    def discourage_caching(response):
        response.headers["Cache-Control"] = "no-store"
        response.headers["Referrer-Policy"] = "no-referrer"
        return response

    @app.post("/termina")
    def production_sandbox_stop():
        request.environ["avp.sandbox.discard"] = True
        return jsonify(ok=True)


def install_production_sandbox(app, app_factory):
    live_wsgi = app.wsgi_app

    @lru_cache(maxsize=1)
    def storage():
        with app.app_context():
            sources = {key: Path(engine.url.database) for key, engine in db.engines.items()}
        identity = hashlib.sha256(str(sources[None].resolve()).encode()).hexdigest()[:16]
        return Path(gettempdir()) / f"avp-production-sandbox-{identity}", sources

    def paths_for(directory, sources):
        return {key: directory / ("main.sqlite" if key is None else f"{key}.sqlite") for key in sources}

    def close_engines(sandbox):
        with sandbox.app_context():
            for engine in db.engines.values():
                engine.dispose()

    @app.post("/admin/sandbox/attiva")
    def production_sandbox_start():
        live_token = get_operator_token()
        owner_hash = hash_token(live_token)
        admin = _live_admin(owner_hash) if live_token else None
        if admin is None:
            abort(403)
        admin_id = admin.id
        root, sources = storage()
        root.mkdir(mode=0o700, exist_ok=True)
        _prune_expired(root)
        sandbox_id = uuid4().hex
        directory = root / sandbox_id
        directory.mkdir(mode=0o700)
        paths = paths_for(directory, sources)
        try:
            for key, source in sources.items():
                _backup_database(source, paths[key])
            sandbox = app_factory(sandbox_db_paths=paths)
            try:
                with sandbox.test_request_context():
                    token = create_operator_session(db.session.get(User, admin_id))
            finally:
                close_engines(sandbox)
            metadata = {"owner": owner_hash, "token": hash_token(token), "expires": time() + _TTL_SECONDS}
            (directory / "session.json").write_text(json.dumps(metadata), encoding="utf-8")
        except Exception:
            shutil.rmtree(directory, ignore_errors=True)
            app.logger.exception("Impossibile creare la sandbox di produzione")
            abort(503, "Impossibile preparare la copia di prova.")
        home = url_for("main.home", tab_session=token, tab=request.form.get("tab") or None)
        suffix = home[len(request.script_root):]
        response = redirect(f"{request.script_root}/sandbox/{sandbox_id}{suffix}", code=303)
        response.headers["Cache-Control"] = "no-store"
        response.headers["Referrer-Policy"] = "no-referrer"
        return response

    def dispatch(environ, start_response):
        path = environ.get("PATH_INFO", "")
        if not path.startswith("/sandbox/"):
            return live_wsgi(environ, start_response)
        _, _, sandbox_id, *rest = path.split("/", 3)
        if not _ID.fullmatch(sandbox_id) or not rest:
            return Response(status=404)(environ, start_response)

        root, sources = storage()
        directory = root / sandbox_id
        metadata_path = directory / "session.json"
        try:
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return Response("Sandbox terminata o non disponibile.", status=410)(environ, start_response)
        if metadata["expires"] <= time():
            return Response("Sandbox scaduta. Avvia una nuova prova dalla produzione.", status=410)(environ, start_response)

        scoped = environ.copy()
        scoped["SCRIPT_NAME"] = environ.get("SCRIPT_NAME", "") + f"/sandbox/{sandbox_id}"
        scoped["PATH_INFO"] = "/" + rest[0]
        if scoped["PATH_INFO"].startswith("/static/"):
            return live_wsgi(scoped, start_response)

        with app.app_context():
            authorized = _live_admin(metadata["owner"]) is not None
        if not authorized:
            return Response(status=403)(environ, start_response)

        paths = paths_for(directory, sources)
        if not all(path.is_file() for path in paths.values()):
            return Response("Copia di prova non disponibile.", status=410)(environ, start_response)
        sandbox = app_factory(sandbox_db_paths=paths)
        _protect_sandbox(sandbox, metadata)

        def dispose():
            close_engines(sandbox)
            if scoped.get("avp.sandbox.discard"):
                shutil.rmtree(directory, ignore_errors=True)

        try:
            response = sandbox.wsgi_app(scoped, start_response)
        except Exception:
            dispose()
            raise
        if scoped.get("avp.sandbox.discard"):
            metadata_path.unlink(missing_ok=True)
        return ClosingIterator(response, dispose)

    app.wsgi_app = dispatch
