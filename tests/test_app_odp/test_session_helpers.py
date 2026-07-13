from types import SimpleNamespace

import pytest

from app_odp.services import session_helpers


@pytest.mark.parametrize(
    ("user", "expected"),
    [
        (SimpleNamespace(username="mario", name="Mario Rossi", email="mario@example.test", id=7), "mario"),
        (SimpleNamespace(username="", name="Mario Rossi", email="mario@example.test", id=7), "Mario Rossi"),
        (SimpleNamespace(username="", name="", email="mario@example.test", id=7), "mario@example.test"),
        (SimpleNamespace(username="", name="", email="", id=7), "7"),
        (SimpleNamespace(username="", name="", email=""), "utente_sconosciuto"),
    ],
)
def test_current_username_uses_first_available_identity(monkeypatch, user, expected):
    monkeypatch.setattr(session_helpers, "active_user", lambda: user)

    assert session_helpers._current_username() == expected


def test_current_user_id_returns_active_user_id(monkeypatch):
    monkeypatch.setattr(session_helpers, "active_user", lambda: SimpleNamespace(id=42))

    assert session_helpers._current_user_id() == 42


def test_current_user_id_uses_default_when_id_is_missing(monkeypatch):
    monkeypatch.setattr(session_helpers, "active_user", lambda: SimpleNamespace())

    assert session_helpers._current_user_id(default=99) == 99
