import hashlib

from flask import Flask, g

from app_odp.operator_session import active_token, get_operator_token, hash_token, new_token


def test_hash_token_returns_sha256_hex_digest():
    assert hash_token("abc") == hashlib.sha256(b"abc").hexdigest()


def test_new_token_returns_non_empty_urlsafe_string():
    token = new_token()

    assert isinstance(token, str)
    assert len(token) >= 48


def test_get_operator_token_prefers_query_then_form_then_header():
    app = Flask("operator_session_test")

    with app.test_request_context("/?tab_session=query-token", headers={"X-Tab-Session": "header-token"}):
        assert get_operator_token() == "query-token"

    with app.test_request_context(
        "/",
        method="POST",
        data={"tab_session": " form-token "},
        headers={"X-Tab-Session": "header-token"},
    ):
        assert get_operator_token() == "form-token"

    with app.test_request_context("/", headers={"X-Tab-Session": " header-token "}):
        assert get_operator_token() == "header-token"


def test_active_token_reads_operator_token_from_flask_g():
    app = Flask("operator_session_test")

    with app.app_context():
        assert active_token() == ""
        g.operator_token = "token-1"
        assert active_token() == "token-1"

