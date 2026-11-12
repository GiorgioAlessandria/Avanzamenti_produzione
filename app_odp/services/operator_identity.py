from flask import g, has_app_context
from sqlalchemy import and_, or_

from app_odp.models import User
from app_odp.operator_session import active_user


def display_username(public_id, legacy_username=None):
    if not public_id:
        return legacy_username
    if not has_app_context():
        return legacy_username or public_id
    if "operator_names" not in g:
        g.operator_names = dict(User.query.with_entities(User.public_id, User.username).all())
    return g.operator_names.get(public_id) or legacy_username or public_id


def order_log_actor(legacy_field, username):
    user = active_user() if has_app_context() else None
    public_id = getattr(user, "public_id", None) if getattr(user, "username", None) == username else None
    return {"ActorPublicId": public_id, legacy_field: None if public_id else username}


def encode_payload_operators(payload):
    for field in ("utente", "operatore_pre", "operatore_post", "operatore_in_carico"):
        username = payload.get(field)
        if not username:
            continue
        public_id = order_log_actor(field, username)["ActorPublicId"]
        if not public_id and has_app_context():
            if "operator_public_ids" not in g:
                g.operator_public_ids = dict(
                    User.query.with_entities(User.username, User.public_id).all()
                )
            public_id = g.operator_public_ids.get(username)
        if public_id:
            payload[field + "_public_id"] = public_id
            del payload[field]
    return payload


def actor_matches(model, legacy_column, username, *, exact=False):
    names = User.username == username if exact else User.username.icontains(username, autoescape=True)
    ids = [public_id for public_id, in User.query.with_entities(User.public_id).filter(names)]
    legacy_match = legacy_column == username if exact else legacy_column.icontains(username, autoescape=True)
    return or_(model.ActorPublicId.in_(ids), and_(model.ActorPublicId.is_(None), legacy_match))
