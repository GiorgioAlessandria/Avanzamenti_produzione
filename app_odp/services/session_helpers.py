from app_odp.operator_session import active_user


def _current_username(default: str = "utente_sconosciuto") -> str:
    user = active_user()

    return (
        getattr(user, "username", None)
        or getattr(user, "name", None)
        or getattr(user, "email", None)
        or str(getattr(user, "id", default))
    )


def _current_user_id(default: int | None = None):
    user = active_user()
    return getattr(user, "id", default)
