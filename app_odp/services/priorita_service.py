# app_odp/services/priorita_service.py

from flask import abort
from sqlalchemy import func, select

from app_odp.models import (
    db,
    Roles,
    User,
    user_roles,
)

from app_odp.operator_session import active_user

from app_odp.routes import (
    PRIORITA_HIDDEN_ROLE_NAMES,
    _current_policy,
)


def _priorita_hidden_user_ids() -> set[int]:
    """
    Utenti da non mostrare nella gestione priorità.
    Esempio: admin.
    """
    hidden_role_names = {name.lower() for name in PRIORITA_HIDDEN_ROLE_NAMES}

    rows = (
        db.session.query(User.id)
        .join(user_roles, user_roles.c.user_id == User.id)
        .join(Roles, Roles.id == user_roles.c.role_id)
        .filter(func.lower(Roles.name).in_(hidden_role_names))
        .all()
    )

    return {int(row[0]) for row in rows}


def _priorita_manageable_role_ids_for_user(user: User) -> set[int]:
    """
    Restituisce tutti i ruoli sottostanti gestibili dall'utente,
    usando la gerarchia roles_manageable_roles.

    Non include i ruoli dell'utente stesso.
    """
    out: set[int] = set()

    for role in getattr(user, "roles", None) or []:
        for managed_role in getattr(role, "iter_manageable_roles", lambda: [])():
            if managed_role is not None and managed_role.id is not None:
                out.add(int(managed_role.id))

    return out


def _priorita_visible_operator_ids_for_current_user() -> set[int]:
    """
    Operatori visibili nella pagina modifica priorità.

    Regole:
    - chi ha priorita_tutti_operatori vede tutti gli utenti attivi,
      tranne se stesso e tranne gli admin
    - gli altri vedono se stessi + utenti sottostanti nella gerarchia roles_manageable_roles
    - gli admin non vengono mai mostrati
    """
    user = active_user()
    policy = _current_policy()

    hidden_user_ids = _priorita_hidden_user_ids()

    if policy.can("priorita_tutti_operatori"):
        return {
            int(user_id)
            for user_id in db.session.execute(
                select(User.id)
                .where(User.active.is_(True))
                .where(User.id != user.id)
                .where(~User.id.in_(hidden_user_ids))
            )
            .scalars()
            .all()
        }

    visible_ids: set[int] = {int(user.id)}

    manageable_role_ids = _priorita_manageable_role_ids_for_user(user)

    if manageable_role_ids:
        users_with_managed_roles = set(
            db.session.execute(
                select(user_roles.c.user_id).where(
                    user_roles.c.role_id.in_(manageable_role_ids)
                )
            )
            .scalars()
            .all()
        )

        users_with_not_managed_roles = set(
            db.session.execute(
                select(user_roles.c.user_id).where(
                    ~user_roles.c.role_id.in_(manageable_role_ids)
                )
            )
            .scalars()
            .all()
        )

        visible_ids.update(
            int(user_id)
            for user_id in users_with_managed_roles - users_with_not_managed_roles
        )

    visible_ids.difference_update(hidden_user_ids)

    return visible_ids


def _get_priorita_visible_operatore_or_403(operatore_id: int) -> User:
    """
    Recupera l'operatore solo se è visibile all'utente corrente.
    Serve per proteggere anche le chiamate manuali agli endpoint.
    """
    try:
        operatore_id = int(operatore_id)
    except (TypeError, ValueError):
        abort(404)

    visible_ids = _priorita_visible_operator_ids_for_current_user()

    if operatore_id not in visible_ids:
        abort(403)

    operatore = User.query.filter(
        User.id == operatore_id,
        User.active.is_(True),
    ).first()

    if operatore is None:
        abort(404)

    return operatore
