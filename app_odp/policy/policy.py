# app_odp/rbac/policy.py
from __future__ import annotations
from sqlalchemy import false, select, and_, or_, func, exists, cast, String
from dataclasses import dataclass
from functools import cached_property

from app_odp.models import (
    Famiglia,
    InputOdp,
    InputOdpRuntime,
    Lavorazioni,
    Macrofamiglia,
    Magazzini,
    Permissions,
    Reparti,
    Risorse,
    db,
    roles_famiglia,
    roles_ineritance,
    roles_lavorazioni,
    roles_macrofamiglia,
    roles_magazzini,
    roles_permission,
    roles_reparti,
    roles_risorse,
    user_roles,
)


def user_role_tree_cte(user_id: int):
    seed = select(user_roles.c.role_id.label("role_id")).where(
        user_roles.c.user_id == user_id
    )
    cte = seed.cte(name="user_role_tree", recursive=True)

    recursive = select(roles_ineritance.c.included_role.label("role_id")).join(
        cte, roles_ineritance.c.role_id == cte.c.role_id
    )

    # UNION evita duplicati ed è più robusto in caso di cicli
    return cte.union(recursive)


def _json_leaf_any_in(expr, allowed: set[str]):
    """
    Matcha se expr è JSON valido e contiene (a qualsiasi profondità) un valore in allowed.
    Gestisce bene JSON annidati tipo: [["20"]] o [["ASS"], ["COL"]]
    """
    if not allowed:
        return false()

    jt = func.json_tree(expr).table_valued("value", "type").alias("jt")
    return and_(
        func.json_valid(expr) == 1,
        exists(
            select(1)
            .select_from(jt)
            .where(
                jt.c.type.in_(("text", "integer", "real")),
                cast(jt.c.value, String).in_(allowed),
            )
        ),
    )


def _codes(allowed):
    if not allowed:
        return set()
    if isinstance(allowed, dict):
        allowed = allowed.keys()

    allowed = list(allowed)
    if not allowed:
        return set()

    first = allowed[0]
    # oggetti Reparti/Risorse/... (hanno attributo Codice)
    if hasattr(first, "Codice"):
        return {
            str(x.Codice) for x in allowed if getattr(x, "Codice", None) is not None
        }

    return {str(x) for x in allowed}


def _match(col, allowed):
    allowed = _codes(allowed)
    if not allowed:
        return false()
    return or_(col.in_(allowed), _json_leaf_any_in(col, allowed))


def _effective_user_subset(role_allowed, user_allowed) -> tuple[set[str], bool]:
    """
    Restituisce:
    - effective_set: insieme finale applicabile
    - enforce: True se questa dimensione va forzata a query
    """

    role_codes = _codes(role_allowed)
    if not role_codes:
        # il ruolo non limita questa dimensione => nessun filtro su questa dimensione
        return set(), False

    user_codes = _codes(user_allowed)
    if not user_codes:
        # nessun override utente => vale il ruolo
        return role_codes, True

    # override utente solo restrittivo
    return role_codes & user_codes, True


@dataclass(frozen=True)
class RbacPolicy:
    user: object  # current_user

    @cached_property
    def role_ids(self) -> set[int]:
        rt = user_role_tree_cte(self.user.id)
        stmt = select(rt.c.role_id)
        return set(db.session.execute(stmt).scalars().all())

    def can(self, perm_code: str) -> bool:
        stmt = (
            select(Permissions.id)
            .select_from(roles_permission)
            .join(Permissions, Permissions.id == roles_permission.c.permission_id)
            .where(
                roles_permission.c.role_id.in_(self.role_ids),
                Permissions.Codice == perm_code,
            )
            .limit(1)
        )
        return db.session.execute(stmt).first() is not None

    @cached_property
    def allowed_reparti(self) -> set[str]:
        stmt = (
            select(Reparti.Codice)
            .distinct()
            .select_from(roles_reparti)
            .join(Reparti, Reparti.id == roles_reparti.c.reparto_id)
            .where(roles_reparti.c.roles_id.in_(self.role_ids))
        )
        return set(db.session.execute(stmt).scalars().all())

    @cached_property
    def allowed_risorse(self) -> set[str]:
        stmt = (
            select(Risorse)
            .distinct()
            .select_from(roles_risorse)
            .join(Risorse, Risorse.id == roles_risorse.c.risorse_id)
            .where(roles_risorse.c.roles_id.in_(self.role_ids))
        )
        return set(db.session.execute(stmt).scalars().all())

    @cached_property
    def allowed_lavorazioni(self) -> set[str]:
        stmt = (
            select(Lavorazioni)
            .distinct()
            .select_from(roles_lavorazioni)
            .join(Lavorazioni, Lavorazioni.id == roles_lavorazioni.c.lavorazioni_id)
            .where(roles_lavorazioni.c.roles_id.in_(self.role_ids))
        )
        return set(db.session.execute(stmt).scalars().all())

    @cached_property
    def allowed_famiglia(self) -> set[str]:
        stmt = (
            select(Famiglia)
            .distinct()
            .select_from(roles_famiglia)
            .join(Famiglia, Famiglia.id == roles_famiglia.c.famiglia_id)
            .where(roles_famiglia.c.roles_id.in_(self.role_ids))
        )
        return set(db.session.execute(stmt).scalars().all())

    @cached_property
    def allowed_macrofamiglia(self) -> set[str]:
        stmt = (
            select(Macrofamiglia)
            .distinct()
            .select_from(roles_macrofamiglia)
            .join(
                Macrofamiglia,
                Macrofamiglia.id == roles_macrofamiglia.c.macrofamiglia_id,
            )
            .where(roles_macrofamiglia.c.roles_id.in_(self.role_ids))
        )
        return set(db.session.execute(stmt).scalars().all())

    @cached_property
    def allowed_magazzini(self) -> set[str]:
        stmt = (
            select(Magazzini)
            .distinct()
            .select_from(roles_magazzini)
            .join(Magazzini, Magazzini.id == roles_magazzini.c.magazzini_id)
            .where(roles_magazzini.c.roles_id.in_(self.role_ids))
        )
        return set(db.session.execute(stmt).scalars().all())

    @cached_property
    def allowed_reparti_descr(self) -> list[str]:
        stmt = (
            select(Reparti.Descrizione)
            .distinct()
            .select_from(roles_reparti)
            .join(Reparti, Reparti.id == roles_reparti.c.reparto_id)
            .where(roles_reparti.c.roles_id.in_(self.role_ids))
        )
        return db.session.scalars(stmt).all()

    def filter_input_odp(self, q):
        if self.can("odp.read_all"):
            return q

        q = q.outerjoin(
            InputOdpRuntime,
            and_(
                InputOdp.IdDocumento == InputOdpRuntime.IdDocumento,
                InputOdp.IdRiga == InputOdpRuntime.IdRiga,
            ),
        )

        conds = []

        # --- RBAC puro ---
        base_filters = [
            (InputOdp.CodReparto, self.allowed_reparti),
            (InputOdpRuntime.RisorsaAttiva, self.allowed_risorse),
            (InputOdpRuntime.LavorazioneAttiva, self.allowed_lavorazioni),
            (InputOdp.CodFamiglia, self.allowed_famiglia),
            (InputOdp.CodMacrofamiglia, self.allowed_macrofamiglia),
            (InputOdp.CodMagPrincipale, self.allowed_magazzini),
        ]

        for col, allowed in base_filters:
            if allowed:
                conds.append(_match(col, allowed))

        # --- RBAC + ABAC utente: RISORSE ---
        effective_risorse, enforce_risorse = _effective_user_subset(
            self.allowed_risorse,
            self.user_allowed_risorse,
        )
        if enforce_risorse:
            if not effective_risorse:
                return q.filter(false())
            conds.append(_match(InputOdpRuntime.RisorsaAttiva, effective_risorse))

        # --- RBAC + ABAC utente: LAVORAZIONI ---
        effective_lavorazioni, enforce_lavorazioni = _effective_user_subset(
            self.allowed_lavorazioni,
            self.user_allowed_lavorazioni,
        )
        if enforce_lavorazioni:
            if not effective_lavorazioni:
                return q.filter(false())
            conds.append(
                _match(InputOdpRuntime.LavorazioneAttiva, effective_lavorazioni)
            )

        if not conds:
            return q

        return q.filter(*conds)

    @cached_property
    def allowed_reparti_menu(self) -> list[tuple[str, str]]:
        stmt = (
            select(Reparti.Codice, Reparti.Descrizione)
            .distinct()
            .select_from(roles_reparti)
            .join(Reparti, Reparti.id == roles_reparti.c.reparto_id)
            .where(roles_reparti.c.roles_id.in_(self.role_ids))
            .order_by(Reparti.Codice)
        )
        return list(db.session.execute(stmt).all())

    def filter_input_odp_for_reparto(self, q, reparto_code: str):
        """
        Applica prima il filtro policy generale, poi restringe al reparto/tab richiesto.
        Usa _match così resta compatibile con colonne salvate come JSON-like.
        """
        q = self.filter_input_odp(q)
        return q.filter(_match(InputOdp.CodReparto, {str(reparto_code)}))

    @cached_property
    def user_allowed_lavorazioni(self) -> set[str]:
        return {
            str(x.Codice)
            for x in (getattr(self.user, "lavorazioni", None) or [])
            if getattr(x, "Codice", None) is not None
        }

    @cached_property
    def user_allowed_risorse(self) -> set[str]:
        return {
            str(x.Codice)
            for x in (getattr(self.user, "risorse", None) or [])
            if getattr(x, "Codice", None) is not None
        }

    @cached_property
    def effective_allowed_lavorazioni(self) -> set[str]:
        effective, _ = _effective_user_subset(
            self.allowed_lavorazioni,
            self.user_allowed_lavorazioni,
        )
        return effective

    @cached_property
    def effective_allowed_risorse(self) -> set[str]:
        effective, _ = _effective_user_subset(
            self.allowed_risorse,
            self.user_allowed_risorse,
        )
        return effective
