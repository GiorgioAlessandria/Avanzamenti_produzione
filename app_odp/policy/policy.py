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
    Roles,
    User,
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


def _norm_text(value) -> str:
    return str(value or "").strip()


def _norm_role_name(value) -> str:
    return _norm_text(value).lower()


PROTECTED_ROLE_NAMES = {
    "responsabile_produzione",
}


@dataclass(frozen=True)
class RbacPolicy:
    user: object  # current_user

    @cached_property
    def role_ids(self) -> set[int]:
        rt = user_role_tree_cte(self.user.id)
        stmt = select(rt.c.role_id)
        return set(db.session.execute(stmt).scalars().all())

    def can(self, perm: str | int) -> bool:
        if perm is None:
            return False

        raw = str(perm).strip()
        if not raw:
            return False

        stmt = (
            select(Permissions.id)
            .select_from(roles_permission)
            .join(Permissions, Permissions.id == roles_permission.c.permission_id)
            .where(roles_permission.c.role_id.in_(self.role_ids))
        )

        try:
            perm_id = int(raw)
        except (TypeError, ValueError):
            perm_id = None

        if perm_id is not None:
            stmt = stmt.where(
                or_(
                    Permissions.id == perm_id,
                    Permissions.Codice == raw,
                )
            )
        else:
            stmt = stmt.where(Permissions.Codice == raw)

        stmt = stmt.limit(1)
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

    @cached_property
    def descendant_manageable_roles(self) -> list[Roles]:
        out = {}

        for role in self.direct_assigned_roles:
            for managed in getattr(role, "iter_manageable_roles", lambda: [])():
                if managed is None:
                    continue

                managed_id = int(managed.id)

                # escludi solo i ruoli assegnati direttamente all'utente
                if managed_id in self.direct_assigned_role_ids:
                    continue

                out[managed_id] = managed

        return sorted(
            out.values(),
            key=lambda r: (
                (r.description or r.name or "").lower(),
                (r.name or "").lower(),
                r.id,
            ),
        )

    @cached_property
    def descendant_manageable_role_ids(self) -> set[int]:
        return {int(role.id) for role in self.descendant_manageable_roles}

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

    @cached_property
    def can_view_user_abac_section(self) -> bool:
        """
        Sezione ABAC:
        - serve il permesso impostazioni_utente
        - serve anche uno scope gestionale reale
        """
        return self.can("impostazioni_utente") and self.user.has_management_scope()

    @cached_property
    def can_view_role_assignment_section(self) -> bool:
        """
        Sezione assegnazione ruoli:
        - basta il permesso dedicato assegnazione_ruoli
        - nel tuo DB corrisponde alla permission id 14
        """
        return self.can("assegnazione_ruoli")

    @cached_property
    def role_assignment_manageable_role_ids(self) -> set[int]:
        if not self.can_view_role_assignment_section:
            return set()

        return set(self.descendant_manageable_role_ids)

    @cached_property
    def can_view_role_links_section(self) -> bool:
        return self.can("modifica_permessi_ruolo")

    @cached_property
    def can_view_role_permission_section(self) -> bool:
        return self.can("modifica_permessi_ruolo")

    @cached_property
    def direct_assigned_roles(self) -> list[Roles]:
        return list(getattr(self.user, "roles", None) or [])

    @cached_property
    def direct_assigned_role_ids(self) -> set[int]:
        return {int(role.id) for role in self.direct_assigned_roles}

    def abac_manageable_roles(self) -> list[Roles]:
        if not self.can_view_user_abac_section:
            return []

        return list(self.descendant_manageable_roles)

    def role_assignment_roles_query(self):
        manageable_ids = self.role_assignment_manageable_role_ids
        if not manageable_ids:
            return Roles.query.filter(false())

        return Roles.query.filter(Roles.id.in_(manageable_ids)).filter(
            func.lower(Roles.name) != "responsabile_produzione"
        )

    def role_assignment_users_query(self):
        manageable_ids = self.role_assignment_manageable_role_ids
        if not manageable_ids:
            return User.query.filter(false())

        ur_allowed = user_roles.alias("ur_allowed")
        ur_forbidden = user_roles.alias("ur_forbidden")

        allowed_exists = exists(
            select(1)
            .select_from(ur_allowed)
            .where(
                and_(
                    ur_allowed.c.user_id == User.id,
                    ur_allowed.c.role_id.in_(manageable_ids),
                )
            )
        )

        forbidden_exists = exists(
            select(1)
            .select_from(ur_forbidden)
            .where(
                and_(
                    ur_forbidden.c.user_id == User.id,
                    ~ur_forbidden.c.role_id.in_(manageable_ids),
                )
            )
        )

        return (
            User.query.filter(User.active.is_(True))
            .filter(User.id != self.user.id)
            .filter(allowed_exists)
            .filter(~forbidden_exists)
        )

    def can_manage_target_user(self, target_user: User | None) -> bool:
        if not self.can_view_role_assignment_section:
            return False

        if target_user is None:
            return False

        if int(target_user.id) == int(self.user.id):
            return False

        target_roles = list(getattr(target_user, "roles", None) or [])
        if not target_roles:
            return False

        return all(self.can_manage_target_role(role) for role in target_roles)

    def can_assign_target_role(self, target_role: Roles | None) -> bool:
        if not self.can_view_role_assignment_section:
            return False

        if target_role is None:
            return False

        if _norm_role_name(target_role.name) == "responsabile_produzione":
            return False

        return self.can_manage_target_role(target_role)

    def permission_manageable_roles(self):
        if not self.can_view_role_permission_section:
            return []

        return list(self.descendant_manageable_roles)

    def role_link_manageable_roles(self):
        if not self.can_view_role_links_section:
            return []

        return list(self.descendant_manageable_roles)

    def permission_manageable_permissions(self):
        if not self.can_view_role_permission_section:
            return []

        forbidden_codes = {"admin"}

        stmt = select(Permissions).order_by(
            func.lower(func.coalesce(Permissions.Descrizione, Permissions.Codice)),
            func.lower(Permissions.Codice),
        )
        perms = db.session.execute(stmt).scalars().all()

        return [
            p for p in perms if (p.Codice or "").strip().lower() not in forbidden_codes
        ]

    def can_manage_target_role(self, target_role: Roles | None) -> bool:
        if target_role is None:
            return False

        return int(target_role.id) in self.descendant_manageable_role_ids
