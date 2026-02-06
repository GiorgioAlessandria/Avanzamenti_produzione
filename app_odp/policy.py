from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property

from sqlalchemy import false, select

from models import (
    InputOdp,
    Permissions,
    Reparti,
    roles_permission,
    roles_reparti,
)


@dataclass(frozen=True)
class RbacPolicy:
    session: object
    user: object  # User

    @cached_property
    def role_ids(self) -> set[int]:
        # usa la tua funzione già esistente (include roles_ineritance)
        return self.user.effective_role_ids()

    def can(self, perm_code: str) -> bool:
        """
        Permission check *con ereditarietà* (role_ids).
        """
        stmt = (
            select(Permissions.id)
            .select_from(roles_permission)
            .join(Permissions, Permissions.id == roles_permission.c.permission_id)
            .where(
                roles_permission.c.role_id.in_(self.role_ids),
                Permissions.code == perm_code,
            )
            .limit(1)
        )
        return self.session.execute(stmt).first() is not None

    @cached_property
    def allowed_reparti_codes(self) -> set[str]:
        stmt = (
            select(Reparti.codice)
            .distinct()
            .select_from(roles_reparti)
            .join(Reparti, Reparti.id == roles_reparti.c.reparto_id)
            .where(roles_reparti.c.roles_id.in_(self.role_ids))
        )
        return set(self.session.execute(stmt).scalars().all())

    def filter_input_odp(self, q):
        """
        Applica lo scope al dataset ODP.

        Regola “safe by default”:
        - se non hai odp.read => 403 nella route (non qui)
        - se non hai odp.read_all => devi avere almeno 1 reparto assegnato,
          altrimenti ritorni 0 righe.
        """
        if self.can("odp.read_all"):
            return q

        reparti = self.allowed_reparti_codes
        if not reparti:
            return q.filter(false())

        return q.filter(InputOdp.CodReparto.in_(reparti))
