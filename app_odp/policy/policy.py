# app_odp/rbac/policy.py
from __future__ import annotations
from sqlalchemy import false, select, and_, or_, func, exists, cast, String, case
from dataclasses import dataclass
from functools import cached_property
import json
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
    users_famiglia,
    Roles,
    User,
    HomeRepartoConfig,
    HomeVisibilityRule,
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


def _phase_text(value) -> str:
    raw = _norm_text(value)
    if not raw:
        return ""

    try:
        n = int(float(raw))
        if n > 0:
            return str(n)
    except (TypeError, ValueError):
        pass

    return raw


def _json_text_list(value) -> list[str]:
    raw = _norm_text(value)
    if not raw:
        return []

    try:
        parsed = json.loads(raw)
    except Exception:
        parsed = raw

    if not isinstance(parsed, list):
        parsed = [parsed]

    out = []
    for item in parsed:
        value = _phase_text(item)
        if value:
            out.append(value)

    return list(dict.fromkeys(out))


def _home_config_reparto_code(config) -> str:
    reparto = getattr(config, "reparto", None)
    return _norm_text(getattr(reparto, "Codice", ""))


def _runtime_join_condition():
    return and_(
        InputOdp.IdDocumento == InputOdpRuntime.IdDocumento,
        InputOdp.IdRiga == InputOdpRuntime.IdRiga,
    )


def _stato_ordine_expr():
    return func.lower(
        func.coalesce(
            InputOdpRuntime.Stato_odp,
            InputOdp.StatoOrdineErp,
            "Pianificata",
        )
    )


def _fase_attiva_expr():
    return cast(func.coalesce(InputOdpRuntime.FaseAttiva, "1"), String)


def _first_phase_expr():
    return cast(
        case(
            (
                func.json_valid(InputOdp.NumFase) == 1,
                func.json_extract(InputOdp.NumFase, "$[0]"),
            ),
            else_="1",
        ),
        String,
    )


def _last_phase_expr():
    return cast(
        case(
            (
                func.json_valid(InputOdp.NumFase) == 1,
                func.json_extract(InputOdp.NumFase, "$[#-1]"),
            ),
            else_=InputOdp.NumFase,
        ),
        String,
    )


def _machine_row_predicate():
    return func.lower(func.coalesce(InputOdp.GestioneMatricola, "")) == "si"


def _semilavorato_row_predicate():
    return func.lower(func.coalesce(InputOdp.GestioneMatricola, "")) == "no"


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
    def can_view_home_config_section(self) -> bool:
        """
        Sezione configurazione home reparti.

        Non usa nomi ruolo hardcoded.
        Il controllo passa dal permesso RBAC 'configurazione_home'.
        """
        return self.can("configurazione_home")

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

    @cached_property
    def can_view_role_creation_section(self) -> bool:
        return self.can("modifica_permessi_ruolo")

    def role_creation_manageable_roles(self) -> list[Roles]:
        if not self.can_view_role_creation_section:
            return []

        return list(self.descendant_manageable_roles)

    @cached_property
    def can_view_role_delete_section(self) -> bool:
        return self.can("modifica_permessi_ruolo")

    @cached_property
    def user_allowed_famiglia(self) -> set[str]:
        return {
            str(x.Codice)
            for x in (getattr(self.user, "famiglie", None) or [])
            if getattr(x, "Codice", None) is not None
        }

    @cached_property
    def effective_allowed_famiglia(self) -> set[str]:
        effective, _ = _effective_user_subset(
            self.allowed_famiglia,
            self.user_allowed_famiglia,
        )
        return effective

    def filter_montaggio_macchine_famiglia_rows(self, rows):
        """
        Filtro ABAC utente per home montaggio.

        Regole:
        - si applica solo agli utenti con permission filtro_macchine;
        - si applica solo agli ordini macchina: GestioneMatricola == 'si';
        - se non sono state selezionate famiglie utente, non filtra nulla;
        - gli ordini semilavorati restano sempre invariati.
        """
        rows = list(rows or [])

        if not self.can("filtro_macchine"):
            return rows

        allowed_famiglia = {
            str(x).strip() for x in self.user_allowed_famiglia if str(x).strip()
        }

        if not allowed_famiglia:
            return rows

        filtered = []

        for ordine in rows:
            gestione_matricola = _norm_text(
                getattr(ordine, "GestioneMatricola", "")
            ).lower()

            if gestione_matricola != "si":
                filtered.append(ordine)
                continue

            cod_famiglia = _norm_text(getattr(ordine, "CodFamiglia", ""))

            if cod_famiglia in allowed_famiglia:
                filtered.append(ordine)

        return filtered

    def role_delete_manageable_roles(self) -> list[Roles]:
        if not self.can_view_role_delete_section:
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

    def _home_visibility_rules_for_config(
        self,
        config: HomeRepartoConfig,
        user=None,
    ) -> tuple[list[HomeVisibilityRule], list[HomeVisibilityRule]]:
        """
        Restituisce:
        - regole di ruolo compatibili con i ruoli effettivi dell'utente;
        - regole specifiche utente.

        Le regole utente sono sempre ulteriormente restrittive.
        """

        user = user or self.user
        user_id = getattr(user, "id", None)

        base = HomeVisibilityRule.query.filter(
            HomeVisibilityRule.attivo.is_(True),
            HomeVisibilityRule.reparto_id == config.reparto_id,
        )

        role_rules = (
            base.filter(
                HomeVisibilityRule.role_id.in_(self.role_ids),
                HomeVisibilityRule.user_id.is_(None),
            )
            .order_by(HomeVisibilityRule.id.asc())
            .all()
        )

        user_rules = []
        if user_id is not None:
            user_rules = (
                base.filter(HomeVisibilityRule.user_id == int(user_id))
                .order_by(HomeVisibilityRule.id.asc())
                .all()
            )

        return role_rules, user_rules

    def _phase_condition_for_home_rule(self, rule: HomeVisibilityRule):
        mode = _norm_text(rule.phase_mode).lower() or "all"

        if mode == "all":
            return None

        fase_expr = _fase_attiva_expr()

        if mode == "exact":
            values = _json_text_list(rule.phase_values)
            if not values:
                return None
            return fase_expr.in_(values)

        if mode == "list":
            values = _json_text_list(rule.phase_values)
            if not values:
                return None
            return fase_expr.in_(values)

        if mode == "last":
            return fase_expr == _last_phase_expr()

        if mode == "not_first":
            return fase_expr != _first_phase_expr()

        return None

    def _apply_to_predicate_for_home_rule(self, apply_to: str):
        apply_to = _norm_text(apply_to).lower() or "macchine"

        if apply_to == "macchine":
            return _machine_row_predicate()

        if apply_to == "semilavorati":
            return _semilavorato_row_predicate()

        return None

    def _predicate_for_home_rules(self, rules: list[HomeVisibilityRule]):
        """
        Combina le regole per scope.

        Esempio:
        - apply_to='macchine', phase_mode='exact', phase_values='["2"]'
          significa:
          lascia passare i semilavorati,
          ma sulle macchine richiede FaseAttiva = 2.
        """

        grouped: dict[str, list] = {}

        for rule in rules or []:
            phase_cond = self._phase_condition_for_home_rule(rule)
            if phase_cond is None:
                continue

            apply_to = _norm_text(rule.apply_to).lower() or "macchine"
            grouped.setdefault(apply_to, []).append(phase_cond)

        if not grouped:
            return None

        final_conds = []

        for apply_to, phase_conds in grouped.items():
            if not phase_conds:
                continue

            phase_predicate = (
                or_(*phase_conds) if len(phase_conds) > 1 else phase_conds[0]
            )

            target_predicate = self._apply_to_predicate_for_home_rule(apply_to)

            if target_predicate is None:
                final_conds.append(phase_predicate)
            else:
                final_conds.append(
                    or_(
                        ~target_predicate,
                        phase_predicate,
                    )
                )

        if not final_conds:
            return None

        return and_(*final_conds)

    def filter_input_odp_for_home_config(
        self,
        q,
        config: HomeRepartoConfig,
        user=None,
    ):
        """
        Filtro unico per home reparto.

        Applica:
        - reparto da HomeRepartoConfig;
        - stato diverso da Chiusa;
        - RBAC ruolo su risorse/lavorazioni/famiglia/macrofamiglia/magazzini;
        - ABAC utente restrittivo su risorse/lavorazioni;
        - ABAC famiglia macchina se permission filtro_macchine;
        - HomeVisibilityRule per fase attiva.
        """

        user = user or self.user
        reparto_code = _home_config_reparto_code(config)

        if not reparto_code:
            return q.filter(false())

        # La home deve sempre restare nello scope dei reparti consentiti.
        # Anche se l'utente ha permessi larghi, la vista reparto resta vincolata al config.
        if reparto_code not in self.allowed_reparti and not self.can("odp.read_all"):
            return q.filter(false())

        q = q.outerjoin(
            InputOdpRuntime,
            _runtime_join_condition(),
        )

        # Reparto della home.
        q = q.filter(_match(InputOdp.CodReparto, {reparto_code}))

        # Stato: la home deve mostrare tutto tranne Chiusa.
        q = q.filter(_stato_ordine_expr() != "chiusa")

        # RBAC puro su dimensioni operative.
        base_filters = [
            (InputOdpRuntime.RisorsaAttiva, self.allowed_risorse),
            (InputOdpRuntime.LavorazioneAttiva, self.allowed_lavorazioni),
            (InputOdp.CodFamiglia, self.allowed_famiglia),
            (InputOdp.CodMacrofamiglia, self.allowed_macrofamiglia),
            (InputOdp.CodMagPrincipale, self.allowed_magazzini),
        ]

        for col, allowed in base_filters:
            if allowed:
                q = q.filter(_match(col, allowed))

        # ABAC utente restrittivo: risorse.
        effective_risorse, enforce_risorse = _effective_user_subset(
            self.allowed_risorse,
            self.user_allowed_risorse,
        )
        if enforce_risorse:
            if not effective_risorse:
                return q.filter(false())
            q = q.filter(_match(InputOdpRuntime.RisorsaAttiva, effective_risorse))

        # ABAC utente restrittivo: lavorazioni.
        effective_lavorazioni, enforce_lavorazioni = _effective_user_subset(
            self.allowed_lavorazioni,
            self.user_allowed_lavorazioni,
        )
        if enforce_lavorazioni:
            if not effective_lavorazioni:
                return q.filter(false())
            q = q.filter(
                _match(InputOdpRuntime.LavorazioneAttiva, effective_lavorazioni)
            )

        # ABAC famiglia macchina.
        # Mantiene la regola attuale: si applica solo a GestioneMatricola = si
        # e solo se l'utente ha permission filtro_macchine.
        if self.can("filtro_macchine"):
            user_famiglie = {
                _norm_text(x) for x in self.user_allowed_famiglia if _norm_text(x)
            }

            if user_famiglie:
                q = q.filter(
                    or_(
                        ~_machine_row_predicate(),
                        _match(InputOdp.CodFamiglia, user_famiglie),
                    )
                )

        # HomeVisibilityRule:
        # 1. prima regole ruolo;
        # 2. poi regole utente, che restringono ulteriormente.
        role_rules, user_rules = self._home_visibility_rules_for_config(
            config,
            user=user,
        )

        role_predicate = self._predicate_for_home_rules(role_rules)
        if role_predicate is not None:
            q = q.filter(role_predicate)

        user_predicate = self._predicate_for_home_rules(user_rules)
        if user_predicate is not None:
            q = q.filter(user_predicate)

        return q
