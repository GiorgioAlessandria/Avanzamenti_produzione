import re
import unittest
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlencode

from jinja2 import ChoiceLoader, DictLoader, Environment, FileSystemLoader


class VenditeTabsTest(unittest.TestCase):
    def test_tabs_labels_and_customer_order_cards(self):
        templates = Path(__file__).resolve().parents[2] / "app_odp/templates"
        env = Environment(loader=ChoiceLoader([
            DictLoader({"base.j2": "{% block styles %}{% endblock %}{% block content %}{% endblock %}{% block extra_js %}{% endblock %}"}),
            FileSystemLoader(templates),
        ]))
        env.globals["operator_url_for"] = lambda endpoint, **args: "/" + endpoint + "?" + urlencode({**args, "tab_session": "operatore-test"})
        expected = ["Ordini cliente", "Situazione produzione per matricola",
                    "Localizzazione ordini", "Note per imballo",
                    "Situazione produzione per modello"]
        cases = [("vendite_assegnazioni.j2", "ordini", "vendite-orders-tab"),
                 ("vendite_assegnazioni.j2", "imballaggio", "vendite-packaging-tab"),
                 ("vendite.j2", "matricola", "vendite-matricole-pane"),
                 ("vendite.j2", "modello", "vendite-modelli-pane")]
        for name, view, pane in cases:
            with self.subTest(template=name, view=view):
                html = env.get_template(name).render(request=SimpleNamespace(args={"vista": view}))
                if name == "vendite.j2":
                    self.assertIn('aria-label="Ordina per Note per produzione"', html)
                    self.assertIn('aria-label="Ordina per Note per imballo"', html)
                    self.assertIn('aria-label="Ordina per Opzione"', html)
                    self.assertIn('data-machine-sort="shipping_date"', html)
                    self.assertIn("(!savingOptionNote && !canChangeMachineView())", html)
                    self.assertIn('colspan="13"', html)
                else:
                    self.assertIn("const orderClass = order.packaged", html)
                    self.assertIn("const rowClass = row.packaged", html)
                    self.assertNotIn('id="vendite-inline-customer-data"', html)
                    self.assertNotIn("Cliente nelle righe (prova)", html)
                    self.assertNotIn("inlineCustomerData", html)
                    self.assertIn('>N. ordine</th>', html)
                    self.assertIn('order.managed ? "Gestionale" : "Manuale"', html)
                    self.assertIn("const columnCount = 13", html)
                    self.assertIn("productionNoteCell(row)", html)
                    self.assertIn("productionNoteCell(machine)", html)
                    self.assertIn('formatDateTime(audit.changed_at, "short")', html)
                    self.assertIn("<th>Opzionamento</th>", html)
                    self.assertIn('machine.option?.optioned_by_name', html)
                    self.assertIn('machine.option?.note', html)
                    self.assertIn("const rowClass = machine.option", html)
                    self.assertIn("if (!editable)", html)
                    self.assertIn("noteContent(row[field])", html)
                    self.assertIn("canEditOrderDetails ?", html)
                    self.assertIn("d-flex flex-nowrap align-items-start gap-2", html)
                    self.assertIn('${save}${deleteRow}', html)
                    self.assertIn('class="flex-grow-1">${packaging}', html)
                    self.assertNotIn('id="vendite-customer-order"', html)
                    self.assertNotIn("customer_order: customerOrder.value", html)
                    self.assertIn("vendite-note-expand-label", html)
                    self.assertNotIn("Macchina completata:", html)
                    self.assertIn("data-choose-delete-row", html)
                    self.assertIn("data-delete-row-url-template", html)
                    self.assertIn("rowActions(row, order)", html)
                    self.assertRegex(html, r'Note di produzione</th>\s*<th[^>]*>Note per produzione</th>')
                    self.assertNotIn("Ogni riga corrisponde", html)
                    self.assertNotIn("vendite-customer-grouping-description", html)
                    self.assertIn('formatDateTime(row.packaging?.confirmed_at, "short")', html)
                    self.assertIn('colspan="10"', html)
                nav = re.search(r'<ul class="nav nav-tabs[^>]*>(.*?)</ul>', html, re.S)[1]
                labels = re.findall(r'<(?:button|a)\b[^>]*>(.*?)</(?:button|a)>', nav, re.S)
                self.assertEqual([label.strip() for label in labels], expected)
                active = re.findall(r'<button\b([^>]*aria-selected="true"[^>]*)>', nav, re.S)
                self.assertEqual(len(active), 1)
                self.assertIn(f'data-bs-target="#{pane}"', active[0])
                self.assertRegex(html, rf'class="tab-pane[^\"]*show active"\s+id="{pane}"')
                for link in re.findall(r'href="([^"]+)"', nav):
                    self.assertIn("tab_session=operatore-test", link)
                if name == "vendite_assegnazioni.j2":
                    for removed in ("vendite-open-machines", "vendite-total-demand",
                                    "vendite-assigned-demand", "vendite-unassigned-demand", "renderSummary("):
                        self.assertNotIn(removed, html)
                    self.assertIn('id="vendite-create-order-panel"', html)
                    self.assertIn('id="vendite-customer-order-form"', html)
                    self.assertRegex(html, r'Note di vendita</th>\s*<th>Note commerciali</th>')
                    self.assertIn('noteTextarea(row, "commercial_note", "Note commerciali", canEditSalesNotes)', html)
                    self.assertIn('payload.commercial_note = value("commercial_note")', html)
                    self.assertIn('class="form-control vendite-line-commercial-note"', html)
                    self.assertNotIn("Note di imballaggio", html)
                    self.assertIn("<th>Note per imballo</th>", html)
                    self.assertNotRegex(html, r"(?i)consegna")
                    self.assertIn("Data di spedizione", html)
                    self.assertIn('dateInput(row, "delivery_date"', html)
                    self.assertIn("delivery_date: row.querySelector", html)
        for allowed in (False, True):
            html = env.get_template("vendite.j2").render(
                request=SimpleNamespace(args={"vista": "matricola"}), can_manage_groups=allowed,
            )
            self.assertEqual('id="vendite-group-manager"' in html, allowed)
            self.assertIn('id="vendite-group-filters"', html)

        html = env.get_template("vendite.j2").render(
            request=SimpleNamespace(args={"vista": "matricola"}),
            can_manage_sales_priorities=True,
        )
        self.assertIn('id="vendite-priority-mode"', html)
        self.assertIn('id="vendite-priority-operator"', html)
        self.assertIn('data-priority-level', html)
        self.assertNotIn('data-priority-position', html)
        self.assertNotIn('Priorità / posizione', html)
        self.assertIn('data-can-manage-sales-priorities="true"', html)

    def test_packaging_user_can_view_customer_orders(self):
        from app_odp.routes_modules.vendite import _can_view_customer_orders

        policy = SimpleNamespace(
            has_direct_admin_role=False,
            can=lambda permission: permission == "utente_imballi",
        )

        self.assertTrue(_can_view_customer_orders(policy))


if __name__ == "__main__":
    unittest.main()
