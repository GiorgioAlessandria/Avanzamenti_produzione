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
                    "Note per imballo", "Situazione produzione per modello"]
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
                    self.assertIn('data-machine-sort="shipping_date"', html)
                    self.assertIn('colspan="12"', html)
                else:
                    self.assertRegex(html, r'Note di produzione</th>\s*<th[^>]*>Note per produzione</th>')
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


if __name__ == "__main__":
    unittest.main()
