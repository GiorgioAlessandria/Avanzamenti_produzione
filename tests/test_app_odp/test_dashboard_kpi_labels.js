// Run with: node tests/test_app_odp/test_dashboard_kpi_labels.js
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const template = fs.readFileSync(path.resolve(__dirname, '../../app_odp/templates/dashboard_produzione.j2'), 'utf8');
const script = template.match(/<script>([\s\S]*?)<\/script>/)[1].replace(/\{\{.*?\}\}/g, '');
new vm.Script(script);

function source(name) {
    const pattern = new RegExp(`            (?:async )?function ${name}\\(`);
    const start = script.search(pattern);
    assert.ok(start >= 0, name);
    const rest = script.slice(start + 1);
    const end = rest.search(/\n            (?:async )?function /);
    return script.slice(start, end < 0 ? undefined : start + 1 + end);
}

async function run() {
    let cards = {};
    const displayed = {};
    const classes = new Set(['text-success']);
    const element = {textContent: '', classList: {
        remove: (...names) => names.forEach(name => classes.delete(name)),
        toggle: (name, value) => value ? classes.add(name) : classes.delete(name),
    }};
    const context = vm.createContext({
        Number, String, document: {
            getElementById: () => ({dataset: {apiUrl: '/kpi'}}),
            querySelector: () => element,
        },
        fetch: async () => ({ok: true, json: async () => ({data: {cards}})}),
        buildUrlWithParams: value => value, getKpiFilters: () => ({}), fetchOptions: () => ({}),
        updateFilterSelects: () => {}, setCard: (attribute, key, value) => {displayed[`${attribute}:${key}`] = value;},
        renderKpiTempoRealeVsPrevisto: () => {}, renderKpiAffidabilita: () => {},
        renderKpiRitardoReparto: () => {}, renderKpiHorizontalPercent: () => {},
    });
    for (const name of ['formatHours', 'formatPercent', 'formatSignedHours', 'setSnapshotDelta', 'loadKpi']) {
        vm.runInContext(source(name), context);
    }
    await context.loadKpi();
    for (const key of ['percentuale_ritardo', 'giorni_medi_ritardo', 'scostamento_percentuale',
                       'affidabilita_tempi', 'tasso_non_conformita', 'lead_time_medio_ore', 'copertura_dati']) {
        assert.equal(displayed[`data-kpi-card:${key}`], 'Non disponibile', key);
    }
    cards = {ordini_chiusi: 12, ordini_con_scadenza: 9, ordini_in_ritardo: 3, percentuale_ritardo: 25,
        giorni_medi_ritardo: 2, tempo_previsto_totale: 10, tempo_reale_totale: 12,
        scostamento_totale: 2, scostamento_percentuale: 20, fasi_tempi_affidabili: 4,
        fasi_tempi_non_affidabili: 1, affidabilita_tempi: 80, fasi_totali: 7,
        quantita_consuntivata: 100, quantita_non_conforme: 1.5, tasso_non_conformita: 1.5,
        lead_time_copertura: 2, lead_time_medio_ore: 48, lead_time_mediano_ore: 40,
        fasi_dati_completi: 6, copertura_dati: 85.71};
    await context.loadKpi();
    assert.equal(displayed['data-kpi-card:scostamento_percentuale'], '+20.00%');
    assert.match(displayed['data-kpi-card-sub:ordini_in_ritardo'], /3 tardivi \/ 12 conclusi · 9 con scadenza/);
    assert.match(displayed['data-kpi-card-sub:affidabilita_tempi'], /4 entro tolleranza \/ 5 fasi valutabili/);
    assert.match(displayed['data-kpi-card-sub:tasso_non_conformita'], /1,5 non conformi/);
    cards.tasso_non_conformita = 0;
    cards.quantita_non_conforme = 0;
    await context.loadKpi();
    assert.equal(displayed['data-kpi-card:tasso_non_conformita'], '0.00%');
    context.setSnapshotDelta('percentuale_ritardo', null);
    assert.equal(element.textContent, 'Confronto non disponibile');
    assert.equal(classes.size, 0);
    context.setSnapshotDelta('percentuale_ritardo', {delta: 5}, ' punti percentuali', false, '2026-06');
    assert.equal(element.textContent, '+5.00 punti percentuali rispetto a 2026-06');
    console.log('KPI labels: script syntax, missing data, valid zero, sample sizes, signed values and monthly comparison passed.');
}
run().catch(error => { console.error(error); process.exitCode = 1; });
