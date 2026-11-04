// Run with: node tests/test_app_odp/test_operational_dashboard_labels.js
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const template = fs.readFileSync(path.resolve(__dirname, '../../app_odp/templates/dashboard_produzione.j2'), 'utf8');
const script = template.match(/<script>([\s\S]*?)<\/script>/)[1].replace(/\{\{.*?\}\}/g, '');
new vm.Script(script);
function source(name) {
    const start = script.search(new RegExp(`            (?:async )?function ${name}\\(`));
    assert.ok(start >= 0, name);
    const next = script.slice(start + 1).search(/\n            (?:async )?function /);
    return script.slice(start, next < 0 ? undefined : start + 1 + next);
}
async function run() {
    let data = {cards: {saturazione_totale: null}};
    const cards = {}, charts = {};
    const context = vm.createContext({Number, String, Math,
        document: {getElementById: () => ({dataset: {apiUrl: '/dashboard'}})},
        fetch: async () => ({ok: true, json: async () => ({data})}),
        buildUrlWithParams: value => value, getCruscottoFilters: () => ({}), fetchOptions: () => ({}),
        updateFilterSelects: () => {}, setCard: (attribute, key, value) => { cards[`${attribute}:${key}`] = value; },
        renderChart: (key, canvas, config) => { charts[key] = config; }, baseOptions: value => value,
        COLORS: {red: 'red', green: 'green', blue: 'blue'},
    });
    for (const name of ['renderCruscottoCarico','renderCruscottoStati','renderCruscottoReparti',
                        'renderCruscottoRisorse','renderCruscottoOrdiniReparto', 'renderCruscottoOrdiniRisorsa','renderCriticitaList']) {
        context[name] = () => {};
    }
    for (const name of ['formatHours','formatPercent','formatSignedHours',
                        'renderCruscottoSaturazione','renderCruscottoColliRisorsa','loadCruscotto']) {
        vm.runInContext(source(name), context);
    }
    await context.loadCruscotto();
    assert.equal(cards['data-dashboard-card:saturazione_totale'], 'Non calcolabile: capacità 0 h');
    assert.equal(cards['data-dashboard-card:incidenza_sospensioni'], 'Non disponibile');
    data = {cards: {ordini_totali: 10, ordini_sospesi: 2, incidenza_sospensioni: 20,
        saturazione_totale: 150, ore_previste_oggi: 12, ore_disponibili_oggi: 8,
        capacita_7_giorni: 40, carico_7_giorni: 50, gap_capacita_7_giorni: -10,
        risorse_collo_bottiglia: 3, risorse_senza_capacita: 1, criticita_totali: 240, ordini_critici: 120},
        criticita: Array(100).fill({})};
    await context.loadCruscotto();
    assert.equal(cards['data-dashboard-card:saturazione_totale'], '150.00%');
    assert.match(cards['data-dashboard-card-sub:gap_capacita_7_giorni'], /40.00 h di capacità − 50.00 h di carico/);
    assert.match(cards['data-dashboard-card-sub:risorse_collo_bottiglia'], /2 oltre capacità · 1 con capacità 0 h/);
    assert.match(cards['data-dashboard-card-sub:criticita'], /100 segnalazioni mostrate \/ 240 totali · 120 ordini/);
    const row = {label: 'R1', capacita: 0, ore_totali: 8, saturazione: null, senza_capacita: true};
    for (const [fn, chart] of [['renderCruscottoSaturazione','cruscotto_saturazione'], ['renderCruscottoColliRisorsa','cruscotto_colli_risorsa']]) {
        context[fn]([row]);
        const config = charts[chart];
        assert.equal(config.data.datasets[0].data[0], 8);
        assert.equal(config.data.datasets[1].data[0], 0);
        assert.equal(config.options.scales.x.title.text, 'Ore');
        const tip = config.options.plugins.tooltip.callbacks.label({dataIndex: 0});
        assert.ok(tip.some(line => line.includes('Non calcolabile: capacità 0 h')));
        assert.ok(tip.every(line => !line.includes('120.00%')));
    }
    console.log('Operational dashboard: JS syntax, capacity zero, sample counts, margins and charts in hours passed.');
}
run().catch(error => {console.error(error); process.exitCode = 1;});
