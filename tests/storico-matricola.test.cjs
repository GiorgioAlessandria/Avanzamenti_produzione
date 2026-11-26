const {test} = require('node:test');
const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const {resolve} = require('node:path');
const vm = require('node:vm');
const template = readFileSync(resolve(__dirname, '../app_odp/templates/storico_ordini.j2'), 'utf8');
const script = template.match(/<script>([\s\S]*?)<\/script>/)[1];

function search({serial = ' 000123 ', from = '2026-10-26', to = '2026-11-25'} = {}) {
    const fields = {
        'storico-matricola': {value: serial},
        'storico-date-from': {value: from, defaultValue: '2026-10-26'},
        'storico-date-to': {value: to, defaultValue: '2026-11-25'},
    };
    let submit;
    fields['storico-filters'] = {addEventListener: (type, handler) => { submit = handler; }};
    let result;
    const context = vm.createContext({
        URLSearchParams, currentPage: 5,
        document: {getElementById: id => fields[id]},
        loadList() { result = context.params(); },
    });
    const functions = script.slice(script.indexOf('function value('), script.indexOf('function shortList('));
    const handler = script.slice(script.indexOf('document.getElementById("storico-filters").addEventListener'), script.indexOf('lotInput.addEventListener'));
    vm.runInContext(functions + handler, context);
    submit({preventDefault() {}});
    return result;
}

test('the full inline script parses', () => { new vm.Script(script); });
test('serial search sends the exact trimmed serial and searches all history when dates are default', () => {
    const params = search();
    assert.equal(params.get('matricola'), '000123');
    assert.equal(params.get('date_from'), '');
    assert.equal(params.get('date_to'), '');
    assert.equal(params.get('page'), '1');
});
test('a deliberately chosen period is preserved', () => {
    const params = search({from: '2026-01-01', to: '2026-01-31'});
    assert.equal(params.get('matricola'), '000123');
    assert.equal(params.get('date_from'), '2026-01-01');
    assert.equal(params.get('date_to'), '2026-01-31');
});
test('ordinary browsing keeps its default dates without a serial', () => {
    const params = search({serial: ''});
    assert.equal(params.get('date_from'), '2026-10-26');
    assert.equal(params.get('date_to'), '2026-11-25');
});
