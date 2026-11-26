const {test} = require('node:test');
const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const {resolve} = require('node:path');
const vm = require('node:vm');
const template = readFileSync(resolve(__dirname, '../app_odp/templates/storico_ordini.j2'), 'utf8');
const handler = template.slice(template.indexOf('async function exportMatricole('), template.indexOf('document.getElementById("storico-matricole-export-form").addEventListener'));

async function exportFile(response) {
    const state = {button: {disabled: false}, status: {}, clicked: false};
    const form = {action: '/api/storico-ordini/matricole/export?tab_session=operator', querySelector: () => state.button};
    const link = {click() { state.clicked = true; }, remove() {}};
    const context = vm.createContext({
        window: {avpFetch: async (url, options) => {
            state.url = url;
            state.options = options;
            assert.equal(state.button.disabled, true);
            if (response instanceof Error) throw response;
            return response;
        }},
        document: {getElementById: () => state.status, createElement: () => link, body: {appendChild() {}}},
        URL: {createObjectURL: () => 'blob:excel', revokeObjectURL() {}},
        FormData: class {constructor(form) { this.form = form; }},
        setTimeout() {},
    });
    vm.runInContext(handler, context);
    await context.exportMatricole({preventDefault() {}, currentTarget: form});
    assert.equal(state.button.disabled, false);
    return state;
}

test('downloads the workbook by POST preserving the operator session', async () => {
    const state = await exportFile({ok: true, headers: {get: () => 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'}, blob: async () => ({})});
    assert.equal(state.options.method, 'POST');
    assert.match(state.url, /tab_session=operator/);
    assert.equal(state.clicked, true);
    assert.match(state.status.textContent, /Excel pronto/);
});
test('shows validation errors without downloading a fake workbook', async () => {
    const state = await exportFile({ok: false, json: async () => ({error: 'Inserire almeno una matricola.'})});
    assert.equal(state.clicked, false);
    assert.match(state.status.textContent, /Inserire almeno/);
});
test('handles expired sessions returning HTML', async () => {
    const state = await exportFile({ok: true, headers: {get: () => 'text/html'}, json: async () => { throw Error('HTML'); }});
    assert.equal(state.clicked, false);
    assert.match(state.status.textContent, /sessione/);
});
test('allows another attempt after network failure', async () => {
    const state = await exportFile(new Error('Connessione assente'));
    assert.equal(state.clicked, false);
    assert.equal(state.status.textContent, 'Connessione assente');
});
