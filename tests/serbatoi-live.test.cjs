const {test} = require('node:test');
const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const {resolve} = require('node:path');
const vm = require('node:vm');

const source = readFileSync(resolve(__dirname, '../app_odp/static/assets/js/serbatoi.js'), 'utf8');
const tick = () => new Promise(resolve => setImmediate(resolve));

function page() {
    const state = {requests: [], intervals: [], dirty: false, panels: [{open: true}, {open: false}], replacements: 0};
    const listeners = {};
    const status = {textContent: ''};
    const warning = {textContent: '', classList: {remove() {}}};
    const sync = {action: '/serbatoi/azione'};
    const makeMain = panels => ({
        panels, setAttribute() {}, removeAttribute() {},
        querySelectorAll: () => panels,
        replaceWith(next) { state.main = next; state.replacements++; },
    });
    state.main = makeMain(state.panels);
    const location = {href: 'http://test/serbatoi?tab=clienti&id=1&tab_session=operator', assign(url) { state.redirect = url; }};
    const document = {
        hidden: false, activeElement: null,
        getElementById: id => ({'serbatoi-sync': sync, 'serbatoi-app': state.main, 'serbatoi-live-stato': status, 'serbatoi-avvisi': warning})[id],
        querySelector: selector => selector.includes('data-dirty') && state.dirty ? {} : null,
        querySelectorAll: () => [],
        addEventListener: (name, handler) => { (listeners[name] ??= []).push(handler); },
    };
    const window = {
        scrollX: 0, scrollY: 150,
        scrollTo: options => { state.scroll = options; },
        addEventListener: (name, handler) => { (listeners[name] ??= []).push(handler); },
    };
    const sandbox = {
        document, window, location, URL, Intl, AbortController,
        history: {state: {}, replaceState() {}, pushState() {}},
        sessionStorage: {getItem: () => null, setItem() {}},
        performance: {getEntriesByType: () => []},
        setInterval: (handler, delay) => state.intervals.push({handler, delay}),
        setTimeout: () => 1, clearTimeout() {},
        FormData: class {},
        DOMParser: class {
            parseFromString() {
                const next = makeMain([{open: false}, {open: true}]);
                return {getElementById: () => state.missingMain ? null : next};
            }
        },
        fetch: (url, options) => {
            state.requests.push({url, options});
            if (options.method === 'POST') return Promise.resolve({ok: true, json: async () => ({ok: true, changed: 0})});
            return state.reply ? state.reply() : Promise.resolve({ok: true, text: async () => '<main></main>'});
        },
    };
    vm.runInNewContext(source, sandbox);
    state.document = document;
    state.status = status;
    state.location = location;
    state.refresh = () => state.intervals[0].handler();
    state.emit = (name, event = {}) => listeners[name]?.forEach(handler => handler(event));
    return state;
}

test('updates the current filtered operator view every 10 seconds without reload and preserves panels and scroll', async () => {
    const state = page();
    assert.equal(state.intervals.length, 1);
    assert.equal(state.intervals[0].delay, 10000);
    await state.refresh();
    assert.equal(state.replacements, 1);
    assert.equal(state.redirect, undefined);
    assert.equal(state.requests[1].url, state.location.href);
    assert.equal(state.requests[1].options.cache, 'no-store');
    assert.deepEqual(state.main.panels.map(panel => panel.open), [true, false]);
    assert.equal(state.scroll.top, 150);
    assert.match(state.status.textContent, /Aggiornato alle/);
});

test('does not replace edited forms, including changes made while a request is pending', async () => {
    const state = page();
    state.dirty = true;
    await state.refresh();
    assert.equal(state.requests.length, 1);
    assert.match(state.status.textContent, /sospeso/);
    state.dirty = false;
    let finish;
    state.reply = () => new Promise(resolve => { finish = resolve; });
    const pending = state.refresh();
    state.dirty = true;
    finish({ok: true, text: async () => ''});
    await pending;
    assert.equal(state.replacements, 0);
});

test('pauses when hidden or focused in a form, resumes on visibility change', async () => {
    const state = page();
    state.document.hidden = true;
    await state.refresh();
    assert.equal(state.requests.length, 1);
    state.document.hidden = false;
    state.document.activeElement = {closest: () => ({})};
    await state.refresh();
    assert.equal(state.requests.length, 1);
    state.document.activeElement = null;
    state.emit('visibilitychange');
    await tick();
    assert.equal(state.replacements, 1);
});

test('network errors and expired sessions keep the page and retry on the next interval', async () => {
    const state = page();
    state.reply = () => Promise.reject(new Error('offline'));
    await state.refresh();
    assert.equal(state.replacements, 0);
    assert.match(state.status.textContent, /non riuscito/);
    state.reply = null;
    state.missingMain = true;
    await state.refresh();
    assert.equal(state.redirect, undefined);
    assert.equal(state.replacements, 0);
    state.missingMain = false;
    await state.refresh();
    assert.equal(state.replacements, 1);
});

test('only one request runs at a time and stale responses cannot overwrite navigation', async () => {
    const state = page();
    let finish;
    state.reply = () => new Promise(resolve => { finish = resolve; });
    const pending = state.refresh();
    await state.refresh();
    assert.equal(state.requests.length, 2);
    state.location.href = 'http://test/serbatoi?tab=produzione';
    finish({ok: true, text: async () => ''});
    await pending;
    assert.equal(state.replacements, 0);
});

test('select changes protect unsaved data and submitting a POST stops background updates', async () => {
    const state = page();
    const form = {dataset: {}, method: 'post', querySelectorAll: () => []};
    state.emit('change', {target: {closest: () => form, matches: () => false}});
    assert.equal(form.dataset.dirty, 'true');
    state.emit('submit', {target: {closest: () => form}});
    await state.refresh();
    assert.equal(state.requests.length, 1);
    state.emit('pageshow');
    await state.refresh();
    assert.equal(state.replacements, 1);
});
