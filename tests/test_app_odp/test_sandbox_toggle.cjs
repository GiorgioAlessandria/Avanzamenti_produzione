const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const {test} = require("node:test");

const template = fs.readFileSync(path.join(__dirname, "../../app_odp/templates/base.j2"), "utf8");
const script = template.match(/<script id="avp-sandbox-toggle">([^]*?)<\/script>/)[1];

function browser() {
    const values = new Map();
    const events = {};
    const messages = [];
    const toggles = {start: {checked: true}, stop: {checked: false}};
    const forms = {};
    for (const name of ["start", "stop"]) {
        forms[name] = {
            action: "https://example.test/sandbox/sessione/termina",
            querySelector: () => toggles[name],
            addEventListener: (_, callback) => { events[name] = callback; },
        };
    }
    let destination;
    let requests = 0;
    const context = {
        URL,
        document: {querySelector: selector => forms[selector.includes("start") ? "start" : "stop"]},
        sessionStorage: {
            setItem: (key, value) => values.set(key, value),
            getItem: key => values.get(key),
            removeItem: key => values.delete(key),
        },
        alert: message => messages.push(message),
        window: {
            location: {
                origin: "https://example.test",
                href: "https://example.test/?tab=officina&tab_session=originale",
                replace: url => { destination = url; },
            },
            avpFetch: async () => {
                requests++;
                return {ok: true, json: async () => ({ok: true})};
            },
        },
    };
    vm.runInNewContext(script, context);
    return {
        context, values, messages, toggles,
        submit: name => events[name]({target: forms[name], preventDefault() {}}),
        destination: () => destination,
        requests: () => requests,
    };
}

test("l'admin attiva e disattiva la sandbox nella stessa scheda e recupera la sessione normale", async () => {
    assert.match(template, /data-sandbox-start/);
    assert.doesNotMatch(template.match(/<form[^>]+data-sandbox-start[^>]*>/)[0], /target=/);
    const page = browser();
    page.submit("start");
    const original = page.context.window.location.href;
    assert.equal(page.values.get("avp-sandbox-return"), original);
    page.context.window.location.href = "https://example.test/sandbox/sessione/?tab_session=prova";
    await page.submit("stop");
    assert.equal(page.requests(), 1);
    assert.equal(page.destination(), original);
    assert.equal(page.values.size, 0);
    assert.deepEqual(page.messages, []);
});

test("un errore in uscita mantiene attiva la sandbox e conserva il ritorno", async () => {
    const page = browser();
    page.submit("start");
    page.context.window.avpFetch = async () => { throw new Error("Connessione interrotta"); };
    await page.submit("stop");
    assert.equal(page.destination(), undefined);
    assert.equal(page.values.size, 1);
    assert.equal(page.toggles.stop.checked, true);
    assert.equal(page.toggles.stop.disabled, false);
    assert.deepEqual(page.messages, ["Connessione interrotta"]);
});
