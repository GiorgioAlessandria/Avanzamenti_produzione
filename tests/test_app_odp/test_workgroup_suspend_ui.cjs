const assert = require("node:assert/strict");
const {readFileSync} = require("node:fs");
const {join} = require("node:path");
const {test} = require("node:test");
const {runInNewContext} = require("node:vm");

const template = readFileSync(join(__dirname, "../../app_odp/templates/base.j2"), "utf8");
const [, groupActions] = template.match(
    /<script>\s*(\(\(\) => \{\s*if \(window\.__avpWorkgroupStatusActionsInit\)[\s\S]*?)<\/script>/
);

function clickSuspend(buttonId, fields) {
    const calls = [];
    let click;
    let intercepted = false;
    const button = {id: buttonId};
    runInNewContext(groupActions, {
        window: {
            avpFetch(url, options) {
                calls.push([url, JSON.parse(options.body)]);
                return Promise.resolve({ok: true, json: async () => ({ok: true})});
            },
        },
        document: {
            addEventListener(type, handler, capture) {
                assert.equal(type, "click");
                assert.equal(capture, true);
                click = handler;
            },
            querySelector: selector => fields[selector] ? {value: fields[selector]} : null,
            getElementById: () => null,
        },
    });
    click({
        target: {closest: selectors => selectors.split(",").includes(`#${buttonId}`) ? button : null},
        preventDefault() {},
        stopPropagation() {},
        stopImmediatePropagation() { intercepted = true; },
    });
    return {calls, intercepted};
}

test("Montaggio semilavorati invia la sospensione del gruppo misto", () => {
    const result = clickSuspend("btn-conferma-sospensione", {
        "#toast-sosp-iddocumento": "GROUP:MIX-123",
        "#causale_attivita": "Pausa",
    });
    assert.equal(result.intercepted, true);
    assert.deepEqual(result.calls, [
        ["/api/ordini/gruppi/sospendi", {
            group_uid: "MIX-123", causale: "Pausa", tempo_non_funzionamento_minuti: "0",
        }],
    ]);
});

test("la sospensione standard invia anche il fermo macchina del gruppo", () => {
    const result = clickSuspend("btn-conferma-sospensione-sl", {
        "#toast2-iddocumento": "GROUP:MIX-123",
        "#causale_attivita_sl": "Pausa",
        "#toast-sosp-sl-tempo-fermo-macchina": "15",
    });
    assert.equal(result.intercepted, true);
    assert.deepEqual(result.calls, [
        ["/api/ordini/gruppi/sospendi", {
            group_uid: "MIX-123", causale: "Pausa", tempo_non_funzionamento_minuti: "15",
        }],
    ]);
});

test("un ordine singolo in Montaggio resta al gestore del singolo", () => {
    const result = clickSuspend("btn-conferma-sospensione", {
        "#toast-sosp-iddocumento": "51584",
    });
    assert.equal(result.intercepted, false);
    assert.deepEqual(result.calls, []);
});
