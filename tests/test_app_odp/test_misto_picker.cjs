const assert = require("node:assert/strict");
const {readFileSync} = require("node:fs");
const {join} = require("node:path");
const {test} = require("node:test");
const {runInNewContext} = require("node:vm");

const script = readFileSync(join(__dirname, "../../app_odp/static/assets/js/workgroups.js"), "utf8");
const uid = "MIX-ricomposto";
const homeGroup = {
    offsetParent: {},
    dataset: {
        isWorkGroup: "1", groupUid: uid, groupType: "MULTIPLO",
        iddocumento: `GROUP:${uid}`, idriga: "GROUP",
        groupLabel: "2026.1.591 1,00 + 2026.1.591 2,00",
        groupDetail: JSON.stringify([{id_riga: "1"}, {id_riga: "2"}]),
        stato: "Attivo", operatore: "operatore",
    },
};
const homeSingle = {offsetParent: {}, dataset: {iddocumento: "10", idriga: "1"}};
const single = {id_documento: "10", id_riga: "1"};

function list(username, apiRows, homeRows = [homeSingle, homeGroup], includeMultipli = true) {
    const context = {window: {AVP_ACTIVE_USERNAME: username}, document: {addEventListener() {}}};
    const exposed = script.replace(/\}\)\(\);\s*$/, "window.orderList = ordersForPicker; })();");
    runInNewContext(exposed, context);
    return Array.from(context.window.orderList(apiRows, homeRows, includeMultipli));
}

test("il multiplo della home compare in alto anche quando manca nella risposta API", () => {
    const [group, order] = list("operatore", [single]);
    assert.equal(group.group_uid, uid);
    assert.equal(group.member_count, 2);
    assert.equal(group.motivo_blocco, "");
    assert.equal(order, single);
});

test("un multiplo di un altro operatore è visibile con il motivo del blocco", () => {
    const [group] = list("admin", [single]);
    assert.match(group.motivo_blocco, /operatore assegnato/);
});

test("usa i dati API per i multipli già presenti nella schermata e non duplica righe", () => {
    const registered = {group_uid: uid, motivo_blocco: "Motivo restituito dal server"};
    const otherTab = {group_uid: "MULT-altro-reparto"};
    const [group, order] = list("operatore", [single, registered, otherTab], [homeGroup, homeGroup, homeSingle]);
    assert.equal(group, registered);
    assert.equal(order, single);
    assert.deepEqual(list("operatore", [single, registered], [homeGroup, homeSingle], false), [single]);
});
