// Eseguire dalla radice del repository: node tests/test_app_odp/test_phase_labels_ui.cjs
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const root = path.resolve(__dirname, "../..");
const base = fs.readFileSync(path.join(root, "app_odp/templates/base.j2"), "utf8");
const source = base.match(/<script id="avp-phase-labels">([\s\S]*?)<\/script>/)[1]
    .replace(/window\.AVP_PHASE_LABELS = \{\{[^\n]*\}\};/, "");
assert.ok(!base.includes("assets/js/phase-labels.js"));
function session(labels) {
    const context = vm.createContext({window: {AVP_PHASE_LABELS: labels}});
    vm.runInContext(source, context);
    return context.window;
}
const alice = session({1: "Montaggio", 2: "Collaudo"});
const bob = session({1: "Assemblaggio"});
for (const value of [1, "01", "1.0", "Fase 1"]) assert.equal(alice.phaseLabel(value), "Montaggio");
assert.equal(bob.phaseLabel(1), "Assemblaggio");
assert.equal(bob.phaseLabel(2), "2");
const defaults = session({});
assert.equal(defaults.phaseLabel(1), "1");
assert.equal(defaults.phaseLabel("Fase 2"), "2");
assert.equal(defaults.phaseLabel("1 + 2"), "1 + 2");
assert.equal(alice.phaseLabel("1 + 2"), "Montaggio + Collaudo");
assert.equal(alice.phaseLabel("1,2"), "Montaggio,Collaudo");
assert.equal(alice.phaseLabel(null), "");
assert.equal(alice.phaseLabel("-"), "-");
assert.equal(alice.phaseLabel("fase finale"), "fase finale");
assert.equal(alice.phaseText("Blocco: fase 1 con export pendente"), "Blocco: Montaggio con export pendente");
assert.equal(alice.phaseLabel("constructor"), "constructor");

function template(name) {
    return fs.readFileSync(path.join(root, "app_odp/templates", name), "utf8");
}
function namedFunction(source, name) {
    const start = source.indexOf(`function ${name}(`);
    assert.ok(start >= 0, name);
    // Queste funzioni non contengono altre dichiarazioni di funzione.
    const ends = ["\n            function ", "\n            async function "]
        .map(marker => source.indexOf(marker, start + 1)).filter(index => index >= 0);
    const end = ends.length ? Math.min(...ends) : -1;
    return source.slice(start, end < 0 ? undefined : end).trim();
}
const vendite = template("vendite.j2");
const context = vm.createContext({window: alice, canEditNotes: false, canConfirmPackaging: false,
    groups: [], selectedGroups: new Set(), collapsedGroups: new Set(), showAllGroups: false,
    sortKey: "model_code", sortDirection: 1,
    machinesBody: {innerHTML: ""}, modelLabel: () => "Modello", stateBadge: () => "Attivo",
    esc: value => String(value ?? "").replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll('"', "&quot;")});
vm.runInContext(["productionNoteCell", "formatShippingDate", "formatUpdateTime", "packagingCell", "machineSections", "sortedMachines", "renderMachines"]
    .map(name => namedFunction(vendite, name)).join("\n"), context);
const machine = {phase: "1", serial_number: "M1", order: "OP1", production_note: "Nota Fase 1"};
context.machines = {machines: [machine]};
vm.runInContext("renderMachines(machines)", context);
assert.ok(context.machinesBody.innerHTML.includes('data-phase-code="1">Montaggio</td>'));
assert.ok(context.machinesBody.innerHTML.includes("Nota Fase 1"));
machine.production_instructions = '<img src=x onerror="alert(1)">\nIstruzioni Fase 1';
machine.packaging_note = '<img src=x onerror="alert(1)">\nImballo Fase 1';
vm.runInContext("renderMachines(machines)", context);
assert.ok(context.machinesBody.innerHTML.includes("&lt;img"));
assert.ok(!context.machinesBody.innerHTML.includes("<img"));
assert.ok(context.machinesBody.innerHTML.includes("Istruzioni Fase 1"));
assert.ok(context.machinesBody.innerHTML.includes("Imballo Fase 1"));
assert.equal((context.machinesBody.innerHTML.match(/<td\b/g) || []).length, 12);
assert.ok(context.machinesBody.innerHTML.indexOf("Nota Fase 1") <
          context.machinesBody.innerHTML.indexOf("Istruzioni Fase 1"));
assert.equal(machine.phase, "1");
machine.missing_components = [{code: "COMP-01", variant: "V1", description: '<img src=x onerror="alert(1)">'}];
machine.last_suspension_cause = "Attesa materiale";
vm.runInContext("renderMachines(machines)", context);
assert.ok(context.machinesBody.innerHTML.includes("Materiali mancanti:"));
assert.ok(context.machinesBody.innerHTML.includes("COMP-01 — V1 — &lt;img"));
assert.ok(!context.machinesBody.innerHTML.includes("<img"));
assert.ok(context.machinesBody.innerHTML.includes("Attesa materiale"));
machine.missing_components = [];
vm.runInContext("renderMachines(machines)", context);
assert.ok(!context.machinesBody.innerHTML.includes("Materiali mancanti:"));
assert.ok(context.machinesBody.innerHTML.includes("Attesa materiale"));
alice.AVP_PHASE_LABELS[1] = '<img src=x onerror="alert(1)">';
vm.runInContext("renderMachines(machines)", context);
assert.ok(context.machinesBody.innerHTML.includes("&lt;img"));
assert.ok(!context.machinesBody.innerHTML.includes("<img"));

const sortContext = vm.createContext({});
context.groups = [
    {id: 1, name: "Grandi <test>", family_codes: ["F1", "F2"]},
    {id: 2, name: "Piccole", family_codes: ["F2"]},
    {id: 3, name: "Vuoto", family_codes: ["F4"]},
];
context.machines = {machines: [
    {...machine, serial_number: "M-late", family_code: "F1", shipping_date: "2027-01-02"},
    {...machine, serial_number: "M-early", family_code: "F2", shipping_date: "2026-12-31"},
    {...machine, serial_number: "M-other", family_code: "", shipping_date: ""},
    {...machine, serial_number: "M-no-date", family_code: "F1", shipping_date: ""},
]};
context.selectedGroups = new Set(["1", "2"]);
context.sortKey = "shipping_date";
context.sortDirection = 1;
vm.runInContext("renderMachines(machines)", context);
let groupedHtml = context.machinesBody.innerHTML;
assert.equal((groupedHtml.match(/M-early/g) || []).length, 1); // Unione senza duplicati.
assert.ok(!groupedHtml.includes("M-other"));
assert.ok(groupedHtml.indexOf("M-early") < groupedHtml.indexOf("M-late"));
assert.ok(groupedHtml.indexOf("M-late") < groupedHtml.indexOf("M-no-date"));
assert.ok(groupedHtml.includes("31/12/2026"));
context.showAllGroups = true;
vm.runInContext("renderMachines(machines)", context);
groupedHtml = context.machinesBody.innerHTML;
assert.equal((groupedHtml.match(/data-group-heading/g) || []).length, 4);
assert.equal((groupedHtml.match(/M-early/g) || []).length, 2);
assert.ok(groupedHtml.includes("Grandi &lt;test>"));
assert.ok(!groupedHtml.includes("Grandi <test>"));
assert.ok(groupedHtml.includes("Senza raggruppamento"));
assert.ok(groupedHtml.includes("Vuoto (0)"));
context.sortDirection = -1;
vm.runInContext("renderMachines(machines)", context);
groupedHtml = context.machinesBody.innerHTML;
assert.ok(groupedHtml.indexOf("M-late") < groupedHtml.indexOf("M-early"));
assert.ok(groupedHtml.indexOf("M-early") < groupedHtml.indexOf("M-no-date"));
assert.ok(groupedHtml.indexOf("M-no-date") < groupedHtml.indexOf("Piccole"));
// Stato di selezione e ordinamento mantenuto al rendering dell'aggiornamento.
vm.runInContext("renderMachines(machines)", context);
assert.equal(context.machinesBody.innerHTML, groupedHtml);
context.collapsedGroups.add("1");
context.collapsedGroups.add("ungrouped");
vm.runInContext("renderMachines(machines)", context);
const collapsedHtml = context.machinesBody.innerHTML;
assert.ok(collapsedHtml.includes('aria-expanded="false"'));
assert.ok(collapsedHtml.includes("Grandi &lt;test>"));
assert.ok(!collapsedHtml.includes("M-late"));
assert.ok(!collapsedHtml.includes("M-other"));
assert.equal((collapsedHtml.match(/M-early/g) || []).length, 1);
vm.runInContext("renderMachines(machines)", context);
assert.equal(context.machinesBody.innerHTML, collapsedHtml);
context.groups[0].name = "Grandi rinominate";
vm.runInContext("renderMachines(machines)", context);
assert.ok(context.machinesBody.innerHTML.includes("Grandi rinominate"));
assert.ok(!context.machinesBody.innerHTML.includes("M-late"));
context.showAllGroups = false;
vm.runInContext("renderMachines(machines)", context);
assert.ok(context.machinesBody.innerHTML.includes("M-late")); // La vista unica non viene nascosta.
context.showAllGroups = true;
context.collapsedGroups.clear();
vm.runInContext("renderMachines(machines)", context);
assert.ok(context.machinesBody.innerHTML.includes("M-late"));
assert.ok(context.machinesBody.innerHTML.includes("M-other"));
assert.ok(!context.machinesBody.innerHTML.includes('aria-expanded="false"'));
context.groups = [];
context.selectedGroups.clear();
vm.runInContext("renderMachines(machines)", context);
assert.ok(context.machinesBody.innerHTML.includes("Senza raggruppamento"));
assert.ok(context.machinesBody.innerHTML.includes("M-other"));
const assignments = template("vendite_assegnazioni.j2");
const noteContext = vm.createContext({
    esc: context.esc, CSS: {escape: String},
    canEditSalesNotes: true, canEditProductionNotes: true,
    customerOrdersBody: {querySelector: () => ({
        dataset: {version: "7"},
        querySelector: selector => ({value: selector.includes("production_instructions")
            ? "  Accessorio speciale\nControllare  " : "Nota precedente"}),
    })},
});
vm.runInContext(namedFunction(assignments, "rowNotesPayload") + "\n" +
                namedFunction(assignments, "noteTextarea"), noteContext);
assert.equal(vm.runInContext("rowNotesPayload(1).production_instructions", noteContext),
             "Accessorio speciale\nControllare");
assert.equal(vm.runInContext("rowNotesPayload(1).version", noteContext), 7);
assert.equal(vm.runInContext("rowNotesPayload(1).production_note", noteContext), undefined);
assert.ok(!assignments.includes('noteTextarea(row, "production_note"'));
assert.ok(assignments.includes('esc(row.production_note || "—")'));
const customerContext = vm.createContext({
    esc: context.esc, customerOrdersBody: {innerHTML: ""},
    canEditSalesNotes: true, canConfirmOrderRead: false, canConfirmShipment: false,
    canDeleteOrders: false, collapsedOrders: new Set(),
    dirtyAssignments: new Set(), dirtyNotes: new Set(), dirtyDates: new Set(), dirtyOrderDetails: new Set(),
    referenceClass: () => "", orderDetails: () => "", formatDateTime: String,
    modelLabel: () => "Modello", dateInput: () => "", assignmentSelect: () => "",
    rowActions: () => "", applyDemandFilter() {},
});
vm.runInContext(namedFunction(assignments, "noteTextarea") + "\n" +
                namedFunction(assignments, "renderCustomerOrders"), customerContext);
customerContext.orders = [{id: 1, customer_name: "Cliente", customer_order: "OC1", rows: [{
    id: 1, position: 1, production_note: "</textarea><img>", production_instructions: "Istruzioni",
}]}];
vm.runInContext("renderCustomerOrders(orders)", customerContext);
assert.ok(!customerContext.customerOrdersBody.innerHTML.includes('data-note-field="production_note"'));
assert.ok(customerContext.customerOrdersBody.innerHTML.includes('data-note-field="production_instructions"'));
assert.ok(customerContext.customerOrdersBody.innerHTML.includes("&lt;/textarea>&lt;img>"));
assert.ok(!customerContext.customerOrdersBody.innerHTML.includes("<img>"));
assert.equal(vm.runInContext("rowNotesPayload(1).commercial_note", noteContext), "Nota precedente");
noteContext.canEditSalesNotes = false;
assert.equal(vm.runInContext("rowNotesPayload(1).production_instructions", noteContext), undefined);
assert.equal(vm.runInContext("rowNotesPayload(1).commercial_note", noteContext), undefined);
noteContext.row = {id: 1, position: 1, production_instructions: "</textarea><img>"};
const readonly = vm.runInContext(
    'noteTextarea(row, "production_instructions", "Note per produzione", false)', noteContext);
assert.ok(readonly.includes("readonly"));
assert.ok(readonly.includes("&lt;/textarea>&lt;img>"));
assert.ok(!readonly.includes("<img>"));
noteContext.row.commercial_note = "</textarea><img>";
const commercialHtml = vm.runInContext(
    'noteTextarea(row, "commercial_note", "Note commerciali", true)', noteContext);
assert.ok(commercialHtml.includes('data-note-field="commercial_note"'));
assert.ok(commercialHtml.includes("&lt;/textarea>&lt;img>"));
assert.ok(!commercialHtml.includes("readonly"));
assert.ok(!commercialHtml.includes("<img>"));
vm.runInContext(namedFunction(template("base.j2"), "cellValue"), sortContext);
sortContext.row = {children: [{dataset: {phaseCode: "2"}, textContent: "Collaudo"}]};
assert.equal(vm.runInContext("cellValue(row, 0)", sortContext), "2");

// Tutti gli script dei template modificati devono mantenere sintassi JavaScript valida.
const files = ["base.j2", "home.j2", "preferenze_fasi.j2", "vendite.j2", "vendite_assegnazioni.j2",
    "admin_ricrea_avp.j2", "priorita_edit.j2", "priorita_view.j2", "storico_ordini.j2",
    "report_settimanale.j2", "home_acquisti.j2", "impostazioni.j2",
    "partials/_home_montaggio.j2", "partials/_home_standard.j2"];
let count = 0;
for (const file of files) {
    for (const match of template(file).matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gi)) {
        if (/\bsrc=|application\/json/.test(match[1])) continue;
        const js = match[2].replace(/\{\{[\s\S]*?\}\}/g, "null").replace(/\{%[\s\S]*?%\}/g, "");
        new vm.Script(js, {filename: file});
        count++;
    }
}
console.log(`OK: etichette per utente, rendering/XSS, codici e note invariati, ordinamento; ${count} script verificati.`);

async function checkGroupedNoteEditing() {
    const handlers = {};
    const copies = [0, 1].map(() => ({
        value: "Originale", disabled: false,
        dataset: {serial: "M1", version: 1, originalValue: "Originale", idDocumento: "D", idRiga: "1"},
        matches: () => true,
    }));
    const versions = [];
    let savedVersion = 1;
    const state = vm.createContext({
        window: {
            addEventListener() {},
            avpFetch: async (_url, options) => {
                assert.ok(copies.every(copy => copy.disabled));
                const body = JSON.parse(options.body);
                versions.push(body.version);
                return {ok: true, json: async () => ({ok: true, data: {machines: [{
                    serial_number: "M1", production_note: body.production_note,
                    production_note_version: ++savedVersion,
                }]}})};
            },
        },
        page: {dataset: {noteUrl: "/note"}},
        machinesBody: {
            querySelectorAll: () => copies,
            addEventListener: (name, handler) => { handlers[name] = handler; },
        },
        groupFilters: {addEventListener() {}},
        machinesTable: {tHead: {addEventListener() {}}},
        groupForm: null, groupFormDirty: false, groupMutation: false,
        errorElement: {textContent: "", classList: {add() {}, remove() {}}},
        currentData: {machines: [{serial_number: "M1", production_note: "Originale"}]},
        collapsedGroups: new Set(), CSS: {escape: String},
        savingNote: false, packagingInFlight: false, noteRevision: 0,
        canEditNotes: true, canConfirmPackaging: false,
        setStatus() {}, refresh: async () => {},
    });
    vm.runInContext(namedFunction(vendite, "hasUnsavedNotes") + "\n" +
                    namedFunction(vendite, "canChangeMachineView"), state);
    const start = vendite.indexOf('            machinesBody.addEventListener("click"');
    const end = vendite.indexOf('            refreshButton.addEventListener("click"', start);
    vm.runInContext(vendite.slice(start, end), state);
    const event = (field, saving) => {
        const button = {disabled: false, closest: () => ({querySelector: () => field})};
        return {target: {closest: selector =>
            selector === (saving ? "[data-save-production-note]" : "[data-cancel-production-note]")
                ? button : null}};
    };
    for (const copy of copies) {
        copy.value = "Nota " + savedVersion;
        handlers.input({target: copy});
        assert.equal(copies[0].value, copies[1].value);
        assert.equal(vm.runInContext("canChangeMachineView()", state), false);
        await handlers.click(event(copy, true));
        assert.ok(copies.every(field => !field.disabled && field.dataset.version === savedVersion));
        assert.ok(copies.every(field => field.value === field.dataset.originalValue));
        assert.equal(state.currentData.machines[0].production_note, copy.value);
    }
    assert.deepEqual(versions, [1, 2]);
    copies[0].value = "Da annullare";
    handlers.input({target: copies[0]});
    await handlers.click(event(copies[0], false));
    assert.ok(copies.every(copy => copy.value === copy.dataset.originalValue));
    assert.equal(vm.runInContext("canChangeMachineView()", state), true);
    let renders = 0, focused = false;
    state.renderMachines = () => { renders++; };
    state.machinesBody.querySelector = () => ({focus: () => { focused = true; }});
    const toggleEvent = {target: {closest: selector => selector === "[data-toggle-machine-group]"
        ? {dataset: {toggleMachineGroup: "1"}} : null}};
    await handlers.click(toggleEvent);
    assert.ok(state.collapsedGroups.has("1"));
    assert.ok(focused);
    await handlers.click(toggleEvent);
    assert.ok(!state.collapsedGroups.has("1"));
    assert.equal(renders, 2);
    copies[0].value = "Non salvata";
    await handlers.click(toggleEvent);
    assert.ok(!state.collapsedGroups.has("1"));
    assert.equal(renders, 2);
    console.log("OK: note sincronizzate tra raggruppamenti, versioni, annullamento e protezione modifiche non salvate.");
}
checkGroupedNoteEditing().catch(error => { console.error(error); process.exitCode = 1; });
