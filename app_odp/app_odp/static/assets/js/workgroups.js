(() => {
    "use strict";
    const esc = value => {
        const el = document.createElement("span");
        el.textContent = value == null ? "" : String(value);
        return el.innerHTML;
    };
    const key = row => `${row.id_documento}|${row.id_riga}`;
    const mode = row => String(row.time_share_mode || "SPLIT").toUpperCase();
    const machine = row => row.is_macchina === true || row.gestione_matricola === true || row.modalita_lotti === "m";
    function singleOutcome(rows, target) {
        const rest = rows.filter(row => key(row) !== key(target));
        const timed = rest.filter(row => mode(row) !== "ZERO");
        const labels = rest.map(row => row.ordine || row.cod_art).join(", ");
        if (!rest.length) return "Il gruppo verrà terminato.";
        if (rest.length === 1 || !timed.length) return `${labels}: ordini residui sospesi e gestibili singolarmente. Il gruppo verrà sciolto.`;
        const state = rows[0]?.group_status === "In Sospeso" ? "sospeso" : "attivo";
        return `Il gruppo resterà ${state} con ${rest.length} ordini, di cui ${rest.length - timed.length} mascherati.`;
    }

    function stopMinutes() {
        const modal = document.getElementById("modalWorkgroupClose");
        return modal?.dataset.enhancedWorkgroup === "1" ? document.getElementById("wg-close-stop")?.value || "0" : "0";
    }

    function validateStop() {
        const input = document.getElementById("wg-close-stop");
        return !input || document.getElementById("modalWorkgroupClose")?.dataset.enhancedWorkgroup !== "1" || input.reportValidity();
    }

    function mountClose(modal, rows) {
        modal.classList.add("wg-modal");
        modal.setAttribute("aria-labelledby", "modal-workgroup-close-title");
        const body = document.getElementById("modal-workgroup-close-body");
        const partialAllowed = rows.length > 0 && rows.every(row => !machine(row));
        body.insertAdjacentHTML("afterbegin", `<div class="wg-close-summary mb-3">
            <div class="wg-eyebrow">REGISTRA PRODUZIONE</div>
            <p class="mb-3">Quantità, scarti e lotti restano distinti per ogni ordine.</p>
            <div class="row g-3">
                <div class="col-md-8"><label class="form-label fw-semibold" for="wg-close-mode">Modalità di avanzamento</label>
                    <select class="form-select" id="wg-close-mode">
                        <option value="total">Termina tutti gli ordini del gruppo</option>
                        ${partialAllowed ? '<option value="partial">Registra quantità parziali e sospendi il gruppo</option>' : ""}
                    </select>
                </div>
                <div class="col-md-4"><label class="form-label fw-semibold" for="wg-close-stop">Fermo del gruppo (min)</label>
                    <input class="form-control" id="wg-close-stop" type="number" min="0" step="1" value="0" required aria-describedby="wg-stop-help">
                </div>
            </div>
            <div id="wg-stop-help" class="small text-muted mt-2">Inserisci i minuti di fermo da sottrarre al gruppo. Indica solo fermi non già registrati.</div>
            <div class="wg-outcome mt-3" id="wg-close-outcome" role="status"></div>
        </div>`);
        const select = document.getElementById("wg-close-mode");
        const confirm = document.getElementById("btn-workgroup-close-confirm");
        const update = () => {
            const partial = select.value === "partial";
            body.querySelectorAll('[data-field="chiusura_parziale"]').forEach(input => { input.checked = partial; });
            body.querySelectorAll(".js-workgroup-close-single").forEach(button => {
                button.disabled = partial;
                button.title = partial ? "La registrazione parziale riguarda tutto il gruppo" : "Chiudi totalmente solo questo ordine";
            });
            document.getElementById("wg-close-outcome").textContent = partial
                ? "Le quantità dichiarate verranno avanzate. I residui resteranno nel gruppo sospeso, pronti per la ripresa."
                : "Ogni ordine verrà avanzato separatamente sulla fase corrente. Il gruppo verrà terminato; eventuali fasi successive seguiranno il flusso normale.";
            confirm.textContent = partial ? "Registra parziali e sospendi" : "Avanza e termina il gruppo";
        };
        select.addEventListener("change", update);
        rows.forEach((row, index) => {
            const card = Array.from(body.querySelectorAll("[data-order-key]")).find(el => el.dataset.orderKey === key(row));
            if (!card) return;
            card.classList.add("wg-close-order");
            card.querySelector('[data-field="chiusura_parziale"]')?.closest(".form-check")?.setAttribute("hidden", "");
            card.insertAdjacentHTML("afterbegin", `<div class="wg-order-meta"><span class="badge bg-light text-dark border">Ordine ${index + 1} di ${rows.length}</span></div>`);
            card.insertAdjacentHTML("beforeend", `<div class="small text-muted mt-2">Se chiudi solo questo ordine: ${esc(singleOutcome(rows, row))}</div>`);
            card.querySelectorAll("input[data-field]").forEach((input, i) => {
                input.id = `wg-order-${index}-${i}`;
                const label = input.parentElement.querySelector("label");
                if (label) label.htmlFor = input.id;
            });
        });
        update();
    }

    function beginClose() {
        const modal = document.getElementById("modalWorkgroupClose");
        if (modal.dataset.submitting === "1") return false;
        modal.dataset.submitting = "1";
        modal.querySelectorAll("button, input, select, textarea").forEach(input => {
            input.dataset.wasDisabled = input.disabled ? "1" : "0";
            input.disabled = true;
        });
        return true;
    }
    function endClose() {
        const modal = document.getElementById("modalWorkgroupClose");
        modal.dataset.submitting = "0";
        modal.querySelectorAll("[data-was-disabled]").forEach(input => {
            input.disabled = input.dataset.wasDisabled === "1";
            delete input.dataset.wasDisabled;
        });
    }
    window.AvpWorkgroups = {singleOutcome, mountClose, stopMinutes, validateStop, beginClose, endClose};

    let currentMode = "", orders = [], submitting = false;
    function ensureOpenModal() {
        if (document.getElementById("modalWorkgroupOpen")) return;
        document.body.insertAdjacentHTML("beforeend", `<div class="modal fade wg-modal" id="modalWorkgroupOpen" tabindex="-1" aria-labelledby="modalWorkgroupOpenTitle" aria-hidden="true">
            <div class="modal-dialog modal-xl modal-dialog-centered modal-dialog-scrollable"><div class="modal-content">
                <div class="modal-header"><div><div class="wg-eyebrow">LAVORAZIONE CONGIUNTA</div><h5 class="modal-title" id="modalWorkgroupOpenTitle">Crea gruppo</h5></div><button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Chiudi"></button></div>
                <div class="modal-body">
                    <p id="wg-open-help" class="text-muted"></p>
                    <div id="modal-workgroup-feedback" class="alert alert-danger d-none" role="alert"></div>
                    <div class="d-flex justify-content-between align-items-center"><label class="form-label" for="wg-order-search">Cerca negli ordini visibili</label><button class="btn btn-link btn-sm" type="button" id="wg-clear-selection">Azzera selezione</button></div>
                    <input class="form-control mb-3" type="search" id="wg-order-search" placeholder="Ordine, codice o descrizione">
                    <div class="wg-open-layout"><div class="table-responsive wg-order-picker"><table class="table table-hover align-middle mb-0"><caption class="visually-hidden">Ordini disponibili per il gruppo</caption><thead id="modal-workgroup-head"></thead><tbody id="modal-workgroup-body"></tbody></table></div>
                    <aside class="wg-open-summary" aria-labelledby="wg-preview-title"><h6 id="wg-preview-title">Anteprima del gruppo</h6><div id="wg-open-preview" role="status" aria-live="polite"></div></aside></div>
                </div>
                <div class="modal-footer"><span class="small text-muted me-auto" id="wg-open-count"></span><button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Annulla</button><button type="button" class="btn btn-primary" id="btn-workgroup-confirm">Avvia gruppo</button></div>
            </div></div></div>`);
        document.getElementById("modal-workgroup-body").addEventListener("change", updateOpen);
        document.getElementById("wg-order-search").addEventListener("input", () => {
            const search = document.getElementById("wg-order-search").value.trim().toLocaleLowerCase("it-IT");
            document.querySelectorAll("#modal-workgroup-body tr").forEach(row => { row.hidden = !row.textContent.toLocaleLowerCase("it-IT").includes(search); });
        });
        document.getElementById("wg-clear-selection").addEventListener("click", () => {
            if (submitting) return;
            document.querySelectorAll("#modal-workgroup-body input").forEach(input => { input.checked = false; });
            updateOpen();
        });
        document.getElementById("btn-workgroup-confirm").addEventListener("click", submitOpen);
    }
    function feedback(message) {
        const el = document.getElementById("modal-workgroup-feedback");
        el.textContent = message;
        el.classList.toggle("d-none", !message);
    }
    function selection() {
        return Array.from(document.querySelectorAll("#modal-workgroup-body input:checked")).map(input => ({input, order: orders[Number(input.value)]}));
    }
    function updateOpen() {
        const selected = selection();
        const main = selected.find(s => s.input.dataset.role === "main");
        const anchor = main?.order || selected[0]?.order;
        const timed = selected.filter(s => s.input.dataset.role !== "mask");
        const masks = selected.filter(s => s.input.dataset.role === "mask");
        const valid = currentMode === "multiplo" ? selected.length >= 2 && selected.length <= 4
            : currentMode === "misto" ? timed.length === 2 && masks.length >= 1 && masks.length <= 3
            : timed.length === 1 && masks.length >= 1;
        document.querySelectorAll("#modal-workgroup-body input").forEach(input => {
            const order = orders[Number(input.value)];
            const otherRole = selected.some(s => key(s.order) === key(order) && s.input !== input);
            let reason = order.motivo_blocco || "";
            if (!reason && anchor && (order.reparto !== anchor.reparto || order.kind !== anchor.kind)) reason = "Reparto o tipo di ordine incompatibile con la selezione";
            if (!reason && otherRole) reason = "Ordine già selezionato con un altro ruolo";
            if (!reason && !input.checked && currentMode === "multiplo" && selected.length >= 4) reason = "Massimo 4 ordini";
            if (!reason && !input.checked && currentMode === "misto" && input.dataset.role === "shared" && timed.length >= 2) reason = "Sono già selezionati i 2 ordini a tempo condiviso";
            if (!reason && !input.checked && currentMode === "misto" && input.dataset.role === "mask" && masks.length >= 3) reason = "Massimo 3 mascherati";
            input.disabled = submitting || !!reason;
            input.title = reason;
            input.closest("td").title = reason;
        });
        document.querySelectorAll("#modal-workgroup-body tr").forEach(row => {
            const inputs = Array.from(row.querySelectorAll("input"));
            const active = inputs.some(i => i.checked);
            row.classList.toggle("wg-selected", active);
            const reason = inputs.every(i => i.disabled) ? inputs[0].title : "";
            row.querySelector(".wg-block-reason").textContent = reason;
        });
        const validSelection = valid && selected.every(s => !s.order.motivo_blocco);
        document.getElementById("btn-workgroup-confirm").disabled = submitting || !validSelection;
        document.getElementById("wg-open-count").textContent = `${selected.length} ordini selezionati${currentMode === "multiplo" ? " · da 2 a 4" : ""}`;
        document.getElementById("wg-open-preview").innerHTML = selected.length ? `<div class="wg-preview-count">${selected.length}<span> ordini</span></div><ul class="list-unstyled mb-3">${selected.map(s => `<li><strong>${esc(s.order.ordine)}</strong><span>${s.input.dataset.role === "mask" ? "Mascherato" : s.input.dataset.role === "main" ? "Principale" : "Membro"}</span></li>`).join("")}</ul><p class="small mb-0">Quantità, lotti e avanzamenti restano separati per ciascun ordine.</p>` : '<p class="text-muted mb-0">Seleziona gli ordini da lavorare insieme.</p>';
    }

    async function openFrom(button) {
        ensureOpenModal();
        currentMode = button.classList.contains("js-open-gruppo-multiplo") ? "multiplo" : button.classList.contains("js-open-gruppo-misto") ? "misto" : "mascherato";
        const selector = button.dataset.workgroupTable;
        const related = {"#ordini_da_eseguire": "#ordini_in_corso", "#ordini_da_eseguire_sl": "#ordini_in_corso_sl", "#ordini_da_eseguire_m": "#ordini_in_corso_m"};
        const visible = new Set([selector, related[selector]].filter(Boolean).flatMap(s => Array.from(document.querySelectorAll(`${s} tbody tr[data-iddocumento][data-idriga]`)))
            .filter(row => row.offsetParent !== null && row.dataset.isWorkGroup !== "1")
            .map(row => `${row.dataset.iddocumento}|${row.dataset.idriga}`));
        const modal = document.getElementById("modalWorkgroupOpen");
        document.getElementById("modalWorkgroupOpenTitle").textContent = {multiplo: "Crea gruppo multiplo", misto: "Crea gruppo misto", mascherato: "Crea gruppo mascherato"}[currentMode];
        document.getElementById("wg-open-help").textContent = {multiplo: "Seleziona da 2 a 4 ordini dello stesso reparto e tipo.", misto: "Seleziona 2 ordini e da 1 a 3 ordini mascherati.", mascherato: "Scegli un ordine principale e almeno un ordine mascherato."}[currentMode];
        document.getElementById("wg-order-search").value = "";
        document.getElementById("modal-workgroup-body").innerHTML = '<tr><td>Caricamento ordini...</td></tr>';
        document.getElementById("modal-workgroup-head").innerHTML = "";
        document.getElementById("wg-open-preview").textContent = "Caricamento...";
        document.getElementById("wg-open-count").textContent = "";
        document.getElementById("btn-workgroup-confirm").disabled = true;
        feedback("");
        bootstrap.Modal.getOrCreateInstance(modal).show();
        try {
            const response = await window.avpFetch("/api/ordini/gruppi/disponibili", {credentials: "same-origin"});
            const data = await response.json();
            if (!response.ok || data.ok === false) throw new Error(data.error || "Impossibile caricare gli ordini");
            orders = data.orders.filter(order => visible.has(key(order)));
            const controls = order => {
                const index = orders.indexOf(order);
                const input = (role, type="checkbox") => `<input class="form-check-input" type="${type}" ${type === "radio" ? 'name="wg-main"' : ""} data-role="${role}" value="${index}" aria-label="${esc(role === "mask" ? "Mascherato" : role === "main" ? "Principale" : "Tempo condiviso")} ${esc(order.ordine)}">`;
                return currentMode === "multiplo" ? `<td>${input("shared")}</td>` : currentMode === "misto" ? `<td>${input("shared")}</td><td>${input("mask")}</td>` : `<td>${input("main", "radio")}</td><td>${input("mask")}</td>`;
            };
            document.getElementById("modal-workgroup-head").innerHTML = `<tr>${currentMode === "multiplo" ? '<th scope="col">Scegli</th>' : `<th scope="col">${currentMode === "misto" ? "Condiviso" : "Principale"}</th><th scope="col">Mascherato</th>`}<th scope="col">Ordine / articolo</th><th scope="col" class="d-none d-md-table-cell">Residuo</th><th scope="col" class="d-none d-md-table-cell">Fase / stato</th></tr>`;
            document.getElementById("modal-workgroup-body").innerHTML = orders.map(order => `<tr>${controls(order)}<td><strong class="d-block">${esc(order.ordine)}</strong><div>${esc(order.cod_art)}</div><div class="small text-muted">${esc(order.descrizione)}</div><div class="small text-muted d-md-none">Residuo ${esc(order.quantita)} · ${esc(window.phaseLabel(order.fase))} · ${esc(order.stato)}</div><div class="small text-danger wg-block-reason"></div></td><td class="d-none d-md-table-cell">${esc(order.quantita)}</td><td class="d-none d-md-table-cell">${esc(window.phaseLabel(order.fase))}<div class="small text-muted">${esc(order.stato)}${order.operatore ? ` · ${esc(order.operatore)}` : ""}</div></td></tr>`).join("");
            if (!orders.length) feedback("Nessun ordine raggruppabile tra quelli visibili. Controlla i filtri della tabella.");
            updateOpen();
        } catch (error) {
            document.getElementById("modal-workgroup-body").innerHTML = "";
            document.getElementById("wg-open-preview").textContent = "Ordini non disponibili";
            feedback(error.message || "Errore di comunicazione con il server");
        }
    }

    async function submitOpen() {
        if (submitting) return;
        const selected = selection();
        const orderKey = order => ({id_documento: order.id_documento, id_riga: order.id_riga});
        const timed = selected.filter(s => s.input.dataset.role !== "mask").map(s => orderKey(s.order));
        const masked = selected.filter(s => s.input.dataset.role === "mask").map(s => orderKey(s.order));
        if (document.getElementById("btn-workgroup-confirm").disabled) return;
        const payload = currentMode === "multiplo" ? {orders: timed} : currentMode === "misto" ? {condivisi: timed, mascherati: masked} : {principale: timed[0], mascherati: masked};
        submitting = true;
        updateOpen();
        feedback("");
        try {
            const response = await window.avpFetch(`/api/ordini/gruppi/${currentMode}/apri`, {method: "POST", credentials: "same-origin", headers: {"Content-Type": "application/json", "X-Requested-With": "XMLHttpRequest"}, body: JSON.stringify(payload)});
            const data = await response.json();
            if (!response.ok || data.ok === false) throw new Error(data.error || "Errore apertura gruppo");
            if (data.fragments && window.applyHomeFragments) window.applyHomeFragments(data.fragments);
            bootstrap.Modal.getOrCreateInstance(document.getElementById("modalWorkgroupOpen")).hide();
            alert(data.message || "Gruppo avviato correttamente");
        } catch (error) { feedback(error.message || "Errore di comunicazione con il server"); }
        finally { submitting = false; updateOpen(); }
    }
    document.addEventListener("click", event => {
        const button = event.target.closest(".js-open-gruppo-multiplo, .js-open-gruppo-mascherato, .js-open-gruppo-misto");
        if (!button) return;
        event.preventDefault();
        if (!submitting) openFrom(button);
    });
})();
