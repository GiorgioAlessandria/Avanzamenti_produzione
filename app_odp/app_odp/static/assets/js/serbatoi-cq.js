(() => {
    function campo(tipo, testo, valore, max) {
        const label = document.createElement("label");
        label.className = "form-label mb-0";
        label.textContent = testo;
        const input = document.createElement("input");
        input.type = tipo;
        input.className = tipo === "checkbox" ? "form-check-input ms-2" : "form-control form-control-sm";
        if (tipo === "number") {
            input.min = testo === "Verificati" ? 1 : 0;
            input.max = max;
            input.step = 1;
            input.value = valore;
        }
        input.required = tipo !== "text";
        if (tipo === "text") input.maxLength = 300;
        label.append(input);
        return {label, input};
    }

    function compila(piani, ordini) {
        const dialog = document.createElement("dialog");
        dialog.className = "border rounded bg-body text-body p-3 shadow";
        dialog.style.cssText = "width:min(54rem,94vw);max-height:90dvh";
        dialog.setAttribute("aria-labelledby", "cq-fresatura-titolo");
        const form = document.createElement("form");
        form.innerHTML = '<h2 class="h5" id="cq-fresatura-titolo">Controllo qualità · Fresatura</h2>'
            + '<p>Verifica i controlli prima di registrare l’avanzamento.</p>';
        const schede = [];
        piani.forEach((piano, indice) => {
            if (!piano) return;
            const ordine = ordini[indice];
            const conformi = Number(ordine.quantita_conforme);
            const ko = Number(ordine.quantita_non_conforme);
            const totale = conformi + ko;
            const scheda = document.createElement("fieldset");
            scheda.className = "border rounded p-3 mb-3";
            const titolo = document.createElement("legend");
            titolo.className = "fs-6 fw-semibold";
            titolo.textContent = `${piano.codice} · ${ordine.riferimento_cq || ordine.id_documento + "/" + ordine.id_riga} · ${conformi} OK / ${ko} KO`;
            scheda.append(titolo);
            const righe = piano.controlli.map(controllo => {
                const riga = document.createElement("div");
                riga.className = "border-top pt-2 mb-3";
                const descrizione = document.createElement("p");
                descrizione.className = "fw-semibold mb-1";
                descrizione.textContent = controllo.descrizione;
                const istruzioni = document.createElement("p");
                istruzioni.className = "small text-body-secondary mb-2";
                istruzioni.textContent = [controllo.frequenza, controllo.strumento, controllo.valore, controllo.note, controllo.reazione].filter(Boolean).join(" · ");
                const minimo = Math.min(totale, controllo.campione || totale);
                const verificati = campo("number", "Verificati", minimo, totale);
                verificati.input.min = minimo;
                const difetti = campo("number", "KO", 0, totale);
                const note = campo("text", "Note", "", totale);
                const eseguito = campo("checkbox", "Eseguito", "", totale);
                const input = document.createElement("div");
                input.className = "d-flex flex-wrap gap-3 align-items-end";
                input.append(verificati.label, difetti.label, note.label, eseguito.label);
                riga.append(descrizione, istruzioni, input);
                scheda.append(riga);
                return {verificati, difetti, note, eseguito};
            });
            form.append(scheda);
            schede.push({ordine, piano, righe});
        });
        const azioni = document.createElement("div");
        const errore = document.createElement("p");
        errore.className = "text-danger mb-2";
        errore.setAttribute("role", "alert");
        azioni.className = "d-flex justify-content-end gap-2";
        azioni.innerHTML = '<button type="button" class="btn btn-outline-secondary">Annulla</button>'
            + '<button type="submit" class="btn btn-primary">Conferma controlli</button>';
        form.append(errore, azioni);
        dialog.append(form);
        document.body.append(dialog);
        return new Promise(resolve => {
            azioni.firstElementChild.addEventListener("click", () => dialog.close());
            form.addEventListener("submit", event => {
                event.preventDefault();
                for (const {ordine, piano, righe} of schede) {
                    const difetti = righe.map(r => Number(r.difetti.input.value));
                    const ko = Number(ordine.quantita_non_conforme);
                    if (difetti.some((n, i) => n > Number(righe[i].verificati.input.value)) ||
                        Math.max(...difetti) > ko || difetti.reduce((tot, n) => tot + n, 0) < ko) {
                        errore.textContent = `${piano.codice}: verifica i KO rilevati. Il totale dell’avanzamento è ${ko} pezzi KO distinti.`;
                        return;
                    }
                }
                schede.forEach(({ordine, piano, righe}) => {
                    ordine.cq_fresatura = {
                        versione: piano.versione,
                        esiti: righe.map(r => ({
                            verificati: r.verificati.input.value, ko: r.difetti.input.value,
                            note: r.note.input.value, eseguito: r.eseguito.input.checked
                        }))
                    };
                });
                dialog.close("conferma");
            });
            dialog.addEventListener("close", () => {
                resolve(dialog.returnValue === "conferma");
                dialog.remove();
            }, {once: true});
            dialog.showModal();
        });
    }

    window.avpCqcFresatura = async ordini => {
        const controllabili = ordini.filter(ordine => ordine.modalita_lotti !== "m");
        if (!controllabili.length) return true;
        const risposta = await window.avpFetch("/api/ordini/piani-cq", {
            method: "POST",
            headers: {"Content-Type": "application/json"},
            body: JSON.stringify(controllabili.map(({id_documento, id_riga}) => ({id_documento, id_riga})))
        });
        if (!risposta.ok) throw Error("Impossibile caricare i piani di controllo qualità.");
        const {piani} = await risposta.json();
        return !piani.some(Boolean) || compila(piani, controllabili);
    };
})();
