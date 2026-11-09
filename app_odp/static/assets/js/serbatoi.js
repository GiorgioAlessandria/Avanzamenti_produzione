(() => {
    const sync = document.getElementById('serbatoi-sync');
    if (!sync) return;
    const show = message => { const target = document.getElementById('serbatoi-avvisi'); target.textContent = message; target.classList.remove('d-none'); };
    const phaseScroll = document.querySelector('.serbatoi-flow-scroll');
    const activePhase = phaseScroll?.querySelector('.serbatoi-flow-step.is-active')?.closest('li');
    if (activePhase && phaseScroll.scrollWidth > phaseScroll.clientWidth) {
        phaseScroll.scrollLeft += activePhase.getBoundingClientRect().left - phaseScroll.getBoundingClientRect().left
            - (phaseScroll.clientWidth - activePhase.offsetWidth) / 2;
    }
    const views = new Map();
    history.scrollRestoration = 'manual';
    let navigation;
    let restoring = false;
    let graficoPrezzi;
    let richiestaMercato;
    const context = url => {
        const key = new URL(url, location.href);
        key.searchParams.delete('id');
        key.searchParams.sort();
        return key.pathname + '?' + key.searchParams;
    };
    const snapshot = () => ({
        x: window.scrollX, y: window.scrollY,
        panels: [...document.querySelectorAll('.serbatoi-list, .serbatoi-flow-scroll, .serbatoi-phase-orders')]
            .map(el => [el.scrollLeft, el.scrollTop]),
    });
    const remember = () => {
        if (restoring || navigation) return;
        const view = snapshot();
        views.set(context(location.href), view);
        try { sessionStorage.setItem('serbatoi-view', JSON.stringify({url: location.href, view})); } catch (_) { /* Storage can be disabled. */ }
        history.replaceState({...history.state, serbatoiView: view}, '');
    };
    const restore = view => {
        if (!view) return;
        restoring = true;
        document.querySelectorAll('.serbatoi-list, .serbatoi-flow-scroll, .serbatoi-phase-orders').forEach((el, i) => {
            const pos = view.panels?.[i];
            if (pos) {
                el.scrollLeft = pos[0];
                el.scrollTop = el.classList.contains('serbatoi-flow-scroll') ? 0 : pos[1];
            }
        });
        window.scrollTo({left: view.x || 0, top: view.y || 0, behavior: 'instant'});
        restoring = false;
    };
    const formatoMese = new Intl.DateTimeFormat('it-IT', {timeZone: 'UTC', year: 'numeric', month: 'short'});
    const formatoData = new Intl.DateTimeFormat('it-IT', {timeZone: 'UTC'});
    const formatoPrezzo = new Intl.NumberFormat('it-IT', {minimumFractionDigits: 2, maximumFractionDigits: 2});
    const taccheMensili = scala => {
        const inizio = new Date(scala.min);
        const fine = new Date(scala.max);
        const primo = inizio.getUTCFullYear() * 12 + inizio.getUTCMonth()
            + (Date.UTC(inizio.getUTCFullYear(), inizio.getUTCMonth(), 1) < scala.min ? 1 : 0);
        const ultimo = fine.getUTCFullYear() * 12 + fine.getUTCMonth();
        const capienza = Math.max(2, Math.floor(scala.width / 105));
        const passo = Math.ceil((ultimo - primo + 1) / capienza);
        const tacche = [];
        for (let mese = primo; mese <= ultimo; mese += passo) {
            const valore = Date.UTC(Math.floor(mese / 12), mese % 12, 1);
            if (valore >= scala.min && valore <= scala.max) tacche.push({value: valore});
        }
        scala.ticks = tacche.length ? tacche : [{value: (scala.min + scala.max) / 2}];
    };
    const aggiornaGraficoPrezzi = (canvas, mercato = {}) => {
        graficoPrezzi?.destroy();
        graficoPrezzi = undefined;
        if (!canvas || !window.Chart) return;

        const {ordini, riferimento, soglia} = JSON.parse(document.getElementById('serbatoi-prezzi-dati').textContent);
        const punti = ordini.map(ordine => ({x: Date.parse(ordine.data), y: ordine.centesimi / 100, ordine: ordine.ordine}));
        const mensili = (mercato.punti || []).map(p => ({x: Date.parse(p.data), y: p.euro_kg}));
        const ultimiMesi = mensili.slice(-24);
        const inizioMercato = Math.min(punti[0]?.x ?? Infinity, ultimiMesi[0]?.x ?? Infinity);
        const quotazioni = mensili.filter(p => p.x >= inizioMercato);
        const stima = mercato.stima && {
            x: Date.parse(mercato.stima.cambio_data || mercato.stima.data),
            y: mercato.stima.euro_kg,
            cambioData: mercato.stima.cambio_data
        };
        const date = [...punti, ...quotazioni, ...(stima ? [stima] : [])].map(p => p.x);
        if (!date.length && riferimento === null) return;

        const giorno = 86400000;
        const inizio = date.length ? Math.min(...date) : Date.now() - 30 * giorno;
        const fine = date.length ? Math.max(...date) : Date.now();
        const margine = Math.min(14 * giorno, Math.max(giorno, (fine - inizio) * .025));
        const min = inizio - margine;
        const max = fine + margine;
        const datasets = [];
        const orizzontale = y => [{x: min, y}, {x: max, y}];
        if (riferimento !== null) {
            const base = riferimento / 100;
            const percentuale = soglia / 100;
            const scarto = base * percentuale / 100;
            const etichetta = percentuale.toLocaleString('it-IT') + '%';
            const limite = {borderColor: '#ae7427', borderWidth: 1, borderDash: [3, 4], pointRadius: 0, pointHitRadius: 0, fascia: true, prioritaLegenda: 4};
            datasets.push(
                {...limite, label: 'Limite inferiore', data: orizzontale(base - scarto), mostraLegenda: false},
                {...limite, label: 'Fascia ±' + etichetta, data: orizzontale(base + scarto), fill: '-1', backgroundColor: 'rgba(188,120,23,.16)'},
                {label: 'Riferimento', data: orizzontale(base), borderColor: '#ae7427', borderWidth: 1.5, borderDash: [8, 4], pointRadius: 0, pointHitRadius: 0, prioritaLegenda: 3}
            );
        }
        if (quotazioni.length) {
            datasets.push({
                label: 'LME Londra',
                data: quotazioni,
                fill: 'origin',
                backgroundColor: 'rgba(17, 122, 116, .30)',
                borderColor: '#117a74',
                borderWidth: 2,
                pointRadius: 0,
                pointHoverRadius: 0,
                pointHitRadius: 8,
                order: 1,
                prioritaLegenda: 1,
                mensile: true
            });
        }
        if (stima && quotazioni.length) {
            datasets.push({label: 'Stima mese in corso', data: [quotazioni[quotazioni.length - 1], stima],
                borderColor: '#8a56c2', borderWidth: 2.5, borderDash: [6, 4], pointRadius: [0, 4], mensile: true, prioritaLegenda: 2});
        }
        if (punti.length) {
            datasets.push({label: 'Prezzi ordini', data: punti, borderColor: '#2465c7', backgroundColor: '#2465c7', borderWidth: 3, pointRadius: 4, prioritaLegenda: 0});
        }
        const stile = getComputedStyle(canvas);
        const color = stile.color;
        const griglia = stile.getPropertyValue('--bs-border-color-translucent').trim() || 'rgba(127,127,127,.2)';
        graficoPrezzi = new window.Chart(canvas, {
            type: 'line',
            data: {datasets},
            options: {
                parsing: false, maintainAspectRatio: false, animation: false, color,
                layout: {padding: {top: 6, right: 8}},
                scales: {
                    x: {type: 'linear', min, max, afterBuildTicks: taccheMensili, grid: {display: false},
                        ticks: {color, maxRotation: 0, padding: 8, font: {size: 13}, callback: value => formatoMese.format(value)}},
                    y: {beginAtZero: quotazioni.length > 0, grid: {color: griglia},
                        ticks: {color, padding: 8, font: {size: 13}, callback: value => formatoPrezzo.format(value) + ' €'}}
                },
                plugins: {
                    legend: {
                        align: 'start',
                        labels: {color, boxWidth: 20, boxHeight: 10, padding: 14, font: {size: 13},
                            filter: item => datasets[item.datasetIndex].mostraLegenda !== false,
                            sort: (a, b) => datasets[a.datasetIndex].prioritaLegenda - datasets[b.datasetIndex].prioritaLegenda},
                        onClick: (event, item, legend) => {
                            if (!datasets[item.datasetIndex].fascia) {
                                window.Chart.defaults.plugins.legend.onClick(event, item, legend);
                                return;
                            }
                            const chart = legend.chart;
                            const visibile = !chart.isDatasetVisible(item.datasetIndex);
                            datasets.forEach((dataset, indice) => {
                                if (dataset.fascia) chart.setDatasetVisibility(indice, visibile);
                            });
                            chart.update();
                        }
                    },
                    tooltip: {callbacks: {
                        title: items => (items[0].dataset.mensile ? formatoMese : formatoData).format(items[0].parsed.x),
                        label: item => (item.raw.ordine || item.dataset.label) + ' · ' + formatoPrezzo.format(item.parsed.y) + ' €/kg',
                        afterLabel: item => item.raw.cambioData ? 'Cambio BCE del ' + formatoData.format(Date.parse(item.raw.cambioData)) : ''
                    }}
                }
            }
        });
    };
    const initialize = () => {
        const canvas = document.getElementById('serbatoi-prezzi-grafico');
        aggiornaGraficoPrezzi(canvas);
        const statoMercato = document.getElementById('serbatoi-mercato-stato');
        if (canvas && statoMercato) {
            richiestaMercato ??= fetch(statoMercato.dataset.url).then(r => {
                if (!r.ok) throw Error('Quotazione non disponibile');
                return r.json();
            });
            richiestaMercato.then(mercato => {
                if (!canvas.isConnected) return;
                aggiornaGraficoPrezzi(canvas, mercato);
                const ultimo = mercato.punti[mercato.punti.length - 1];
                const messaggi = [];
                if (ultimo) {
                    messaggi.push(`Ultimo mese pubblicato ${formatoMese.format(Date.parse(ultimo.data))} · ${formatoPrezzo.format(ultimo.euro_kg)} €/kg`);
                }
                if (mercato.stima) {
                    const {data, euro_kg, cambio_data, metodo} = mercato.stima;
                    const cambio = cambio_data ? ` · cambio BCE del ${formatoData.format(Date.parse(cambio_data))}` : ' · cambio giornaliero non disponibile';
                    const base = metodo === 'ultima_quotazione' ? ' · basata sull’ultima quotazione mensile' : '';
                    messaggi.push(`Stima ${formatoMese.format(Date.parse(data))} · ${formatoPrezzo.format(euro_kg)} €/kg${cambio}${base}`);
                }
                if (mercato.copia_locale) messaggi.push('Ultima serie disponibile in copia locale');
                statoMercato.textContent = messaggi.join(' · ') || 'Serie mensile non disponibile.';
            }).catch(() => {
                richiestaMercato = undefined;
                statoMercato.textContent = 'Quotazione mensile non disponibile.';
            });
        }
        document.querySelectorAll('[data-serbatoi-open]').forEach(link => {
            const details = document.getElementById(link.dataset.serbatoiOpen);
            if (!details) return;
            link.setAttribute('aria-expanded', String(details.open));
            details.addEventListener('toggle', () => link.setAttribute('aria-expanded', String(details.open)));
        });
    };
    const navigate = async (url, push = true, view) => {
        if (push) remember();
        const targetView = view || views.get(context(url)) || snapshot();
        navigation?.abort();
        const controller = new AbortController();
        navigation = controller;
        const main = document.getElementById('serbatoi-app');
        main.setAttribute('aria-busy', 'true');
        try {
            const response = await fetch(url, {signal: controller.signal});
            const html = new DOMParser().parseFromString(await response.text(), 'text/html');
            const replacement = html.getElementById('serbatoi-app');
            if (!response.ok || !replacement) {
                location.assign(response.redirected ? response.url : url);
                return;
            }
            if (controller.signal.aborted) return;
            if (push) history.pushState({...history.state, serbatoiView: targetView}, '', url);
            main.replaceWith(replacement);
            initialize();
            restore(targetView);
        } catch (error) {
            if (error.name !== 'AbortError') {
                const warning = document.getElementById('serbatoi-avvisi');
                warning.textContent = 'Impossibile aprire il dettaglio. Riprova o aggiorna la pagina.';
                warning.classList.remove('d-none');
            }
        } finally {
            if (navigation === controller) navigation = null;
            document.getElementById('serbatoi-app')?.removeAttribute('aria-busy');
        }
    };
    initialize();
    let initialView = history.state?.serbatoiView;
    try {
        const saved = JSON.parse(sessionStorage.getItem('serbatoi-view'));
        if (performance.getEntriesByType('navigation')[0]?.type === 'reload' && saved?.url === location.href) initialView = saved.view;
    } catch (_) { /* History still works without storage. */ }
    restore(initialView);
    window.addEventListener('pagehide', remember);
    let scrollTimer;
    document.addEventListener('scroll', () => {
        clearTimeout(scrollTimer);
        scrollTimer = setTimeout(remember, 150);
    }, true);
    window.addEventListener('popstate', event => navigate(location.href, false, event.state?.serbatoiView));
    document.addEventListener('click', event => {
        if (event.button !== 0 || event.ctrlKey || event.metaKey || event.shiftKey || event.altKey) return;
        const opener = event.target.closest('[data-serbatoi-open]');
        if (opener) {
            event.preventDefault();
            const details = document.getElementById(opener.dataset.serbatoiOpen);
            details.open = true;
            details.scrollIntoView({block: 'nearest'});
            details.querySelector('input[name="quantita"]')?.focus({preventScroll: true});
            return;
        }
        let link = event.target.closest('a[href]');
        if (!link && !event.target.closest('button, input, select, textarea, label, summary')) {
            link = event.target.closest('[data-serbatoi-row]')?.querySelector('a[href]');
        }
        if (!link || !link.closest('#serbatoi-app') || link.target || link.hasAttribute('download')) return;
        const url = new URL(link.href, location.href);
        if (url.origin !== location.origin || url.pathname !== location.pathname || url.hash) return;
        event.preventDefault();
        navigate(url.href);
    });
    document.addEventListener('keydown', event => {
        if (event.target.matches('[data-serbatoi-row]') && ['Enter', ' '].includes(event.key)) {
            event.preventDefault();
            event.target.querySelector('a[href]')?.click();
        }
    });
    fetch(sync.action, {method: 'POST', body: new FormData(sync), headers: {'X-Requested-With': 'XMLHttpRequest'}})
        .then(async response => {
            if (!response.ok) throw new Error('Acquisizione ordini non completata. Ricarica la pagina.');
            const data = await response.json();
            if (!data.ok) throw new Error(data.error);
            if (data.avvisi?.length) show(data.avvisi.join(' '));
            if (data.changed && !document.querySelector('form[data-dirty="true"]')) { remember(); location.reload(); }
        }).catch(error => show(error.message));
    document.addEventListener('input', event => {
        const form = event.target.closest('.serbatoi form:not(#serbatoi-sync)');
        if (form) form.dataset.dirty = 'true';
    });
    document.addEventListener('submit', event => {
        const form = event.target.closest('.serbatoi form:not(#serbatoi-sync)');
        if (!form) return;
        remember();
        if (form.method.toLowerCase() === 'post') {
            form.querySelectorAll('button').forEach(button => { button.disabled = true; });
        }
    });
})();
