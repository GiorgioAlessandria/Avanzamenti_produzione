(() => {
    const sync = document.getElementById('serbatoi-sync');
    if (!sync) return;
    const warn = document.getElementById('serbatoi-avvisi');
    const show = message => { warn.textContent = message; warn.classList.remove('d-none'); };
    fetch(sync.action, {method: 'POST', body: new FormData(sync), headers: {'X-Requested-With': 'XMLHttpRequest'}})
        .then(async response => {
            if (!response.ok) throw new Error('Acquisizione ordini non completata. Ricarica la pagina.');
            const data = await response.json();
            if (!data.ok) throw new Error(data.error);
            if (data.avvisi?.length) show(data.avvisi.join(' '));
            if (data.changed && !document.querySelector('form[data-dirty="true"]')) location.reload();
        }).catch(error => show(error.message));
    document.querySelectorAll('.serbatoi form:not(#serbatoi-sync)').forEach(form => {
        form.addEventListener('input', () => { form.dataset.dirty = 'true'; });
        form.addEventListener('submit', () => {
            if (form.method.toLowerCase() === 'post') {
                form.querySelectorAll('button').forEach(button => { button.disabled = true; });
            }
        });
    });
})();
