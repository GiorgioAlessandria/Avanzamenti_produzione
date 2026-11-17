// La delega mantiene attivi anche i pulsanti ricreati dagli aggiornamenti AJAX.
document.addEventListener('click', function(event) {
	var toastTrigger = event.target.closest('[data-bs-toggle="toast"]');
	if (!toastTrigger || toastTrigger.disabled) return;
	var toastSelector = toastTrigger.getAttribute('data-bs-target');

	if (!toastSelector) return;

	try {
		var toastEl = document.querySelector(toastSelector);

		if (!toastEl) return;

		var toast = bootstrap.Toast.getOrCreateInstance(toastEl);
		toast.show();
	}
	catch(e) {
		console.error(e);
	}
}, false);
