(function () {
  const init = () => {
    const form = document.querySelector('[data-registration-form]');
    if (!form) {
      return;
    }

    const accountFieldName = form.getAttribute('data-account-type-field');
    if (!accountFieldName) {
      return;
    }

    const radios = form.querySelectorAll(`input[name="${accountFieldName}"]`);
    if (!radios.length) {
      return;
    }

    const sections = form.querySelectorAll('[data-account-type-section]');
    const toggleAccountSections = () => {
      const selected = form.querySelector(`input[name="${accountFieldName}"]:checked`);
      const selectedValue = selected ? selected.value : null;

      sections.forEach((section) => {
        const isActive = section.dataset.accountType === selectedValue;
        section.classList.toggle('d-none', !isActive);
        section.toggleAttribute('hidden', !isActive);
        section.style.display = isActive ? '' : 'none';
        section.querySelectorAll('input, select, textarea').forEach((field) => {
          field.disabled = !isActive;
          const requires = field.dataset.required === 'true';
          field.required = isActive && requires;
        });
      });

      radios.forEach((radio) => {
        const label = form.querySelector(`label[for="${radio.id}"]`);
        if (label) {
          label.classList.toggle('active', radio.checked);
        }
      });
    };

    radios.forEach((radio) => {
      radio.addEventListener('change', toggleAccountSections);
    });

    toggleAccountSections();
  };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init, { once: true });
  } else {
    init();
  }
})();
