const toggleButtonLoading = (button, loading) => {
  if (!button) return;
  const spinner = button.querySelector('[data-loading-spinner]');
  const label = button.querySelector('[data-loading-label]');
  if (loading) {
    button.setAttribute('aria-busy', 'true');
    button.setAttribute('aria-disabled', 'true');
    button.classList.add('disabled');
    if (!button.dataset.originalText && label) {
      button.dataset.originalText = label.textContent;
    }
    if (spinner) {
      spinner.classList.remove('d-none');
    }
  } else {
    button.removeAttribute('aria-busy');
    button.removeAttribute('aria-disabled');
    button.classList.remove('disabled');
    if (spinner) {
      spinner.classList.add('d-none');
    }
  }
  button.disabled = loading;
};

const initLoadingForms = () => {
  document.querySelectorAll('form[data-loading-form]').forEach((form) => {
    if (form.dataset.loadingBound === 'true') {
      return;
    }
    form.dataset.loadingBound = 'true';
    const button = form.querySelector('[data-loading-button]');
    if (!button) {
      return;
    }
    form.addEventListener('submit', () => {
      toggleButtonLoading(button, true);
    });
  });
};

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initLoadingForms, { once: true });
} else {
  initLoadingForms();
}

export { toggleButtonLoading };
