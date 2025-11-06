import { toggleButtonLoading } from './form-spinner.js';

const form = document.getElementById('contactForm');
const successAlert = document.querySelector('[data-contact-success]');
const errorAlert = document.querySelector('[data-contact-error]');

const buildErrorMessage = (messages = []) => messages.join('\n');

const clearFieldErrors = () => {
  document.querySelectorAll('[data-error-for]').forEach((node) => {
    node.textContent = '';
  });
  document.querySelectorAll('#contactForm .is-invalid').forEach((node) => {
    node.classList.remove('is-invalid');
  });
};

const showFieldErrors = (errors) => {
  if (!errors) return;
  Object.entries(errors).forEach(([field, messages]) => {
    const feedback = document.querySelector(`[data-error-for="${field}"]`);
    const input = document.querySelector(`#contactForm [name="${field}"]`);
    if (feedback) {
      feedback.textContent = buildErrorMessage(messages);
    }
    if (input instanceof HTMLElement) {
      input.classList.add('is-invalid');
    }
  });
  const firstErrorField = document.querySelector('#contactForm .is-invalid');
  if (firstErrorField instanceof HTMLElement) {
    firstErrorField.focus({ preventScroll: false });
  }
};

const resetAlerts = () => {
  if (successAlert) {
    successAlert.classList.add('d-none');
  }
  if (errorAlert) {
    errorAlert.classList.add('d-none');
  }
};

const showSuccess = (message) => {
  if (!successAlert) return;
  successAlert.classList.remove('d-none');
  const textNode = successAlert.querySelector('span');
  if (textNode) {
    textNode.textContent = message || 'Your message has been sent successfully.';
  }
};

const showError = (message) => {
  if (!errorAlert) return;
  errorAlert.classList.remove('d-none');
  const textNode = errorAlert.querySelector('span');
  if (textNode) {
    textNode.textContent = message || 'Something went wrong. Please try again.';
  }
};

const handleSubmit = async (event) => {
  if (!form) return;
  event.preventDefault();
  const submitButton = form.querySelector('[data-loading-button]');
  toggleButtonLoading(submitButton, true);
  resetAlerts();
  clearFieldErrors();

  const formData = new FormData(form);

  try {
    const response = await fetch(form.action, {
      method: 'POST',
      headers: {
        'X-Requested-With': 'XMLHttpRequest',
      },
      body: formData,
    });

    const payload = await response.json().catch(() => ({}));

    if (!response.ok || payload.status !== 'success') {
      if (payload?.errors) {
        showFieldErrors(payload.errors);
      }
      showError(payload?.message || 'Unable to send your message right now.');
      return;
    }

    form.reset();
    clearFieldErrors();
    showSuccess(payload.message);
    window.setTimeout(() => {
      if (successAlert) {
        successAlert.classList.add('d-none');
      }
    }, 6000);
  } catch (error) {
    console.error('Contact form submission failed', error);
    showError('We could not submit your message. Please try again in a moment.');
  } finally {
    toggleButtonLoading(submitButton, false);
  }
};

if (form) {
  form.addEventListener('submit', handleSubmit);
}
