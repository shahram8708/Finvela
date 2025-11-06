import { toggleButtonLoading } from './form-spinner.js';

const feedbackVariants = {
  ok: 'alert-success',
  error: 'alert-danger',
  warning: 'alert-warning',
};

const updateFeedback = (element, message, variant = 'ok') => {
  if (!element) return;
  element.classList.remove('alert', ...Object.values(feedbackVariants));
  if (!message) {
    element.textContent = '';
    element.setAttribute('hidden', 'true');
    return;
  }
  element.removeAttribute('hidden');
  element.classList.add('alert', feedbackVariants[variant] || feedbackVariants.ok, 'py-2', 'px-3');
  element.textContent = message;
};

const initResendButton = () => {
  const button = document.querySelector('[data-resend-button]');
  if (!button) return;
  const label = button.querySelector('[data-loading-label]');
  const feedback = document.querySelector('[data-resend-feedback]');
  const url = button.dataset.resendUrl;
  const email = button.dataset.email;
  const purpose = button.dataset.purpose;
  const throttle = Number.parseInt(button.dataset.throttle || '60', 10);
  let cooldown = Number.parseInt(button.dataset.cooldown || '0', 10);
  const originalLabel = label ? label.textContent : button.textContent;
  let cooldownTimer = null;

  const setCooldown = (seconds) => {
    if (!label) return;
    if (cooldownTimer) {
      window.clearTimeout(cooldownTimer);
    }
    const run = () => {
      if (seconds <= 0) {
        label.textContent = originalLabel;
        button.disabled = false;
        button.removeAttribute('aria-disabled');
        return;
      }
      label.textContent = `${originalLabel} (${seconds}s)`;
      button.disabled = true;
      button.setAttribute('aria-disabled', 'true');
      seconds -= 1;
      cooldownTimer = window.setTimeout(run, 1000);
    };
    run();
  };

  if (cooldown > 0) {
    setCooldown(cooldown);
  }

  button.addEventListener('click', async (event) => {
    event.preventDefault();
    if (!url || !email) {
      updateFeedback(feedback, 'Unable to resend the code right now.', 'error');
      return;
    }
    toggleButtonLoading(button, true);
    updateFeedback(feedback, '');
    try {
      const csrfToken = document.querySelector('input[name="csrf_token"]')?.value ?? '';
      const response = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-CSRFToken': csrfToken,
        },
        body: JSON.stringify({ email, purpose }),
      });
      const data = await response.json().catch(() => ({}));
      if (!response.ok) {
        const variant = response.status === 429 || response.status === 423 ? 'warning' : 'error';
        updateFeedback(feedback, data.message || 'Could not resend the code. Try again later.', variant);
        return;
      }
      updateFeedback(feedback, data.message || 'We sent a new code.', 'ok');
      const nextCooldown = Number.parseInt(data.cooldown, 10);
      setCooldown(Number.isNaN(nextCooldown) ? throttle : nextCooldown);
    } catch (error) {
      console.error('OTP resend failed', error);
      updateFeedback(feedback, 'Network error. Please retry in a moment.', 'error');
    } finally {
      toggleButtonLoading(button, false);
    }
  });
};

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initResendButton, { once: true });
} else {
  initResendButton();
}
