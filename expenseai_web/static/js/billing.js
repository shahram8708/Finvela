const contextEl = document.getElementById('billingContext');
if (contextEl) {
  let context = {};
  try {
    context = JSON.parse(contextEl.textContent || '{}') || {};
  } catch (err) {
    console.error('Failed to parse billing context', err); // eslint-disable-line no-console
  }

  const desiredInput = document.getElementById('desiredLimitInput');
  const summaryDesired = document.getElementById('summaryDesired');
  const summaryAdditional = document.getElementById('summaryAdditional');
  const summaryAmount = document.getElementById('summaryAmount');
  const paymentError = document.getElementById('billingPaymentError');
  const paymentInfo = document.getElementById('billingPaymentInfo');
  const confirmForm = document.getElementById('billingConfirmForm');
  const confirmOrderId = document.getElementById('confirmOrderId');
  const confirmPaymentId = document.getElementById('confirmPaymentId');
  const confirmSignature = document.getElementById('confirmSignature');
  const confirmDesiredLimit = document.getElementById('confirmDesiredLimit');

  const currency = (context.currency || 'INR').toUpperCase();
  const perUserPriceMinor = Number(context.per_user_price_minor || 0);
  const currentLimit = Number(context.current_limit || 0);

  const formatCurrency = (minorUnits) => {
    try {
      const formatter = new Intl.NumberFormat(undefined, {
        style: 'currency',
        currency,
        currencyDisplay: 'symbol',
        minimumFractionDigits: 2,
      });
      return formatter.format((Number(minorUnits) || 0) / 100);
    } catch (err) {
      const major = ((Number(minorUnits) || 0) / 100).toFixed(2);
      return `${currency} ${major}`;
    }
  };

  const updateSummary = () => {
    if (!desiredInput) return;
    const desired = Math.max(parseInt(desiredInput.value, 10) || currentLimit, currentLimit);
    const additional = Math.max(desired - currentLimit, 0);
    const amountMinor = additional * perUserPriceMinor;
    if (summaryDesired) summaryDesired.textContent = desired.toString();
    if (summaryAdditional) summaryAdditional.textContent = additional.toString();
    if (summaryAmount) summaryAmount.textContent = formatCurrency(amountMinor);
  };

  desiredInput?.addEventListener('input', updateSummary);
  desiredInput?.addEventListener('change', updateSummary);
  updateSummary();

  const showError = (message) => {
    if (!paymentError) return;
    paymentError.textContent = message;
    paymentError.classList.remove('d-none');
  };

  const hideError = () => {
    paymentError?.classList.add('d-none');
  };

  const showInfo = (visible) => {
    if (!paymentInfo) return;
    if (visible) {
      paymentInfo.classList.remove('d-none');
    } else {
      paymentInfo.classList.add('d-none');
    }
  };

  const ensureRazorpay = () =>
    new Promise((resolve, reject) => {
      if (window.Razorpay) {
        resolve(window.Razorpay);
        return;
      }
      let attempts = 0;
      const maxAttempts = 40;
      const interval = setInterval(() => {
        if (window.Razorpay) {
          clearInterval(interval);
          resolve(window.Razorpay);
          return;
        }
        attempts += 1;
        if (attempts >= maxAttempts) {
          clearInterval(interval);
          reject(new Error('Razorpay SDK failed to load in time.'));
        }
      }, 150);
    });

  const launchCheckout = (order) => {
    if (!order || !confirmForm) return;
    hideError();
    showInfo(true);
    ensureRazorpay()
      .then(() => {
        const options = {
          key: order.key_id,
          name: order.organization_name || 'Finvela',
          currency: order.currency,
          amount: order.amount,
          order_id: order.order_id,
          description: `Increase organization seats to ${order.pricing?.desired_limit ?? ''}`,
          notes: order.notes || {},
          prefill: {
            name: order.customer?.name || '',
            email: order.customer?.email || '',
          },
          handler: (response) => {
            if (!confirmOrderId || !confirmPaymentId || !confirmSignature || !confirmDesiredLimit) {
              showError('Payment processed but confirmation form is unavailable. Please contact support.');
              return;
            }
            confirmOrderId.value = response.razorpay_order_id || '';
            confirmPaymentId.value = response.razorpay_payment_id || '';
            confirmSignature.value = response.razorpay_signature || '';
            confirmDesiredLimit.value = String(order.pricing?.desired_limit ?? '');
            confirmForm.submit();
          },
          modal: {
            ondismiss: () => {
              showInfo(false);
            },
          },
        };

        const rzp = new window.Razorpay(options);
        rzp.on('payment.failed', (event) => {
          const reason = event?.error?.description || 'Payment was cancelled. No charges were made.';
          showError(reason);
          showInfo(false);
        });
        rzp.open();
      })
      .catch((err) => {
        console.error(err); // eslint-disable-line no-console
        showError('Unable to load the Razorpay checkout experience. Please refresh and try again.');
        showInfo(false);
      });
  };

  if (context?.order?.order_id) {
    launchCheckout(context.order);
  } else {
    showInfo(false);
  }
}
