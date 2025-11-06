import { connectSSE } from './sse.js';

const prefersDark = window.matchMedia('(prefers-color-scheme: dark)');
const THEME_STORAGE_KEY = 'expenseai-theme';

function applyTheme(mode) {
  const root = document.documentElement;
  const toggle = document.getElementById('themeToggle');
  root.setAttribute('data-bs-theme', mode);
  if (toggle) {
    toggle.innerHTML = mode === 'dark' ? '<i class="bi bi-moon-fill"></i>' : '<i class="bi bi-sun-fill"></i>';
  }
}

function initThemeToggle() {
  const saved = localStorage.getItem(THEME_STORAGE_KEY);
  if (saved) {
    applyTheme(saved);
  } else if (prefersDark.matches) {
    applyTheme('dark');
  }

  const toggle = document.getElementById('themeToggle');
  toggle?.addEventListener('click', () => {
    const current = document.documentElement.getAttribute('data-bs-theme') === 'dark' ? 'dark' : 'light';
    const next = current === 'dark' ? 'light' : 'dark';
    applyTheme(next);
    localStorage.setItem(THEME_STORAGE_KEY, next);
  });
}

function initToasts() {
  document.querySelectorAll('.toast').forEach((toastEl) => {
    const toast = new window.bootstrap.Toast(toastEl, { autohide: true, delay: 4000 });
    toast.show();
  });
}

function updateSSEStatusBadge(state) {
  const badge = document.getElementById('sseStatus');
  if (!badge) return;
  if (state === 'open') {
    badge.classList.remove('text-bg-secondary');
    badge.classList.add('text-bg-success');
    badge.innerHTML = '<i class="bi bi-broadcast-pin me-1"></i>Live';
  } else if (state === 'error') {
    badge.classList.remove('text-bg-success');
    badge.classList.add('text-bg-secondary');
    badge.innerHTML = '<i class="bi bi-broadcast me-1"></i>Offline';
  }
}

function getCsrfToken() {
  const tokenEl = document.querySelector('input[name="csrf_token"]');
  return tokenEl ? tokenEl.value : '';
}

function parseJsonSafe(value, fallback = null) {
  if (value === null || value === undefined || value === '') {
    return fallback;
  }
  try {
    return JSON.parse(value);
  } catch (err) {
    return fallback;
  }
}

function formatTimelineTimestamp(value) {
  if (!value) return '';
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) {
    return String(value);
  }
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: 'medium',
    timeStyle: 'short',
  }).format(date);
}

function renderPayloadTableRows(payload, tableBody, emptyState) {
  if (!tableBody) return;
  tableBody.innerHTML = '';

  const coerceJsonLike = (value) => {
    if (typeof value !== 'string') return value;
    const trimmed = value.trim();
    if (!trimmed) return value;
    const first = trimmed.charAt(0);
    const last = trimmed.charAt(trimmed.length - 1);
    const couldBeJson = (first === '{' && last === '}') || (first === '[' && last === ']');
    if (!couldBeJson) return value;
    const fallback = {};
    const parsed = parseJsonSafe(trimmed, fallback);
    return parsed === fallback ? value : parsed;
  };

  payload = coerceJsonLike(payload);

  const showEmpty = () => {
    if (emptyState) {
      emptyState.classList.remove('d-none');
    }
  };

  const hideEmpty = () => {
    if (emptyState) {
      emptyState.classList.add('d-none');
    }
  };

  const buildValueNode = (value) => {
    value = coerceJsonLike(value);
    if (value === null || value === undefined) {
      return document.createTextNode('—');
    }
    if (value instanceof Date) {
      return document.createTextNode(value.toISOString());
    }
    if (Array.isArray(value)) {
      const nestedTable = document.createElement('table');
      nestedTable.className = 'table table-sm table-borderless mb-0 payload-table-nested';
      const nestedBody = document.createElement('tbody');
      nestedTable.appendChild(nestedBody);
      renderPayloadTableRows(value, nestedBody);
      return nestedTable;
    }
    if (value && typeof value === 'object') {
      const tag = Object.prototype.toString.call(value);
      if (tag === '[object Object]') {
        const nestedTable = document.createElement('table');
        nestedTable.className = 'table table-sm table-borderless mb-0 payload-table-nested';
        const nestedBody = document.createElement('tbody');
        nestedTable.appendChild(nestedBody);
        renderPayloadTableRows(value, nestedBody);
        return nestedTable;
      }
      const pre = document.createElement('pre');
      pre.className = 'small mb-0 text-wrap';
      pre.textContent = JSON.stringify(value, null, 2);
      return pre;
    }
    const span = document.createElement('span');
    span.textContent = String(value);
    return span;
  };

  const appendRow = (field, value) => {
    const row = document.createElement('tr');
    const fieldCell = document.createElement('th');
    fieldCell.scope = 'row';
    fieldCell.className = 'text-muted small text-uppercase';
    fieldCell.textContent = String(field);
    const valueCell = document.createElement('td');
    valueCell.appendChild(buildValueNode(value));
    row.append(fieldCell, valueCell);
    tableBody.appendChild(row);
  };

  if (Array.isArray(payload)) {
    if (!payload.length) {
      showEmpty();
      return;
    }
    hideEmpty();
    payload.forEach((value, index) => {
      appendRow(`#${index + 1}`, value);
    });
    return;
  }

  if (payload && typeof payload === 'object') {
    const entries = Object.entries(payload);
    if (!entries.length) {
      showEmpty();
      return;
    }
    hideEmpty();
    entries.forEach(([key, value]) => {
      appendRow(key, value);
    });
    return;
  }

  if (payload === null || payload === undefined || payload === '') {
    showEmpty();
    return;
  }

  hideEmpty();
  appendRow('value', payload);
}

function formatStatusLabel(status) {
  if (!status) return '';
  return status
    .toString()
    .split(/[_\s]+/)
    .filter(Boolean)
    .map((segment) => segment.charAt(0).toUpperCase() + segment.slice(1).toLowerCase())
    .join(' ');
}

function updateStatusClasses(element, status) {
  if (!element) return;
  const existing = Array.from(element.classList).filter((cls) => cls.startsWith('status-'));
  existing.forEach((cls) => element.classList.remove(cls));
  element.classList.add(`status-${status.toLowerCase()}`);
  element.textContent = formatStatusLabel(status);
}

function toggleParseOverlay(visible) {
  const overlay = document.getElementById('previewParsingOverlay');
  if (!overlay) return;
  if (visible) {
    overlay.removeAttribute('hidden');
  } else {
    overlay.setAttribute('hidden', 'true');
  }
}

function setParseButtonState(status) {
  const button = document.getElementById('parseWithAIButton');
  const spinner = document.getElementById('parseSpinner');
  const indicator = document.getElementById('aiStatusIndicator');
  if (!button) return;
  const busy = status === 'PARSING' || status === 'QUEUED';
  button.disabled = busy;
  spinner?.classList.add('d-none');
  if (indicator) {
    indicator.textContent = formatStatusLabel(status);
  }
  toggleParseOverlay(busy && status !== 'UPLOADED');
}

let riskChartInstance = null;

function setRiskButtonState(status, { busy: forcedBusy } = {}) {
  const normalized = (status || 'PENDING').toString().toUpperCase();
  const button = document.getElementById('computeRiskButton');
  const spinner = document.getElementById('riskSpinner');
  const effectiveBusy = typeof forcedBusy === 'boolean' ? forcedBusy : normalized === 'IN_PROGRESS';
  if (button) {
    button.disabled = effectiveBusy;
    button.dataset.riskStatus = normalized;
  }
  if (spinner) {
    if (effectiveBusy) {
      spinner.classList.remove('d-none');
    } else {
      spinner.classList.add('d-none');
    }
  }
}

function showRiskFeedback(message, variant = 'info') {
  const alertEl = document.getElementById('riskError');
  if (!alertEl) return;
  const variants = ['alert-danger', 'alert-warning', 'alert-success', 'alert-info'];
  variants.forEach((cls) => alertEl.classList.remove(cls));
  if (!message) {
    alertEl.classList.add('d-none');
    alertEl.textContent = '';
    return;
  }
  alertEl.textContent = message;
  alertEl.classList.remove('d-none');
  alertEl.classList.add(`alert-${variant}`);
}

function renderRiskContributors(contributors) {
  const container = document.getElementById('riskContributorList');
  if (!container) return;
  container.innerHTML = '';

  if (!Array.isArray(contributors) || !contributors.length) {
    container.innerHTML = '<p class="text-muted small mb-0">No risk contributors available yet.</p>';
    return;
  }

  const list = document.createElement('div');
  list.className = 'list-group list-group-flush';

  contributors.forEach((entry) => {
    const listItem = document.createElement('div');
    listItem.className = 'list-group-item';

    const header = document.createElement('div');
    header.className = 'd-flex justify-content-between align-items-start gap-3';

    const left = document.createElement('div');
    const label = document.createElement('div');
    label.className = 'fw-semibold';

    const name = typeof entry?.name === 'string' && entry.name.trim() ? entry.name : 'Contributor';
    label.textContent = formatStatusLabel(name);
    left.appendChild(label);

    const detailSummary =
      entry?.details?.summary ||
      entry?.details?.message ||
      entry?.details?.notes ||
      entry?.details?.reason ||
      '';
    if (detailSummary) {
      const detailEl = document.createElement('div');
      detailEl.className = 'small text-muted';
      detailEl.textContent = detailSummary;
      left.appendChild(detailEl);
    }

    const right = document.createElement('div');
    right.className = 'text-end small';
    const contribution = Number(entry?.contribution ?? 0) * 100;
    if (!Number.isNaN(contribution)) {
      const contributionBadge = document.createElement('span');
      contributionBadge.className = 'badge text-bg-primary';
      contributionBadge.textContent = `${contribution.toFixed(1)}% impact`;
      right.appendChild(contributionBadge);
    }

    header.append(left, right);
    listItem.appendChild(header);

    const weight = Number(entry?.weight ?? 0);
    const rawScore = Number(entry?.raw_score ?? 0);
    const detailParts = [];
    if (!Number.isNaN(weight)) {
      detailParts.push(`Weight ${weight.toFixed(2)}`);
    }
    if (!Number.isNaN(rawScore)) {
      detailParts.push(`Raw ${rawScore.toFixed(2)}`);
    }
    if (detailParts.length) {
      const metrics = document.createElement('div');
      metrics.className = 'small text-muted mt-1';
      metrics.textContent = detailParts.join(' · ');
      listItem.appendChild(metrics);
    }

    list.appendChild(listItem);
  });

  container.appendChild(list);
}

function renderRiskOutliers(contributors) {
  const container = document.getElementById('riskOutlierList');
  if (!container) return;
  container.innerHTML = '';
  const market = Array.isArray(contributors)
    ? contributors.find((item) => (item.name ?? '').toString() === 'market_outlier')
    : null;
  const topOutliers = market?.details?.top_outliers;
  if (!Array.isArray(topOutliers) || !topOutliers.length) {
    container.innerHTML = '<p class="text-muted small mb-0">No outlier line items identified.</p>';
    return;
  }
  const list = document.createElement('div');
  list.className = 'list-group list-group-flush';
  topOutliers.slice(0, 5).forEach((entry, idx) => {
    const item = document.createElement('div');
    item.className = 'list-group-item';
    const title = document.createElement('div');
    title.className = 'd-flex justify-content-between align-items-start gap-3';
    const left = document.createElement('div');
    left.innerHTML = `<strong>Line ${idx + 1}</strong><div class="small text-muted">${entry.description || '—'}</div>`;
    const right = document.createElement('div');
    right.className = 'text-end small';
    if (entry.outlier_score !== undefined) {
      right.innerHTML = `<div>Score ${(Number(entry.outlier_score) * 100).toFixed(1)}%</div>`;
    }
    if (entry.robust_z !== undefined) {
      right.innerHTML += `<div>z ${(Number(entry.robust_z)).toFixed(2)}</div>`;
    }
    title.append(left, right);
    item.appendChild(title);
    list.appendChild(item);
  });
  container.appendChild(list);
}

function renderRiskChart(contributors) {
  const canvas = document.getElementById('riskWaterfallChart');
  const summary = document.getElementById('riskWaterfallSummary');
  if (!canvas) return;
  if (!Array.isArray(contributors) || !contributors.length || !window.Chart) {
    if (riskChartInstance) {
      riskChartInstance.destroy();
      riskChartInstance = null;
    }
    canvas.setAttribute('hidden', 'true');
    if (summary) {
      summary.textContent = 'No contributor data available yet.';
    }
    return;
  }

  const labels = contributors.map((item) => (item.name ?? '').toString().replace(/[_\s]+/g, ' '));
  const data = contributors.map((item) => Number(item.contribution ?? 0) * 100);
  if (riskChartInstance) {
    riskChartInstance.destroy();
  }
  riskChartInstance = new window.Chart(canvas, {
    type: 'bar',
    data: {
      labels,
      datasets: [
        {
          label: 'Contribution %',
          data,
          backgroundColor: '#d63384',
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        x: {
          ticks: { color: '#6c757d' },
        },
        y: {
          ticks: { callback: (value) => `${value}%` },
        },
      },
      plugins: {
        legend: { display: false },
      },
    },
  });
  canvas.removeAttribute('hidden');
  canvas.parentElement?.classList.add('position-relative');
  if (summary) {
    summary.textContent = labels.map((label, idx) => `${label}: ${data[idx].toFixed(1)}%`).join(', ');
  }
}

function applyRiskPayload(payload) {
  if (!payload) return;
  const status = (payload.risk_status || (payload.computed ? 'READY' : 'PENDING')).toUpperCase();
  setRiskButtonState(status);

  const riskSection = document.getElementById('riskSection');
  if (riskSection) {
    riskSection.dataset.riskStatus = status;
  }

  const badge = document.getElementById('riskBadge');
  if (badge) {
    badge.textContent = formatStatusLabel(status);
  }

  const contributors = payload.contributors || payload.score?.contributors || [];
  renderRiskContributors(contributors);
  renderRiskOutliers(contributors);
  renderRiskChart(contributors);

  if (payload.risk_notes) {
    showRiskFeedback(payload.risk_notes, 'info');
  } else if (!payload.computed) {
    showRiskFeedback('No risk run yet.', 'info');
  } else {
    showRiskFeedback('', 'info');
  }

  if (payload.computed && typeof payload.composite === 'number') {
    const riskBadge = document.getElementById('riskBadge');
    if (riskBadge) {
      riskBadge.textContent = `${formatStatusLabel(status)} · ${(payload.composite * 100).toFixed(1)}%`;
    }
  }
}

async function refreshRiskData(invoiceId, { showLoading = false } = {}) {
  const spinner = document.getElementById('riskSpinner');
  if (showLoading) {
    spinner?.classList.remove('d-none');
  }
  const response = await fetch(`/invoices/${invoiceId}/risk`, {
    headers: {
      Accept: 'application/json',
      'X-Requested-With': 'XMLHttpRequest',
    },
    credentials: 'same-origin',
  });
  if (!response.ok) {
    throw new Error('Failed to fetch risk data');
  }
  const data = await response.json();
  applyRiskPayload(data);
  const button = document.getElementById('computeRiskButton');
  if (button && !button.disabled) {
    spinner?.classList.add('d-none');
  }
  return data;
}

function updateConfidenceBars(root) {
  root.querySelectorAll('[data-confidence]').forEach((wrapper) => {
    const rawValue = Number(wrapper.getAttribute('data-confidence') || '0');
    const value = Number.isFinite(rawValue) ? Math.min(Math.max(rawValue, 0), 100) : 0;
    const bar = wrapper.querySelector('[data-progress-bar="true"]');
    const label = wrapper.parentElement?.querySelector('[data-confidence-label="true"]');
    if (bar) {
      bar.style.width = `${value}%`;
      bar.classList.remove('bg-primary', 'bg-success', 'bg-warning', 'bg-danger');
      if (value >= 80) {
        bar.classList.add('bg-success');
      } else if (value >= 50) {
        bar.classList.add('bg-warning');
      } else {
        bar.classList.add('bg-danger');
      }
    }
    if (label) {
      label.textContent = `${value.toFixed(1)}%`;
    }
  });
}

async function refreshExtractedData(invoiceId) {
  const summaryContainer = document.getElementById('extractionSummary');
  const response = await fetch(`/invoices/${invoiceId}/extracted`, { headers: { Accept: 'application/json' } });
  if (!response.ok) {
    throw new Error('Failed to fetch extracted data');
  }
  const data = await response.json();

  const statusBadge = document.getElementById('invoiceStatusBadge');
  if (statusBadge) {
    updateStatusClasses(statusBadge, data.processing_status || 'UPLOADED');
  }
  setParseButtonState(data.processing_status || 'UPLOADED');

  const summaryList = document.getElementById('extractionSummaryList');
  const placeholder = document.getElementById('extractionPlaceholder');
  const confidenceWrap = document.getElementById('extractionConfidenceBadge');
  if (data.header?.length) {
    placeholder?.classList.add('d-none');
    confidenceWrap?.classList.remove('d-none');
    summaryList?.classList.remove('d-none');
    const headerMap = Object.fromEntries(data.header.map((field) => [field.field_name, field]));
    summaryList?.querySelectorAll('[data-summary-field]').forEach((el) => {
      const key = el.getAttribute('data-summary-field');
      if (!key) return;
      if (key === 'totals') {
        const subtotal = headerMap.subtotal?.value || '—';
        const tax = headerMap.tax_total?.value || '—';
        const total = headerMap.grand_total?.value || '—';
        el.textContent = `${subtotal ?? '—'} / ${tax ?? '—'} / ${total ?? '—'}`;
        return;
      }
      el.textContent = headerMap[key]?.value || '—';
    });
    const confidenceBadge = document.getElementById('confidenceBadgeLabel');
    if (confidenceBadge) {
      const confidence = typeof data.extraction_confidence === 'number' ? (data.extraction_confidence * 100).toFixed(1) : 'n/a';
      confidenceBadge.textContent = `Mean confidence: ${confidence}%`;
    }
  } else {
    summaryList?.classList.add('d-none');
    confidenceWrap?.classList.add('d-none');
    placeholder?.classList.remove('d-none');
  }

  const headerTbody = document.querySelector('#extractedHeaderTable tbody');
  if (headerTbody) {
    headerTbody.innerHTML = '';
    if (data.header?.length) {
      data.header.forEach((field) => {
        const row = document.createElement('tr');
        const nameCell = document.createElement('td');
        nameCell.className = 'text-uppercase small';
        nameCell.textContent = field.field_name;
        const valueCell = document.createElement('td');
        valueCell.textContent = field.value || '—';
        const confidenceCell = document.createElement('td');
        confidenceCell.className = 'text-end';
        const progressWrapper = document.createElement('div');
        progressWrapper.className = 'progress progress-thin';
        progressWrapper.setAttribute('role', 'progressbar');
        const pct = Number(field.confidence || 0) * 100;
        progressWrapper.setAttribute('aria-valuenow', pct.toFixed(1));
        progressWrapper.setAttribute('aria-valuemin', '0');
        progressWrapper.setAttribute('aria-valuemax', '100');
        progressWrapper.setAttribute('data-confidence', pct.toFixed(1));
  const bar = document.createElement('div');
  bar.className = 'progress-bar';
        bar.dataset.progressBar = 'true';
        bar.style.width = '0%';
        progressWrapper.appendChild(bar);
  const label = document.createElement('span');
  label.className = 'small text-muted';
        label.setAttribute('data-confidence-label', 'true');
        label.textContent = `${pct.toFixed(1)}%`;
        confidenceCell.appendChild(progressWrapper);
        confidenceCell.appendChild(label);
        row.append(nameCell, valueCell, confidenceCell);
        headerTbody.appendChild(row);
      });
    } else {
      const row = document.createElement('tr');
      const cell = document.createElement('td');
      cell.colSpan = 3;
      cell.className = 'small text-muted';
      cell.textContent = 'No extracted header fields yet.';
      row.appendChild(cell);
      headerTbody.appendChild(row);
    }
  }

  const lineTbody = document.querySelector('#extractedLineItemsTable tbody');
  if (lineTbody) {
    lineTbody.innerHTML = '';
    if (data.line_items?.length) {
      data.line_items.forEach((item) => {
        const row = document.createElement('tr');
        const formatValue = (value, fallback = '—') => {
          if (value === null || value === undefined) return fallback;
          if (typeof value === 'string' && value.trim() === '') return fallback;
          return value;
        };
        const confidenceNumeric = Number(item.confidence);
        const confidenceValue = Number.isFinite(confidenceNumeric)
          ? `${(Math.min(Math.max(confidenceNumeric * 100, 0), 100)).toFixed(1)}%`
          : '—';
        const columns = [
          { value: formatValue(item.line_no) },
          { value: formatValue(item.description_raw) },
          { value: formatValue(item.hsn_sac) },
          { value: formatValue(item.qty), className: 'text-end' },
          { value: formatValue(item.unit_price), className: 'text-end' },
          { value: formatValue(item.gst_rate), className: 'text-end' },
          { value: formatValue(item.line_subtotal), className: 'text-end' },
          { value: formatValue(item.line_tax), className: 'text-end' },
          { value: formatValue(item.line_total), className: 'text-end' },
          { value: confidenceValue, className: 'text-end' },
        ];
        columns.forEach((column) => {
          const cell = document.createElement('td');
          if (column.className) {
            cell.className = column.className;
          }
          cell.textContent = column.value;
          row.appendChild(cell);
        });
        lineTbody.appendChild(row);
      });
    } else {
      const row = document.createElement('tr');
      const cell = document.createElement('td');
      cell.colSpan = 10;
      cell.className = 'small text-muted';
      cell.textContent = 'No line items extracted yet.';
      row.appendChild(cell);
      lineTbody.appendChild(row);
    }
  }

  if (summaryContainer) {
    updateConfidenceBars(summaryContainer);
  }
  if (headerTbody) {
    updateConfidenceBars(headerTbody);
  }
}

function prependTimelineEvent(event) {
  const container = document.getElementById('timelineContainer');
  if (!container) return;
  let tableBody = container.querySelector('#invoiceTimelineTable tbody');
  if (!tableBody) {
    container.innerHTML = `
      <div class="table-responsive" style="max-height: 70vh; overflow-y: auto;">
        <table class="table table-sm table-hover align-middle mb-0" id="invoiceTimelineTable">
          <thead class="bg-body-secondary text-uppercase small text-muted">
            <tr>
              <th scope="col" style="width: 4rem;">#</th>
              <th scope="col">Event</th>
              <th scope="col">Payload summary</th>
              <th scope="col" class="text-end">Recorded</th>
              <th scope="col" class="text-end" style="width: 8rem;">Action</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    `;
    tableBody = container.querySelector('#invoiceTimelineTable tbody');
  }
  if (!tableBody || document.getElementById(`event-${event.event_id}`)) return;
  const payload = event.payload;
  const isMapping = payload && typeof payload === 'object' && !Array.isArray(payload);
  const isArray = Array.isArray(payload);
  const payloadKeys = isMapping ? Object.keys(payload) : [];
  const summary = isMapping ? payload.message || payload.notes || payload.status || '' : '';
  const hasPayload = (() => {
    if (isMapping) return payloadKeys.length > 0;
    if (isArray) return payload.length > 0;
    return payload !== null && payload !== undefined && payload !== '';
  })();
  const createdLabel = formatTimelineTimestamp(event.created_at);

  const row = document.createElement('tr');
  row.id = `event-${event.event_id}`;

  const indexCell = document.createElement('td');
  indexCell.className = 'text-muted small';
  row.appendChild(indexCell);

  const eventCell = document.createElement('td');
  const eventWrap = document.createElement('div');
  eventWrap.className = 'd-flex align-items-center gap-2';
  const eventBadge = document.createElement('span');
  eventBadge.className = 'badge text-bg-light text-uppercase small';
  eventBadge.textContent = event.event_type;
  eventWrap.appendChild(eventBadge);
  if (summary) {
    const summarySpan = document.createElement('span');
    summarySpan.className = 'text-muted small';
    summarySpan.textContent = summary;
    eventWrap.appendChild(summarySpan);
  }
  eventCell.appendChild(eventWrap);
  row.appendChild(eventCell);

  const payloadCell = document.createElement('td');
  if (payloadKeys.length) {
    const badgeWrap = document.createElement('div');
    badgeWrap.className = 'd-flex flex-wrap gap-1';
    payloadKeys.slice(0, 6).forEach((key) => {
      const badge = document.createElement('span');
      badge.className = 'badge rounded-pill text-bg-secondary text-uppercase small';
      badge.textContent = key;
      badgeWrap.appendChild(badge);
    });
    if (payloadKeys.length > 6) {
      const remainder = document.createElement('span');
      remainder.className = 'badge rounded-pill text-bg-secondary small';
      remainder.textContent = `+${payloadKeys.length - 6}`;
      badgeWrap.appendChild(remainder);
    }
    payloadCell.appendChild(badgeWrap);
  } else if (isArray) {
    const listBadge = document.createElement('span');
    listBadge.className = 'badge rounded-pill text-bg-secondary small';
    listBadge.textContent = `List (${payload.length})`;
    payloadCell.appendChild(listBadge);
  } else if (payload !== null && payload !== undefined && payload !== '') {
    const valueSpan = document.createElement('span');
    valueSpan.className = 'text-muted small';
    valueSpan.textContent = String(payload);
    payloadCell.appendChild(valueSpan);
  } else {
    const empty = document.createElement('span');
    empty.className = 'text-muted';
    empty.textContent = 'No payload';
    payloadCell.appendChild(empty);
  }
  row.appendChild(payloadCell);

  const createdCell = document.createElement('td');
  createdCell.className = 'text-end';
  const createdSpan = document.createElement('span');
  createdSpan.className = 'text-muted small';
  createdSpan.textContent = createdLabel;
  createdCell.appendChild(createdSpan);
  row.appendChild(createdCell);

  const actionCell = document.createElement('td');
  actionCell.className = 'text-end';
  const button = document.createElement('button');
  button.type = 'button';
  button.className = 'btn btn-outline-primary btn-sm';
  button.innerHTML = '<i class="bi bi-table"></i><span class="ms-1">View payload</span>';
  button.setAttribute('data-payload-modal-trigger', 'true');
  button.setAttribute('data-event-id', event.event_id);
  button.setAttribute('data-event-type', event.event_type);
  button.setAttribute('data-event-created', createdLabel);
  button.setAttribute('data-payload', JSON.stringify(payload ?? null));
  button.disabled = !hasPayload;
  actionCell.appendChild(button);
  row.appendChild(actionCell);

  tableBody.prepend(row);

  Array.from(tableBody.querySelectorAll('tr')).forEach((tr, index) => {
    const cell = tr.querySelector('td');
    if (cell) {
      cell.textContent = String(index + 1);
    }
  });
}

function handleInvoiceEvent(invoiceId, payload) {
  if (payload.invoice_id !== invoiceId) return;
  prependTimelineEvent(payload);
  if (payload.event_type === 'GST_VALIDATION') {
    const subject = payload.payload?.subject;
    if (subject) {
      const verifiedAt = payload.payload?.verified_at || payload.created_at;
      const check = {
        status: payload.payload?.status,
        status_label: payload.payload?.status_label,
        summary: payload.payload?.summary,
        details: {
          gstin: payload.payload?.gstin,
          subject,
          provider: payload.payload?.provider,
          result: payload.payload?.result,
          mode: payload.payload?.mode,
          verified_by: payload.payload?.verified_by,
          verified_at: payload.payload?.verified_at,
        },
        updated_at: verifiedAt,
        updated_at_display: formatTimelineTimestamp(verifiedAt),
      };
      document.dispatchEvent(
        new CustomEvent('gst-verification-event', {
          detail: { subject, check, origin: 'sse' },
        })
      );
    }
  }
  const newStatus = payload.payload?.to || payload.payload?.status;
  if (payload.event_type === 'STATUS_CHANGED' && newStatus) {
    updateStatusClasses(document.getElementById('invoiceStatusBadge'), newStatus);
    setParseButtonState(newStatus);
  }
  if (payload.event_type === 'PARSING_ENQUEUED') {
    setParseButtonState('QUEUED');
  }
  if (payload.event_type === 'PARSING_STARTED') {
    setParseButtonState('PARSING');
  }
  if (payload.event_type === 'PARSING_RESULT_SUMMARY') {
    refreshExtractedData(invoiceId).catch((err) => console.error(err)); // eslint-disable-line no-console
  }
  if (payload.event_type === 'PARSING_ERROR') {
    const errorEl = document.getElementById('parseError');
    if (errorEl && payload.payload?.message) {
      errorEl.textContent = payload.payload.message;
      errorEl.classList.remove('d-none');
    }
    setParseButtonState('ERROR');
  }
  if (payload.event_type === 'RISK_STARTED') {
    setRiskButtonState('IN_PROGRESS', { busy: true });
    showRiskFeedback('Risk computation started…', 'info');
  }
  if (payload.event_type === 'RISK_STATUS_CHANGED') {
    const riskStatus = (payload.payload?.to || '').toUpperCase();
    if (riskStatus) {
      setRiskButtonState(riskStatus);
      if (riskStatus === 'READY') {
        refreshRiskData(invoiceId).catch((err) => console.error(err)); // eslint-disable-line no-console
      }
    }
  }
  if (payload.event_type === 'RISK_SUMMARY') {
    refreshRiskData(invoiceId).catch((err) => console.error(err)); // eslint-disable-line no-console
  }
  if (payload.event_type === 'RISK_READY') {
    refreshRiskData(invoiceId).catch((err) => console.error(err)); // eslint-disable-line no-console
    showRiskFeedback('Risk computation completed.', 'success');
  }
  if (payload.event_type === 'RISK_ERROR') {
    const message = payload.payload?.error || 'Risk computation failed.';
    showRiskFeedback(message, 'danger');
    setRiskButtonState('ERROR', { busy: false });
  }
}

function initInvoiceDetailPage() {
  const container = document.querySelector('[data-invoice-id]');
  if (!container) return;
  updateConfidenceBars(container);
  const { Modal } = window.bootstrap || {};
  const payloadModalEl = document.getElementById('payloadModal');
  const payloadModalLabel = payloadModalEl?.querySelector('#payloadModalLabel');
  const payloadModalMeta = payloadModalEl?.querySelector('#payloadModalMeta');
  const payloadModalTableBody = payloadModalEl?.querySelector('#payloadModalTableBody');
  const payloadModalEmpty = payloadModalEl?.querySelector('#payloadModalEmpty');
  let payloadModalInstance = null;
  const renderPayloadTable = (payload) => {
    if (!payloadModalTableBody) return;
    renderPayloadTableRows(payload, payloadModalTableBody, payloadModalEmpty);
  };

  const openPayloadModal = (button) => {
    if (!Modal || !payloadModalEl || !payloadModalTableBody) return;
    const parsed = parseJsonSafe(button.getAttribute('data-payload')) || {};
    renderPayloadTable(parsed);
    if (payloadModalLabel) {
      const eventType = button.getAttribute('data-event-type') || 'Event';
      payloadModalLabel.textContent = `${eventType} payload`;
    }
    if (payloadModalMeta) {
      const eventId = button.getAttribute('data-event-id');
      const created = button.getAttribute('data-event-created');
      const metaParts = [];
      if (eventId) metaParts.push(`Event #${eventId}`);
      if (created) metaParts.push(created);
      payloadModalMeta.textContent = metaParts.join(' • ');
    }
    if (!payloadModalInstance) {
      payloadModalInstance = new Modal(payloadModalEl);
    }
    payloadModalInstance.show();
  };

  container.addEventListener('click', (event) => {
    const target = event.target.closest('[data-payload-modal-trigger]');
    if (!target || target.disabled) return;
    openPayloadModal(target);
  });

  const invoiceId = Number(container.getAttribute('data-invoice-id'));
  const gstSection = document.getElementById('gstVerificationSection');
  if (gstSection) {
    const verifyUrlTemplate = gstSection.dataset.verifyUrlTemplate || '';
    const initialChecks = parseJsonSafe(gstSection.dataset.gstChecks || '{}') || {};
    const storedChecks = {
      vendor: null,
      company: null,
      ...(typeof initialChecks === 'object' && initialChecks ? initialChecks : {}),
    };

    const statusToBadgeClass = (status) => {
      const value = (status || '').toUpperCase();
      if (value === 'PASS') return 'text-bg-success';
      if (value === 'FAIL') return 'text-bg-danger';
      if (value === 'WARN') return 'text-bg-warning';
      if (value === 'NEEDS_API') return 'text-bg-secondary';
      return 'text-bg-secondary';
    };

    const setGstFeedback = (kind, message, variant = 'danger') => {
      const card = gstSection.querySelector(`[data-gst-card="${kind}"]`);
      if (!card) return;
      const feedbackEl = card.querySelector('[data-gst-feedback]');
      if (!feedbackEl) return;
      if (!message) {
        feedbackEl.classList.add('d-none');
        feedbackEl.textContent = '';
        return;
      }
      const variants = ['alert-danger', 'alert-warning', 'alert-success', 'alert-info', 'alert-secondary', 'alert-primary'];
      variants.forEach((cls) => feedbackEl.classList.remove(cls));
      feedbackEl.classList.remove('d-none');
      feedbackEl.classList.add(`alert-${variant}`);
      feedbackEl.textContent = message;
    };

    const applyGstCheckState = (kind, data) => {
      const card = gstSection.querySelector(`[data-gst-card="${kind}"]`);
      if (!card) return;
      const statusBadge = card.querySelector('[data-gst-status]');
      const summaryEl = card.querySelector('[data-gst-summary]');
      const lastEl = card.querySelector('[data-gst-last-verified]');
      const viewBtn = card.querySelector('[data-gst-view-details]');
      const verifyBtn = card.querySelector('[data-gst-verify-button]');
      const labelEl = verifyBtn?.querySelector('[data-label]') || null;
      const gstValue = (verifyBtn?.dataset.gstValue || '').trim();
      const normalizedStatus = (data?.status || '').toUpperCase();
      const badgeClass = statusToBadgeClass(normalizedStatus);

      if (statusBadge) {
        statusBadge.className = `badge text-uppercase small ${badgeClass}`;
        statusBadge.textContent = data?.status_label || (normalizedStatus ? formatStatusLabel(normalizedStatus) : 'Unverified');
      }

      if (verifyBtn && labelEl) {
        labelEl.textContent = data ? 'Re-verify GST' : 'Verify GST';
        verifyBtn.disabled = !gstValue;
      }

      if (summaryEl) {
        if (!gstValue) {
          summaryEl.textContent = 'Add a GSTIN to enable verification.';
        } else if (data?.summary) {
          summaryEl.textContent = data.summary;
        } else {
          summaryEl.textContent = 'Verification not yet performed.';
        }
      }

      const hasDetails = data && data.details && Object.keys(data.details).length > 0;
      if (viewBtn) {
        viewBtn.hidden = !hasDetails;
      }

      if (lastEl) {
        if (data?.updated_at_display) {
          lastEl.textContent = `Last verified ${data.updated_at_display}`;
        } else {
          lastEl.textContent = 'Last verified —';
        }
      }

      card.setAttribute('data-gst-payload', JSON.stringify(data?.details || {}));
      card.setAttribute('data-gst-updated', data?.updated_at || '');

      storedChecks[kind] = data || null;
      gstSection.dataset.gstChecks = JSON.stringify(storedChecks);
    };

    ['vendor', 'company'].forEach((kind) => {
      applyGstCheckState(kind, storedChecks[kind] || null);
    });

    gstSection.addEventListener('click', async (event) => {
      const verifyBtn = event.target.closest('[data-gst-verify-button]');
      if (verifyBtn) {
        event.preventDefault();
        const kind = verifyBtn.dataset.gstKind;
        if (!kind) return;
        const gstValue = (verifyBtn.dataset.gstValue || '').trim();
        if (!gstValue) {
          setGstFeedback(kind, 'GSTIN missing on this invoice.', 'warning');
          return;
        }
        if (!verifyUrlTemplate) {
          setGstFeedback(kind, 'GST verification endpoint is not configured.', 'danger');
          return;
        }
        const url = verifyUrlTemplate.replace('__KIND__', kind);
        const spinner = verifyBtn.querySelector('[data-spinner]');
        const labelEl = verifyBtn.querySelector('[data-label]');
        const originalLabel = labelEl?.textContent || 'Verify GST';
        const csrfToken = getCsrfToken();
        setGstFeedback(kind, '');
        verifyBtn.disabled = true;
        if (spinner) spinner.classList.remove('d-none');
        if (labelEl) labelEl.textContent = 'Verifying…';
        try {
          const response = await fetch(url, {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              Accept: 'application/json',
              'X-CSRFToken': csrfToken,
            },
            credentials: 'same-origin',
            body: JSON.stringify({}),
          });
          let payload = null;
          try {
            payload = await response.json();
          } catch (err) {
            payload = null;
          }
          if (!response.ok || !payload || payload.status !== 'ok') {
            const message = payload?.message || `GST verification failed (${response.status}).`;
            setGstFeedback(kind, message, 'danger');
          } else {
            applyGstCheckState(kind, payload.check || null);
            if (payload.check) {
              const statusValue = (payload.check.status || '').toUpperCase();
              const variant = statusValue === 'PASS' ? 'success' : statusValue === 'WARN' ? 'warning' : statusValue === 'FAIL' ? 'danger' : 'info';
              const label = payload.check.status_label || formatStatusLabel(statusValue);
              setGstFeedback(kind, `Verification result: ${label}`, variant);
            } else {
              setGstFeedback(kind, 'Verification status cleared.', 'info');
            }
          }
        } catch (err) {
          setGstFeedback(kind, 'Network error while verifying GST.', 'danger');
        } finally {
          if (spinner) spinner.classList.add('d-none');
          if (labelEl) labelEl.textContent = originalLabel;
          verifyBtn.disabled = !gstValue;
        }
        return;
      }

      const viewBtn = event.target.closest('[data-gst-view-details]');
      if (viewBtn) {
        event.preventDefault();
        const kind = viewBtn.dataset.gstViewDetails;
        if (!kind || !Modal || !payloadModalEl) return;
        const card = gstSection.querySelector(`[data-gst-card="${kind}"]`);
        if (!card) return;
        const payloadRaw = card.getAttribute('data-gst-payload') || '{}';
        const parsed = parseJsonSafe(payloadRaw) || {};
        renderPayloadTable(parsed);
        if (payloadModalLabel) {
          const title = kind === 'vendor' ? 'Vendor GST verification' : 'Company GST verification';
          payloadModalLabel.textContent = `${title} details`;
        }
        if (payloadModalMeta) {
          const updatedIso = card.getAttribute('data-gst-updated');
          const timestamp = updatedIso ? formatTimelineTimestamp(updatedIso) : '';
          const title = kind === 'vendor' ? 'Vendor GST' : 'Company GST';
          payloadModalMeta.textContent = [title, timestamp].filter(Boolean).join(' • ');
        }
        if (!payloadModalInstance) {
          payloadModalInstance = new Modal(payloadModalEl);
        }
        payloadModalInstance.show();
      }
    });

    document.addEventListener('gst-verification-event', (evt) => {
      const detail = evt.detail || {};
      const subject = detail.subject;
      if (!subject) return;
      const kind = subject.toLowerCase();
      applyGstCheckState(kind, detail.check || null);
      if (detail.origin === 'sse' && detail.check) {
        const statusValue = (detail.check.status || '').toUpperCase();
        const variant = statusValue === 'PASS' ? 'success' : statusValue === 'WARN' ? 'warning' : statusValue === 'FAIL' ? 'danger' : 'info';
        const label = detail.check.status_label || formatStatusLabel(statusValue);
        setGstFeedback(kind, `Verification result: ${label}`, variant);
      }
    });
  }

  const marketSection = document.getElementById('marketBenchmarkSection');
  if (marketSection) {
    const runUrl = marketSection.dataset.runUrl || '';
    const fetchUrl = marketSection.dataset.fetchUrl || '';
    const storedBenchmarks = parseJsonSafe(marketSection.dataset.benchmarks || '[]') || [];
    const runButton = marketSection.querySelector('[data-benchmark-run]');
    const spinner = runButton?.querySelector('[data-spinner]') || null;
    const label = runButton?.querySelector('[data-label]') || null;
    const tableWrapper = marketSection.querySelector('[data-benchmark-table]');
    const tableBody = marketSection.querySelector('[data-benchmark-rows]');
    const emptyState = marketSection.querySelector('[data-benchmark-empty]');
    const feedback = marketSection.querySelector('[data-benchmark-feedback]');
    const lastRun = marketSection.querySelector('[data-benchmark-last-run]');
    let benchmarkCache = Array.isArray(storedBenchmarks) ? [...storedBenchmarks] : [];

    if (runButton && !runUrl) {
      runButton.disabled = true;
    }

    const setFeedback = (message, variant = 'info') => {
      if (!feedback) return;
      const classes = ['alert-danger', 'alert-warning', 'alert-success', 'alert-info'];
      classes.forEach((cls) => feedback.classList.remove(cls));
      if (!message) {
        feedback.classList.add('d-none');
        feedback.textContent = '';
        return;
      }
      feedback.textContent = message;
      feedback.classList.remove('d-none');
      feedback.classList.add(`alert-${variant}`);
    };

    const formatMoney = (value, currency) => {
      if (value === null || value === undefined) return '—';
      const numeric = Number(value);
      const code = (currency || 'INR').toUpperCase();
      if (!Number.isFinite(numeric)) {
        return `${value} ${code}`.trim();
      }
      try {
        return new Intl.NumberFormat(undefined, {
          style: 'currency',
          currency: code,
          maximumFractionDigits: 2,
        }).format(numeric);
      } catch (err) {
        return `${numeric.toFixed(2)} ${code}`;
      }
    };

    const formatDelta = (value) => {
      if (value === null || value === undefined) return '—';
      const numeric = Number(value);
      if (!Number.isFinite(numeric)) return String(value);
      const label = numeric > 0 ? `+${numeric.toFixed(1)}%` : `${numeric.toFixed(1)}%`;
      return label;
    };

    const badgeClassForDelta = (value) => {
      if (value === null || value === undefined) return 'text-bg-secondary';
      const numeric = Number(value);
      if (!Number.isFinite(numeric) || numeric === 0) return 'text-bg-secondary';
      return numeric > 0 ? 'text-bg-danger' : 'text-bg-success';
    };

    const renderSources = (container, sources) => {
      if (!container) return;
      container.innerHTML = '';
      if (!Array.isArray(sources) || !sources.length) return;
      const list = document.createElement('div');
      list.className = 'd-flex flex-wrap gap-1';
      sources.slice(0, 3).forEach((entry) => {
        const url = (entry?.url || '').toString();
        const title = (entry?.title || 'Source').toString();
        if (!url) return;
        const link = document.createElement('a');
        link.href = url;
        link.target = '_blank';
        link.rel = 'noopener noreferrer';
        link.className = 'badge text-bg-light text-decoration-none';
        link.textContent = title.trim() || 'Source';
        list.appendChild(link);
      });
      if (list.children.length) {
        container.appendChild(list);
      }
    };

    const renderBenchmarks = (entries, { runAt } = {}) => {
      if (tableBody) {
        tableBody.innerHTML = '';
      }
      if (!Array.isArray(entries) || !entries.length) {
        if (tableWrapper) tableWrapper.hidden = true;
        if (emptyState) emptyState.classList.remove('d-none');
        if (lastRun) lastRun.textContent = 'Last run —';
        return;
      }
      if (tableWrapper) tableWrapper.hidden = false;
      if (emptyState) emptyState.classList.add('d-none');

      const timestamps = [];

      entries.forEach((entry) => {
        const row = document.createElement('tr');
        const lineCell = document.createElement('th');
        lineCell.scope = 'row';
        lineCell.textContent = entry?.line_no ? `#${entry.line_no}` : '—';

        const productCell = document.createElement('td');
        const name = document.createElement('div');
        name.className = 'fw-semibold';
        name.textContent = (entry?.product_name || entry?.search_query || 'Line item').toString();
        productCell.appendChild(name);
        if (entry?.summary) {
          const summary = document.createElement('div');
          summary.className = 'small text-muted';
          summary.textContent = entry.summary;
          productCell.appendChild(summary);
        }
        if (entry?.confidence !== undefined && entry?.confidence !== null) {
          const confidence = Number(entry.confidence);
          if (Number.isFinite(confidence)) {
            const confidenceBadge = document.createElement('span');
            confidenceBadge.className = 'badge text-bg-info mt-1';
            confidenceBadge.textContent = `Confidence ${(confidence * 100).toFixed(0)}%`;
            productCell.appendChild(confidenceBadge);
          }
        }
        const sourceHolder = document.createElement('div');
        sourceHolder.className = 'mt-1';
        renderSources(sourceHolder, entry?.sources);
        if (sourceHolder.childElementCount) {
          productCell.appendChild(sourceHolder);
        }

        const billedCell = document.createElement('td');
        billedCell.className = 'text-end';
        billedCell.textContent = formatMoney(entry?.billed_price, entry?.billed_currency);

        const marketCell = document.createElement('td');
        marketCell.className = 'text-end';
        marketCell.textContent = formatMoney(entry?.market_price, entry?.market_currency);

        const deltaCell = document.createElement('td');
        deltaCell.className = 'text-end';
        const deltaBadge = document.createElement('span');
        deltaBadge.className = `badge ${badgeClassForDelta(entry?.delta_percent)}`;
        deltaBadge.textContent = formatDelta(entry?.delta_percent);
        deltaCell.appendChild(deltaBadge);

        row.append(lineCell, productCell, billedCell, marketCell, deltaCell);
        if (tableBody) {
          tableBody.appendChild(row);
        }

        const updatedAt = entry?.updated_at || entry?.created_at;
        if (updatedAt) {
          timestamps.push(updatedAt);
        }
      });

      const latest = runAt || (timestamps.length ? timestamps.sort().slice(-1)[0] : null);
      if (lastRun) {
        lastRun.textContent = latest ? `Last run ${formatTimelineTimestamp(latest)}` : 'Last run —';
      }
    };

    renderBenchmarks(benchmarkCache);

    const toggleBusy = (busy) => {
      if (!runButton) return;
      runButton.disabled = busy || !runUrl;
      if (spinner) spinner.classList.toggle('d-none', !busy);
      if (label) label.textContent = busy ? 'Running…' : 'Run market check';
    };

    const refreshFromServer = async () => {
      if (!fetchUrl) return;
      try {
        const response = await fetch(fetchUrl, { headers: { Accept: 'application/json' }, credentials: 'same-origin' });
        if (!response.ok) return;
        const payload = await response.json();
        if (payload?.status === 'ok' && Array.isArray(payload?.benchmarks)) {
          benchmarkCache = payload.benchmarks;
          marketSection.dataset.benchmarks = JSON.stringify(benchmarkCache);
          renderBenchmarks(benchmarkCache, { runAt: payload?.run_at });
        }
      } catch (err) {
        // silent refresh failure
      }
    };

    if (benchmarkCache.length && fetchUrl) {
      refreshFromServer();
    }

    marketSection.addEventListener('click', async (event) => {
      const button = event.target.closest('[data-benchmark-run]');
      if (!button || button.disabled) return;
      event.preventDefault();
      if (!runUrl) {
        setFeedback('Market benchmark endpoint is not configured.', 'danger');
        return;
      }
      const csrfToken = getCsrfToken();
      setFeedback('');
      toggleBusy(true);
      try {
        const response = await fetch(runUrl, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            Accept: 'application/json',
            'X-CSRFToken': csrfToken,
          },
          credentials: 'same-origin',
          body: JSON.stringify({}),
        });
        const payload = await response.json().catch(() => null);
        if (!response.ok || !payload || payload.status !== 'ok') {
          const message = payload?.message || `Market benchmark failed (${response.status}).`;
          setFeedback(message, 'danger');
          return;
        }
        benchmarkCache = Array.isArray(payload.benchmarks) ? payload.benchmarks : [];
        marketSection.dataset.benchmarks = JSON.stringify(benchmarkCache);
        renderBenchmarks(benchmarkCache, { runAt: payload.run_at });
        if (Array.isArray(payload.errors) && payload.errors.length) {
          setFeedback(`Benchmarked with ${payload.errors.length} warning(s).`, 'warning');
        } else {
          setFeedback('Market benchmark completed successfully.', 'success');
        }
      } catch (err) {
        setFeedback('Network error while running market benchmark.', 'danger');
      } finally {
        toggleBusy(false);
      }
    });
  }
  const parseButton = document.getElementById('parseWithAIButton');
  const parseSpinner = document.getElementById('parseSpinner');
  const parseError = document.getElementById('parseError');
  const actionForm = document.getElementById('invoiceActionForm');
  const actionField = document.getElementById('actionField');
  const assigneeSelect = document.getElementById('assigneeSelect');
  const assignmentSection = document.querySelector('[data-assignment-section]');
  const assignmentSummary = assignmentSection?.querySelector('[data-assignment-summary]');
  const assignmentControl = assignmentSection?.querySelector('[data-assignment-control]');
  const assignmentName = assignmentSection?.querySelector('[data-assignment-name]');
  const assignmentMeta = assignmentSection?.querySelector('[data-assignment-meta]');
  const assignmentChangeButton = assignmentSection?.querySelector('[data-assignment-change]');
  let assignmentState = parseJsonSafe(assignmentSection?.dataset.assignment || null);
  if (!assignmentState || typeof assignmentState !== 'object') {
    assignmentState = null;
  }

  const formatAssignmentLabel = (state) => {
    if (!state) return '';
    if (state.assigned_at_display) return `Assigned ${state.assigned_at_display}`;
    if (state.assigned_at) return `Assigned ${formatTimelineTimestamp(state.assigned_at)}`;
    return 'Assigned';
  };

  const showAssignmentSummary = (state) => {
    if (!assignmentSection) return;
    const info = state && typeof state === 'object' ? state : null;
    if (assignmentControl) {
      assignmentControl.setAttribute('hidden', 'true');
    }
    if (assignmentSummary) {
      if (info) {
        assignmentSummary.removeAttribute('hidden');
      } else {
        assignmentSummary.setAttribute('hidden', 'true');
      }
    }
    if (assignmentName) {
      assignmentName.textContent = info?.assignee_name || '';
    }
    if (assignmentMeta) {
      assignmentMeta.textContent = info ? formatAssignmentLabel(info) : '';
    }
  };

  const showAssignmentControl = () => {
    if (!assignmentSection) return;
    if (assignmentControl) {
      assignmentControl.removeAttribute('hidden');
    }
    if (assignmentSummary) {
      assignmentSummary.setAttribute('hidden', 'true');
    }
    if (assigneeSelect) {
      assigneeSelect.value = '0';
    }
  };

  const setAssignmentState = (state) => {
    if (!assignmentSection) return;
    assignmentState = state && typeof state === 'object' ? state : null;
    assignmentSection.dataset.assignment = JSON.stringify(assignmentState || {});
    if (assignmentState && assignmentState.assignee_id) {
      if (assigneeSelect) {
        assigneeSelect.value = String(assignmentState.assignee_id);
      }
      showAssignmentSummary(assignmentState);
    } else {
      showAssignmentControl();
    }
  };

  if (assignmentSection) {
    if (assignmentState && assignmentState.assignee_id) {
      showAssignmentSummary(assignmentState);
    } else {
      showAssignmentControl();
      assignmentState = null;
      assignmentSection.dataset.assignment = JSON.stringify({});
    }

    assignmentChangeButton?.addEventListener('click', (event) => {
      event.preventDefault();
      showAssignmentControl();
      window.setTimeout(() => {
        assigneeSelect?.focus();
      }, 0);
    });
  }
  const statusSelect = document.getElementById('statusSelect');
  const actionFeedback = document.getElementById('actionError');
  const actionEndpoint = actionForm?.getAttribute('action') || '';
  const computeRiskButton = document.getElementById('computeRiskButton');
  const riskSpinner = document.getElementById('riskSpinner');
  const riskSection = document.getElementById('riskSection');

  const counterfactualSection = document.getElementById('counterfactualSection');
  if (counterfactualSection) {
    const endpoint = counterfactualSection.dataset.counterfactualEndpoint || '';
    const maxLines = Number(counterfactualSection.dataset.counterfactualMaxLines || '5');
    const form = counterfactualSection.querySelector('#counterfactualForm');
    const linesContainer = counterfactualSection.querySelector('[data-counterfactual-lines]');
    const addLineButton = counterfactualSection.querySelector('#counterfactualAddLine');
    const resetButton = counterfactualSection.querySelector('#counterfactualResetButton');
    const runButton = counterfactualSection.querySelector('#counterfactualRunButton');
    const spinner = counterfactualSection.querySelector('#counterfactualSpinner');
    const errorBox = counterfactualSection.querySelector('#counterfactualError');
    const resultBox = counterfactualSection.querySelector('#counterfactualResult');
    const hint = counterfactualSection.querySelector('#counterfactualHint');
    let lineIdCounter = 0;

    const rawOptions = parseJsonSafe(counterfactualSection.dataset.counterfactualLines || '[]') || [];
    const optionMap = new Map();
    rawOptions.forEach((entry) => {
      const lineNo = Number(entry?.line_no);
      if (!Number.isFinite(lineNo) || lineNo <= 0) return;
      optionMap.set(lineNo, {
        line_no: lineNo,
        description: (entry?.description || '').toString(),
        hsn_sac: (entry?.hsn_sac || '').toString(),
        qty: entry?.qty !== undefined && entry?.qty !== null ? entry.qty.toString() : '',
        unit_price: entry?.unit_price !== undefined && entry?.unit_price !== null ? entry.unit_price.toString() : '',
        gst_rate: entry?.gst_rate !== undefined && entry?.gst_rate !== null ? entry.gst_rate.toString() : '',
      });
    });
    const lineOptions = Array.from(optionMap.values()).sort((a, b) => a.line_no - b.line_no);

    const updateAddState = (forceDisable = false) => {
      if (!addLineButton || !linesContainer) return;
      if (forceDisable) {
        addLineButton.disabled = true;
        return;
      }
      const current = linesContainer.querySelectorAll('[data-counterfactual-line]').length;
      addLineButton.disabled = !endpoint || (maxLines && current >= maxLines);
    };

    const toggleBusy = (busy) => {
      if (runButton) runButton.disabled = busy || !endpoint;
      if (resetButton) resetButton.disabled = busy;
      if (spinner) spinner.classList.toggle('d-none', !busy);
      updateAddState(busy);
    };

    const setError = (message) => {
      if (!errorBox) return;
      if (!message) {
        errorBox.classList.add('d-none');
        errorBox.textContent = '';
        return;
      }
      errorBox.textContent = message;
      errorBox.classList.remove('d-none');
    };

    const clearResult = () => {
      if (!resultBox) return;
      resultBox.classList.add('d-none');
      resultBox.replaceChildren();
    };

    const formatNumber = (value, { minimumFractionDigits = 2, maximumFractionDigits = 2 } = {}) => {
      if (value === null || value === undefined) return '—';
      const numeric = Number(value);
      if (Number.isNaN(numeric)) return String(value);
      return new Intl.NumberFormat(undefined, { minimumFractionDigits, maximumFractionDigits }).format(numeric);
    };

    const renderResult = (payload) => {
      if (!resultBox) return;
      resultBox.replaceChildren();

      const totals = [
        ['Subtotal', payload?.totals_before?.subtotal, payload?.totals_after?.subtotal, payload?.totals_delta?.subtotal],
        ['Tax', payload?.totals_before?.tax_total, payload?.totals_after?.tax_total, payload?.totals_delta?.tax_total],
        ['Grand Total', payload?.totals_before?.grand_total, payload?.totals_after?.grand_total, payload?.totals_delta?.grand_total],
      ];

      const header = document.createElement('div');
      header.className = 'd-flex justify-content-between flex-wrap align-items-center gap-2 mb-3';
      const title = document.createElement('h4');
      title.className = 'h6 mb-0';
      title.textContent = 'Simulation results';
      header.appendChild(title);

      const deltaComposite = Number(payload?.delta_composite || 0);
      const deltaBadge = document.createElement('span');
      const deltaVariant = deltaComposite < 0 ? 'text-bg-success' : deltaComposite > 0 ? 'text-bg-danger' : 'text-bg-secondary';
      deltaBadge.className = `badge ${deltaVariant}`;
      const deltaLabel = deltaComposite >= 0 ? `+${formatNumber(deltaComposite, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}` : formatNumber(deltaComposite, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
      deltaBadge.textContent = `Risk Δ ${deltaLabel}`;
      header.appendChild(deltaBadge);
      resultBox.appendChild(header);

      const totalsCard = document.createElement('div');
      totalsCard.className = 'bg-white rounded-3 p-3 shadow-sm';
      const totalsTitle = document.createElement('h5');
      totalsTitle.className = 'h6 mb-3';
      totalsTitle.textContent = 'Totals comparison';
      totalsCard.appendChild(totalsTitle);

      const totalsTable = document.createElement('table');
      totalsTable.className = 'table table-sm counterfactual-summary-table mb-0';
      const totalsBody = document.createElement('tbody');
      totals.forEach(([label, before, after, delta]) => {
        const row = document.createElement('tr');
        const labelCell = document.createElement('th');
        labelCell.scope = 'row';
        labelCell.textContent = label;
        const beforeCell = document.createElement('td');
        beforeCell.textContent = formatNumber(before);
        const afterCell = document.createElement('td');
        afterCell.textContent = formatNumber(after);
        const deltaCell = document.createElement('td');
        const deltaValue = Number(delta ?? 0);
        const deltaSpan = document.createElement('span');
        deltaSpan.className = deltaValue < 0 ? 'text-success fw-semibold' : deltaValue > 0 ? 'text-danger fw-semibold' : 'text-muted';
        const deltaFormatted = deltaValue >= 0 ? `+${formatNumber(deltaValue)}` : formatNumber(deltaValue);
        deltaSpan.textContent = deltaFormatted;
        deltaCell.appendChild(deltaSpan);
        row.append(labelCell, beforeCell, afterCell, deltaCell);
        totalsBody.appendChild(row);
      });
      totalsTable.appendChild(totalsBody);
      totalsCard.appendChild(totalsTable);
      resultBox.appendChild(totalsCard);

      const riskCard = document.createElement('div');
      riskCard.className = 'bg-white rounded-3 p-3 shadow-sm mt-3';
      const riskTitle = document.createElement('h5');
      riskTitle.className = 'h6 mb-3';
      riskTitle.textContent = 'Risk profile';
      riskCard.appendChild(riskTitle);

      const riskGrid = document.createElement('div');
      riskGrid.className = 'row g-3';

      const makeRiskCol = (labelText, value) => {
        const col = document.createElement('div');
        col.className = 'col-12 col-sm-4';
        const label = document.createElement('div');
        label.className = 'text-uppercase text-muted small';
        label.textContent = labelText;
        const val = document.createElement('div');
        val.className = 'fw-semibold fs-5';
        val.textContent = formatNumber(value, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        col.append(label, val);
        return col;
      };

      riskGrid.append(
        makeRiskCol('Before', payload?.risk_before?.composite),
        makeRiskCol('After', payload?.risk_after?.composite),
      );

      const policyCol = document.createElement('div');
      policyCol.className = 'col-12 col-sm-4';
      const policyLabel = document.createElement('div');
      policyLabel.className = 'text-uppercase text-muted small';
      policyLabel.textContent = 'Policy version';
      const policyValue = document.createElement('div');
      policyValue.className = 'fw-semibold';
      policyValue.textContent = payload?.risk_after?.policy_version || payload?.risk_before?.policy_version || '—';
      policyCol.append(policyLabel, policyValue);
      riskGrid.appendChild(policyCol);

      riskCard.appendChild(riskGrid);

      const contributors = Array.isArray(payload?.risk_after?.contributors) ? payload.risk_after.contributors.slice(0, 3) : [];
      if (contributors.length) {
        const listTitle = document.createElement('div');
        listTitle.className = 'text-uppercase text-muted small mt-3';
        listTitle.textContent = 'Top contributors';
        riskCard.appendChild(listTitle);

        const list = document.createElement('ul');
        list.className = 'list-unstyled mb-0 small';
        contributors.forEach((entry) => {
          const item = document.createElement('li');
          item.className = 'd-flex justify-content-between align-items-center py-1';
          const left = document.createElement('span');
          left.textContent = entry?.name || 'Contributor';
          const right = document.createElement('span');
          right.className = 'text-muted';
          const impact = Number(entry?.contribution ?? 0) * 100;
          right.textContent = Number.isFinite(impact) ? `${impact.toFixed(1)}% impact` : '';
          item.append(left, right);
          list.appendChild(item);
        });
        riskCard.appendChild(list);
      }

      resultBox.appendChild(riskCard);

      const notes = Array.isArray(payload?.notes) ? payload.notes.filter((note) => Boolean(note)) : [];
      if (notes.length) {
        const notesCard = document.createElement('div');
        notesCard.className = 'bg-white rounded-3 p-3 shadow-sm mt-3';
        const notesTitle = document.createElement('h5');
        notesTitle.className = 'h6 mb-2';
        notesTitle.textContent = 'Notes';
        notesCard.appendChild(notesTitle);
        const notesList = document.createElement('ul');
        notesList.className = 'mb-0 ps-3 small';
        notes.forEach((note) => {
          const li = document.createElement('li');
          li.textContent = note;
          notesList.appendChild(li);
        });
        notesCard.appendChild(notesList);
        resultBox.appendChild(notesCard);
      }

      resultBox.classList.remove('d-none');
    };

    const updateDescription = (row, lineNo) => {
      if (!row) return;
      const descriptionEl = row.querySelector('[data-counterfactual-description]');
      if (!descriptionEl) return;
      if (!lineNo || !optionMap.has(lineNo)) {
        descriptionEl.textContent = '';
        descriptionEl.classList.add('d-none');
        return;
      }
      const meta = optionMap.get(lineNo);
      const parts = [];
      if (meta.description) parts.push(meta.description);
      if (meta.qty) parts.push(`Qty ${meta.qty}`);
      if (meta.unit_price) parts.push(`Unit ${meta.unit_price}`);
      if (meta.gst_rate) parts.push(`GST ${meta.gst_rate}%`);
      descriptionEl.textContent = parts.join(' • ');
      descriptionEl.classList.toggle('d-none', parts.length === 0);
    };

    const buildLineRow = (defaults = {}) => {
      if (!linesContainer) return null;
      const row = document.createElement('div');
      row.className = 'counterfactual-line';
      row.dataset.counterfactualLine = 'true';
      const rowId = `counterfactual-line-${lineIdCounter += 1}`;

      const header = document.createElement('div');
      header.className = 'd-flex justify-content-between align-items-center gap-2 mb-3 flex-wrap';

      const lineGroup = document.createElement('div');
      lineGroup.className = 'd-flex align-items-center gap-2 flex-wrap';
      const lineLabel = document.createElement('label');
      lineLabel.className = 'form-label mb-0';
      lineLabel.setAttribute('for', `${rowId}-selector`);
      lineLabel.textContent = 'Line';

      let lineField;
      if (lineOptions.length) {
        lineField = document.createElement('select');
        lineField.className = 'form-select form-select-sm';
        lineField.id = `${rowId}-selector`;
        lineField.dataset.field = 'line_no';
        const placeholderOption = document.createElement('option');
        placeholderOption.value = '';
        placeholderOption.textContent = 'Choose…';
        lineField.appendChild(placeholderOption);
        lineOptions.forEach((option) => {
          const opt = document.createElement('option');
          opt.value = option.line_no;
          const labelParts = [option.line_no];
          if (option.description) labelParts.push(option.description);
          opt.textContent = labelParts.join(' · ');
          lineField.appendChild(opt);
        });
      } else {
        lineField = document.createElement('input');
        lineField.type = 'number';
        lineField.min = '1';
        lineField.className = 'form-control form-control-sm';
        lineField.id = `${rowId}-selector`;
        lineField.dataset.field = 'line_no';
        lineField.placeholder = 'Line #';
        if (hint) {
          hint.textContent = 'Enter the line number and overrides to evaluate a counterfactual scenario.';
        }
      }
      if (defaults.line_no) {
        lineField.value = defaults.line_no;
      }

      lineGroup.append(lineLabel, lineField);
      header.appendChild(lineGroup);

      const removeBtn = document.createElement('button');
      removeBtn.type = 'button';
      removeBtn.className = 'btn btn-outline-danger btn-sm';
      removeBtn.innerHTML = '<i class="bi bi-x-lg"></i>';
      removeBtn.addEventListener('click', () => {
        row.remove();
        updateAddState();
      });
      header.appendChild(removeBtn);
      row.appendChild(header);

      const fieldRow = document.createElement('div');
      fieldRow.className = 'row g-2';

      const createField = (colClass, labelText, placeholder, key, initialValue) => {
        const col = document.createElement('div');
        col.className = colClass;
        const label = document.createElement('label');
        label.className = 'form-label mb-1';
        label.textContent = labelText;
        const input = document.createElement('input');
        input.className = 'form-control form-control-sm';
        if (['qty', 'unit_price', 'gst_rate'].includes(key)) {
          input.type = 'number';
          input.step = '0.0001';
        } else {
          input.type = 'text';
        }
        input.placeholder = placeholder;
        input.dataset.field = key;
        if (initialValue) {
          input.value = initialValue;
        }
        col.append(label, input);
        return { col, input };
      };

      const fields = [
        createField('col-12', 'HSN / SAC', 'Unchanged', 'hsn_sac', defaults.hsn_sac),
        createField('col-6', 'Quantity', 'Unchanged', 'qty', defaults.qty),
        createField('col-6', 'Unit price', 'Unchanged', 'unit_price', defaults.unit_price),
        createField('col-6', 'GST %', 'Unchanged', 'gst_rate', defaults.gst_rate),
      ];
      fields.forEach(({ col }) => fieldRow.appendChild(col));
      row.appendChild(fieldRow);

      const description = document.createElement('div');
      description.className = 'form-text mt-2 small d-none';
      description.dataset.counterfactualDescription = 'true';
      row.appendChild(description);

      linesContainer.appendChild(row);

      if (lineField instanceof HTMLSelectElement || lineField instanceof HTMLInputElement) {
        lineField.addEventListener('change', () => {
          const value = Number(lineField.value);
          const meta = optionMap.get(value);
          fields.forEach(({ input }) => {
            if (input.value) return;
            const key = input.dataset.field;
            if (meta && meta[key]) {
              input.placeholder = `Current ${meta[key]}`;
            } else {
              input.placeholder = 'Unchanged';
            }
          });
          updateDescription(row, value);
        });
        lineField.dispatchEvent(new Event('change'));
      }

      updateAddState();
      return row;
    };

    const collectChanges = () => {
      const rows = Array.from(linesContainer?.querySelectorAll('[data-counterfactual-line]') || []);
      const changes = [];
      const seen = new Set();
      rows.forEach((row) => {
        const lineField = row.querySelector('[data-field="line_no"]');
        if (!lineField) return;
        const rawValue = lineField.value?.toString().trim() || '';
        const lineNo = Number(rawValue);
        if (!lineNo || Number.isNaN(lineNo)) {
          throw new Error('Each adjustment requires a valid line number.');
        }
        if (seen.has(lineNo)) {
          throw new Error('Each adjustment must reference a unique line number.');
        }
        const change = { line_no: lineNo };
        let hasOverride = false;
        row.querySelectorAll('[data-field]').forEach((input) => {
          const key = input.dataset.field;
          if (key === 'line_no') return;
          const value = input.value?.toString().trim() || '';
          if (!value) return;
          change[key] = value;
          hasOverride = true;
        });
        if (!hasOverride) {
          throw new Error(`Provide at least one override for line ${lineNo}.`);
        }
        changes.push(change);
        seen.add(lineNo);
      });
      return changes;
    };

    const addLineRow = (defaults = {}) => {
      if (!linesContainer) return;
      const current = linesContainer.querySelectorAll('[data-counterfactual-line]').length;
      if (maxLines && current >= maxLines) {
        setError(`Limit of ${maxLines} line adjustments reached.`);
        return;
      }
      buildLineRow(defaults);
    };

    addLineButton?.addEventListener('click', (event) => {
      event.preventDefault();
      setError('');
      addLineRow({});
    });

    resetButton?.addEventListener('click', (event) => {
      event.preventDefault();
      setError('');
      clearResult();
      linesContainer?.replaceChildren();
      if (lineOptions.length) {
        addLineRow({ line_no: lineOptions[0].line_no });
      } else {
        addLineRow({});
      }
    });

    form?.addEventListener('submit', async (event) => {
      event.preventDefault();
      if (!endpoint) {
        setError('Counterfactual endpoint is not configured.');
        return;
      }
      let changes;
      try {
        changes = collectChanges();
      } catch (validationError) {
        setError(validationError instanceof Error ? validationError.message : 'Validation failed.');
        return;
      }
      if (!changes.length) {
        setError('Add at least one line adjustment before running the simulation.');
        return;
      }
      setError('');
      clearResult();
      toggleBusy(true);
      try {
        const response = await fetch(endpoint, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            Accept: 'application/json',
            'X-CSRFToken': getCsrfToken(),
          },
          credentials: 'same-origin',
          body: JSON.stringify({ line_changes: changes }),
        });
        const payload = await response.json();
        if (!response.ok || payload?.status !== 'ok' || !payload?.result) {
          throw new Error(payload?.message || 'Counterfactual evaluation failed.');
        }
        renderResult(payload.result);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Unexpected error during counterfactual evaluation.');
      } finally {
        toggleBusy(false);
      }
    });

    if (lineOptions.length) {
      addLineRow({ line_no: lineOptions[0].line_no });
    } else {
      addLineRow({});
    }
  }

  const clearActionFeedback = () => {
    if (!actionFeedback) return;
    actionFeedback.textContent = '';
    actionFeedback.setAttribute('hidden', 'true');
    actionFeedback.classList.remove('text-success');
    if (!actionFeedback.classList.contains('text-danger')) {
      actionFeedback.classList.add('text-danger');
    }
  };

  const showActionFeedback = (message, variant = 'danger') => {
    if (!actionFeedback) return;
    actionFeedback.textContent = message;
    actionFeedback.removeAttribute('hidden');
    if (variant === 'success') {
      actionFeedback.classList.remove('text-danger');
      actionFeedback.classList.add('text-success');
    } else {
      actionFeedback.classList.remove('text-success');
      actionFeedback.classList.add('text-danger');
    }
  };

  const statusBadge = document.getElementById('invoiceStatusBadge');
  if (statusBadge) {
    const initialStatus = statusBadge.textContent?.trim().toUpperCase() || 'UPLOADED';
    setParseButtonState(initialStatus);
  }

  if (riskSection) {
    const initialRiskStatus = (riskSection.dataset.riskStatus || '').toUpperCase();
    setRiskButtonState(initialRiskStatus || 'PENDING', { busy: initialRiskStatus === 'IN_PROGRESS' });
    const initialRiskPayload = parseJsonSafe(riskSection.dataset.riskInitial || null);
    if (initialRiskPayload) {
      applyRiskPayload({
        ...initialRiskPayload,
        computed: true,
        risk_status: initialRiskStatus || 'READY',
      });
    } else if (['READY', 'IN_PROGRESS', 'ERROR'].includes(initialRiskStatus)) {
      refreshRiskData(invoiceId).catch((err) => console.error(err)); // eslint-disable-line no-console
    }
  }

  const validateAction = (action) => {
    if (action === 'assign') {
      const value = assigneeSelect?.value || '';
      if (!value || value === '__None' || value === '0') {
        showActionFeedback('Select a reviewer before assigning.', 'danger');
        return false;
      }
    }
    if (action === 'status') {
      const value = statusSelect?.value || '';
      if (!value) {
        showActionFeedback('Choose a status before updating.', 'danger');
        return false;
      }
    }
    return true;
  };

  const handleActionResponse = (payload, action) => {
    if (payload.invoice?.processing_status) {
      updateStatusClasses(document.getElementById('invoiceStatusBadge'), payload.invoice.processing_status);
      setParseButtonState(payload.invoice.processing_status);
    }
    if (payload.event) {
      prependTimelineEvent(payload.event);
    }

    switch (action) {
      case 'assign': {
        const assignment = payload.assignment
          || (payload.event?.payload && payload.event.payload.assignee_id
            ? {
                assignee_id: payload.event.payload.assignee_id,
                assignee_name: payload.event.payload.assignee_name,
                assignee_email: payload.event.payload.assignee_email,
                assigned_at: payload.event.payload.assigned_at,
              }
            : null);
        if (assignment && !assignment.assigned_at_display && assignment.assigned_at) {
          assignment.assigned_at_display = formatTimelineTimestamp(assignment.assigned_at);
        }
        setAssignmentState(assignment);
        const selectedOption = assigneeSelect?.options[assigneeSelect.selectedIndex];
        const label = assignment?.assignee_name || selectedOption?.textContent?.trim() || 'Assignee updated';
        showActionFeedback(`Assigned to ${label}.`, 'success');
        break;
      }
      case 'status':
        showActionFeedback('Status updated.', 'success');
        break;
      case 'request_docs':
        showActionFeedback('Document request recorded.', 'success');
        break;
      case 'approve':
        showActionFeedback('Invoice approved.', 'success');
        break;
      case 'reject':
        showActionFeedback('Invoice rejected.', 'success');
        break;
      default:
        showActionFeedback('Action completed.', 'success');
    }

    window.setTimeout(() => {
      clearActionFeedback();
    }, 4000);
  };

  const submitActionForm = async (action, button) => {
    if (!actionForm || !actionField) return;
    if (!actionEndpoint) {
      showActionFeedback('Invoice action endpoint not configured.', 'danger');
      return;
    }
    clearActionFeedback();
    if (!validateAction(action)) {
      return;
    }

    actionField.value = action;
    button.disabled = true;

    try {
      const formToken = actionForm.querySelector('input[name="csrf_token"]')?.value || getCsrfToken();
      const params = new URLSearchParams();
      if (formToken) {
        params.set('csrf_token', formToken);
      }
      params.set('action', action);

      const noteField = actionForm.querySelector('[name="note"]');
      const noteValue = noteField instanceof HTMLTextAreaElement ? noteField.value : '';
      if (noteValue) {
        params.set('note', noteValue);
      }

      if (assigneeSelect) {
        const assigneeValue = assigneeSelect.value || '';
        if (assigneeValue && assigneeValue !== '__None' && assigneeValue !== '0') {
          params.set('assignee_id', assigneeValue);
        }
      }

      if (statusSelect) {
        const statusValue = statusSelect.value || '';
        if (statusValue) {
          params.set('status', statusValue);
        }
      }

      const response = await fetch(actionEndpoint, {
        method: 'POST',
        body: params,
        headers: {
          'X-CSRFToken': getCsrfToken(),
          Accept: 'application/json',
          'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
          'X-Requested-With': 'XMLHttpRequest',
        },
        credentials: 'same-origin',
      });

      let payload;
      try {
        payload = await response.json();
      } catch (jsonError) {
        throw new Error('Unexpected server response.');
      }

      if (!response.ok || payload.status !== 'ok') {
        const message = payload?.message || Object.values(payload?.errors || {})
          .flat()
          .join('\n');
        throw new Error(message || 'Unable to complete action.');
      }

      handleActionResponse(payload, action);
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Unexpected error performing action.';
      showActionFeedback(message, 'danger');
    } finally {
      actionField.value = '';
      button.disabled = false;
    }
  };

  if (actionForm) {
    const actionButtons = Array.from(actionForm.querySelectorAll('button[data-action]'));
    actionButtons.forEach((button) => {
      button.addEventListener('click', (event) => {
        event.preventDefault();
        const action = button.getAttribute('data-action');
        if (!action) return;
        submitActionForm(action, button);
      });
    });

    actionForm.addEventListener('submit', (event) => {
      event.preventDefault();
    });
  }

  parseButton?.addEventListener('click', async () => {
    if (!parseButton) return;
    parseError?.classList.add('d-none');
    parseSpinner?.classList.remove('d-none');
    parseButton.disabled = true;
    try {
      const response = await fetch(`/invoices/${invoiceId}/parse`, {
        method: 'POST',
        headers: {
          'X-CSRFToken': getCsrfToken(),
          Accept: 'application/json',
        },
      });
      const payload = await response.json();
      if (!response.ok || payload.status !== 'ok') {
        throw new Error(payload.message || 'Failed to queue invoice for parsing.');
      }
      if (payload.invoice?.processing_status) {
        updateStatusClasses(document.getElementById('invoiceStatusBadge'), payload.invoice.processing_status);
      }
      if (payload.mode === 'inline') {
        setParseButtonState(payload.invoice?.processing_status || 'READY');
        try {
          await refreshExtractedData(invoiceId);
        } catch (refreshErr) {
          console.error(refreshErr); // eslint-disable-line no-console
        }
      } else {
        setParseButtonState('QUEUED');
      }
    } catch (err) {
      if (parseError) {
        parseError.textContent = err instanceof Error ? err.message : 'An unexpected error occurred.';
        parseError.classList.remove('d-none');
      }
      parseButton.disabled = false;
    } finally {
      parseSpinner?.classList.add('d-none');
    }
  });

  computeRiskButton?.addEventListener('click', async () => {
    if (!computeRiskButton) return;
    showRiskFeedback('', 'danger');
    computeRiskButton.disabled = true;
    riskSpinner?.classList.remove('d-none');
    try {
      const response = await fetch(`/invoices/${invoiceId}/risk/run`, {
        method: 'POST',
        headers: {
          'X-CSRFToken': getCsrfToken(),
          Accept: 'application/json',
          'X-Requested-With': 'XMLHttpRequest',
        },
        credentials: 'same-origin',
      });
      const payload = await response.json();
      if (!response.ok || !payload?.queued) {
        throw new Error(payload?.message || 'Unable to queue risk computation.');
      }
      setRiskButtonState('IN_PROGRESS', { busy: true });
      showRiskFeedback('Risk computation started. Results will appear shortly.', 'info');
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Unexpected error when starting risk run.';
      showRiskFeedback(message, 'danger');
      setRiskButtonState('PENDING', { busy: false });
      computeRiskButton.disabled = false;
      riskSpinner?.classList.add('d-none');
    }
  });

  connectSSE('/events/stream', {
    onEvent: (payload) => handleInvoiceEvent(invoiceId, payload),
    onStatusChange: updateSSEStatusBadge,
  });
}

function initActivityFeed() {
  const feed = document.getElementById('activityFeed');
  if (!feed) return;
  const { Modal } = window.bootstrap || {};
  const modalEl = document.getElementById('activityPayloadModal');
  const modalLabel = modalEl?.querySelector('#activityPayloadModalLabel');
  const modalMeta = modalEl?.querySelector('#activityPayloadMeta');
  const modalTableBody = modalEl?.querySelector('#activityPayloadTableBody');
  const modalEmpty = modalEl?.querySelector('#activityPayloadEmpty');
  let modalInstance = null;

  const openModalForItem = (item) => {
    if (!Modal || !modalEl || !modalTableBody || !item) return;
    const payload = parseJsonSafe(item.dataset.eventPayload) || {};
    renderPayloadTableRows(payload, modalTableBody, modalEmpty);
    const eventType = item.dataset.eventType || 'Activity';
    if (modalLabel) {
      modalLabel.textContent = `${eventType} payload`;
    }
    if (modalMeta) {
      const parts = [];
      if (item.dataset.eventId) parts.push(`Event #${item.dataset.eventId}`);
      if (item.dataset.eventInvoice) parts.push(`Invoice #${item.dataset.eventInvoice}`);
      if (item.dataset.eventCreated) parts.push(item.dataset.eventCreated);
      modalMeta.textContent = parts.join(' • ');
    }
    if (!modalInstance) {
      modalInstance = new Modal(modalEl);
    }
    modalInstance.show();
  };

  feed.addEventListener('click', (event) => {
    const trigger = event.target.closest('[data-activity-payload-trigger]');
    if (!trigger || trigger.disabled) return;
    event.preventDefault();
    const item = trigger.closest('[data-activity-item]');
    if (!item) return;
    openModalForItem(item);
  });

  const appendActivityItem = (eventData) => {
    if (!eventData) return;
    const payload = eventData.payload;
    const isMapping = payload && typeof payload === 'object' && !Array.isArray(payload);
    const isArray = Array.isArray(payload);
    const hasPayload = (() => {
      if (isMapping) return Object.keys(payload).length > 0;
      if (isArray) return payload.length > 0;
      return payload !== null && payload !== undefined && payload !== '';
    })();

    const item = document.createElement('div');
    item.className = 'list-group-item';
    item.setAttribute('role', 'listitem');
    item.dataset.activityItem = 'true';
    if (eventData.event_id !== undefined) item.dataset.eventId = eventData.event_id;
    if (eventData.event_type) item.dataset.eventType = eventData.event_type;
    if (eventData.invoice_id !== undefined) item.dataset.eventInvoice = eventData.invoice_id;
    const createdLabel = formatTimelineTimestamp(eventData.created_at);
    if (createdLabel) item.dataset.eventCreated = createdLabel;
    try {
      item.dataset.eventPayload = JSON.stringify(payload ?? null);
    } catch (err) {
      item.dataset.eventPayload = 'null';
    }

    const wrapper = document.createElement('div');
    wrapper.className = 'd-flex justify-content-between align-items-start gap-3 flex-wrap';

    const left = document.createElement('div');
    const badge = document.createElement('span');
    badge.className = 'badge rounded-pill text-bg-light text-uppercase mb-2';
    badge.textContent = eventData.event_type || 'EVENT';
    const title = document.createElement('p');
    title.className = 'mb-1';
    title.textContent = `Invoice #${eventData.invoice_id ?? '—'}`;
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'btn btn-outline-primary btn-sm';
    button.setAttribute('data-activity-payload-trigger', 'true');
    button.innerHTML = '<i class="bi bi-table"></i><span class="ms-1">View payload</span>';
    button.disabled = !hasPayload;
    left.append(badge, title, button);

    const timestamp = document.createElement('span');
    timestamp.className = 'small text-muted';
    timestamp.textContent = createdLabel || new Date(eventData.created_at).toLocaleTimeString();

    wrapper.append(left, timestamp);
    item.appendChild(wrapper);

    const emptyState = feed.querySelector('[data-empty-state]');
    emptyState?.remove();

    feed.prepend(item);
    const items = Array.from(feed.querySelectorAll('[data-activity-item]'));
    items.forEach((entry, index) => {
      if (index > 20) {
        entry.remove();
      }
    });
  };

  connectSSE('/events/stream', {
    onEvent: appendActivityItem,
    onStatusChange: updateSSEStatusBadge,
  });
}

function initInvoiceUpload() {
  const form = document.getElementById('invoiceUploadForm');
  const dropzone = document.getElementById('uploadDropzone');
  const input = document.getElementById('invoiceFileInput');
  const browse = document.getElementById('browseButton');
  const submitButton = form?.querySelector('[data-upload-submit]');
  if (!form || !dropzone || !input) return;

  const statusLabel = dropzone.querySelector('[data-upload-selected]');

  const setSubmitState = (files) => {
    if (!submitButton) return;
    const hasFile = Boolean(files?.length);
    submitButton.classList.toggle('visually-hidden', !hasFile);
    submitButton.disabled = !hasFile;
  };

  const setSelectedLabel = (files) => {
    setSubmitState(files);
    if (!statusLabel) return;
    if (files?.length) {
      const names = Array.from(files).map((file) => file.name).join(', ');
      statusLabel.textContent = names;
      statusLabel.classList.remove('visually-hidden');
    } else {
      statusLabel.textContent = '';
      statusLabel.classList.add('visually-hidden');
    }
  };

  const openPicker = () => {
    input.click();
  };

  browse?.addEventListener('click', (event) => {
    event.preventDefault();
    openPicker();
  });

  dropzone.addEventListener('click', (event) => {
    if (event.target === browse) return;
    openPicker();
  });

  dropzone.addEventListener('keydown', (event) => {
    if (event.key === 'Enter' || event.key === ' ') {
      event.preventDefault();
      openPicker();
    }
  });

  const setDragState = (active) => {
    dropzone.classList.toggle('drag-active', active);
  };

  ['dragenter', 'dragover'].forEach((type) => {
    dropzone.addEventListener(type, (event) => {
      event.preventDefault();
      setDragState(true);
    });
  });

  ['dragleave', 'dragend'].forEach((type) => {
    dropzone.addEventListener(type, (event) => {
      event.preventDefault();
      setDragState(false);
    });
  });

  dropzone.addEventListener('drop', (event) => {
    event.preventDefault();
    setDragState(false);
    if (!event.dataTransfer?.files?.length) return;
    if (typeof DataTransfer !== 'undefined') {
      const transfer = new DataTransfer();
      Array.from(event.dataTransfer.files).forEach((file) => {
        transfer.items.add(file);
      });
      input.files = transfer.files;
    } else {
      input.files = event.dataTransfer.files;
    }
    setSelectedLabel(input.files);
  });

  input.addEventListener('change', () => {
    setSelectedLabel(input.files);
  });

  form.addEventListener('reset', () => {
    setSelectedLabel(null);
    setDragState(false);
  });

  setSubmitState(null);
}

document.addEventListener('DOMContentLoaded', () => {
  initThemeToggle();
  initToasts();
  initInvoiceUpload();
  initInvoiceDetailPage();
  initActivityFeed();
});
