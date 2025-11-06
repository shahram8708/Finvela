const invoiceListEl = document.getElementById('contextInvoiceList');
const sessionListEl = document.getElementById('contextSessionList');
const messagesEl = document.getElementById('contextMessages');
const selectedCountEl = document.getElementById('contextSelectedCount');
const createButtonEl = document.getElementById('contextCreateButton');
const createErrorEl = document.getElementById('contextCreateError');
const sessionNameEl = document.getElementById('contextSessionName');
const refreshInvoicesEl = document.getElementById('contextRefreshInvoices');
const sessionTitleEl = document.getElementById('contextSessionTitle');
const sessionMetaEl = document.getElementById('contextSessionMeta');
const sessionSourcesEl = document.getElementById('contextSessionSources');
const chatFormEl = document.getElementById('contextChatForm');
const chatInputEl = document.getElementById('contextChatInput');
const chatErrorEl = document.getElementById('contextChatError');
const sendButtonEl = document.getElementById('contextSendButton');

const getJson = (id) => {
  const el = document.getElementById(id);
  if (!el) return null;
  try {
    return JSON.parse(el.textContent || 'null');
  } catch (error) {
    console.error('Failed to parse JSON payload', id, error);
    return null;
  }
};

const toDate = (value) => {
  if (!value) return null;
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
};

const formatTimestamp = (value) => {
  const date = toDate(value);
  if (!date) return '';
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: 'medium',
    timeStyle: 'short',
  }).format(date);
};

const formatRelative = (value) => {
  const date = toDate(value);
  if (!date) return '';
  const diffMs = date.getTime() - Date.now();
  const minutes = Math.round(diffMs / 60000);
  const rtf = new Intl.RelativeTimeFormat(undefined, { numeric: 'auto' });
  if (Math.abs(minutes) < 60) {
    return rtf.format(minutes, 'minute');
  }
  const hours = Math.round(minutes / 60);
  if (Math.abs(hours) < 24) {
    return rtf.format(hours, 'hour');
  }
  const days = Math.round(hours / 24);
  return rtf.format(days, 'day');
};

const sanitizeMarkdown = (markdown) => {
  const parser = window.marked || null;
  const purifier = window.DOMPurify || null;
  const html = parser ? parser.parse(markdown ?? '') : String(markdown ?? '');
  return purifier ? purifier.sanitize(html) : html;
};

const csrfToken = document.getElementById('contextChatCsrfToken')?.value || '';

const state = {
  invoiceSessions: getJson('contextChatInvoiceSessions') || [],
  sessions: getJson('contextChatSessions') || [],
  activeSession: getJson('contextChatActiveSession'),
  messages: getJson('contextChatActiveMessages') || [],
  historyLimit: getJson('contextChatHistoryLimit') || 5,
  selectedInvoiceIds: new Set(),
  busy: false,
};

const upsertSession = (session) => {
  if (!session) return;
  const index = state.sessions.findIndex((item) => item.id === session.id);
  if (index >= 0) {
    state.sessions[index] = { ...state.sessions[index], ...session };
  } else {
    state.sessions.push(session);
  }
  state.sessions.sort((a, b) => {
    const aTime = toDate(a.updated_at)?.getTime() ?? 0;
    const bTime = toDate(b.updated_at)?.getTime() ?? 0;
    return bTime - aTime;
  });
};

const renderSelectedInfo = () => {
  if (selectedCountEl) {
    selectedCountEl.textContent = String(state.selectedInvoiceIds.size);
  }
  if (createButtonEl) {
    createButtonEl.disabled = state.selectedInvoiceIds.size === 0;
  }
  if (createErrorEl) {
    createErrorEl.hidden = true;
  }
};

const getInvoiceById = (id) => state.invoiceSessions.find((item) => item.id === id);

const renderInvoiceList = () => {
  if (!invoiceListEl) return;
  invoiceListEl.innerHTML = '';
  if (!state.invoiceSessions.length) {
    const empty = document.createElement('div');
    empty.className = 'context-empty';
    empty.innerHTML = '<i class="bi bi-collection"></i><p class="mb-0">No invoice chats with Finvela replies yet.</p>';
    invoiceListEl.appendChild(empty);
    renderSelectedInfo();
    return;
  }

  state.invoiceSessions.forEach((session) => {
    const wrapper = document.createElement('div');
    wrapper.className = 'form-check form-switch border rounded-4 px-3 py-2 mb-2';

    const input = document.createElement('input');
    input.type = 'checkbox';
    input.className = 'form-check-input';
    input.id = `invoice-source-${session.id}`;
    input.value = String(session.id);
    input.checked = state.selectedInvoiceIds.has(session.id);

    const label = document.createElement('label');
    label.className = 'form-check-label w-100';
    label.setAttribute('for', input.id);

    const title = document.createElement('div');
    title.className = 'fw-semibold text-truncate';
    title.textContent = session.title || `Invoice chat #${session.id}`;

    const meta = document.createElement('div');
    meta.className = 'text-muted small text-truncate';
    const fileLabel = session.file_name ? session.file_name : 'No file name';
    meta.textContent = `${fileLabel} · ${formatRelative(session.updated_at)}`;

    const preview = document.createElement('div');
    preview.className = 'small text-muted mt-1';
    preview.textContent = session.preview ? `${session.preview}…` : 'No Finvela response captured yet.';

    label.append(title, meta, preview);
    wrapper.append(input, label);
    invoiceListEl.appendChild(wrapper);
  });

  renderSelectedInfo();
};

const renderSessionList = () => {
  if (!sessionListEl) return;
  sessionListEl.innerHTML = '';
  if (!state.sessions.length) {
    const empty = document.createElement('div');
    empty.className = 'context-empty';
    empty.innerHTML = '<i class="bi bi-chat-square"></i><p class="mb-0">Create a session to see it here.</p>';
    sessionListEl.appendChild(empty);
    return;
  }

  state.sessions.forEach((session) => {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'btn text-start border rounded-4 w-100 mb-2 p-3';
    button.dataset.sessionId = String(session.id);
    if (state.activeSession && session.id === state.activeSession.id) {
      button.classList.add('btn-primary');
      button.classList.add('text-white');
    } else {
      button.classList.add('btn-outline-primary');
    }

    const title = document.createElement('div');
    title.className = 'fw-semibold text-truncate';
    title.textContent = session.title || `Context chat #${session.id}`;

    const meta = document.createElement('div');
    meta.className = 'small text-muted text-truncate';
    const sourceCount = session.source_count ?? (session.source_session_ids?.length ?? 0);
    meta.textContent = `${sourceCount} source${sourceCount === 1 ? '' : 's'} · ${formatRelative(session.updated_at)}`;

    button.append(title, meta);
    sessionListEl.appendChild(button);
  });
};

const scrollMessagesToBottom = () => {
  if (!messagesEl) return;
  requestAnimationFrame(() => {
    messagesEl.scrollTop = messagesEl.scrollHeight;
  });
};

const renderMessages = () => {
  if (!messagesEl) return;
  messagesEl.innerHTML = '';
  if (!state.messages.length) {
    const empty = document.createElement('div');
    empty.className = 'context-empty';
    empty.innerHTML = '<i class="bi bi-stars"></i><p class="mb-0">Your conversation will appear here.</p>';
    messagesEl.appendChild(empty);
    return;
  }

  state.messages.forEach((message) => {
    const wrapper = document.createElement('div');
    wrapper.className = `context-message ${message.role === 'user' ? 'context-message-user' : 'context-message-assistant'}`;

    const header = document.createElement('div');
    header.className = 'd-flex justify-content-between context-message-header';
    const author = document.createElement('span');
    author.textContent = message.role === 'user' ? 'You' : 'Finvela Copilot';
    const timestamp = document.createElement('span');
    timestamp.textContent = formatTimestamp(message.created_at);
    header.append(author, timestamp);

    const body = document.createElement('div');
    body.className = 'context-message-body markdown-display';
    body.innerHTML = sanitizeMarkdown(message.content);

    wrapper.append(header, body);
    messagesEl.appendChild(wrapper);
  });

  scrollMessagesToBottom();
};

const renderSessionSources = () => {
  if (!sessionSourcesEl) return;
  sessionSourcesEl.innerHTML = '';
  if (!state.activeSession) {
    return;
  }
  const sourceIds = state.activeSession.source_session_ids || [];
  if (!sourceIds.length) {
    const badge = document.createElement('span');
    badge.className = 'context-badge';
    badge.textContent = 'No context sources attached';
    sessionSourcesEl.appendChild(badge);
    return;
  }
  sourceIds.forEach((id) => {
    const invoice = getInvoiceById(id);
    const badge = document.createElement('span');
    badge.className = 'context-badge';
    badge.innerHTML = `<i class="bi bi-link-45deg"></i>${invoice ? invoice.title : `Session #${id}`}`;
    sessionSourcesEl.appendChild(badge);
  });
};

const renderActiveSession = () => {
  if (!sessionTitleEl || !sessionMetaEl) return;
  if (!state.activeSession) {
    sessionTitleEl.textContent = 'Start a Finvela-powered chat';
    sessionMetaEl.textContent = 'Choose invoice chats to unlock insights.';
    if (chatInputEl) chatInputEl.disabled = true;
    if (sendButtonEl) sendButtonEl.disabled = true;
    renderSessionSources();
    renderMessages();
    return;
  }

  sessionTitleEl.textContent = state.activeSession.title || `Context chat #${state.activeSession.id}`;
  const sourceCount = state.activeSession.source_count ?? (state.activeSession.source_session_ids?.length ?? 0);
  sessionMetaEl.textContent = `${sourceCount} source${sourceCount === 1 ? '' : 's'} · Updated ${formatRelative(state.activeSession.updated_at)}`;
  if (chatInputEl) chatInputEl.disabled = false;
  if (sendButtonEl) sendButtonEl.disabled = false;
  renderSessionSources();
  renderMessages();
};

const setBusy = (busy) => {
  state.busy = busy;
  if (chatFormEl) {
    chatFormEl.classList.toggle('pe-none', busy);
    chatFormEl.classList.toggle('opacity-75', busy);
  }
};

const syncSelectedFromInputs = (target) => {
  const id = Number.parseInt(target.value || '', 10);
  if (!Number.isFinite(id)) return;
  if (target.checked) {
    state.selectedInvoiceIds.add(id);
  } else {
    state.selectedInvoiceIds.delete(id);
  }
  renderSelectedInfo();
};

const handleInvoiceChange = (event) => {
  const input = event.target.closest('input[type="checkbox"]');
  if (!input) return;
  syncSelectedFromInputs(input);
};

const handleSessionClick = async (event) => {
  const button = event.target.closest('button[data-session-id]');
  if (!button) return;
  const sessionId = Number.parseInt(button.dataset.sessionId || '', 10);
  if (!Number.isFinite(sessionId)) return;
  if (state.activeSession && state.activeSession.id === sessionId) return;
  try {
    const response = await fetch(`/ai-chat/contextual/sessions/${sessionId}/messages`);
    if (!response.ok) {
      throw new Error('Failed to load chat history');
    }
    const payload = await response.json();
    state.activeSession = state.sessions.find((item) => item.id === sessionId) || state.activeSession;
    state.messages = payload.messages || [];
    renderSessionList();
    renderActiveSession();
  } catch (error) {
    console.error(error);
  }
};

const handleCreateSession = async () => {
  if (!createButtonEl || createButtonEl.disabled) return;
  const sourceIds = Array.from(state.selectedInvoiceIds.values());
  if (!sourceIds.length) {
    if (createErrorEl) {
      createErrorEl.textContent = 'Pick at least one invoice chat.';
      createErrorEl.hidden = false;
    }
    return;
  }
  const body = {
    source_session_ids: sourceIds,
  };
  const name = sessionNameEl?.value?.trim();
  if (name) {
    body.title = name;
  }
  try {
    const response = await fetch('/ai-chat/contextual/sessions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...(csrfToken ? { 'X-CSRFToken': csrfToken } : {}),
      },
      body: JSON.stringify(body),
    });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.error || 'Unable to create session');
    }
    const session = payload.session;
    upsertSession(session);
    state.activeSession = session;
    state.messages = [];
    sessionNameEl.value = '';
    state.selectedInvoiceIds.clear();
    renderInvoiceList();
    renderSessionList();
    renderActiveSession();
  } catch (error) {
    console.error(error);
    if (createErrorEl) {
      createErrorEl.textContent = error.message || 'Unable to create session.';
      createErrorEl.hidden = false;
    }
  }
};

const handleSendMessage = async (event) => {
  event.preventDefault();
  if (!state.activeSession || !chatInputEl) return;
  const text = chatInputEl.value.trim();
  if (!text) {
    if (chatErrorEl) {
      chatErrorEl.textContent = 'Message cannot be empty.';
      chatErrorEl.style.display = 'block';
    }
    return;
  }
  if (chatErrorEl) {
    chatErrorEl.style.display = 'none';
  }
  setBusy(true);
  try {
    const response = await fetch(`/ai-chat/contextual/sessions/${state.activeSession.id}/messages`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...(csrfToken ? { 'X-CSRFToken': csrfToken } : {}),
      },
      body: JSON.stringify({ message: text }),
    });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.error || 'Failed to send message');
    }
    chatInputEl.value = '';
    const { messages, session } = payload;
    state.messages = (state.messages || []).concat(messages || []);
    upsertSession(session);
    state.activeSession = session;
    renderSessionList();
    renderActiveSession();
  } catch (error) {
    console.error(error);
    if (chatErrorEl) {
      chatErrorEl.textContent = error.message || 'Message failed. Try again.';
      chatErrorEl.style.display = 'block';
    }
  } finally {
    setBusy(false);
  }
};

const handleRefreshInvoices = async () => {
  try {
    const response = await fetch('/ai-chat/contextual/invoice-sources');
    if (!response.ok) {
      throw new Error('Failed to refresh invoice chats');
    }
    const payload = await response.json();
    const sources = payload.sources || [];
    const retained = new Set();
    state.selectedInvoiceIds.forEach((id) => {
      if (sources.some((item) => item.id === id)) {
        retained.add(id);
      }
    });
    state.invoiceSessions = sources;
    state.selectedInvoiceIds = retained;
    renderInvoiceList();
  } catch (error) {
    console.error(error);
  }
};

const initSelections = () => {
  if (state.activeSession && Array.isArray(state.activeSession.source_session_ids)) {
    state.activeSession.source_session_ids.forEach((id) => {
      if (getInvoiceById(id)) {
        state.selectedInvoiceIds.add(id);
      }
    });
  }
};

const initEventListeners = () => {
  invoiceListEl?.addEventListener('change', handleInvoiceChange);
  sessionListEl?.addEventListener('click', handleSessionClick);
  createButtonEl?.addEventListener('click', handleCreateSession);
  chatFormEl?.addEventListener('submit', handleSendMessage);
  refreshInvoicesEl?.addEventListener('click', handleRefreshInvoices);
};

const init = () => {
  initSelections();
  renderInvoiceList();
  renderSessionList();
  renderActiveSession();
  initEventListeners();
};

init();
