const sessionListEl = document.getElementById('chatSessionList');
const messagesEl = document.getElementById('chatMessages');
const uploadRegionEl = document.getElementById('chatUploadRegion');
const uploadProgressEl = document.getElementById('chatUploadProgress');
const uploadSummaryEl = document.getElementById('chatUploadSummary');
const uploadFileNameEl = document.getElementById('chatUploadedFileName');
const uploadFileMetaEl = document.getElementById('chatUploadedFileMeta');
const uploadBadgeEl = document.getElementById('chatUploadBadge');
const uploadInputEl = document.getElementById('chatFileInput');
const uploadStatusBadgeEl = document.getElementById('chatStatusBadge');
const sessionTitleEl = document.getElementById('chatSessionTitle');
const sessionMetaEl = document.getElementById('chatSessionMeta');
const sessionModelNameEl = document.getElementById('chatModelName');
const messageFormEl = document.getElementById('chatMessageForm');
const messageInputEl = document.getElementById('chatMessageInput');
const messageErrorEl = document.getElementById('chatMessageError');
const sendButtonEl = document.getElementById('chatSendButton');
const newChatButtonEl = document.getElementById('newChatButton');

const getJson = (id) => {
  const el = document.getElementById(id);
  if (!el) return null;
  try {
    return JSON.parse(el.textContent || 'null');
  } catch (err) {
    console.error('Failed to parse JSON payload', id, err);
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

const csrfToken = document.getElementById('chatCsrfToken')?.value || '';

const state = {
  sessions: getJson('chatInitialSessions') || [],
  activeSession: getJson('chatActiveSession'),
  messages: getJson('chatInitialMessages') || [],
  historyLimit: getJson('chatHistoryLimit') || 5,
  busy: false,
};

const sanitizeMarkdown = (markdown) => {
  const parser = window.marked || null;
  const purifier = window.DOMPurify || null;
  const html = parser ? parser.parse(markdown ?? '') : String(markdown ?? '');
  return purifier ? purifier.sanitize(html) : html;
};

const scrollMessagesToBottom = () => {
  requestAnimationFrame(() => {
    messagesEl.scrollTop = messagesEl.scrollHeight;
  });
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

const renderSessions = () => {
  if (!sessionListEl) return;
  sessionListEl.innerHTML = '';
  if (!state.sessions.length) {
    const empty = document.createElement('div');
    empty.className = 'list-group-item text-center text-muted py-5';
    empty.innerHTML = '<i class="bi bi-chat-square-dots mb-2 d-block fs-4"></i>No chats yet. Start by uploading an invoice.';
    sessionListEl.appendChild(empty);
    return;
  }
  state.sessions.forEach((session) => {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'list-group-item list-group-item-action chat-session-item';
    if (state.activeSession && session.id === state.activeSession.id) {
      button.classList.add('active');
    }
    button.dataset.sessionId = String(session.id);
    const title = document.createElement('div');
    title.className = 'session-title text-truncate';
    title.textContent = session.title || 'Untitled chat';
    const meta = document.createElement('div');
    meta.className = 'session-meta text-truncate';
    const fileLabel = session.file_name ? session.file_name : 'No file yet';
    meta.textContent = `${fileLabel} · ${formatRelative(session.updated_at)}`;
    button.append(title, meta);
    sessionListEl.appendChild(button);
  });
};

const renderMessages = () => {
  if (!messagesEl) return;
  messagesEl.innerHTML = '';
  if (!state.messages.length) {
    const empty = document.createElement('div');
    empty.className = 'text-center text-muted py-5';
    empty.innerHTML = '<i class="bi bi-arrow-up-circle fs-3 mb-2 d-block"></i>Upload an invoice to begin the conversation.';
    messagesEl.appendChild(empty);
    return;
  }
  state.messages.forEach((message) => {
    const wrapper = document.createElement('div');
    wrapper.className = `chat-message ${message.role === 'user' ? 'chat-message-user' : 'chat-message-assistant'}`;

    const header = document.createElement('div');
    header.className = 'd-flex justify-content-between align-items-center gap-3 mb-2';
    const author = document.createElement('span');
    author.className = 'fw-semibold';
    author.textContent = message.role === 'user' ? 'You' : 'Finvela Copilot';
    const timestamp = document.createElement('span');
    timestamp.className = 'chat-timestamp';
    timestamp.textContent = formatTimestamp(message.created_at);
    header.append(author, timestamp);

    const body = document.createElement('div');
    body.className = 'chat-message-content markdown-display';
    body.innerHTML = sanitizeMarkdown(message.content);

    wrapper.append(header, body);
    messagesEl.appendChild(wrapper);
  });
  scrollMessagesToBottom();
};

const setUploadState = () => {
  if (!state.activeSession) return;
  const hasFile = Boolean(state.activeSession.has_file);
  const fileName = state.activeSession.file_name;
  if (hasFile) {
    uploadRegionEl?.classList.remove('drag-active');
    uploadProgressEl?.setAttribute('hidden', 'true');
    uploadSummaryEl?.classList.remove('d-none');
    uploadRegionEl?.classList.add('uploaded');
    if (uploadFileNameEl) uploadFileNameEl.textContent = fileName || 'Uploaded invoice';
    if (uploadFileMetaEl) uploadFileMetaEl.textContent = `Updated ${formatTimestamp(state.activeSession.updated_at)}`;
    if (uploadBadgeEl) uploadBadgeEl.innerHTML = '<i class="bi bi-check-circle me-1"></i>Ready';
    if (uploadStatusBadgeEl) uploadStatusBadgeEl.innerHTML = '<i class="bi bi-robot me-1"></i>Analysis ready';
    uploadInputEl?.setAttribute('disabled', 'true');
    messageInputEl?.removeAttribute('disabled');
    sendButtonEl?.removeAttribute('disabled');
  } else {
    uploadSummaryEl?.classList.add('d-none');
    uploadProgressEl?.setAttribute('hidden', 'true');
    uploadRegionEl?.classList.remove('uploaded');
    if (uploadStatusBadgeEl) uploadStatusBadgeEl.innerHTML = '<i class="bi bi-cloud-arrow-up me-1"></i>Waiting for file';
    uploadInputEl?.removeAttribute('disabled');
    messageInputEl?.setAttribute('disabled', 'true');
    sendButtonEl?.setAttribute('disabled', 'true');
  }
};

const renderActiveSession = () => {
  if (!state.activeSession) {
    if (sessionTitleEl) sessionTitleEl.textContent = 'Invoice workspace';
    if (sessionMetaEl) sessionMetaEl.textContent = 'Create a chat to begin.';
    if (sessionModelNameEl) sessionModelNameEl.textContent = '';
    setUploadState();
    renderMessages();
    return;
  }
  if (sessionTitleEl) sessionTitleEl.textContent = state.activeSession.title || 'Invoice workspace';
  const meta = [];
  if (state.activeSession.file_name) {
    meta.push(state.activeSession.file_name);
  } else {
    meta.push('No file uploaded yet');
  }
  meta.push(`Updated ${formatRelative(state.activeSession.updated_at)}`);
  if (sessionMetaEl) sessionMetaEl.textContent = meta.join(' · ');
  if (sessionModelNameEl) {
    sessionModelNameEl.textContent = state.activeSession.model_name || sessionModelNameEl.textContent;
  }
  setUploadState();
  renderMessages();
};

const handleSessionClick = async (event) => {
  const button = event.target.closest('.chat-session-item');
  if (!button) return;
  const sessionId = Number.parseInt(button.dataset.sessionId || '', 10);
  if (!Number.isFinite(sessionId) || (state.activeSession && state.activeSession.id === sessionId)) {
    return;
  }
  try {
    const response = await fetch(`/ai-chat/sessions/${sessionId}/messages`);
    if (!response.ok) {
      throw new Error('Failed to load chat history');
    }
    const payload = await response.json();
    state.activeSession = state.sessions.find((item) => item.id === sessionId) || state.activeSession;
    state.messages = payload.messages || [];
    renderSessions();
    renderActiveSession();
  } catch (error) {
    console.error(error);
  }
};

const handleNewChat = async () => {
  if (!csrfToken) {
    console.warn('Missing CSRF token');
  }
  try {
    const response = await fetch('/ai-chat/sessions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-CSRFToken': csrfToken,
      },
      body: JSON.stringify({}),
    });
    if (!response.ok) {
      throw new Error('Failed to create chat');
    }
    const payload = await response.json();
    const session = payload.session;
    upsertSession(session);
    state.activeSession = session;
    state.messages = [];
    renderSessions();
    renderActiveSession();
  } catch (error) {
    console.error(error);
  }
};

const setBusy = (busy) => {
  state.busy = busy;
  if (busy) {
    messageFormEl?.classList.add('pe-none', 'opacity-75');
  } else {
    messageFormEl?.classList.remove('pe-none', 'opacity-75');
  }
};

const handleFileSelected = async (file) => {
  if (!file || !state.activeSession) return;
  if (state.activeSession.has_file) {
    return;
  }
  const formData = new FormData();
  formData.append('file', file);
  if (csrfToken) {
    formData.append('csrf_token', csrfToken);
  }
  uploadProgressEl?.removeAttribute('hidden');
  if (uploadFileNameEl) uploadFileNameEl.textContent = file.name;
  if (uploadStatusBadgeEl) uploadStatusBadgeEl.innerHTML = '<i class="bi bi-arrow-repeat me-1"></i>Analysing…';
  try {
    const response = await fetch(`/ai-chat/sessions/${state.activeSession.id}/upload`, {
      method: 'POST',
      headers: csrfToken ? { 'X-CSRFToken': csrfToken } : {},
      body: formData,
    });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.error || 'Upload failed');
    }
    const { session, messages } = payload;
    upsertSession(session);
    state.activeSession = session;
    state.messages = (state.messages || []).concat(messages || []);
    renderSessions();
    renderActiveSession();
  } catch (error) {
    console.error(error);
    if (uploadStatusBadgeEl) uploadStatusBadgeEl.innerHTML = '<i class="bi bi-exclamation-triangle me-1"></i>Error';
    if (messageErrorEl) {
      messageErrorEl.textContent = error.message || 'Unable to process the invoice.';
      messageErrorEl.style.display = 'block';
    }
  } finally {
    uploadProgressEl?.setAttribute('hidden', 'true');
    uploadInputEl.value = '';
  }
};

const handleFileInputChange = (event) => {
  const [file] = event.target.files || [];
  if (file) {
    handleFileSelected(file);
  }
};

const handleDrop = (event) => {
  event.preventDefault();
  uploadRegionEl?.classList.remove('drag-active');
  const files = event.dataTransfer?.files;
  if (files && files[0]) {
    handleFileSelected(files[0]);
  }
};

const handleDragOver = (event) => {
  event.preventDefault();
  if (state.activeSession?.has_file) return;
  uploadRegionEl?.classList.add('drag-active');
};

const handleDragLeave = (event) => {
  event.preventDefault();
  uploadRegionEl?.classList.remove('drag-active');
};

const handleSendMessage = async (event) => {
  event.preventDefault();
  if (!state.activeSession || !messageInputEl) return;
  if (!state.activeSession.has_file) {
    messageErrorEl.textContent = 'Upload an invoice first.';
    messageErrorEl.style.display = 'block';
    return;
  }
  const text = messageInputEl.value.trim();
  if (!text) {
    messageErrorEl.textContent = 'Message cannot be empty.';
    messageErrorEl.style.display = 'block';
    return;
  }
  messageErrorEl.style.display = 'none';
  setBusy(true);
  try {
    const response = await fetch(`/ai-chat/sessions/${state.activeSession.id}/messages`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-CSRFToken': csrfToken,
      },
      body: JSON.stringify({ message: text }),
    });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.error || 'Failed to send message');
    }
    messageInputEl.value = '';
    const { messages, session } = payload;
    state.messages = (state.messages || []).concat(messages || []);
    upsertSession(session);
    state.activeSession = session;
    renderSessions();
    renderActiveSession();
  } catch (error) {
    console.error(error);
    messageErrorEl.textContent = error.message || 'Message failed. Try again.';
    messageErrorEl.style.display = 'block';
  } finally {
    setBusy(false);
  }
};

const initEventListeners = () => {
  sessionListEl?.addEventListener('click', handleSessionClick);
  newChatButtonEl?.addEventListener('click', handleNewChat);
  uploadInputEl?.addEventListener('change', handleFileInputChange);
  uploadRegionEl?.addEventListener('drop', handleDrop);
  uploadRegionEl?.addEventListener('dragover', handleDragOver);
  uploadRegionEl?.addEventListener('dragleave', handleDragLeave);
  uploadRegionEl?.addEventListener('dragend', handleDragLeave);
  messageFormEl?.addEventListener('submit', handleSendMessage);
};

const bootstrap = () => {
  if (!messagesEl) return;
  if (window.marked) {
    window.marked.setOptions({
      gfm: true,
      breaks: true,
      headerIds: false,
    });
  }
  if (messageErrorEl) {
    messageErrorEl.style.display = 'none';
  }
  renderSessions();
  renderActiveSession();
  initEventListeners();
};

bootstrap();
