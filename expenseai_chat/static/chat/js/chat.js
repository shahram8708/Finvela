const CSRF_HEADER = 'X-CSRFToken';

const findCsrfToken = () => {
  const input = document.querySelector('input[name="csrf_token"]');
  return input ? input.value : '';
};

const formatTimestamp = (value) => {
  if (!value) return '';
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) {
    return String(value);
  }
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: 'medium',
    timeStyle: 'short',
  }).format(date);
};

const parseInitialMessages = () => {
  const el = document.getElementById('initial-chat-data');
  if (!el) return [];
  const raw = el.textContent || '[]';
  try {
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed)) {
      return parsed;
    }
  } catch (err) {
    console.error('Failed to parse initial chat payload', err);
  }
  return [];
};

const scrollToBottom = (container) => {
  requestAnimationFrame(() => {
    container.scrollTop = container.scrollHeight;
  });
};

const shouldStickToBottom = (container) => {
  const threshold = 96;
  const distanceFromBottom = container.scrollHeight - container.scrollTop - container.clientHeight;
  return distanceFromBottom <= threshold;
};

const buildBubble = (message, mode) => {
  const bubble = document.createElement('div');
  bubble.className = `message-bubble ${message.sender_is_self ? 'sent' : 'received'}`;
  bubble.dataset.messageId = String(message.id);

  if (mode === 'group' && !message.sender_is_self && message.sender_name) {
    const author = document.createElement('div');
    author.className = 'message-author';
    author.textContent = message.sender_name;
    bubble.appendChild(author);
  }

  const text = document.createElement('div');
  text.className = 'message-text';
  text.textContent = message.message;
  bubble.appendChild(text);

  const meta = document.createElement('div');
  meta.className = 'message-meta';
  meta.textContent = formatTimestamp(message.timestamp);
  bubble.appendChild(meta);

  return bubble;
};

const bootstrapChat = (root) => {
  const mode = root.dataset.mode || 'direct';
  const fetchUrl = root.dataset.fetchUrl;
  const sendUrl = root.dataset.sendUrl;
  const receiverId = root.dataset.receiverId || null;
  const pollInterval = Number.parseInt(root.dataset.pollInterval || '4000', 10);

  if (!fetchUrl || !sendUrl) {
    console.warn('Chat root missing data attributes');
    return;
  }

  const chatBox = root.querySelector('[data-chat-box]');
  const form = root.querySelector('[data-chat-form]');
  const input = root.querySelector('[data-chat-input]');
  const feedback = root.querySelector('[data-chat-feedback]');

  if (!chatBox || !form || !input) {
    console.warn('Chat template missing expected elements');
    return;
  }

  const csrfToken = findCsrfToken();
  const initialMessages = parseInitialMessages();
  const messagesById = new Map();
  const state = {
    messages: [],
    lastMessageId: null,
  };

  const setFeedback = (message, variant = 'danger') => {
    if (!feedback) return;
    if (!message) {
      feedback.textContent = '';
      feedback.setAttribute('hidden', 'true');
      feedback.classList.remove('text-danger', 'text-success');
      return;
    }
    feedback.textContent = message;
    feedback.classList.remove('text-danger', 'text-success');
    feedback.classList.add(variant === 'success' ? 'text-success' : 'text-danger');
    feedback.removeAttribute('hidden');
  };

  const renderMessages = () => {
    const autoscroll = shouldStickToBottom(chatBox);
    chatBox.innerHTML = '';
    state.messages.forEach((message) => {
      chatBox.appendChild(buildBubble(message, mode));
      state.lastMessageId = message.id;
    });
    if (autoscroll || state.messages.length <= 4) {
      scrollToBottom(chatBox);
    }
  };

  const upsertMessages = (items) => {
    let mutated = false;
    items.forEach((item) => {
      if (!item || typeof item.id !== 'number') {
        return;
      }
      if (!messagesById.has(item.id)) {
        messagesById.set(item.id, item);
        state.messages.push(item);
        mutated = true;
      }
    });
    if (!mutated) return;
    state.messages.sort((a, b) => {
      if (a.id === b.id) return 0;
      return a.id < b.id ? -1 : 1;
    });
    renderMessages();
  };

  upsertMessages(initialMessages);

  let pollingHandle = null;

  const buildFetchUrl = () => {
    const url = new URL(fetchUrl, window.location.origin);
    if (state.lastMessageId) {
      url.searchParams.set('after_id', String(state.lastMessageId));
    }
    return url.toString();
  };

  const poll = async () => {
    try {
      const response = await fetch(buildFetchUrl(), {
        headers: { Accept: 'application/json' },
        credentials: 'same-origin',
      });
      if (!response.ok) {
        throw new Error(`Failed to fetch messages (${response.status})`);
      }
      const payload = await response.json();
      if (payload?.messages) {
        upsertMessages(payload.messages);
      }
    } catch (err) {
      console.error('Chat poll failed', err);
    }
  };

  const startPolling = () => {
    stopPolling();
    poll();
    const interval = Number.isFinite(pollInterval) && pollInterval >= 2000 ? pollInterval : 3000;
    pollingHandle = window.setInterval(poll, interval);
  };

  const stopPolling = () => {
    if (pollingHandle) {
      window.clearInterval(pollingHandle);
      pollingHandle = null;
    }
  };

  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    const message = input.value.trim();
    if (!message) {
      setFeedback('Message cannot be empty.');
      return;
    }

    const body = { message };
    if (mode === 'direct') {
      if (!receiverId) {
        setFeedback('Unable to determine who you are messaging.');
        return;
      }
      body.receiver_id = Number.parseInt(receiverId, 10);
    }

    setFeedback('');
    input.value = '';

    try {
      const response = await fetch(sendUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Accept: 'application/json',
          [CSRF_HEADER]: csrfToken,
        },
        credentials: 'same-origin',
        body: JSON.stringify(body),
      });
      const payload = await response.json();
      if (!response.ok || payload?.status !== 'ok') {
        const messageText = payload?.message || 'Unable to send your message. Try again.';
        throw new Error(messageText);
      }
      if (payload.message) {
        upsertMessages([payload.message]);
        scrollToBottom(chatBox);
      }
      setFeedback('Message sent', 'success');
      window.setTimeout(() => setFeedback(''), 1500);
    } catch (err) {
      console.error('Failed to send chat message', err);
      setFeedback(err instanceof Error ? err.message : 'Unable to send your message.');
    }
  });

  document.addEventListener('visibilitychange', () => {
    if (document.hidden) {
      stopPolling();
    } else {
      startPolling();
    }
  });

  startPolling();
};

const roots = document.querySelectorAll('[data-chat-root]');
if (roots.length) {
  roots.forEach((root) => bootstrapChat(root));
}
