// Simple helper for consuming Server-Sent Events with lifecycle callbacks.
export function connectSSE(url, { onEvent, onStatusChange } = {}) {
  const source = new EventSource(url);

  source.addEventListener('open', () => {
    onStatusChange?.('open');
  });

  source.addEventListener('error', (event) => {
    onStatusChange?.('error', event);
  });

  source.addEventListener('invoice', (event) => {
    try {
      const payload = JSON.parse(event.data);
      onEvent?.(payload);
    } catch (err) {
      console.error('Failed to parse SSE payload', err); // eslint-disable-line no-console
    }
  });

  return source;
}
