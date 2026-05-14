const STATUS_COPY = {
  connected: {
    label: "Online",
    title: "Realtime connection is online.",
  },
  connecting: {
    label: "Connecting...",
    title: "Realtime connection is not established yet.",
  },
  disconnected: {
    label: "Offline",
    title: "Realtime connection is offline. The browser is reconnecting.",
  },
};

function statusCopy(status) {
  return STATUS_COPY[status] || STATUS_COPY.disconnected;
}

export function createRealtimeConnectionStatusController({ getIndicator }) {
  let status = "connected";

  function render() {
    const indicator = getIndicator?.();
    if (!(indicator instanceof HTMLElement)) {
      return;
    }

    const copy = statusCopy(status);
    indicator.dataset.connectionStatus = status;
    indicator.title = copy.title;
    indicator.setAttribute("aria-label", copy.title);
    indicator.hidden = status === "connected";

    const label = indicator.querySelector("[data-sse-connection-status-label]");
    if (label instanceof HTMLElement) {
      label.textContent = copy.label;
    }
  }

  function setStatus(nextStatus) {
    if (status === nextStatus) {
      render();
      return;
    }
    status = nextStatus;
    render();
  }

  return {
    render,
    setConnected() {
      setStatus("connected");
    },
    setConnecting() {
      setStatus("connecting");
    },
    setDisconnected() {
      setStatus("disconnected");
    },
  };
}
