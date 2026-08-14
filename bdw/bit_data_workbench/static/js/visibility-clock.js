export const UI_CLOCK_INTERVAL_MS = 1000;

export function createVisibilityAwareClock(callback, intervalMs = UI_CLOCK_INTERVAL_MS) {
  let enabled = false;
  let handle = null;

  function stop() {
    if (handle !== null) {
      window.clearInterval(handle);
      handle = null;
    }
  }

  function sync() {
    if (!enabled || document.visibilityState === "hidden") {
      stop();
      return;
    }
    callback();
    if (handle === null) {
      handle = window.setInterval(callback, Math.max(UI_CLOCK_INTERVAL_MS, intervalMs));
    }
  }

  function setEnabled(nextEnabled) {
    enabled = Boolean(nextEnabled);
    sync();
  }

  document.addEventListener("visibilitychange", sync);
  return Object.freeze({
    setEnabled,
    refresh: () => {
      if (document.visibilityState !== "hidden") {
        callback();
      }
    },
    dispose: () => {
      enabled = false;
      stop();
      document.removeEventListener("visibilitychange", sync);
    },
  });
}
