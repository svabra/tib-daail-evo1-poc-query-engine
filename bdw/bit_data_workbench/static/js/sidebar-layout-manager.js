export function createSidebarLayoutManager({
  readSidebarCollapsed,
  sidebarMaxWidth = 720,
  sidebarMinWidth = 360,
  sidebarResizeStep = 32,
  sidebarToggles,
}) {
  const sidebarResizeState = {
    active: false,
    pointerId: null,
    startX: 0,
    startWidth: 0,
  };

  function sidebarRoot() {
    return document.querySelector("[data-sidebar]");
  }

  function sidebarResizer() {
    return document.querySelector("[data-sidebar-resizer]");
  }

  function clampSidebarWidth(width) {
    return Math.min(sidebarMaxWidth, Math.max(sidebarMinWidth, Number(width) || sidebarMinWidth));
  }

  function currentSidebarWidth() {
    return sidebarRoot()?.getBoundingClientRect().width ?? sidebarMinWidth;
  }

  function resolveSidebarWidthValue(width) {
    if (Number.isFinite(width)) {
      return clampSidebarWidth(width);
    }

    const numericWidth = Number(width);
    if (Number.isFinite(numericWidth)) {
      return clampSidebarWidth(numericWidth);
    }

    const inlineWidth = Number.parseFloat(
      document.documentElement.style.getPropertyValue("--sidebar-width") || ""
    );
    if (Number.isFinite(inlineWidth)) {
      return clampSidebarWidth(inlineWidth);
    }

    return clampSidebarWidth(currentSidebarWidth());
  }

  function syncSidebarResizerAria(width) {
    const resizer = sidebarResizer();
    if (!resizer) {
      return;
    }

    const nextWidth = Math.round(resolveSidebarWidthValue(width));
    resizer.setAttribute("aria-valuemin", String(sidebarMinWidth));
    resizer.setAttribute("aria-valuemax", String(sidebarMaxWidth));
    resizer.setAttribute("aria-valuenow", String(nextWidth));
    resizer.setAttribute("aria-valuetext", `${nextWidth} pixels`);
  }

  function applySidebarWidth(width) {
    const nextWidth = clampSidebarWidth(width);
    document.documentElement.style.setProperty("--sidebar-width", `${nextWidth}px`);
    syncSidebarResizerAria(nextWidth);
    return nextWidth;
  }

  function finishSidebarResize() {
    if (!sidebarResizeState.active) {
      return;
    }

    sidebarResizeState.active = false;
    sidebarResizeState.pointerId = null;
    document.body.classList.remove("sidebar-resizing");
    window.removeEventListener("pointermove", handleSidebarResizePointerMove);
    window.removeEventListener("pointerup", handleSidebarResizePointerUp);
    window.removeEventListener("pointercancel", handleSidebarResizePointerUp);
    window.requestAnimationFrame(() => syncSidebarResizerAria());
  }

  function handleSidebarResizePointerMove(event) {
    if (!sidebarResizeState.active) {
      return;
    }

    applySidebarWidth(sidebarResizeState.startWidth + (event.clientX - sidebarResizeState.startX));
  }

  function handleSidebarResizePointerUp() {
    finishSidebarResize();
  }

  function handleSidebarResizePointerDown(event) {
    if (
      event.button !== 0 ||
      document.body.classList.contains("sidebar-collapsed") ||
      window.matchMedia("(max-width: 1080px)").matches
    ) {
      return;
    }

    event.preventDefault();
    sidebarResizeState.active = true;
    sidebarResizeState.pointerId = event.pointerId;
    sidebarResizeState.startX = event.clientX;
    sidebarResizeState.startWidth = currentSidebarWidth();
    document.body.classList.add("sidebar-resizing");
    window.addEventListener("pointermove", handleSidebarResizePointerMove);
    window.addEventListener("pointerup", handleSidebarResizePointerUp);
    window.addEventListener("pointercancel", handleSidebarResizePointerUp);
  }

  function handleSidebarResizeKeyDown(event) {
    if (
      document.body.classList.contains("sidebar-collapsed") ||
      window.matchMedia("(max-width: 1080px)").matches
    ) {
      return;
    }

    if (event.key === "ArrowLeft") {
      event.preventDefault();
      applySidebarWidth(currentSidebarWidth() - sidebarResizeStep);
      return;
    }

    if (event.key === "ArrowRight") {
      event.preventDefault();
      applySidebarWidth(currentSidebarWidth() + sidebarResizeStep);
      return;
    }

    if (event.key === "Home") {
      event.preventDefault();
      applySidebarWidth(sidebarMinWidth);
      return;
    }

    if (event.key === "End") {
      event.preventDefault();
      applySidebarWidth(sidebarMaxWidth);
    }
  }

  function resetSidebarWidth() {
    document.documentElement.style.removeProperty("--sidebar-width");
    window.requestAnimationFrame(() => syncSidebarResizerAria());
  }

  function applySidebarCollapsedState(collapsed) {
    document.body.classList.toggle("sidebar-collapsed", collapsed);

    sidebarToggles().forEach((toggle) => {
      const labelText = collapsed ? "Expand navigation" : "Collapse navigation";
      toggle.setAttribute("aria-expanded", collapsed ? "false" : "true");
      toggle.setAttribute("aria-label", labelText);
      toggle.title = labelText;

      const label = toggle.querySelector(".sidebar-toggle-label");
      if (label) {
        label.textContent = labelText;
      }
    });

    syncSidebarResizerAria();
  }

  function initializeSidebarToggle() {
    applySidebarCollapsedState(readSidebarCollapsed());
  }

  function initializeSidebarResizer() {
    const resizer = sidebarResizer();
    if (!resizer) {
      return;
    }

    if (resizer.dataset.bound !== "true") {
      resizer.dataset.bound = "true";
      resizer.addEventListener("pointerdown", handleSidebarResizePointerDown);
      resizer.addEventListener("keydown", handleSidebarResizeKeyDown);
      resizer.addEventListener("dblclick", () => {
        resetSidebarWidth();
      });
      window.addEventListener("resize", () => syncSidebarResizerAria());
    }

    syncSidebarResizerAria();
  }

  return {
    applySidebarCollapsedState,
    initializeSidebarResizer,
    initializeSidebarToggle,
    syncSidebarResizerAria,
  };
}