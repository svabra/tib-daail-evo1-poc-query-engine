export function createAppTooltipController() {
  let activeTarget = null;
  let lastPointer = null;

  function tooltipCopy(target) {
    return String(
      target?.dataset?.appTooltip ||
        target?.dataset?.pipelineTooltip ||
        target?.getAttribute?.("title") ||
        ""
    ).trim();
  }

  function tooltipTarget(event) {
    return (
      event.target?.closest?.("[data-app-tooltip], [data-pipeline-tooltip], [title]") ||
      null
    );
  }

  function ensureTooltipRoot() {
    let tooltip = document.querySelector("[data-app-floating-tooltip]");
    if (!tooltip) {
      tooltip = document.createElement("div");
      tooltip.className = "app-floating-tooltip";
      tooltip.dataset.appFloatingTooltip = "true";
      tooltip.setAttribute("role", "tooltip");
      tooltip.hidden = true;
      document.body.appendChild(tooltip);
    }
    return tooltip;
  }

  function suppressNativeTitle(target) {
    if (!target?.hasAttribute?.("title")) {
      return;
    }
    if (!target.dataset.appNativeTooltip) {
      target.dataset.appNativeTooltip = target.getAttribute("title") || "";
    }
    target.removeAttribute("title");
  }

  function restoreNativeTitle(target) {
    if (!target?.dataset?.appNativeTooltip) {
      return;
    }
    target.setAttribute("title", target.dataset.appNativeTooltip);
    delete target.dataset.appNativeTooltip;
  }

  function anchorForTarget(target) {
    const rect = target.getBoundingClientRect();
    return {
      x: rect.left + rect.width / 2,
      y: rect.top,
      fallbackY: rect.bottom,
    };
  }

  function positionTooltip(target, tooltip, pointer = null) {
    if (!target || !tooltip || tooltip.hidden) {
      return;
    }
    const anchor = pointer || anchorForTarget(target);
    const tooltipRect = tooltip.getBoundingClientRect();
    const viewportWidth = document.documentElement.clientWidth || window.innerWidth;
    const viewportHeight = document.documentElement.clientHeight || window.innerHeight;
    const margin = 8;
    const offset = 14;
    let left = anchor.x - tooltipRect.width / 2;
    left = Math.max(margin, Math.min(left, viewportWidth - tooltipRect.width - margin));
    let top = anchor.y - tooltipRect.height - offset;
    if (top < margin) {
      const fallbackY = pointer ? anchor.y : anchor.fallbackY;
      top = Math.min(viewportHeight - tooltipRect.height - margin, fallbackY + offset);
    }
    tooltip.style.left = `${Math.round(left)}px`;
    tooltip.style.top = `${Math.round(Math.max(margin, top))}px`;
  }

  function showTooltip(target, pointer = null) {
    const copy = tooltipCopy(target);
    if (!copy) {
      return;
    }
    hideTooltip();
    activeTarget = target;
    lastPointer = pointer;
    suppressNativeTitle(target);
    const tooltip = ensureTooltipRoot();
    tooltip.textContent = copy;
    tooltip.hidden = false;
    tooltip.classList.add("is-visible");
    positionTooltip(target, tooltip, pointer);
  }

  function hideTooltip() {
    const tooltip = document.querySelector("[data-app-floating-tooltip]");
    if (tooltip) {
      tooltip.classList.remove("is-visible");
      tooltip.hidden = true;
    }
    if (activeTarget) {
      restoreNativeTitle(activeTarget);
    }
    activeTarget = null;
    lastPointer = null;
  }

  function install() {
    const root = document.documentElement;
    if (root.classList.contains("app-tooltips-ready")) {
      return;
    }
    root.classList.add("app-tooltips-ready");
    document.addEventListener("pointerover", (event) => {
      const target = tooltipTarget(event);
      if (!target) {
        return;
      }
      showTooltip(target, { x: event.clientX, y: event.clientY });
    });
    document.addEventListener("pointerout", (event) => {
      const target = tooltipTarget(event);
      const leavingActiveTarget =
        activeTarget &&
        event.target instanceof Node &&
        activeTarget.contains(event.target) &&
        !(event.relatedTarget instanceof Node && activeTarget.contains(event.relatedTarget));
      if (leavingActiveTarget) {
        hideTooltip();
        return;
      }
      if (target && !target.contains(event.relatedTarget)) {
        hideTooltip();
      }
    });
    document.addEventListener("pointermove", (event) => {
      const target = tooltipTarget(event);
      const tooltip = document.querySelector("[data-app-floating-tooltip]");
      if (
        !target &&
        activeTarget &&
        event.target instanceof Node &&
        !activeTarget.contains(event.target)
      ) {
        hideTooltip();
        return;
      }
      if (target && tooltip && !tooltip.hidden) {
        lastPointer = { x: event.clientX, y: event.clientY };
        positionTooltip(target, tooltip, lastPointer);
      }
    });
    document.addEventListener("focusin", (event) => {
      const target = tooltipTarget(event);
      if (target) {
        showTooltip(target);
      }
    });
    document.addEventListener("focusout", (event) => {
      const target = tooltipTarget(event);
      if (target) {
        hideTooltip();
      }
    });
    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape") {
        hideTooltip();
      }
    });
    window.addEventListener("scroll", hideTooltip, true);
    window.addEventListener("resize", hideTooltip);
  }

  return {
    install,
  };
}
