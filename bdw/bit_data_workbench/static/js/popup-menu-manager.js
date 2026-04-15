export function createPopupMenuManager({
  closeCellActionMenus,
  closeResultActionMenus,
  closeS3ExplorerActionMenus,
  closeSourceActionMenus,
  closeWorkspaceActionMenus,
  getQueryNotificationMenu,
  getSettingsMenu,
}) {
  function closeSettingsMenus() {
    document.querySelectorAll("[data-settings-menu][open]").forEach((menu) => {
      menu.removeAttribute("open");
    });
  }

  function menuContainsPointer(menu, event, panelSelector = ":scope > .topbar-notification-panel") {
    if (!(menu instanceof Element) || typeof event?.clientX !== "number" || typeof event?.clientY !== "number") {
      return false;
    }

    const summary = menu.querySelector(":scope > summary");
    const panel = menu.querySelector(panelSelector);
    const rects = [summary, panel]
      .filter((node) => node instanceof Element)
      .map((node) => node.getBoundingClientRect())
      .filter((rect) => rect.width > 0 && rect.height > 0);

    if (!rects.length) {
      return false;
    }

    const left = Math.min(...rects.map((rect) => rect.left));
    const right = Math.max(...rects.map((rect) => rect.right));
    const top = Math.min(...rects.map((rect) => rect.top));
    const bottom = Math.max(...rects.map((rect) => rect.bottom));

    return (
      event.clientX >= left
      && event.clientX <= right
      && event.clientY >= top
      && event.clientY <= bottom
    );
  }

  function anyOpenMenuContainsPointer(selector, event, panelSelector = ":scope > .workspace-action-menu-panel") {
    if (typeof event?.clientX !== "number" || typeof event?.clientY !== "number") {
      return false;
    }

    return Array.from(document.querySelectorAll(`${selector}[open]`)).some((menu) => (
      menuContainsPointer(menu, event, panelSelector)
    ));
  }

  function closePopupMenusForTarget(target, event = null) {
    const activeTarget = target instanceof Element ? target : null;

    if (
      !activeTarget?.closest("[data-workspace-action-menu]")
      && !anyOpenMenuContainsPointer("[data-workspace-action-menu]", event)
    ) {
      closeWorkspaceActionMenus();
    }
    if (
      !activeTarget?.closest("[data-cell-action-menu]")
      && !anyOpenMenuContainsPointer("[data-cell-action-menu]", event)
    ) {
      closeCellActionMenus();
    }
    if (
      !activeTarget?.closest("[data-source-action-menu]")
      && !anyOpenMenuContainsPointer("[data-source-action-menu]", event)
    ) {
      closeSourceActionMenus();
    }
    if (
      !activeTarget?.closest("[data-result-action-menu]")
      && !anyOpenMenuContainsPointer("[data-result-action-menu]", event)
    ) {
      closeResultActionMenus();
    }
    if (
      !activeTarget?.closest("[data-s3-explorer-action-menu]")
      && !anyOpenMenuContainsPointer("[data-s3-explorer-action-menu]", event)
    ) {
      closeS3ExplorerActionMenus();
    }

    const notifications = getQueryNotificationMenu();
    if (!activeTarget?.closest("[data-query-notifications]") && !menuContainsPointer(notifications, event)) {
      notifications?.removeAttribute("open");
    }

    const settings = getSettingsMenu();
    if (!activeTarget?.closest("[data-settings-menu]") && !menuContainsPointer(settings, event)) {
      closeSettingsMenus();
    }
  }

  return {
    closePopupMenusForTarget,
    closeSettingsMenus,
  };
}