export function createWorkspaceScrollManager(options = {}) {
  const defaultBehavior = options.defaultBehavior ?? "auto";

  function resolveWorkspaceScrollTarget() {
    return (
      document.querySelector("[data-workspace-notebook]") ??
      document.getElementById("workspace-panel") ??
      null
    );
  }

  function scrollWorkspaceNotebookIntoView({ behavior = defaultBehavior } = {}) {
    const target = resolveWorkspaceScrollTarget();
    if (!(target instanceof Element)) {
      return;
    }

    window.requestAnimationFrame(() => {
      window.scrollTo({ top: 0, behavior });
    });
  }

  return {
    scrollWorkspaceNotebookIntoView,
  };
}
