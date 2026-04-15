function folderOptionMarkup(folderPath, selectedFolderPath, optionAttribute, helpers) {
  const normalizedFolderPath = helpers.normalizeFolderPath(folderPath);
  const depth = helpers.folderDepth(normalizedFolderPath);
  const selected = normalizedFolderPath === selectedFolderPath;
  const locationCopy = helpers.displayPath(normalizedFolderPath);

  return `
    <button
      type="button"
      class="local-workspace-folder-option${selected ? " is-selected" : ""}"
      ${optionAttribute}
      data-local-workspace-folder-path="${helpers.escapeHtml(normalizedFolderPath)}"
      style="--local-workspace-folder-depth: ${helpers.escapeHtml(String(depth))}"
      title="${helpers.escapeHtml(locationCopy)}"
    >
      <span class="local-workspace-folder-option-name">${helpers.escapeHtml(helpers.folderName(normalizedFolderPath))}</span>
      <span class="local-workspace-folder-option-path">${helpers.escapeHtml(locationCopy)}</span>
    </button>
  `;
}

function folderListMarkup(folderPaths, selectedFolderPath, optionAttribute, emptyMessage, helpers) {
  if (!folderPaths.length) {
    return emptyMessage;
  }

  return folderPaths
    .map((folderPath) => folderOptionMarkup(folderPath, selectedFolderPath, optionAttribute, helpers))
    .join("");
}

function breadcrumbsMarkup(folderPath, breadcrumbAttribute, helpers) {
  const segments = helpers.normalizeFolderPath(folderPath)
    .split("/")
    .filter(Boolean);
  const crumbs = [
    { label: "Local Workspace", path: "" },
    ...segments.map((segment, index) => ({
      label: segment,
      path: segments.slice(0, index + 1).join("/"),
    })),
  ];

  return crumbs
    .map((crumb, index) => {
      const current = index === crumbs.length - 1;
      return `
        <button
          type="button"
          class="result-export-breadcrumb${current ? " is-current" : ""}"
          ${breadcrumbAttribute}
          data-local-workspace-folder-path="${helpers.escapeHtml(crumb.path)}"
          ${current ? 'aria-current="true"' : ""}
        >${helpers.escapeHtml(crumb.label)}</button>
        ${current ? "" : '<span class="result-export-breadcrumb-separator" aria-hidden="true">/</span>'}
      `;
    })
    .join("");
}

function renderBreadcrumbs(root, folderPath, breadcrumbAttribute, helpers) {
  if (!(root instanceof Element)) {
    return;
  }

  root.innerHTML = breadcrumbsMarkup(folderPath, breadcrumbAttribute, helpers);
}

export function createLocalWorkspacePickerUi(helpers) {
  return {
    localWorkspaceFolderListMarkup(folderPaths) {
      return folderListMarkup(
        folderPaths,
        helpers.getSaveState().folderPath,
        "data-local-workspace-folder-option",
        '<p class="local-workspace-folder-empty">No Local Workspace folders exist yet. Save into Root or create a new folder.</p>',
        helpers
      );
    },

    localWorkspaceMoveFolderListMarkup(folderPaths) {
      return folderListMarkup(
        folderPaths,
        helpers.getMoveState().folderPath,
        "data-local-workspace-move-folder-option",
        '<p class="local-workspace-folder-empty">No Local Workspace folders exist yet. Move into Root or create a new folder.</p>',
        helpers
      );
    },

    renderLocalWorkspaceSaveBreadcrumbs(folderPath = "") {
      renderBreadcrumbs(
        helpers.getSaveBreadcrumbRoot(),
        folderPath,
        "data-local-workspace-breadcrumb",
        helpers
      );
    },

    renderLocalWorkspaceMoveBreadcrumbs(folderPath = "") {
      renderBreadcrumbs(
        helpers.getMoveBreadcrumbRoot(),
        folderPath,
        "data-local-workspace-move-breadcrumb",
        helpers
      );
    },
  };
}