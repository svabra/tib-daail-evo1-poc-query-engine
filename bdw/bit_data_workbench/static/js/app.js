import { EditorView, basicSetup } from "../vendor/codemirror.bundle.mjs";
import { sql, PostgreSQL } from "../vendor/lang-sql.bundle.mjs";

const editorRegistry = new WeakMap();
let draggedNotebook = null;
let restoreController = null;

const notebookTreeStorageKey = "bdw.notebookTree.v1";
const notebookMetadataStorageKey = "bdw.notebookMeta.v1";
const lastNotebookStorageKey = "bdw.lastNotebook.v1";
const unassignedFolderName = "Unassigned";

function folderNameDialog() {
  return document.querySelector("[data-folder-name-dialog]");
}

function confirmDialog() {
  return document.querySelector("[data-confirm-dialog]");
}

function readSchema() {
  const element = document.getElementById("sql-schema");
  if (!element) {
    return {};
  }

  try {
    return JSON.parse(element.textContent ?? "{}");
  } catch (_error) {
    return {};
  }
}

function notebookSection() {
  return document.querySelector("[data-notebook-section]");
}

function dataSourcesSection() {
  return document.querySelector("[data-data-sources-section]");
}

function notebookTreeRoot() {
  return document.querySelector("[data-notebook-tree]");
}

function currentActiveNotebookId() {
  return document.querySelector(".notebook-link.is-active")?.dataset.notebookId ?? null;
}

function workspaceNotebookId(root = document) {
  return (
    root.querySelector("input[name='notebook_id']")?.value ??
    root.querySelector("[data-notebook-meta]")?.dataset.notebookId ??
    null
  );
}

function activateNotebookLink(notebookId) {
  document.querySelectorAll(".notebook-link").forEach((link) => {
    link.classList.toggle("is-active", link.dataset.notebookId === notebookId);
  });
}

function notebookLinks(notebookId) {
  return Array.from(document.querySelectorAll("[data-notebook-id]")).filter(
    (link) => link.dataset.notebookId === notebookId
  );
}

function readLastNotebookId() {
  try {
    return window.localStorage.getItem(lastNotebookStorageKey);
  } catch (_error) {
    return null;
  }
}

function writeLastNotebookId(notebookId) {
  try {
    window.localStorage.setItem(lastNotebookStorageKey, notebookId);
  } catch (_error) {
    // Ignore persistence failures and keep the session functional.
  }
}

function closeDialog(dialog, returnValue = "") {
  if (!dialog) {
    return;
  }

  if (typeof dialog.close === "function") {
    dialog.close(returnValue);
  }
}

function showFolderNameDialog({ title, copy, submitLabel, initialValue = "" }) {
  const dialog = folderNameDialog();
  if (!dialog) {
    const fallback = window.prompt(copy, initialValue);
    return Promise.resolve(fallback ? fallback.trim() : null);
  }

  const form = dialog.querySelector("[data-folder-name-form]");
  const titleNode = dialog.querySelector("[data-folder-name-title]");
  const copyNode = dialog.querySelector("[data-folder-name-copy]");
  const input = dialog.querySelector("[data-folder-name-input]");
  const submit = dialog.querySelector("[data-folder-name-submit]");
  const cancel = dialog.querySelector("[data-modal-cancel]");

  titleNode.textContent = title;
  copyNode.textContent = copy;
  submit.textContent = submitLabel;
  input.value = initialValue;

  return new Promise((resolve) => {
    const teardown = () => {
      form.removeEventListener("submit", onSubmit);
      cancel?.removeEventListener("click", onCancel);
      dialog.removeEventListener("close", onClose);
    };

    const onSubmit = (event) => {
      event.preventDefault();
      closeDialog(dialog, "confirm");
    };

    const onCancel = () => closeDialog(dialog, "cancel");

    const onClose = () => {
      const confirmed = dialog.returnValue === "confirm";
      const value = confirmed ? input.value.trim() : null;
      teardown();
      resolve(value || null);
    };

    form.addEventListener("submit", onSubmit);
    cancel?.addEventListener("click", onCancel);
    dialog.addEventListener("close", onClose, { once: true });
    dialog.showModal();
    input.focus();
    input.select();
  });
}

function showConfirmDialog({ title, copy, confirmLabel }) {
  const dialog = confirmDialog();
  if (!dialog) {
    return Promise.resolve(window.confirm(copy));
  }

  const titleNode = dialog.querySelector("[data-confirm-title]");
  const copyNode = dialog.querySelector("[data-confirm-copy]");
  const submit = dialog.querySelector("[data-confirm-submit]");
  const cancel = dialog.querySelector("[data-modal-cancel]");

  titleNode.textContent = title;
  copyNode.textContent = copy;
  submit.textContent = confirmLabel;

  return new Promise((resolve) => {
    const onCancel = () => closeDialog(dialog, "cancel");
    const onClose = () => {
      cancel?.removeEventListener("click", onCancel);
      resolve(dialog.returnValue === "confirm");
    };

    cancel?.addEventListener("click", onCancel);
    dialog.addEventListener("close", onClose, { once: true });
    dialog.showModal();
  });
}

function normalizeTags(tags) {
  const uniqueTags = [];
  const seen = new Set();

  for (const value of tags) {
    const tag = String(value ?? "").trim();
    if (!tag) {
      continue;
    }

    const normalized = tag.toLowerCase();
    if (seen.has(normalized)) {
      continue;
    }

    seen.add(normalized);
    uniqueTags.push(tag);
  }

  return uniqueTags;
}

function readStoredNotebookMetadata() {
  try {
    const rawValue = window.localStorage.getItem(notebookMetadataStorageKey);
    if (!rawValue) {
      return {};
    }

    const parsed = JSON.parse(rawValue);
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch (_error) {
    return {};
  }
}

function writeStoredNotebookMetadata(state) {
  try {
    window.localStorage.setItem(notebookMetadataStorageKey, JSON.stringify(state));
  } catch (_error) {
    // Ignore persistence failures and keep the in-memory editor functional.
  }
}

function readNotebookDefaults(notebookId) {
  const link = notebookLinks(notebookId)[0];
  return {
    title: link?.dataset.notebookTitle ?? "",
    summary: link?.dataset.notebookSummary ?? "",
    tags: normalizeTags(
      Array.from(link?.querySelectorAll(".notebook-tag") ?? []).map((tag) => tag.textContent ?? "")
    ),
  };
}

function notebookMetadata(notebookId) {
  const defaults = readNotebookDefaults(notebookId);
  const storedState = readStoredNotebookMetadata()[notebookId];
  if (!storedState || !Array.isArray(storedState.tags)) {
    return defaults;
  }

  return {
    ...defaults,
    tags: normalizeTags(storedState.tags),
  };
}

function persistNotebookTags(notebookId, tags) {
  const state = readStoredNotebookMetadata();
  state[notebookId] = {
    tags: normalizeTags(tags),
  };
  writeStoredNotebookMetadata(state);
}

function createEditor(root) {
  if (editorRegistry.has(root)) {
    return editorRegistry.get(root);
  }

  const textarea = root.querySelector("[data-editor-source]");
  if (!textarea) {
    return null;
  }

  const schema = readSchema();
  const form = root.closest("form");
  const shell = document.createElement("div");
  shell.className = "editor-shell";
  root.appendChild(shell);

  try {
    const editor = new EditorView({
      doc: textarea.value,
      extensions: [
        basicSetup,
        sql({
          dialect: PostgreSQL,
          schema,
        }),
        EditorView.updateListener.of((update) => {
          if (update.docChanged) {
            textarea.value = update.state.doc.toString();
          }
        }),
      ],
      parent: shell,
    });

    editor.dom.addEventListener("keydown", (event) => {
      if ((event.ctrlKey || event.metaKey) && event.key === "Enter" && form) {
        event.preventDefault();
        textarea.value = editor.state.doc.toString();
        form.requestSubmit();
      }
    });

    root.classList.add("editor-ready");
    editorRegistry.set(root, editor);
    return editor;
  } catch (error) {
    shell.remove();
    console.error("Failed to initialize CodeMirror. Falling back to textarea.", error);
    return null;
  }
}

function initializeEditors(root = document) {
  root.querySelectorAll("[data-editor-root]").forEach((editorRoot) => {
    createEditor(editorRoot);
  });
}

function createSidebarTag(tag) {
  const node = document.createElement("small");
  node.className = "notebook-tag";
  node.textContent = tag;
  return node;
}

function renderSidebarTags(link, tags) {
  let container = link.querySelector(".notebook-tags");
  if (!tags.length) {
    container?.remove();
    return;
  }

  if (!container) {
    container = document.createElement("span");
    container.className = "notebook-tags";
    link.appendChild(container);
  }

  container.replaceChildren(...tags.map((tag) => createSidebarTag(tag)));
}

function updateNotebookSearchableItem(link, tags) {
  const title = link.dataset.notebookTitle ?? "";
  const summary = link.dataset.notebookSummary ?? "";
  link.dataset.searchableItem = `${title} ${summary} ${tags.join(" ")}`.trim();
}

function createWorkspaceTagChip(tag) {
  const button = document.createElement("button");
  button.type = "button";
  button.className = "workspace-tag-chip";
  button.dataset.tagValue = tag;

  const label = document.createElement("span");
  label.textContent = tag;

  const remove = document.createElement("span");
  remove.className = "workspace-tag-remove";
  remove.setAttribute("aria-hidden", "true");
  remove.textContent = "\u00D7";

  button.append(label, remove);
  return button;
}

function renderWorkspaceTags(metaRoot, tags) {
  const tagList = metaRoot.querySelector("[data-tag-list]");
  if (!tagList) {
    return;
  }

  tagList.replaceChildren(...tags.map((tag) => createWorkspaceTagChip(tag)));
}

function applyNotebookMetadata() {
  document.querySelectorAll("[data-draggable-notebook]").forEach((link) => {
    const notebookId = link.dataset.notebookId;
    if (!notebookId) {
      return;
    }

    const metadata = notebookMetadata(notebookId);
    renderSidebarTags(link, metadata.tags);
    updateNotebookSearchableItem(link, metadata.tags);
  });

  document.querySelectorAll("[data-notebook-meta]").forEach((metaRoot) => {
    const notebookId = metaRoot.dataset.notebookId;
    if (!notebookId) {
      return;
    }

    renderWorkspaceTags(metaRoot, notebookMetadata(notebookId).tags);
  });

  applySidebarSearchFilter();
}

function setNotebookTags(notebookId, tags) {
  persistNotebookTags(notebookId, normalizeTags(tags));
  applyNotebookMetadata();
}

function directChildrenContainer(folder) {
  return folder?.querySelector(":scope > [data-tree-children]") ?? null;
}

function readStoredNotebookTree() {
  try {
    const rawValue = window.localStorage.getItem(notebookTreeStorageKey);
    if (!rawValue) {
      return null;
    }

    const parsed = JSON.parse(rawValue);
    return Array.isArray(parsed) ? parsed : null;
  } catch (_error) {
    return null;
  }
}

function writeStoredNotebookTree(state) {
  try {
    window.localStorage.setItem(notebookTreeStorageKey, JSON.stringify(state));
  } catch (_error) {
    // Ignore persistence failures and keep the in-memory tree functional.
  }
}

function updateFolderCounts(root = document) {
  root.querySelectorAll("[data-tree-folder]").forEach((folder) => {
    const countLabel = folder.querySelector(":scope > summary .tree-folder-count");
    const children = directChildrenContainer(folder);
    if (!countLabel || !children) {
      return;
    }

    const notebookCount = children.querySelectorAll("[data-draggable-notebook]").length;
    countLabel.textContent = String(notebookCount);
  });
}

function slugifyFolderSegment(name) {
  return String(name ?? "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "-");
}

function deriveFolderId(name, parentFolderId = "") {
  const slug = slugifyFolderSegment(name);
  if (!slug) {
    return parentFolderId || "";
  }

  return parentFolderId ? `${parentFolderId}-${slug}` : slug;
}

function defaultFolderPermissions(folderId = "") {
  if (folderId === "smoke-tests" || folderId.startsWith("smoke-tests-")) {
    return {
      canEdit: false,
      canDelete: false,
    };
  }

  return {
    canEdit: true,
    canDelete: true,
  };
}

function applyFolderActionState(button, { allowed, enabledTitle, disabledTitle }) {
  button.classList.toggle("is-action-disabled", !allowed);
  button.disabled = !allowed;
  button.title = allowed ? enabledTitle : disabledTitle;
}

function createFolderNode(
  name,
  { open = false, folderId = "", canEdit = true, canDelete = true } = {}
) {
  const folder = document.createElement("details");
  folder.className = "tree-folder";
  folder.dataset.treeFolder = "";
  folder.open = open;
  folder.dataset.folderId = folderId || "";
  folder.dataset.canEdit = String(canEdit);
  folder.dataset.canDelete = String(canDelete);

  const summary = document.createElement("summary");
  summary.className = "tree-folder-summary";
  summary.dataset.searchableItem = name;

  const label = document.createElement("span");
  label.className = "tree-folder-label";
  label.textContent = name;

  const tools = document.createElement("span");
  tools.className = "tree-folder-tools";

  const renameButton = document.createElement("button");
  renameButton.type = "button";
  renameButton.className = "tree-add-button tree-add-button-inline";
  renameButton.dataset.renameTreeFolder = "";
  renameButton.setAttribute("aria-label", "Rename folder");
  renameButton.textContent = "Edit";
  applyFolderActionState(renameButton, {
    allowed: canEdit,
    enabledTitle: "Rename folder",
    disabledTitle: "This folder cannot be renamed.",
  });

  const deleteButton = document.createElement("button");
  deleteButton.type = "button";
  deleteButton.className = "tree-add-button tree-add-button-inline tree-delete-button";
  deleteButton.dataset.deleteTreeFolder = "";
  deleteButton.setAttribute("aria-label", "Delete folder");
  deleteButton.textContent = "Delete";
  applyFolderActionState(deleteButton, {
    allowed: canDelete,
    enabledTitle: "Delete folder. Notebooks will be moved to the unassigned folder.",
    disabledTitle: "This folder cannot be deleted.",
  });

  const addButton = document.createElement("button");
  addButton.type = "button";
  addButton.className = "tree-add-button tree-add-button-inline";
  addButton.dataset.addTreeItem = "";
  addButton.title = "Add subfolder";
  addButton.setAttribute("aria-label", "Add subfolder");
  addButton.textContent = "+";

  const count = document.createElement("span");
  count.className = "tree-folder-count";
  count.textContent = "0";

  tools.append(renameButton, deleteButton, addButton, count);
  summary.append(label, tools);
  folder.append(summary);

  const children = document.createElement("div");
  children.className = "tree-children";
  children.dataset.treeChildren = "";
  folder.append(children);

  return folder;
}

function folderLabel(folder) {
  return folder?.querySelector(":scope > summary .tree-folder-label") ?? null;
}

function isUnassignedFolder(folder) {
  if (!(folder instanceof Element) || !folder.matches("[data-tree-folder]")) {
    return false;
  }

  return (
    folder.dataset.systemFolder === "unassigned" ||
    folderLabel(folder)?.textContent?.trim() === unassignedFolderName
  );
}

function folderCanEdit(folder) {
  if (!(folder instanceof Element)) {
    return false;
  }

  return folder.dataset.canEdit !== "false";
}

function folderCanDelete(folder) {
  if (!(folder instanceof Element)) {
    return false;
  }

  return folder.dataset.canDelete !== "false";
}

function notebookCountInFolder(folder) {
  if (!(folder instanceof Element)) {
    return 0;
  }

  return folder.querySelectorAll("[data-draggable-notebook]").length;
}

function rootUnassignedFolder() {
  const root = notebookTreeRoot();
  if (!root) {
    return null;
  }

  return (
    Array.from(root.querySelectorAll(":scope > [data-tree-folder]")).find((folder) =>
      isUnassignedFolder(folder)
    ) ?? null
  );
}

function syncRootUnassignedFolder() {
  const root = notebookTreeRoot();
  const folder = rootUnassignedFolder();
  if (!root || !folder) {
    return null;
  }

  folder.dataset.systemFolder = "unassigned";
  if (notebookCountInFolder(folder) === 0) {
    folder.remove();
    return null;
  }

  root.appendChild(folder);
  return folder;
}

function ensureRootUnassignedFolder() {
  const root = notebookTreeRoot();
  if (!root) {
    return null;
  }

  const existing = rootUnassignedFolder();
  if (existing) {
    existing.dataset.systemFolder = "unassigned";
    existing.open = true;
    root.appendChild(existing);
    return existing;
  }

  const folder = createFolderNode(unassignedFolderName, { open: true });
  folder.dataset.systemFolder = "unassigned";
  root.appendChild(folder);
  return folder;
}

function collectFolderNotebooks(folder) {
  return Array.from(folder.querySelectorAll("[data-draggable-notebook]"));
}

function deleteTreeFolder(folder) {
  const notebooks = collectFolderNotebooks(folder);
  if (isUnassignedFolder(folder) && notebooks.length > 0) {
    return;
  }

  let targetContainer = null;
  if (notebooks.length > 0) {
    const targetFolder = ensureRootUnassignedFolder();
    targetContainer = directChildrenContainer(targetFolder);
    if (!targetContainer) {
      return;
    }
  }

  for (const notebook of notebooks) {
    targetContainer?.appendChild(notebook);
  }

  folder.remove();
  updateFolderCounts();
  persistNotebookTree();
  applyNotebookMetadata();
}

function serializeTreeNode(node) {
  if (!(node instanceof Element)) {
    return null;
  }

  if (node.matches("[data-tree-folder]")) {
    const name =
      node.querySelector(":scope > summary .tree-folder-label")?.textContent?.trim() || "Folder";
    const children = directChildrenContainer(node);
    return {
      type: "folder",
      folderId: node.dataset.folderId || null,
      name,
      open: node.open,
      systemFolder: node.dataset.systemFolder || null,
      canEdit: node.dataset.canEdit !== "false",
      canDelete: node.dataset.canDelete !== "false",
      children: children
        ? Array.from(children.children)
            .map((child) => serializeTreeNode(child))
            .filter(Boolean)
        : [],
    };
  }

  if (node.matches("[data-draggable-notebook]")) {
    return {
      type: "notebook",
      notebookId: node.dataset.notebookId,
    };
  }

  return null;
}

function persistNotebookTree() {
  const root = notebookTreeRoot();
  if (!root) {
    return;
  }

  syncRootUnassignedFolder();
  const state = Array.from(root.children)
    .map((child) => serializeTreeNode(child))
    .filter(Boolean);
  writeStoredNotebookTree(state);
}

function renderStoredTreeNode(nodeState, notebookLookup, parentFolderId = "") {
  if (!nodeState || typeof nodeState !== "object") {
    return null;
  }

  if (nodeState.type === "notebook") {
    const notebook = notebookLookup.get(nodeState.notebookId);
    if (!notebook) {
      return null;
    }

    notebookLookup.delete(nodeState.notebookId);
    return notebook;
  }

  if (nodeState.type === "folder") {
    const resolvedFolderId = nodeState.folderId || deriveFolderId(nodeState.name || "Folder", parentFolderId);
    const fallbackPolicy = defaultFolderPermissions(resolvedFolderId);
    const folder = createFolderNode(nodeState.name || "Folder", {
      open: Boolean(nodeState.open),
      folderId: resolvedFolderId,
      canEdit: fallbackPolicy.canEdit
        ? typeof nodeState.canEdit === "boolean"
          ? nodeState.canEdit
          : true
        : false,
      canDelete: fallbackPolicy.canDelete
        ? typeof nodeState.canDelete === "boolean"
          ? nodeState.canDelete
          : true
        : false,
    });
    if (nodeState.systemFolder) {
      folder.dataset.systemFolder = nodeState.systemFolder;
    }
    const container = directChildrenContainer(folder);

    for (const child of nodeState.children ?? []) {
      const renderedChild = renderStoredTreeNode(child, notebookLookup, resolvedFolderId);
      if (renderedChild) {
        container.appendChild(renderedChild);
      }
    }

    return folder;
  }

  return null;
}

function resolveAddTarget(button) {
  const folder = button.closest("[data-tree-folder]");
  if (folder) {
    folder.open = true;
    return directChildrenContainer(folder);
  }

  return notebookTreeRoot();
}

function clearDropTargets() {
  document.querySelectorAll(".tree-children.is-drag-over").forEach((node) => {
    node.classList.remove("is-drag-over");
  });
  document.querySelectorAll(".tree-folder.is-drag-over").forEach((node) => {
    node.classList.remove("is-drag-over");
  });
}

function clearDragState() {
  clearDropTargets();
  if (draggedNotebook) {
    draggedNotebook.classList.remove("is-dragging");
  }
}

function resolveDropTarget(target) {
  if (!(target instanceof Element)) {
    return null;
  }

  const explicitContainer = target.closest("[data-tree-children]");
  if (explicitContainer) {
    return explicitContainer;
  }

  const folder = target.closest("[data-tree-folder]");
  if (folder) {
    return directChildrenContainer(folder);
  }

  return notebookTreeRoot();
}

function initializeNotebookTree(root = document) {
  const treeRoot =
    root instanceof Element && root.matches("[data-notebook-tree]") ? root : notebookTreeRoot();

  if (!treeRoot) {
    return;
  }

  const storedTree = readStoredNotebookTree();
  if (storedTree) {
    const notebookLookup = new Map(
      Array.from(treeRoot.querySelectorAll("[data-draggable-notebook]")).map((notebook) => [
        notebook.dataset.notebookId,
        notebook,
      ])
    );
    const fragment = document.createDocumentFragment();

    for (const nodeState of storedTree) {
      const renderedNode = renderStoredTreeNode(nodeState, notebookLookup);
      if (renderedNode) {
        fragment.appendChild(renderedNode);
      }
    }

    for (const notebook of notebookLookup.values()) {
      fragment.appendChild(notebook);
    }

    treeRoot.replaceChildren(fragment);
  } else {
    persistNotebookTree();
  }

  syncRootUnassignedFolder();
  updateFolderCounts(root);
}

function revealNotebookLink(notebookId) {
  const link = notebookLinks(notebookId)[0];
  if (!link) {
    return;
  }

  notebookSection()?.setAttribute("open", "");

  let parent = link.parentElement;
  while (parent) {
    const folder = parent.closest("[data-tree-folder]");
    if (!folder) {
      break;
    }
    folder.open = true;
    parent = folder.parentElement;
  }

  persistNotebookTree();
}

function applySidebarSearchFilter() {
  const search = document.querySelector("[data-sidebar-search]");
  const sidebar = document.getElementById("sidebar");
  if (!search || !sidebar) {
    return;
  }

  const term = search.value.trim().toLowerCase();
  const matches = (element) => {
    const haystack = (element?.dataset.searchableItem ?? "").toLowerCase();
    return !term || haystack.includes(term);
  };

  sidebar.querySelectorAll("[data-draggable-notebook]").forEach((link) => {
    link.dataset.searchHidden = matches(link) ? "false" : "true";
  });

  const notebookFolders = Array.from(sidebar.querySelectorAll("[data-tree-folder]")).reverse();
  for (const folder of notebookFolders) {
    const selfMatches = matches(folder.querySelector(":scope > summary"));
    const visibleChildren = folder.querySelector(
      ":scope > [data-tree-children] > :not([data-search-hidden='true'])"
    );
    const visible = !term || selfMatches || Boolean(visibleChildren);
    folder.dataset.searchHidden = visible ? "false" : "true";
    if (term && visibleChildren) {
      folder.open = true;
    }
  }

  sidebar.querySelectorAll(".source-object").forEach((item) => {
    item.dataset.searchHidden = matches(item) ? "false" : "true";
  });

  const sourceSchemas = Array.from(sidebar.querySelectorAll("[data-source-schema]")).reverse();
  for (const schema of sourceSchemas) {
    const selfMatches = matches(schema.querySelector(":scope > summary"));
    const visibleChildren = schema.querySelector(
      ":scope > .source-object-list > :not([data-search-hidden='true'])"
    );
    const visible = !term || selfMatches || Boolean(visibleChildren);
    schema.dataset.searchHidden = visible ? "false" : "true";
    if (term && visibleChildren) {
      schema.open = true;
    }
  }

  const sourceCatalogs = Array.from(sidebar.querySelectorAll("[data-source-catalog]")).reverse();
  for (const catalog of sourceCatalogs) {
    const selfMatches = matches(catalog.querySelector(":scope > summary"));
    const visibleChildren = catalog.querySelector(
      ":scope > :not(summary):not([data-search-hidden='true'])"
    );
    const visible = !term || selfMatches || Boolean(visibleChildren);
    catalog.dataset.searchHidden = visible ? "false" : "true";
    if (term && visibleChildren) {
      catalog.open = true;
    }
  }

  if (term && sidebar.querySelector("[data-draggable-notebook][data-search-hidden='false']")) {
    notebookSection()?.setAttribute("open", "");
  }

  if (term && sidebar.querySelector("[data-source-catalog][data-search-hidden='false']")) {
    dataSourcesSection()?.setAttribute("open", "");
  }
}

function initializeSidebarSearch() {
  const search = document.querySelector("[data-sidebar-search]");
  const sidebar = document.getElementById("sidebar");
  if (!search || !sidebar || search.dataset.bound === "true") {
    return;
  }

  search.dataset.bound = "true";
  search.addEventListener("input", () => applySidebarSearchFilter());
  applySidebarSearchFilter();
}

async function loadNotebookWorkspace(notebookId) {
  const panel = document.getElementById("workspace-panel");
  if (!panel || !notebookId) {
    return;
  }

  const controller = new AbortController();
  restoreController = controller;

  const response = await window.fetch(`/notebooks/${encodeURIComponent(notebookId)}`, {
    headers: { "HX-Request": "true" },
    signal: controller.signal,
  });
  if (controller.signal.aborted || restoreController !== controller) {
    return;
  }
  if (restoreController === controller) {
    restoreController = null;
  }
  if (!response.ok) {
    throw new Error(`Failed to load notebook ${notebookId}: ${response.status}`);
  }

  const workspaceMarkup = await response.text();
  if (controller.signal.aborted) {
    return;
  }

  panel.innerHTML = workspaceMarkup;
  initializeEditors(panel);
  applyNotebookMetadata();
  activateNotebookLink(notebookId);
  revealNotebookLink(notebookId);
  writeLastNotebookId(notebookId);
}

async function restoreLastNotebook() {
  const storedNotebookId = readLastNotebookId();
  const activeNotebookId = currentActiveNotebookId();
  const notebookId = storedNotebookId || activeNotebookId;

  if (!notebookId) {
    return;
  }

  if (activeNotebookId === notebookId) {
    revealNotebookLink(notebookId);
    writeLastNotebookId(notebookId);
    return;
  }

  try {
    await loadNotebookWorkspace(notebookId);
  } catch (error) {
    if (error?.name === "AbortError") {
      return;
    }
    console.error("Failed to restore the last active notebook.", error);
    if (activeNotebookId) {
      revealNotebookLink(activeNotebookId);
      writeLastNotebookId(activeNotebookId);
    }
  }
}

document.body.addEventListener("click", async (event) => {
  const tagAddButton = event.target.closest("[data-tag-add]");
  if (tagAddButton) {
    event.preventDefault();

    const metaRoot = tagAddButton.closest("[data-notebook-meta]");
    const input = metaRoot?.querySelector("[data-tag-input]");
    const notebookId = metaRoot?.dataset.notebookId;
    if (!input || !notebookId) {
      return;
    }

    const nextTag = input.value.trim();
    if (!nextTag) {
      return;
    }

    setNotebookTags(notebookId, [...notebookMetadata(notebookId).tags, nextTag]);
    input.value = "";
    return;
  }

  const tagChip = event.target.closest(".workspace-tag-chip");
  if (tagChip) {
    event.preventDefault();

    const metaRoot = tagChip.closest("[data-notebook-meta]");
    const notebookId = metaRoot?.dataset.notebookId;
    const tagValue = tagChip.dataset.tagValue;
    if (!notebookId || !tagValue) {
      return;
    }

    const remainingTags = notebookMetadata(notebookId).tags.filter((tag) => tag !== tagValue);
    setNotebookTags(notebookId, remainingTags);
    return;
  }

  const renameFolderButton = event.target.closest("[data-rename-tree-folder]");
  if (renameFolderButton) {
    event.preventDefault();
    event.stopPropagation();

    const folder = renameFolderButton.closest("[data-tree-folder]");
    const label = folderLabel(folder);
    if (!folder || !label || !folderCanEdit(folder)) {
      return;
    }

    const nextName = await showFolderNameDialog({
      title: "Rename folder",
      copy: "Update the folder name used in the notebook tree.",
      submitLabel: "Rename",
      initialValue: label.textContent?.trim() ?? "",
    });
    if (!nextName) {
      return;
    }

    label.textContent = nextName;
    const summary = folder.querySelector(":scope > summary");
    if (summary) {
      summary.dataset.searchableItem = nextName;
    }
    persistNotebookTree();
    applySidebarSearchFilter();
    return;
  }

  const deleteFolderButton = event.target.closest("[data-delete-tree-folder]");
  if (deleteFolderButton) {
    event.preventDefault();
    event.stopPropagation();

    const folder = deleteFolderButton.closest("[data-tree-folder]");
    const label = folderLabel(folder)?.textContent?.trim() ?? "this folder";
    if (!folder || !folderCanDelete(folder)) {
      return;
    }

    const confirmed = await showConfirmDialog({
      title: "Delete folder",
      copy: `Delete "${label}"? All notebooks in this folder will be moved to "${unassignedFolderName}" at the bottom of the notebook tree.`,
      confirmLabel: "Delete folder",
    });
    if (!confirmed) {
      return;
    }

    deleteTreeFolder(folder);
    return;
  }

  const addButton = event.target.closest("[data-add-tree-item]");
  if (addButton) {
    event.preventDefault();
    event.stopPropagation();

    const folderName = await showFolderNameDialog({
      title: "New folder",
      copy: "Enter a name for the new notebook folder.",
      submitLabel: "Create folder",
    });
    if (!folderName) {
      return;
    }

    const target = resolveAddTarget(addButton);
    if (!target) {
      return;
    }

    const parentFolder = addButton.closest("[data-tree-folder]");
    const nextFolderId = deriveFolderId(folderName, parentFolder?.dataset.folderId || "");
    const nextFolderPolicy = defaultFolderPermissions(nextFolderId);

    target.appendChild(
      createFolderNode(folderName, {
        open: true,
        folderId: nextFolderId,
        canEdit: nextFolderPolicy.canEdit,
        canDelete: nextFolderPolicy.canDelete,
      })
    );
    updateFolderCounts();
    persistNotebookTree();
    applySidebarSearchFilter();
    return;
  }

  const link = event.target.closest(".notebook-link");
  if (link) {
    restoreController?.abort();
    restoreController = null;
    activateNotebookLink(link.dataset.notebookId);
    revealNotebookLink(link.dataset.notebookId);
    writeLastNotebookId(link.dataset.notebookId);
    return;
  }

  const workspaceRoot = event.target.closest("[data-workspace-notebook]");
  if (!workspaceRoot) {
    return;
  }

  const notebookId = workspaceNotebookId(workspaceRoot);
  if (!notebookId) {
    return;
  }

  activateNotebookLink(notebookId);
  revealNotebookLink(notebookId);
  writeLastNotebookId(notebookId);
});

document.body.addEventListener("dragstart", (event) => {
  const notebook = event.target.closest("[data-draggable-notebook]");
  if (!notebook) {
    return;
  }

  draggedNotebook = notebook;
  draggedNotebook.classList.add("is-dragging");

  if (event.dataTransfer) {
    event.dataTransfer.effectAllowed = "move";
    event.dataTransfer.setData("text/plain", notebook.dataset.notebookId ?? "");
  }
});

document.body.addEventListener("dragover", (event) => {
  if (!draggedNotebook) {
    return;
  }

  const dropTarget = resolveDropTarget(event.target);
  if (!dropTarget) {
    return;
  }

  event.preventDefault();
  clearDropTargets();
  dropTarget.classList.add("is-drag-over");

  const folder = dropTarget.closest("[data-tree-folder]");
  if (folder) {
    folder.open = true;
    folder.classList.add("is-drag-over");
  }
});

document.body.addEventListener("drop", (event) => {
  if (!draggedNotebook) {
    return;
  }

  const dropTarget = resolveDropTarget(event.target);
  if (!dropTarget) {
    return;
  }

  event.preventDefault();
  dropTarget.appendChild(draggedNotebook);
  clearDragState();
  updateFolderCounts();
  syncRootUnassignedFolder();
  persistNotebookTree();
  draggedNotebook = null;
});

document.body.addEventListener("dragend", () => {
  clearDragState();
  draggedNotebook = null;
});

document.body.addEventListener(
  "toggle",
  (event) => {
    const folder = event.target;
    if (!(folder instanceof HTMLDetailsElement) || !folder.matches("[data-tree-folder]")) {
      return;
    }

    persistNotebookTree();
  },
  true
);

document.body.addEventListener("keydown", (event) => {
  if (event.key !== "Enter") {
    return;
  }

  const input = event.target.closest("[data-tag-input]");
  const metaRoot = input?.closest("[data-notebook-meta]");
  const notebookId = metaRoot?.dataset.notebookId;
  if (!input || !notebookId) {
    return;
  }

  event.preventDefault();
  const nextTag = input.value.trim();
  if (!nextTag) {
    return;
  }

  setNotebookTags(notebookId, [...notebookMetadata(notebookId).tags, nextTag]);
  input.value = "";
});

document.body.addEventListener("htmx:afterSwap", (event) => {
  initializeEditors(event.target);
  initializeSidebarSearch();
  initializeNotebookTree();
  applyNotebookMetadata();

  const notebookId =
    event.detail?.requestConfig?.parameters?.notebook_id ??
    event.detail?.requestConfig?.elt?.closest?.(".notebook-link")?.dataset?.notebookId ??
    workspaceNotebookId();

  if (notebookId) {
    activateNotebookLink(notebookId);
    revealNotebookLink(notebookId);
    writeLastNotebookId(notebookId);
  }
});

initializeEditors();
initializeSidebarSearch();
initializeNotebookTree();
applyNotebookMetadata();
restoreLastNotebook();
