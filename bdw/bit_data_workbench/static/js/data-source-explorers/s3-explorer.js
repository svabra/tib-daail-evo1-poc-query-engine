import {
  actionButtonMarkup,
  detailCardMarkup,
  explorerEmptyStateMarkup,
  publicationBadgeMarkup,
  publicationLinksMarkup,
  sourceObjectElement,
  sourceSchemaElement,
} from "./utils.js";

export function createS3DataSourceExplorer(helpers) {
  const {
    escapeHtml,
    fetchJsonOrThrow,
    formatByteCount,
    openDataProductPublishDialog,
    showMessageDialog,
    downloadSourceS3Object,
  } = helpers;

  const stateByRoot = new WeakMap();

  function explorerState(root) {
    return stateByRoot.get(root) ?? null;
  }

  function navigationRoot(root) {
    return root.querySelector("[data-data-source-explorer-navigation]");
  }

  function detailRoot(root) {
    return root.querySelector("[data-data-source-explorer-detail]");
  }

  function currentLocationCopy(snapshot) {
    if (snapshot.prefix) {
      return snapshot.path || `${snapshot.bucket}/${snapshot.prefix}`;
    }
    if (snapshot.bucket) {
      return snapshot.path || snapshot.bucket;
    }
    return "All buckets";
  }

  function selectedFileDescriptor(state) {
    const selectedEntry = state.selectedEntry;
    if (!selectedEntry || selectedEntry.entryKind !== "file") {
      return null;
    }
    return sourceObjectElement({
      name: selectedEntry.name,
      displayName: selectedEntry.name,
      kind: "file",
      sourceOptionId: "workspace.s3",
      s3Bucket: selectedEntry.bucket,
      s3Key: selectedEntry.prefix,
      s3Path: selectedEntry.path,
      s3FileFormat: selectedEntry.fileFormat,
      s3Downloadable: true,
      sizeBytes: selectedEntry.sizeBytes,
    });
  }

  function renderNavigation(root) {
    const state = explorerState(root);
    const navigation = navigationRoot(root);
    if (!state || !(navigation instanceof Element)) {
      return;
    }

    const snapshot = state.snapshot;
    if (!snapshot) {
      navigation.innerHTML = explorerEmptyStateMarkup(
        "The Shared Workspace explorer is unavailable.",
        { tone: "danger" },
        escapeHtml
      );
      return;
    }

    navigation.innerHTML = `
      <div class="data-source-explorer-tree">
        <div class="data-source-explorer-breadcrumbs">
          ${(snapshot.breadcrumbs || [])
            .map(
              (breadcrumb) => `
                <button
                  type="button"
                  class="data-source-explorer-breadcrumb"
                  data-data-source-explorer-s3-location
                  data-bucket="${escapeHtml(breadcrumb.bucket || "")}"
                  data-prefix="${escapeHtml(breadcrumb.prefix || "")}"
                >
                  ${escapeHtml(breadcrumb.label || "Buckets")}
                </button>
              `
            )
            .join("")}
        </div>
        ${
          (snapshot.entries || []).length
            ? `
                <div class="data-source-explorer-group-body">
                  ${snapshot.entries
                    .map((entry) => {
                      const isFile = entry.entryKind === "file";
                      const active =
                        isFile &&
                        state.selectedEntry?.bucket === entry.bucket &&
                        state.selectedEntry?.prefix === entry.prefix
                          ? " is-active"
                          : "";
                      return `
                        <button
                          type="button"
                          class="data-source-explorer-object${active}"
                          ${
                            isFile
                              ? `data-data-source-explorer-s3-file="${escapeHtml(entry.prefix || "")}"`
                              : `data-data-source-explorer-s3-location`
                          }
                          data-bucket="${escapeHtml(entry.bucket || "")}"
                          data-prefix="${escapeHtml(entry.prefix || "")}"
                          data-entry-kind="${escapeHtml(entry.entryKind || "")}"
                        >
                          <span class="data-source-explorer-object-copy">
                            <span class="data-source-explorer-object-title-row">
                              <strong>${escapeHtml(entry.name || "")}</strong>
                              ${publicationBadgeMarkup(entry.publishedDataProducts, escapeHtml)}
                            </span>
                            <span>${
                              isFile
                                ? escapeHtml(
                                    `${String(entry.fileFormat || "file").toUpperCase()} • ${formatByteCount(
                                      entry.sizeBytes
                                    )}`
                                  )
                                : escapeHtml(String(entry.entryKind || "").toUpperCase())
                            }</span>
                          </span>
                        </button>
                      `;
                    })
                    .join("")}
                </div>
              `
            : explorerEmptyStateMarkup(
                snapshot.emptyMessage || "This Shared Workspace location is empty.",
                {},
                escapeHtml
              )
        }
      </div>
    `;
  }

  function renderDetail(root) {
    const state = explorerState(root);
    const detail = detailRoot(root);
    if (!state || !(detail instanceof Element)) {
      return;
    }

    const snapshot = state.snapshot;
    if (!snapshot) {
      detail.innerHTML = explorerEmptyStateMarkup(
        "The Shared Workspace explorer is unavailable.",
        { tone: "danger" },
        escapeHtml
      );
      return;
    }

    if (state.selectedEntry?.entryKind === "file") {
      detail.innerHTML = detailCardMarkup(
        {
          eyebrow: `${snapshot.bucket} • ${String(state.selectedEntry.fileFormat || "file").toUpperCase()}`,
          title: state.selectedEntry.name || "Selected object",
          copy: `Download the selected object or publish it as a managed data product.`,
          actions: [
            actionButtonMarkup("Download", "download", escapeHtml),
            actionButtonMarkup("Create Data Product ...", "create-data-product", escapeHtml),
          ].join(""),
          body: `
            ${publicationLinksMarkup(state.selectedEntry.publishedDataProducts, escapeHtml)}
            <ul class="sidebar-source-field-list">
              <li class="sidebar-source-field">
                <span class="sidebar-source-field-name"><span class="sidebar-source-field-name-text">Path</span></span>
                <span class="sidebar-source-field-type">${escapeHtml(state.selectedEntry.path || "")}</span>
              </li>
              <li class="sidebar-source-field">
                <span class="sidebar-source-field-name"><span class="sidebar-source-field-name-text">Size</span></span>
                <span class="sidebar-source-field-type">${escapeHtml(formatByteCount(state.selectedEntry.sizeBytes))}</span>
              </li>
            </ul>
          `,
        },
        escapeHtml
      );
      return;
    }

    const canPublishBucket = Boolean(snapshot.bucket) && !snapshot.prefix;
    detail.innerHTML = detailCardMarkup(
      {
        eyebrow: "Shared Workspace",
        title: snapshot.bucket
          ? snapshot.prefix
            ? "Prefix"
            : snapshot.bucket
          : "Buckets",
        copy: `Current location: ${currentLocationCopy(snapshot)}.`,
        actions: canPublishBucket
          ? actionButtonMarkup("Create Data Product ...", "publish-bucket", escapeHtml)
          : "",
        body: `
          ${publicationLinksMarkup(snapshot.publishedDataProducts, escapeHtml)}
          <ul class="sidebar-source-field-list">
            <li class="sidebar-source-field">
              <span class="sidebar-source-field-name"><span class="sidebar-source-field-name-text">Location</span></span>
              <span class="sidebar-source-field-type">${escapeHtml(currentLocationCopy(snapshot))}</span>
            </li>
            <li class="sidebar-source-field">
              <span class="sidebar-source-field-name"><span class="sidebar-source-field-name-text">Entries</span></span>
              <span class="sidebar-source-field-type">${escapeHtml(String((snapshot.entries || []).length))}</span>
            </li>
          </ul>
        `,
      },
      escapeHtml
    );
  }

  async function render(root) {
    renderNavigation(root);
    renderDetail(root);
  }

  async function loadSnapshot(root, { bucket = "", prefix = "" } = {}) {
    const state = explorerState(root);
    if (!state) {
      return;
    }

    const params = new URLSearchParams();
    if (bucket) {
      params.set("bucket", bucket);
    }
    if (prefix) {
      params.set("prefix", prefix);
    }

    const payload = await fetchJsonOrThrow(
      `/api/data-sources/workspace.s3/explorer${params.toString() ? `?${params.toString()}` : ""}`
    );
    state.snapshot = payload?.snapshot || null;
    state.selectedEntry = null;
    await render(root);
  }

  async function initialize(root) {
    stateByRoot.set(root, {
      snapshot: null,
      selectedEntry: null,
    });

    const navigation = navigationRoot(root);
    if (navigation instanceof Element) {
      navigation.innerHTML = explorerEmptyStateMarkup(
        "Loading Shared Workspace explorer...",
        {},
        escapeHtml
      );
    }

    try {
      await loadSnapshot(root);
    } catch (error) {
      const detail = detailRoot(root);
      if (navigation instanceof Element) {
        navigation.innerHTML = explorerEmptyStateMarkup(
          error instanceof Error ? error.message : "The Shared Workspace explorer could not be loaded.",
          { tone: "danger" },
          escapeHtml
        );
      }
      if (detail instanceof Element) {
        detail.innerHTML = explorerEmptyStateMarkup(
          "The Shared Workspace explorer is unavailable right now.",
          { tone: "danger" },
          escapeHtml
        );
      }
    }
  }

  async function handleClick(event, root) {
    const locationButton = event.target.closest(
      "[data-data-source-explorer-s3-location]"
    );
    if (locationButton && root.contains(locationButton)) {
      event.preventDefault();
      event.stopPropagation();
      await loadSnapshot(root, {
        bucket: locationButton.dataset.bucket || "",
        prefix: locationButton.dataset.prefix || "",
      });
      return true;
    }

    const fileButton = event.target.closest("[data-data-source-explorer-s3-file]");
    if (fileButton && root.contains(fileButton)) {
      event.preventDefault();
      event.stopPropagation();
      const state = explorerState(root);
      if (!state?.snapshot) {
        return true;
      }
      state.selectedEntry = (state.snapshot.entries || []).find(
        (entry) =>
          entry.entryKind === "file" &&
          String(entry.prefix || "") ===
            String(fileButton.dataset.dataSourceExplorerS3File || "").trim() &&
          String(entry.bucket || "") === String(fileButton.dataset.bucket || "").trim()
      ) || null;
      await render(root);
      return true;
    }

    const actionButton = event.target.closest("[data-data-source-explorer-action]");
    if (!(actionButton && root.contains(actionButton))) {
      return false;
    }

    event.preventDefault();
    event.stopPropagation();

    const action = String(
      actionButton.dataset.dataSourceExplorerAction || ""
    ).trim();
    const state = explorerState(root);
    if (!state?.snapshot) {
      return true;
    }

    if (action === "download") {
      const descriptor = selectedFileDescriptor(state);
      if (!(descriptor instanceof Element) || downloadSourceS3Object(descriptor) === false) {
        await showMessageDialog({
          title: "S3 download unavailable",
          copy: "Choose a concrete Shared Workspace object before downloading it.",
        });
      }
      return true;
    }

    if (action === "create-data-product") {
      const descriptor = selectedFileDescriptor(state);
      if (descriptor instanceof Element) {
        await openDataProductPublishDialog({
          sourceObjectRoot: descriptor,
        });
      }
      return true;
    }

    if (action === "publish-bucket") {
      await openDataProductPublishDialog({
        sourceSchemaRoot: sourceSchemaElement(state.snapshot.bucket || ""),
      });
      return true;
    }

    return false;
  }

  return {
    initialize,
    handleClick,
  };
}
