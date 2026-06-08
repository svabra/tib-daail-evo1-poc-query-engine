import {
  actionButtonMarkup,
  detailCardMarkup,
  explorerEmptyStateMarkup,
  publicationLinksMarkup,
  sourceActionMenuMarkup,
  sourceObjectElement,
  sourceObjectRowMarkup,
  sourcePublicationBadgeMarkup,
  sourceSchemaElement,
} from "./utils.js";

export function createS3DataSourceExplorer(helpers) {
  const {
    escapeHtml,
    fetchJsonOrThrow,
    formatByteCount,
    copySourceQueryPath,
    openDataProductPublishDialog,
    querySourceInCurrentNotebook,
    querySourceInNewNotebook,
    showMessageDialog,
    viewSourceData,
    downloadSourceObjectDdl,
    downloadSourceS3Object,
    downloadJobsController,
    prepareSourceS3Download,
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
    if (snapshot.queryPath) {
      return snapshot.queryPath;
    }
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
      relation: selectedEntry.relation,
      queryAlias: selectedEntry.queryAlias,
      queryReference: selectedEntry.queryReference || selectedEntry.queryPath,
      querySql: selectedEntry.querySql,
      name: selectedEntry.name,
      displayName: selectedEntry.displayName || selectedEntry.name,
      kind: "file",
      sourceOptionId: "workspace.s3",
      s3Bucket: selectedEntry.bucket,
      s3Key: selectedEntry.prefix,
      s3Path: selectedEntry.path,
      s3FileFormat: selectedEntry.fileFormat,
      s3Downloadable: true,
      sizeBytes: selectedEntry.sizeBytes,
      s3PartCount: selectedEntry.s3PartCount,
    });
  }

  function entrySecondaryText(entry) {
    if (entry.entryKind === "file") {
      return entry.queryReference || entry.queryAlias ? "" : "Not queryable yet";
    }
    return entry.queryPath || String(entry.entryKind || "").toUpperCase();
  }

  function publicationMenuItems(publishedProducts) {
    const normalizedProducts = Array.isArray(publishedProducts) ? publishedProducts : [];
    return normalizedProducts.map((product) => ({
      label: normalizedProducts.length === 1
        ? "Open Data Product"
        : `Open Data Product - ${product.title || product.slug || "source"}`,
      href: product.documentationPath || "",
      title: "Open the published Data Product page",
    }));
  }

  function fileActionMenuMarkup(entry) {
    const publishedItems = publicationMenuItems(entry.publishedDataProducts);
    const queryable = Boolean(String(entry.queryReference || entry.queryAlias || entry.relation || "").trim());
    const isCsv = String(entry.fileFormat || "").trim().toLowerCase() === "csv";
    return sourceActionMenuMarkup(
      [
        ...publishedItems,
        publishedItems.length ? "separator" : null,
        {
          label: "View Data",
          action: "view",
          attrs: { "data-view-source-data": true },
          title: queryable
            ? "Insert and run a query with all fields in the current notebook"
            : "This object is not queryable yet.",
          disabled: !queryable,
        },
        {
          label: "Query in current notebook",
          action: "query-current",
          attrs: { "data-query-source-current": true },
          title: queryable
            ? "Insert a query into the current notebook"
            : "This object is not queryable yet.",
          disabled: !queryable,
        },
        {
          label: "Query in new notebook",
          action: "query-new",
          attrs: { "data-query-source-new": true },
          title: queryable
            ? "Create a new notebook with this query"
            : "This object is not queryable yet.",
          disabled: !queryable,
        },
        {
          label: "Copy source reference",
          action: "copy-query-path",
          attrs: { "data-copy-query-path": true },
          title: queryable
            ? "Copy the SQL source reference for this object"
            : "This object is not queryable yet.",
          disabled: !queryable,
        },
        {
          label: "Create data product ...",
          action: "create-data-product",
          attrs: { "data-create-data-product": true },
          title: "Publish this object as a managed data product",
        },
        "separator",
        {
          label: "Download S3 object",
          action: "download",
          attrs: { "data-download-source-s3-object": true },
          title: "Download the underlying S3 object",
        },
        isCsv
          ? {
              label: "Prepare ZIP download",
              action: "prepare-zip",
              attrs: { "data-prepare-source-s3-download": true },
              title: "Prepare a resumable ZIP download for this CSV object",
            }
          : null,
        {
          label: "Download DDL",
          action: "download-ddl",
          attrs: { "data-download-source-ddl": true },
          title: "Download DDL for this source",
        },
      ],
      escapeHtml
    );
  }

  function locationActionMenuMarkup(entry) {
    const publishedItems = publicationMenuItems(entry.publishedDataProducts);
    const canPublishBucket = String(entry.entryKind || "").toLowerCase() === "bucket" && entry.bucket && !entry.prefix;
    return sourceActionMenuMarkup(
      [
        ...publishedItems,
        publishedItems.length ? "separator" : null,
        canPublishBucket
          ? {
              label: "Create data product ...",
              action: "publish-bucket",
              attrs: { "data-create-data-product-bucket": true },
              title: "Publish this bucket as a managed data product",
            }
          : null,
      ],
      escapeHtml,
      { label: "Location actions" }
    );
  }

  function fileRowMarkup(entry, state) {
    const queryPath = String(entry.queryReference || entry.queryAlias || entry.relation || "").trim();
    const displayName = entry.displayName || entry.name || "";
    const fileFormat = String(entry.fileFormat || "file").toUpperCase();
    return sourceObjectRowMarkup(
      {
        kind: "file",
        displayName,
        title: `${displayName}${queryPath ? ` | Source reference: ${queryPath}` : ""}`,
        searchable: `${displayName} ${entry.name || ""} ${queryPath} ${entry.path || ""} ${fileFormat}`,
        selected:
          state.selectedEntry?.bucket === entry.bucket &&
          state.selectedEntry?.prefix === entry.prefix,
        attrs: {
          "data-source-object": true,
          "data-source-object-kind": "file",
          "data-source-object-name": entry.name || "",
          "data-source-object-display-name": displayName,
          "data-source-object-relation": entry.relation || "",
          "data-source-object-query-alias": entry.queryAlias || "",
          "data-source-object-query-reference": entry.queryReference || entry.queryPath || "",
          "data-source-object-query-sql": entry.querySql || "",
          "data-source-option-id": "workspace.s3",
          "data-s3-bucket": entry.bucket || "",
          "data-s3-key": entry.prefix || "",
          "data-s3-path": entry.path || "",
          "data-s3-file-format": entry.fileFormat || "",
          "data-s3-downloadable": "true",
          "data-s3-size-bytes": entry.sizeBytes || 0,
          "data-s3-part-count": entry.s3PartCount || 0,
          "data-data-source-explorer-s3-file": entry.prefix || "",
          "data-bucket": entry.bucket || "",
          "data-prefix": entry.prefix || "",
          "data-entry-kind": entry.entryKind || "",
          "data-published-data-products": JSON.stringify(entry.publishedDataProducts || []),
        },
        meta: `
          ${sourcePublicationBadgeMarkup(entry.publishedDataProducts, escapeHtml)}
          ${
            queryPath
              ? `<small class="source-query-path-label" title="${escapeHtml(`Source reference: ${queryPath}`)}">${escapeHtml(queryPath)}</small>`
              : `<small>${escapeHtml(entrySecondaryText(entry))}</small>`
          }
          <small>${escapeHtml(fileFormat)}</small>
          <small>${escapeHtml(formatByteCount(entry.sizeBytes))}</small>
        `,
        labelExtras: downloadJobsController?.s3IndicatorMarkup?.(entry.bucket, entry.prefix) || "",
        actions: fileActionMenuMarkup(entry),
      },
      escapeHtml
    );
  }

  function locationRowMarkup(entry) {
    const kind = String(entry.entryKind || "").toLowerCase() === "bucket" ? "bucket" : "folder";
    const title = entry.queryPath || entry.path || entry.name || "";
    return sourceObjectRowMarkup(
      {
        kind,
        displayName: entry.name || "",
        title,
        searchable: `${entry.name || ""} ${entry.queryPath || ""} ${entry.path || ""} ${entry.entryKind || ""}`,
        attrs: {
          "data-data-source-explorer-s3-location": true,
          "data-bucket": entry.bucket || "",
          "data-prefix": entry.prefix || "",
          "data-entry-kind": entry.entryKind || "",
        },
        meta: `
          ${sourcePublicationBadgeMarkup(entry.publishedDataProducts, escapeHtml)}
          ${entrySecondaryText(entry) ? `<small>${escapeHtml(entrySecondaryText(entry))}</small>` : ""}
        `,
        actions: locationActionMenuMarkup(entry),
        extraClass: "data-source-explorer-location-object",
      },
      escapeHtml
    );
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
      <div class="source-tree data-source-explorer-source-tree">
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
                  ${escapeHtml(breadcrumb.queryLabel || breadcrumb.label || "s3")}
                </button>
              `
            )
            .join("")}
        </div>
        ${
          (snapshot.entries || []).length
            ? `
                <ul class="source-object-list data-source-explorer-root-object-list">
                  ${snapshot.entries
                    .map((entry) =>
                      entry.entryKind === "file"
                        ? fileRowMarkup(entry, state)
                        : locationRowMarkup(entry)
                    )
                    .join("")}
                </ul>
              `
            : explorerEmptyStateMarkup(
                snapshot.emptyMessage || "This Shared Workspace location is empty.",
                {},
                escapeHtml
              )
        }
      </div>
    `;
    downloadJobsController?.syncPreparedDownloadIndicators?.(navigation);
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
      const queryReference = String(
        state.selectedEntry.queryReference ||
          state.selectedEntry.queryPath ||
          state.selectedEntry.queryAlias ||
          ""
      ).trim();
      const canPrepareZip = String(state.selectedEntry.fileFormat || "").trim().toLowerCase() === "csv";
      const preparedZipJob = downloadJobsController?.jobForS3Object?.(
        state.selectedEntry.bucket,
        state.selectedEntry.prefix
      );
      const canDownloadPreparedZip =
        preparedZipJob?.status === "ready" && Boolean(preparedZipJob.downloadUrl);
      detail.innerHTML = detailCardMarkup(
        {
          eyebrow: `${snapshot.bucket} • ${String(state.selectedEntry.fileFormat || "file").toUpperCase()}`,
          title: queryReference || state.selectedEntry.name || "Selected object",
          copy: queryReference
            ? `Use this source reference in SQL. Raw object: ${state.selectedEntry.path || ""}.`
            : `This object is visible in S3 but is not queryable yet. Raw object: ${state.selectedEntry.path || ""}.`,
          actions: [
            actionButtonMarkup("Copy source reference", "copy-query-path", escapeHtml, {
              disabled: !queryReference,
              title: queryReference
                ? "Copy the SQL source reference for this object"
                : "This object is not queryable yet.",
            }),
            actionButtonMarkup("Download", "download", escapeHtml),
            canPrepareZip ? actionButtonMarkup("Prepare ZIP download", "prepare-zip", escapeHtml) : "",
            canDownloadPreparedZip
              ? `<button
                  type="button"
                  class="data-source-explorer-action"
                  data-download-job-download="${escapeHtml(preparedZipJob.jobId)}"
                >
                  Download prepared ZIP file
                </button>`
              : "",
            actionButtonMarkup("Download DDL", "download-ddl", escapeHtml),
            actionButtonMarkup("Create Data Product ...", "create-data-product", escapeHtml),
          ].join(""),
          body: `
            ${publicationLinksMarkup(state.selectedEntry.publishedDataProducts, escapeHtml)}
            ${downloadJobsController?.s3IndicatorMarkup?.(state.selectedEntry.bucket, state.selectedEntry.prefix) || ""}
            <ul class="sidebar-source-field-list">
              <li class="sidebar-source-field">
                <span class="sidebar-source-field-name"><span class="sidebar-source-field-name-text">Source reference</span></span>
                <span class="sidebar-source-field-type">${escapeHtml(queryReference || "Not queryable yet")}</span>
              </li>
              <li class="sidebar-source-field">
                <span class="sidebar-source-field-name"><span class="sidebar-source-field-name-text">Storage path</span></span>
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
              <span class="sidebar-source-field-name"><span class="sidebar-source-field-name-text">Query hierarchy</span></span>
              <span class="sidebar-source-field-type">${escapeHtml(currentLocationCopy(snapshot))}</span>
            </li>
            <li class="sidebar-source-field">
              <span class="sidebar-source-field-name"><span class="sidebar-source-field-name-text">Storage location</span></span>
              <span class="sidebar-source-field-type">${escapeHtml(snapshot.path || "")}</span>
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

  async function selectFile(root, fileButton, { renderAfter = true } = {}) {
    const state = explorerState(root);
    if (!state?.snapshot) {
      return;
    }
    state.selectedEntry = (state.snapshot.entries || []).find(
      (entry) =>
        entry.entryKind === "file" &&
        String(entry.prefix || "") ===
          String(fileButton.dataset.dataSourceExplorerS3File || "").trim() &&
        String(entry.bucket || "") === String(fileButton.dataset.bucket || "").trim()
    ) || null;
    if (renderAfter) {
      await render(root);
    }
  }

  async function handleClick(event, root) {
    if (downloadJobsController?.handleClick && (await downloadJobsController.handleClick(event))) {
      event.stopPropagation();
      return true;
    }

    const actionButton = event.target.closest("[data-data-source-explorer-action]");
    if (actionButton && root.contains(actionButton)) {
      event.preventDefault();
      event.stopPropagation();

      const actionFile = actionButton.closest("[data-data-source-explorer-s3-file]");
      if (actionFile && root.contains(actionFile)) {
        await selectFile(root, actionFile, { renderAfter: true });
      }

      const state = explorerState(root);
      if (!state?.snapshot) {
        return true;
      }

      const action = String(
        actionButton.dataset.dataSourceExplorerAction || ""
      ).trim();

      if (action === "view") {
        const descriptor = selectedFileDescriptor(state);
        const viewed =
          descriptor instanceof Element ? await viewSourceData?.(descriptor) : false;
        if (viewed === false) {
          await showMessageDialog({
            title: "Notebook required",
            copy: "Open an editable notebook first, or use 'Query In New Notebook'.",
          });
        }
        return true;
      }

      if (action === "query-current") {
        const descriptor = selectedFileDescriptor(state);
        const inserted =
          descriptor instanceof Element ? await querySourceInCurrentNotebook?.(descriptor) : false;
        if (inserted === false) {
          await showMessageDialog({
            title: "Notebook required",
            copy: "Open an editable notebook first, or use 'Query In New Notebook'.",
          });
        }
        return true;
      }

      if (action === "query-new") {
        const descriptor = selectedFileDescriptor(state);
        if (descriptor instanceof Element) {
          await querySourceInNewNotebook?.(descriptor);
        }
        return true;
      }

      if (action === "copy-query-path") {
        const descriptor = selectedFileDescriptor(state);
        if (!(descriptor instanceof Element) || (await copySourceQueryPath?.(descriptor)) === false) {
          await showMessageDialog({
            title: "Source reference unavailable",
            copy: "This Shared Workspace object does not expose a source reference yet.",
          });
        }
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

      if (action === "download-ddl") {
        const descriptor = selectedFileDescriptor(state);
        if (!(descriptor instanceof Element) || (await downloadSourceObjectDdl(descriptor)) === false) {
          await showMessageDialog({
            title: "DDL download unavailable",
            copy: "Choose a concrete Shared Workspace object before downloading DDL.",
          });
        }
        return true;
      }

      if (action === "prepare-zip") {
        const descriptor = selectedFileDescriptor(state);
        if (!(descriptor instanceof Element) || (await prepareSourceS3Download?.(descriptor)) === false) {
          await showMessageDialog({
            title: "Prepared ZIP unavailable",
            copy: "Choose a concrete CSV object before preparing a ZIP download.",
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
        const locationButton = actionButton.closest("[data-data-source-explorer-s3-location]");
        await openDataProductPublishDialog({
          sourceSchemaRoot: sourceSchemaElement(
            locationButton?.dataset.bucket || state.snapshot.bucket || ""
          ),
        });
        return true;
      }

      return false;
    }

    if (event.target.closest("[data-source-action-menu]")) {
      return false;
    }

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
      await selectFile(root, fileButton);
      return true;
    }

    return false;
  }

  return {
    initialize,
    handleClick,
  };
}
