export function createIngestionUi(helpers) {
  const {
    dataGenerationJobCompletedCopy,
    dataGenerationJobCopy,
    dataGenerationJobElapsedMs,
    dataGenerationJobEventDateTimeCopy,
    dataGenerationJobIsRunning,
    dataGenerationJobStartedCopy,
    dataGenerationJobStatusCopy,
    dataGenerationJobTimingCopy,
    escapeHtml,
    formatDataGenerationSize,
    formatQueryDuration,
    getSpotlightIngestionRunbookId,
    notebookUrl,
    resolveSelectedIngestionRunbookId,
  } = helpers;

  function dataGeneratorCardMarkup(generator) {
    const isSelected = generator.generatorId === resolveSelectedIngestionRunbookId();
    const isSpotlighted = generator.generatorId === getSpotlightIngestionRunbookId();
    const tagsMarkup = (generator.tags || [])
      .map((tag) => `<span class="ingestion-generator-tag">${escapeHtml(tag)}</span>`)
      .join("");
    const linkedNotebooks = Array.isArray(generator.linkedNotebooks)
      ? generator.linkedNotebooks
      : [];
    const linkedNotebookMarkup = linkedNotebooks.length
      ? `
        <div class="ingestion-linked-notebooks">
          <span class="ingestion-linked-notebooks-label">${
            linkedNotebooks.length === 1 ? "Linked notebook" : "Linked notebooks"
          }</span>
          <div class="ingestion-linked-notebooks-list">
            ${linkedNotebooks
              .map(
                (notebook) => `
                  <a
                    href="${escapeHtml(notebookUrl(notebook.notebookId) || "#")}"
                    class="ingestion-linked-notebook"
                    data-open-query-notebook="${escapeHtml(notebook.notebookId)}"
                    title="Open ${escapeHtml(notebook.title)}"
                  >${escapeHtml(notebook.title)}</a>
                `
              )
              .join("")}
          </div>
        </div>
      `
      : "";
    const downloadableFiles = Array.isArray(generator.downloadableFiles)
      ? generator.downloadableFiles
      : [];
    const downloadableFilesMarkup = downloadableFiles.length
      ? `
        <section class="ingestion-manual-upload" aria-label="Manueller CSV-Schritt">
          <div class="ingestion-manual-upload-heading">
            <span class="ingestion-manual-upload-step">Live-Demo</span>
            <h5>CSV manuell einlesen</h5>
          </div>
          <ol>
            ${downloadableFiles
              .map(
                (file) => `
                  <li>
                    <a
                      class="ingestion-manual-download"
                      href="${escapeHtml(file.downloadUrl)}"
                      download="${escapeHtml(file.fileName || "")}"
                      data-loader-downloadable-file
                    >${escapeHtml(file.label || file.fileName || "CSV-Datei herunterladen")}</a>
                  </li>
                  <li>
                    Im <a href="/ingestion-workbench/manual">Ingestion Workbench</a>
                    hochladen nach
                    <span class="ingestion-manual-target">
                      <button
                        type="button"
                        class="ingestion-manual-target-copy"
                        data-copy-loader-target-path="${escapeHtml(file.targetPath)}"
                        data-copy-success-message="Copied to Clipbard"
                        aria-label="Speicherpfad in die Zwischenablage kopieren: ${escapeHtml(file.targetPath)}"
                        title="Speicherpfad kopieren"
                      ><code>${escapeHtml(file.targetPath)}</code></button>
                      <span
                        class="ingestion-manual-copy-feedback"
                        data-loader-target-copy-feedback
                        role="status"
                        aria-live="polite"
                        aria-atomic="true"
                      ></span>
                    </span>.
                    ${
                      file.replaceExisting
                        ? "Ein erneuter Upload ersetzt nur diese manuelle Datei."
                        : ""
                    }
                    ${
                      file.storageFormat
                        ? `
                          <div
                            class="ingestion-manual-format"
                            data-loader-required-storage-format="${escapeHtml(file.storageFormat)}"
                          >
                            <strong>Erforderliches Speicherformat:</strong>
                            <span class="ingestion-manual-format-badge">${escapeHtml(
                              file.storageFormatLabel || file.storageFormat.toUpperCase()
                            )}</span>
                            <span>${escapeHtml(
                              file.storageFormatInstruction ||
                                "Die Datei in diesem Format speichern, damit das verknüpfte Notebook sie lesen kann."
                            )}</span>
                          </div>
                        `
                        : ""
                    }
                  </li>
                `
              )
              .join("")}
          </ol>
        </section>
      `
      : "";

    return `
      <article class="ingestion-generator-card${isSelected ? " is-selected" : ""}${isSpotlighted ? " is-spotlighted" : ""}" data-generator-card data-generator-id="${escapeHtml(generator.generatorId)}">
        <div class="ingestion-generator-card-header">
          <div class="ingestion-generator-copy">
            <h4>${escapeHtml(generator.title)}</h4>
            <p class="ingestion-generator-description">${escapeHtml(generator.description)}</p>
          </div>
          <div class="ingestion-generator-tags">
            <span class="ingestion-generator-tag">${escapeHtml(generator.targetKind.toUpperCase())}</span>
            ${tagsMarkup}
          </div>
        </div>
        ${downloadableFilesMarkup}
        <div class="ingestion-generator-controls">
          <div class="ingestion-generator-size">
            <label for="generator-size-${escapeHtml(generator.generatorId)}">Generate size</label>
            <div class="ingestion-generator-size-input">
              <input
                id="generator-size-${escapeHtml(generator.generatorId)}"
                class="modal-input"
                type="number"
                min="${escapeHtml(generator.minSizeGb)}"
                max="${escapeHtml(generator.maxSizeGb)}"
                step="0.01"
                value="${escapeHtml(generator.defaultSizeGb)}"
                data-ingestion-size-input
              >
              <span class="ingestion-generator-size-unit" aria-hidden="true">GB</span>
            </div>
          </div>
          <button
            type="button"
            class="modal-button ingestion-generator-run"
            data-start-data-generation="${escapeHtml(generator.generatorId)}"
          >
            Run Module
          </button>
          <p class="ingestion-job-meta-copy">
            Default target: <strong>${escapeHtml(generator.defaultTargetName || generator.generatorId)}</strong><br>
            Module: <code>${escapeHtml(generator.moduleName || generator.generatorId)}</code>
          </p>
          ${linkedNotebookMarkup}
        </div>
      </article>
    `;
  }

  function dataGenerationJobFactsMarkup(job) {
    const facts = [
      ["Target", job.targetKind.toUpperCase()],
      ["Requested size", formatDataGenerationSize(job.requestedSizeGb)],
      ["Started", dataGenerationJobStartedCopy(job)],
      ["Ended", dataGenerationJobCompletedCopy(job)],
      ["Elapsed", formatQueryDuration(dataGenerationJobElapsedMs(job))],
      ["Generated size", formatDataGenerationSize(job.generatedSizeGb || 0)],
      [
        "Rows",
        Number(job.generatedRows || 0) > 0
          ? Number(job.generatedRows).toLocaleString()
          : "0",
      ],
    ];

    return `
      <div class="ingestion-job-facts">
        ${facts
          .map(
            ([label, value]) => `
              <div class="ingestion-job-fact">
                <span class="ingestion-job-fact-label">${escapeHtml(label)}</span>
                <span class="ingestion-job-fact-value${label === "Elapsed" ? " ingestion-job-fact-value-live" : ""}"${
                  label === "Elapsed"
                    ? ` data-ingestion-job-duration data-job-id="${escapeHtml(job.jobId)}"`
                    : ""
                }>${escapeHtml(String(value))}</span>
              </div>
            `
          )
          .join("")}
      </div>
    `;
  }

  function dataGenerationJobProgressMarkup(job) {
    const progressValue =
      typeof job.progress === "number" && Number.isFinite(job.progress)
        ? Math.max(0, Math.min(100, job.progress * 100))
        : null;
    const showTrack = dataGenerationJobIsRunning(job) || progressValue !== null;

    return `
      <div class="ingestion-job-progress">
        <div class="ingestion-job-progress-header">
          <strong>${escapeHtml(job.progressLabel || dataGenerationJobStatusCopy(job))}</strong>
        </div>
        ${
          showTrack
            ? `
              <div class="ingestion-job-progress-track${progressValue === null ? " is-indeterminate" : ""}">
                <span style="${progressValue === null ? "" : `width:${progressValue}%;`}"></span>
              </div>
            `
            : ""
        }
      </div>
    `;
  }

  function dataGenerationTargetStatusCopy(status) {
    const normalizedStatus = String(status ?? "").trim().toLowerCase();
    if (normalizedStatus === "written") {
      return "Written";
    }
    if (normalizedStatus === "writing") {
      return "Writing";
    }
    if (normalizedStatus === "cleaned") {
      return "Cleaned";
    }
    return "Pending";
  }

  function dataGenerationTargetSummary(job) {
    const targets = Array.isArray(job.writtenTargets) ? job.writtenTargets : [];
    if (!targets.length) {
      return "";
    }

    const finalizedCount = targets.filter((target) =>
      ["written", "cleaned"].includes(
        String(target.status ?? "").trim().toLowerCase()
      )
    ).length;
    const pendingCount = targets.length - finalizedCount;
    if (pendingCount > 0) {
      return `${finalizedCount}/${targets.length} targets materialized`;
    }
    return `${targets.length} targets materialized`;
  }

  function dataGenerationJobTargetDetailsMarkup(job) {
    const targets = Array.isArray(job.writtenTargets) ? job.writtenTargets : [];
    if (!targets.length) {
      return "";
    }

    const summary = dataGenerationTargetSummary(job);
    return `
      <details class="ingestion-job-target-details" data-ingestion-job-target-details>
        <summary class="ingestion-job-target-details-summary">
          <span class="ingestion-job-target-details-title">Write targets</span>
          <span class="ingestion-job-target-details-count">${escapeHtml(summary)}</span>
        </summary>
        <div class="ingestion-job-target-details-list">
          ${targets
            .map(
              (target) => `
                <div class="ingestion-job-target-detail">
                  <div class="ingestion-job-target-detail-header">
                    <span class="ingestion-job-target-detail-label">${escapeHtml(target.label || target.location)}</span>
                    <span class="ingestion-job-target-detail-status ingestion-job-target-detail-status-${escapeHtml(
                      String(target.status ?? "pending").trim().toLowerCase() ||
                        "pending"
                    )}">${escapeHtml(dataGenerationTargetStatusCopy(target.status))}</span>
                  </div>
                  <code class="ingestion-job-target-detail-location">${escapeHtml(target.location)}</code>
                </div>
              `
            )
            .join("")}
        </div>
      </details>
    `;
  }

  function dataGenerationJobCardMarkup(job) {
    const statusClass = String(job.status || "unknown").trim().toLowerCase().replace(/[^a-z0-9]+/g, "-") || "unknown";
    return `
      <article class="ingestion-job-card" data-data-generation-job-card data-job-id="${escapeHtml(job.jobId)}">
        <div class="ingestion-job-card-header">
          <div class="ingestion-job-copy">
            <h4>${escapeHtml(job.title)}</h4>
            <p class="ingestion-job-description">${escapeHtml(job.description)}</p>
          </div>
          <div class="ingestion-job-actions">
            <span class="ingestion-job-status is-${escapeHtml(statusClass)}${dataGenerationJobIsRunning(job) ? " is-live" : ""}">${escapeHtml(
              dataGenerationJobStatusCopy(job)
            )}</span>
            ${
              dataGenerationJobIsRunning(job) && job.canCancel
                ? `<button type="button" class="ingestion-job-cancel" data-cancel-data-generation-job="${escapeHtml(job.jobId)}">Cancel</button>`
                : ""
            }
            ${
              job.canCleanup
                ? `<button type="button" class="ingestion-job-clean" data-cleanup-data-generation-job="${escapeHtml(job.jobId)}">Clean loader data</button>`
                : ""
            }
          </div>
        </div>
        ${dataGenerationJobProgressMarkup(job)}
        ${dataGenerationJobFactsMarkup(job)}
        ${dataGenerationJobTargetDetailsMarkup(job)}
        <p class="ingestion-job-message">${escapeHtml(job.message || "")}</p>
        ${job.error ? `<pre class="ingestion-job-error">${escapeHtml(job.error)}</pre>` : ""}
      </article>
    `;
  }

  function dataGenerationNotificationItemMarkup(job) {
    const statusClass = String(job.status || "unknown").trim().toLowerCase().replace(/[^a-z0-9]+/g, "-") || "unknown";
    return `
      <button
        type="button"
        class="topbar-notification-item"
        data-open-loader-workbench
        data-focus-generation-job="${escapeHtml(job.jobId)}"
        title="Open the Loader Workbench"
      >
        <span class="topbar-notification-item-status topbar-notification-item-status-notice is-${escapeHtml(statusClass)}${dataGenerationJobIsRunning(job) ? " is-live" : ""}">${escapeHtml(
          `${dataGenerationJobStatusCopy(job)} loader`
        )}</span>
        <span class="topbar-notification-item-title">${escapeHtml(job.title)}</span>
        <span class="topbar-notification-item-copy" data-data-generation-notification-copy data-job-id="${escapeHtml(
          job.jobId
        )}">${escapeHtml(dataGenerationJobCopy(job))}</span>
        <span class="topbar-notification-item-copy topbar-notification-item-copy-secondary">${escapeHtml(dataGenerationJobTimingCopy(job))}</span>
        <span class="topbar-notification-item-copy topbar-notification-item-copy-secondary">${escapeHtml(
          dataGenerationJobEventDateTimeCopy(job)
        )}</span>
      </button>
    `;
  }

  function dataGenerationMonitorItemMarkup(job) {
    const running = dataGenerationJobIsRunning(job);
    const sizeCopy = formatDataGenerationSize(
      job.generatedSizeGb || job.requestedSizeGb
    );
    const rowsCopy =
      Number(job.generatedRows || 0) > 0
        ? `${Number(job.generatedRows).toLocaleString()} rows`
        : "No rows yet";
    const summaryCopy =
      job.targetRelation || job.targetPath || job.message || job.description || "";

    return `
      <article class="query-monitor-item query-monitor-item-${escapeHtml(job.status)}" data-data-generation-job-id="${escapeHtml(job.jobId)}">
        <div class="query-monitor-item-copy">
          <button
            type="button"
            class="query-monitor-open"
            data-open-loader-workbench
            data-focus-generation-job="${escapeHtml(job.jobId)}"
            title="Open ${escapeHtml(job.title)}"
          >
            ${escapeHtml(job.title)}
          </button>
          <div class="query-monitor-item-meta">
            <span class="query-monitor-status-badge${running ? " is-live" : ""}">${escapeHtml(
              dataGenerationJobStatusCopy(job)
            )}</span>
            <span data-generation-monitor-duration data-job-id="${escapeHtml(job.jobId)}">${escapeHtml(
              formatQueryDuration(dataGenerationJobElapsedMs(job))
            )}</span>
            <span>${escapeHtml(sizeCopy)}</span>
            <span>${escapeHtml(rowsCopy)}</span>
          </div>
          <p class="query-monitor-sql">${escapeHtml(summaryCopy)}</p>
        </div>
        <div class="query-monitor-item-actions">
          ${
            running
              ? job.canCancel
                ? `<button type="button" class="query-monitor-cancel" data-cancel-data-generation-job="${escapeHtml(job.jobId)}">Cancel</button>`
                : `<span class="query-monitor-cancel is-cancelling">Cancelling</span>`
              : ""
          }
          <div class="query-monitor-timestamps">
            <span class="query-monitor-updated">Start ${escapeHtml(dataGenerationJobStartedCopy(job))}</span>
            <span class="query-monitor-updated">End ${escapeHtml(dataGenerationJobCompletedCopy(job))}</span>
          </div>
        </div>
      </article>
    `;
  }

  return {
    dataGeneratorCardMarkup,
    dataGenerationJobCardMarkup,
    dataGenerationMonitorItemMarkup,
    dataGenerationNotificationItemMarkup,
  };
}
