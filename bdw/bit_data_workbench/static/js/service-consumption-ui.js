const CHART_JS_ASSET_PATH = "/static/chartjs/chart.umd.min.js";
const DEFAULT_WINDOW = "24h";
const openServiceKeys = new Set();
const WINDOW_REFRESH_INTERVAL_MS = {
  "15m": 3000,
  "1h": 15000,
  "6h": 60000,
  "24h": 300000,
  "48h": 600000,
};
const METRIC_COLORS = {
  cpu: "#d52b1e",
  memory: "#0b4479",
  s3: "#1c6a48",
  pv: "#8a5a17",
  node: "#7a8c9e",
  actualPrevious: "#6b7f93",
  actual: "#1c6a48",
  plan: "#0b4479",
  forecast: "#b96b00",
};
const SERVICE_CATEGORY_COLORS = {
  container: "#d52b1e",
  application: "#0b4479",
  filesystem: "#8a5a17",
  s3: "#1c6a48",
  pg: "#6b7f93",
  fallback: "#7a8c9e",
};
const LEGEND_TOOLTIPS = {
  cpu: {
    "Workbench service":
      "CPU cores used by the running workbench container, derived from the delta of cumulative cgroup CPU time across the sampling interval.",
    "Current node":
      "CPU cores used on the Kubernetes node that currently hosts the workbench pod, read from the metrics.k8s.io node usage endpoint.",
  },
  memory: {
    "Workbench service":
      "Memory currently charged to the running workbench container cgroup, read from the container memory usage counter.",
    "Current node":
      "Memory currently used on the Kubernetes node that currently hosts the workbench pod, read from the metrics.k8s.io node usage endpoint.",
  },
  s3: {
    "Visible buckets":
      "Total object bytes across all S3 buckets that the current workbench credentials can enumerate. This value refreshes hourly and is reused between hourly samples.",
  },
  persistentVolume: {
    "Mounted volume":
      "Occupied bytes under the mounted service-consumption storage path inside the running workbench pod. This tracks persisted monitor history stored on the PVC.",
  },
};

function financialLegendTooltips(currentYear, comparisonYear) {
  const tooltips = {
    [`Actual Spend ${currentYear}`]:
      "Estimated cumulative CHF burn for the current UTC year, derived from measured usage and the configured rate card.",
    [`Planned Budget ${currentYear}`]:
      "Linear cumulative budget plan across the current UTC year based on the shared annual app budget.",
    [`Forecast ${currentYear}`]:
      "Projected cumulative CHF burn for the remaining months, based on the rolling last 30 days average daily spend.",
  };
  if (comparisonYear) {
    tooltips[`Actual Spend ${comparisonYear}`] =
      "Mocked cumulative CHF burn for the prior client year, shown as a comparison line on the same annual chart.";
  }
  return tooltips;
}

function serviceMixLegendTooltips(services) {
  const tooltips = {};
  (Array.isArray(services) ? services : []).forEach((service) => {
    const label = String(service?.label || "").trim();
    if (!label) {
      return;
    }
    tooltips[label] =
      service?.subtitle ||
      "Estimated spend year-to-date for this service category.";
  });
  return tooltips;
}

function pageRoot() {
  return document.querySelector("[data-service-consumption-page]");
}

function technicalSummaryValue(root, metric) {
  return root?.querySelector?.(
    `[data-service-consumption-summary="${metric}"] [data-summary-value]`
  );
}

function technicalSummaryMeta(root, metric) {
  return root?.querySelector?.(
    `[data-service-consumption-summary="${metric}"] [data-summary-meta]`
  );
}

function financialSummaryValue(root, metric) {
  return root?.querySelector?.(
    `[data-service-consumption-financial-summary="${metric}"] [data-summary-value]`
  );
}

function financialSummaryMeta(root, metric) {
  return root?.querySelector?.(
    `[data-service-consumption-financial-summary="${metric}"] [data-summary-meta]`
  );
}

function breakdownValue(root, metric) {
  return root?.querySelector?.(
    `[data-service-consumption-breakdown="${metric}"] [data-summary-value]`
  );
}

function breakdownMeta(root, metric) {
  return root?.querySelector?.(
    `[data-service-consumption-breakdown="${metric}"] [data-summary-meta]`
  );
}

function financialStatusNode() {
  return document.querySelector("[data-service-consumption-financial-status]");
}

function pageStatusNode() {
  return document.querySelector("[data-service-consumption-node-status]");
}

function pageStatusS3() {
  return document.querySelector("[data-service-consumption-s3-status]");
}

function pageStatusPersistentVolume() {
  return document.querySelector("[data-service-consumption-pv-status]");
}

function chartLimitCopy(metric) {
  return document.querySelector(`[data-service-consumption-chart-limit="${metric}"]`);
}

function sampledAtNode() {
  return document.querySelector("[data-service-consumption-sampled-at]");
}

function topologyValue(field) {
  return document.querySelector(`[data-service-consumption-topology-${field}]`);
}

function budgetYearNode() {
  return document.querySelector("[data-service-consumption-budget-year]");
}

function budgetInputNode() {
  return document.querySelector("[data-service-consumption-budget-input]");
}

function budgetSaveButton() {
  return document.querySelector("[data-service-consumption-budget-save]");
}

function budgetStatusNode() {
  return document.querySelector("[data-service-consumption-budget-status]");
}

function budgetHighlightValueNode() {
  return document.querySelector("[data-service-consumption-budget-highlight-value]");
}

function budgetHighlightValueMetaNode() {
  return document.querySelector("[data-service-consumption-budget-highlight-value-meta]");
}

function budgetHighlightUsedNode() {
  return document.querySelector("[data-service-consumption-budget-highlight-used]");
}

function budgetHighlightUsedMetaNode() {
  return document.querySelector("[data-service-consumption-budget-highlight-used-meta]");
}

function budgetHighlightRemainingNode() {
  return document.querySelector("[data-service-consumption-budget-highlight-remaining]");
}

function budgetHighlightRemainingMetaNode() {
  return document.querySelector("[data-service-consumption-budget-highlight-remaining-meta]");
}

function budgetProgressBarNode() {
  return document.querySelector("[data-service-consumption-budget-progress-bar]");
}

function budgetProgressCopyNode() {
  return document.querySelector("[data-service-consumption-budget-progress-copy]");
}

function budgetFormNode() {
  return document.querySelector("[data-service-consumption-budget-form]");
}

function serviceListNode() {
  return document.querySelector("[data-service-consumption-services]");
}

function financialChartCanvas() {
  return document.querySelector("[data-service-consumption-financial-chart]");
}

function financialServiceMixChartCanvas() {
  return document.querySelector("[data-service-consumption-financial-service-mix-chart]");
}

function cpuChartCanvas() {
  return document.querySelector("[data-service-consumption-cpu-chart]");
}

function memoryChartCanvas() {
  return document.querySelector("[data-service-consumption-memory-chart]");
}

function s3ChartCanvas() {
  return document.querySelector("[data-service-consumption-s3-chart]");
}

function persistentVolumeChartCanvas() {
  return document.querySelector("[data-service-consumption-pv-chart]");
}

function financialLegendContainer() {
  return document.querySelector("[data-service-consumption-financial-legend]");
}

function financialServiceMixLegendContainer() {
  return document.querySelector(
    "[data-service-consumption-financial-service-mix-legend]"
  );
}

function cpuLegendContainer() {
  return document.querySelector("[data-service-consumption-cpu-legend]");
}

function memoryLegendContainer() {
  return document.querySelector("[data-service-consumption-memory-legend]");
}

function s3LegendContainer() {
  return document.querySelector("[data-service-consumption-s3-legend]");
}

function persistentVolumeLegendContainer() {
  return document.querySelector("[data-service-consumption-pv-legend]");
}

function windowButtons() {
  return Array.from(document.querySelectorAll("[data-service-consumption-window]"));
}

function formatTimestampLabel(value, { short = false } = {}) {
  const date = new Date(value || "");
  if (Number.isNaN(date.getTime())) {
    return "";
  }
  return new Intl.DateTimeFormat("en-CH", {
    month: "short",
    day: "2-digit",
    hour: short ? undefined : "2-digit",
    minute: short ? undefined : "2-digit",
    timeZone: "UTC",
  }).format(date);
}

function formatSampledAt(value, prefix = "Latest sample") {
  const date = new Date(value || "");
  if (Number.isNaN(date.getTime())) {
    return "No samples yet.";
  }
  return `${prefix} ${new Intl.DateTimeFormat("en-CH", {
    year: "numeric",
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    timeZone: "UTC",
  }).format(date)} UTC`;
}

function normalizeNumericSeries(values = []) {
  return values.map((value) => {
    if (value === null || value === undefined || value === "") {
      return null;
    }
    const normalized = Number(value);
    return Number.isFinite(normalized) ? normalized : null;
  });
}

function metricPath(source, path) {
  return path.reduce((current, segment) => {
    if (!current || typeof current !== "object") {
      return null;
    }
    return current[segment] ?? null;
  }, source);
}

function formatCpuValue(value) {
  if (value === null || value === undefined) {
    return "Unavailable";
  }
  const normalized = Number(value);
  if (!Number.isFinite(normalized)) {
    return "Unavailable";
  }
  return `${normalized.toFixed(normalized >= 10 ? 1 : 2)} cores`;
}

function formatMemoryLimitValue(value) {
  if (value === null || value === undefined) {
    return "Unavailable";
  }
  const normalized = Number(value);
  if (!Number.isFinite(normalized) || normalized < 0) {
    return "Unavailable";
  }

  const gib = 1024 ** 3;
  const mib = 1024 ** 2;
  if (normalized >= gib) {
    const valueGiB = normalized / gib;
    return `${valueGiB.toFixed(valueGiB >= 10 ? 0 : 1)} GB RAM`;
  }
  if (normalized >= mib) {
    const valueMiB = normalized / mib;
    return `${valueMiB.toFixed(valueMiB >= 10 ? 0 : 1)} MB RAM`;
  }
  return `${normalized} B RAM`;
}

function formatPercent(value) {
  if (value === null || value === undefined) {
    return "Unavailable";
  }
  const normalized = Number(value);
  if (!Number.isFinite(normalized)) {
    return "Unavailable";
  }
  return `${normalized.toFixed(1)}%`;
}

function formatBytesOrUnavailable(value, formatByteCount) {
  if (value === null || value === undefined) {
    return "Unavailable";
  }
  return formatByteCount(value);
}

function formatCurrentAndMax(current, maxValue, formatter) {
  const formattedCurrent = formatter(current);
  if (formattedCurrent === "Unavailable") {
    return "Unavailable";
  }
  const formattedMax = formatter(maxValue);
  if (formattedMax === "Unavailable") {
    return formattedCurrent;
  }
  const currentText = formattedCurrent.replace(/\s+(cores?)$/, "");
  return `${currentText} / ${formattedMax}`;
}

function formatChfValue(value, { whenNull = "Unavailable" } = {}) {
  if (value === null || value === undefined) {
    return whenNull;
  }
  const normalized = Number(value);
  if (!Number.isFinite(normalized)) {
    return whenNull;
  }
  return new Intl.NumberFormat("en-CH", {
    style: "currency",
    currency: "CHF",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(normalized);
}

function formatChfAxisValue(value) {
  const normalized = Number(value);
  if (!Number.isFinite(normalized)) {
    return "";
  }
  return new Intl.NumberFormat("en-CH", {
    style: "currency",
    currency: "CHF",
    notation: Math.abs(normalized) >= 1000 ? "compact" : "standard",
    minimumFractionDigits: 0,
    maximumFractionDigits: Math.abs(normalized) >= 1000 ? 1 : 0,
  }).format(normalized);
}

function compactParts(parts) {
  return parts.filter(Boolean).join(" - ");
}

function formatShareOfTotal(value) {
  if (value === null || value === undefined) {
    return "0.0% of total";
  }
  const normalized = Number(value);
  if (!Number.isFinite(normalized)) {
    return "0.0% of total";
  }
  return `${normalized.toFixed(1)}% of total`;
}

function formatSizeGb(value) {
  if (value === null || value === undefined) {
    return "Unavailable";
  }
  const normalized = Number(value);
  if (!Number.isFinite(normalized) || normalized < 0) {
    return "Unavailable";
  }
  return `${normalized.toFixed(normalized >= 10 ? 0 : 1)} GB`;
}

function formatRatePerGbMonth(value) {
  if (value === null || value === undefined) {
    return "Not configured";
  }
  return `${formatChfValue(value)} / GB-month`;
}

function chartTooltipLabel(context, formatter) {
  return `${context.dataset.label}: ${formatter(context.parsed.y)}`;
}

function setText(node, value) {
  if (node) {
    node.textContent = value;
  }
}

function setInputValue(node, value) {
  if (node instanceof HTMLInputElement) {
    node.value = value;
  }
}

function formatBudgetInputValue(value) {
  if (value === null || value === undefined) {
    return "";
  }
  const normalized = Number(value);
  if (!Number.isFinite(normalized)) {
    return "";
  }
  return String(Math.round(normalized));
}

function renderLegend(container, datasets, tooltipMap = {}) {
  if (!container) {
    return;
  }
  container.replaceChildren();
  datasets.forEach((dataset) => {
    const item = document.createElement("span");
    item.className = "service-consumption-legend-item";
    item.title = tooltipMap[dataset.label] || dataset.label || "";

    const swatch = document.createElement("span");
    swatch.className = "service-consumption-legend-swatch";
    swatch.style.backgroundColor = dataset.borderColor || "transparent";

    const label = document.createElement("span");
    label.className = "service-consumption-legend-label";
    label.textContent = dataset.label || "";

    item.append(swatch, label);
    container.append(item);
  });
}

function appendServiceDetail(grid, { label, value, note = "" }) {
  const card = document.createElement("div");
  card.className = "service-consumption-service-detail-item";

  const labelNode = document.createElement("span");
  labelNode.className = "service-consumption-service-detail-label";
  labelNode.textContent = label;

  const valueNode = document.createElement("strong");
  valueNode.className = "service-consumption-service-detail-value";
  valueNode.textContent = value;

  card.append(labelNode, valueNode);
  if (note) {
    const noteNode = document.createElement("span");
    noteNode.className = "service-consumption-service-detail-note";
    noteNode.textContent = note;
    card.append(noteNode);
  }
  grid.append(card);
}

function renderServiceList(financial, formatByteCount) {
  const container = serviceListNode();
  if (!container) {
    return;
  }
  container.replaceChildren();

  const services = Array.isArray(financial?.services) ? financial.services : [];
  if (!services.length) {
    const empty = document.createElement("p");
    empty.className = "service-consumption-budget-status";
    empty.textContent = "Service-cost details will appear here once estimated CHF data is available.";
    container.append(empty);
    return;
  }

  services.forEach((service) => {
    const serviceKey = String(service?.key || "");
    const details = document.createElement("details");
    details.className = "service-consumption-service-card";
    details.dataset.serviceKey = serviceKey;
    if (serviceKey && openServiceKeys.has(serviceKey)) {
      details.open = true;
    }
    details.addEventListener("toggle", () => {
      if (!serviceKey) {
        return;
      }
      if (details.open) {
        openServiceKeys.add(serviceKey);
      } else {
        openServiceKeys.delete(serviceKey);
      }
    });

    const summary = document.createElement("summary");
    summary.className = "service-consumption-service-summary";

    const summaryMain = document.createElement("div");
    summaryMain.className = "service-consumption-service-summary-main";

    const titleRow = document.createElement("div");
    titleRow.className = "service-consumption-service-summary-title-row";

    const title = document.createElement("span");
    title.className = "service-consumption-service-summary-title";
    title.textContent = service?.label || "Service";

    titleRow.append(title);

    const subtitle = document.createElement("p");
    subtitle.className = "service-consumption-service-summary-subtitle";
    subtitle.textContent = service?.subtitle || "";

    summaryMain.append(titleRow, subtitle);

    const summarySide = document.createElement("div");
    summarySide.className = "service-consumption-service-summary-side";

    const cost = document.createElement("strong");
    cost.className = "service-consumption-service-cost";
    cost.textContent = formatChfValue(service?.costYtdChf, { whenNull: "CHF 0.00" });

    const share = document.createElement("span");
    share.className = "service-consumption-service-share";
    share.textContent = formatShareOfTotal(service?.shareOfTotalPercent);

    const status = document.createElement("span");
    status.className = `service-consumption-service-status is-${
      service?.status?.state || "not-configured"
    }`;
    status.textContent = service?.status?.label || "Unknown";

    const chevron = document.createElement("span");
    chevron.className = "service-consumption-service-summary-chevron";
    chevron.setAttribute("aria-hidden", "true");
    chevron.textContent = "▾";

    const summaryAside = document.createElement("div");
    summaryAside.className = "service-consumption-service-summary-aside";
    summarySide.append(cost, share, status);
    summaryAside.append(summarySide, chevron);
    summary.append(summaryMain, summaryAside);

    const body = document.createElement("div");
    body.className = "service-consumption-service-body";

    const grid = document.createElement("div");
    grid.className = "service-consumption-service-detail-grid";

    const detail = service?.details || {};

    if (serviceKey === "container") {
      appendServiceDetail(grid, {
        label: "CPU cost YTD",
        value: formatChfValue(detail?.cpuChf, { whenNull: "CHF 0.00" }),
        note: "Weighted CPU share of the active node rate.",
      });
      appendServiceDetail(grid, {
        label: "RAM cost YTD",
        value: formatChfValue(detail?.ramChf, { whenNull: "CHF 0.00" }),
        note: "Weighted RAM share of the active node rate.",
      });
      appendServiceDetail(grid, {
        label: "Node hourly rate",
        value: detail?.nodeChfPerHour === null || detail?.nodeChfPerHour === undefined
          ? "Not configured"
          : `${formatChfValue(detail.nodeChfPerHour)} / hour`,
        note: "Operator-managed compute rate from the ConfigMap.",
      });
      appendServiceDetail(grid, {
        label: "Weighting model",
        value: `CPU ${Number(detail?.cpuWeight || 0).toFixed(2)} / RAM ${Number(
          detail?.ramWeight || 0
        ).toFixed(2)}`,
        note: "Used to split the container estimate into CPU and RAM.",
      });
    } else if (serviceKey === "application") {
      appendServiceDetail(grid, {
        label: "Annual service fee",
        value: formatChfValue(detail?.annualFeeChf, { whenNull: "CHF 500.00" }),
        note: "Fixed PoC-wide DAAIFL application fee per year.",
      });
      appendServiceDetail(grid, {
        label: "Monthly service fee",
        value:
          detail?.monthlyFeeChf === null || detail?.monthlyFeeChf === undefined
            ? "Unavailable"
            : `${formatChfValue(detail.monthlyFeeChf)} / month`,
        note: "Prorated monthly across the running PoC year.",
      });
      appendServiceDetail(grid, {
        label: "Application cost YTD",
        value: formatChfValue(detail?.costYtdChf, { whenNull: "CHF 0.00" }),
        note: "Estimated DAAIFL application fee accumulated year-to-date.",
      });
    } else if (serviceKey === "filesystem") {
      appendServiceDetail(grid, {
        label: "Provisioned capacity",
        value: formatBytesOrUnavailable(detail?.provisionedBytes, formatByteCount),
        note: "PVC capacity provisioned for the app storage volume.",
      });
      appendServiceDetail(grid, {
        label: "Rate",
        value: formatRatePerGbMonth(detail?.rateChfPerGbMonth),
        note: "Applied to the provisioned PVC capacity over time.",
      });
      appendServiceDetail(grid, {
        label: "FileSystem cost YTD",
        value: formatChfValue(detail?.costYtdChf, { whenNull: "CHF 0.00" }),
        note: "Persistent storage estimate accumulated this year.",
      });
    } else if (serviceKey === "s3") {
      appendServiceDetail(grid, {
        label: "Visible storage",
        value: formatBytesOrUnavailable(detail?.totalBytes, formatByteCount),
        note: "Object bytes across the visible shared-workspace buckets.",
      });
      appendServiceDetail(grid, {
        label: "Visible buckets",
        value: `${Number(detail?.bucketCount || 0)} bucket(s)`,
        note: "Count of buckets reachable with the current S3 credentials.",
      });
      appendServiceDetail(grid, {
        label: "Rate",
        value: formatRatePerGbMonth(detail?.rateChfPerGbMonth),
        note: "Applied to the visible S3 storage footprint over time.",
      });
      appendServiceDetail(grid, {
        label: "S3 cost YTD",
        value: formatChfValue(detail?.costYtdChf, { whenNull: "CHF 0.00" }),
        note: "Estimated object-storage cost accumulated this year.",
      });
    } else if (serviceKey === "pg") {
      const instances = Array.isArray(detail?.instances) ? detail.instances : [];
      instances.forEach((instance) => {
        appendServiceDetail(grid, {
          label: `${instance?.label || "PG"} instance`,
          value: formatSizeGb(instance?.sizeGb),
          note: "Static placeholder size for the current PoC cost model.",
        });
      });
      appendServiceDetail(grid, {
        label: "Annual fee per instance",
        value: formatChfValue(detail?.annualFeePerInstanceChf, {
          whenNull: "CHF 15’000.00",
        }),
        note: "Fixed yearly fee for one 80 GB PostgreSQL instance.",
      });
      appendServiceDetail(grid, {
        label: "Daily fee per instance",
        value: formatChfValue(detail?.dailyFeePerInstanceChf, {
          whenNull: "Unavailable",
        }),
        note: "Used to accrue PG service cost day by day.",
      });
      appendServiceDetail(grid, {
        label: "Combined PG size",
        value: formatSizeGb(detail?.totalGb),
        note: "OLTP and OLAP instance sizes combined.",
      });
      appendServiceDetail(grid, {
        label: "Monthly PG fee",
        value: formatChfValue(detail?.monthlyFeeChf, { whenNull: "Unavailable" }),
        note: "Combined monthly fee for the static OLTP and OLAP instances.",
      });
      appendServiceDetail(grid, {
        label: "PG cost YTD",
        value: formatChfValue(detail?.costYtdChf, { whenNull: "CHF 0.00" }),
        note: "Estimated PostgreSQL service cost accumulated year-to-date.",
      });
    }

    body.append(grid);
    details.append(summary, body);
    container.append(details);
  });
}

function timeAxisOptions(maxTicksLimit = 8) {
  return {
    ticks: {
      autoSkip: true,
      maxTicksLimit,
      minRotation: 0,
      maxRotation: 0,
    },
  };
}

export function createServiceConsumptionUi({ fetchJsonOrThrow, formatByteCount }) {
  let chartJsPromise = null;
  let currentWindow = DEFAULT_WINDOW;
  let stateVersion = 0;
  let latestState = null;
  let historyRefreshInFlight = null;
  let lastHistoryRefreshAt = 0;
  let budgetSaveInFlight = false;
  let budgetInputDirty = false;
  let budgetStatusMessage = "";
  let budgetInteractionController = null;
  let budgetInteractionRoot = null;
  const pageCharts = {
    financial: null,
    financialServiceMix: null,
    cpu: null,
    memory: null,
    s3: null,
    persistentVolume: null,
  };

  function version() {
    return stateVersion;
  }

  function visible() {
    return Boolean(pageRoot());
  }

  function syncControls() {
    if (!pageRoot()) {
      return;
    }

    windowButtons().forEach((button) => {
      button.classList.toggle(
        "is-active",
        String(button.dataset.serviceConsumptionWindow || "") === currentWindow
      );
    });

    const saveButton = budgetSaveButton();
    if (saveButton instanceof HTMLButtonElement) {
      saveButton.disabled = budgetSaveInFlight;
      saveButton.textContent = budgetSaveInFlight ? "Saving..." : "Save budget";
    }
  }

  function ensureFinancialSummary(financial) {
    const root = pageRoot();
    if (!root) {
      return;
    }

    const budgetYear = Number(financial?.yearUtc || new Date().getUTCFullYear());
    const annualBudgetChf = financial?.annualBudgetChf;
    const spentYearToDateChf = financial?.spentYearToDateChf;
    const remainingBudgetChf = financial?.remainingBudgetChf;
    const forecastYearEndChf = financial?.forecastYearEndChf;
    const financialStatus = financial?.status || {};
    const percentUsed =
      Number(annualBudgetChf) > 0 && Number.isFinite(Number(spentYearToDateChf))
        ? (Number(spentYearToDateChf) / Number(annualBudgetChf)) * 100
        : null;
    const normalizedPercentUsed =
      percentUsed === null ? null : Math.max(0, Math.min(percentUsed, 100));

    setText(budgetYearNode(), String(budgetYear));
    if (!budgetInputDirty && !budgetSaveInFlight) {
      setInputValue(budgetInputNode(), formatBudgetInputValue(annualBudgetChf));
    }
    setText(
      budgetHighlightValueNode(),
      formatChfValue(annualBudgetChf, { whenNull: "Set annual budget" })
    );
    setText(
      budgetHighlightValueMetaNode(),
      annualBudgetChf === null || annualBudgetChf === undefined
        ? "Shared app budget for the active UTC year."
        : `Shared app budget configured for ${budgetYear}.`
    );
    setText(
      budgetHighlightUsedNode(),
      formatChfValue(spentYearToDateChf, { whenNull: "Awaiting cost data" })
    );
    setText(
      budgetHighlightUsedMetaNode(),
      annualBudgetChf === null || annualBudgetChf === undefined
        ? "Estimated CHF burned year-to-date."
        : `Estimated burn recorded YTD in ${budgetYear}.`
    );
    setText(
      budgetHighlightRemainingNode(),
      annualBudgetChf === null || annualBudgetChf === undefined
        ? "Set budget first"
        : formatChfValue(remainingBudgetChf)
    );
    setText(
      budgetHighlightRemainingMetaNode(),
      annualBudgetChf === null || annualBudgetChf === undefined
        ? "Remaining annual CHF budget for this app."
        : remainingBudgetChf !== null && Number(remainingBudgetChf) < 0
          ? "The shared annual budget is already overspent."
          : "Estimated CHF still available this year."
    );
    const progressBar = budgetProgressBarNode();
    if (progressBar) {
      progressBar.style.width =
        normalizedPercentUsed === null ? "0%" : `${normalizedPercentUsed.toFixed(1)}%`;
      progressBar.style.background =
        percentUsed !== null && percentUsed > 100
          ? "linear-gradient(90deg, rgba(213, 43, 30, 0.92), rgba(185, 107, 0, 0.88))"
          : "linear-gradient(90deg, rgba(28, 106, 72, 0.95), rgba(11, 68, 121, 0.88))";
    }
    setText(
      budgetProgressCopyNode(),
      annualBudgetChf === null || annualBudgetChf === undefined
        ? compactParts([
            `Estimated spend so far ${formatChfValue(spentYearToDateChf, { whenNull: "CHF 0.00" })}.`,
            "Set the shared annual budget to track what is left.",
          ])
        : remainingBudgetChf !== null && Number(remainingBudgetChf) < 0
          ? compactParts([
              `${formatPercent(percentUsed)} of the annual budget is burned YTD.`,
              `${formatChfValue(Math.abs(Number(remainingBudgetChf)))} above budget.`,
              `Forecast year-end ${formatChfValue(forecastYearEndChf)}.`,
            ])
          : compactParts([
              `${formatPercent(percentUsed)} of the annual budget is burned YTD.`,
              `${formatChfValue(remainingBudgetChf)} remains.`,
              `Forecast year-end ${formatChfValue(forecastYearEndChf)}.`,
            ])
    );

    setText(
      financialSummaryValue(root, "annual-budget"),
      formatChfValue(annualBudgetChf, { whenNull: "Not set" })
    );
    setText(
      financialSummaryMeta(root, "annual-budget"),
      `Shared app budget for ${budgetYear}`
    );
    setText(
      financialSummaryValue(root, "spend-ytd"),
      formatChfValue(spentYearToDateChf)
    );
    setText(
      financialSummaryMeta(root, "spend-ytd"),
      "Estimated cumulative spend YTD in the current UTC year"
    );
    setText(
      financialSummaryValue(root, "remaining-budget"),
      annualBudgetChf === null || annualBudgetChf === undefined
        ? "Not set"
        : formatChfValue(remainingBudgetChf)
    );
    setText(
      financialSummaryMeta(root, "remaining-budget"),
      annualBudgetChf === null || annualBudgetChf === undefined
        ? "Set an annual budget to track the remaining CHF."
        : "Budget minus estimated spend year-to-date"
    );
    setText(
      financialSummaryValue(root, "forecast-year-end"),
      formatChfValue(forecastYearEndChf)
    );
    setText(
      financialSummaryMeta(root, "forecast-year-end"),
      "Projected from the rolling last 30 days average daily burn"
    );

    setText(
      financialStatusNode(),
      financialStatus?.detail || "Estimated CHF has not been calculated yet."
    );
    setText(
      budgetStatusNode(),
      budgetStatusMessage ||
        (annualBudgetChf === null || annualBudgetChf === undefined
          ? "Annual budget has not been set yet. Use the CHF input above to set the shared PoC budget."
          : compactParts([
              `Shared annual budget for ${budgetYear} is set.`,
              `Estimated spend YTD ${formatChfValue(spentYearToDateChf, { whenNull: "CHF 0.00" })}.`,
              financial?.budgetUpdatedAtUtc
                ? formatSampledAt(financial.budgetUpdatedAtUtc, "Last updated")
                : "",
            ]))
    );
    renderServiceList(financial, formatByteCount);
  }

  function ensureTechnicalSummary(latest, status, topology) {
    const root = pageRoot();
    if (!root) {
      return;
    }

    setText(
      technicalSummaryValue(root, "cpu-service"),
      formatCurrentAndMax(
        metricPath(latest, ["cpu", "app", "coresUsed"]),
        metricPath(latest, ["cpu", "app", "limitCores"]),
        formatCpuValue
      )
    );
    setText(
      technicalSummaryMeta(root, "cpu-service"),
      `Limit usage ${formatPercent(metricPath(latest, ["cpu", "app", "percentOfLimit"]))}`
    );
    setText(
      chartLimitCopy("cpu"),
      (() => {
        const limit = formatCpuValue(metricPath(latest, ["cpu", "app", "limitCores"]));
        return limit === "Unavailable" ? "" : `Max ${limit}`;
      })()
    );
    setText(
      technicalSummaryValue(root, "cpu-node"),
      formatCurrentAndMax(
        metricPath(latest, ["cpu", "node", "coresUsed"]),
        metricPath(latest, ["cpu", "node", "capacityCores"]),
        formatCpuValue
      )
    );
    setText(
      technicalSummaryMeta(root, "cpu-node"),
      `Node load ${formatPercent(metricPath(latest, ["cpu", "node", "percentOfCapacity"]))}`
    );
    setText(
      technicalSummaryValue(root, "memory-service"),
      formatCurrentAndMax(
        metricPath(latest, ["memory", "app", "bytesUsed"]),
        metricPath(latest, ["memory", "app", "limitBytes"]),
        (value) => formatBytesOrUnavailable(value, formatByteCount)
      )
    );
    setText(
      technicalSummaryMeta(root, "memory-service"),
      `Limit usage ${formatPercent(metricPath(latest, ["memory", "app", "percentOfLimit"]))}`
    );
    setText(
      chartLimitCopy("memory"),
      (() => {
        const limit = formatMemoryLimitValue(metricPath(latest, ["memory", "app", "limitBytes"]));
        return limit === "Unavailable" ? "" : `Max ${limit}`;
      })()
    );
    setText(
      technicalSummaryValue(root, "memory-node"),
      formatCurrentAndMax(
        metricPath(latest, ["memory", "node", "bytesUsed"]),
        metricPath(latest, ["memory", "node", "capacityBytes"]),
        (value) => formatBytesOrUnavailable(value, formatByteCount)
      )
    );
    setText(
      technicalSummaryMeta(root, "memory-node"),
      `Node load ${formatPercent(metricPath(latest, ["memory", "node", "percentOfCapacity"]))}`
    );
    setText(
      technicalSummaryValue(root, "s3-total"),
      formatBytesOrUnavailable(metricPath(latest, ["s3", "totalBytes"]), formatByteCount)
    );
    setText(
      technicalSummaryMeta(root, "s3-total"),
      `Updated hourly - ${metricPath(latest, ["s3", "bucketCount"]) || 0} bucket(s) visible`
    );
    setText(
      technicalSummaryValue(root, "pv-total"),
      formatCurrentAndMax(
        metricPath(latest, ["persistentVolume", "bytesUsed"]),
        metricPath(latest, ["persistentVolume", "bytesCapacity"]),
        (value) => formatBytesOrUnavailable(value, formatByteCount)
      )
    );
    setText(
      technicalSummaryMeta(root, "pv-total"),
      (() => {
        const percent = formatPercent(
          metricPath(latest, ["persistentVolume", "percentOfCapacity"])
        );
        if (percent !== "Unavailable") {
          return `Capacity usage ${percent}`;
        }
        const mountPath = metricPath(latest, ["persistentVolume", "mountPath"]);
        return mountPath
          ? `Occupied bytes in mount ${mountPath}`
          : "Occupied bytes in mounted service-consumption storage";
      })()
    );

    setText(
      pageStatusNode(),
      status?.nodeMetricsAvailable
        ? "Node metrics available."
        : `Node metrics unavailable. ${status?.nodeMetrics?.detail || ""}`.trim()
    );
    setText(
      pageStatusS3(),
      status?.s3MetricsAvailable
        ? formatSampledAt(status?.s3SampledAtUtc, "S3 usage available. Latest hourly sample")
        : `S3 usage unavailable. ${status?.s3Metrics?.detail || ""}`.trim()
    );
    setText(
      pageStatusPersistentVolume(),
      status?.persistentVolumeMetricsAvailable
        ? compactParts([
            "Persistent volume usage available.",
            status?.persistentVolumeMountPath
              ? `Mount ${status.persistentVolumeMountPath}`
              : status?.persistentVolumeMetrics?.detail || "",
          ])
        : `Persistent volume usage unavailable. ${
            status?.persistentVolumeMetrics?.detail || ""
          }`.trim()
    );
    setText(sampledAtNode(), formatSampledAt(latest?.timestampUtc));
    setText(topologyValue("node"), topology?.nodeName || "Unknown");
    setText(topologyValue("pod"), topology?.podName || "Unknown");
    setText(topologyValue("namespace"), topology?.podNamespace || "Unknown");
  }

  async function ensureChartJs() {
    if (window.Chart) {
      return window.Chart;
    }
    if (chartJsPromise) {
      return chartJsPromise;
    }
    chartJsPromise = new Promise((resolve, reject) => {
      const existing = document.querySelector(`script[src="${CHART_JS_ASSET_PATH}"]`);
      if (existing) {
        existing.addEventListener("load", () => resolve(window.Chart), { once: true });
        existing.addEventListener("error", () => reject(new Error("Failed to load Chart.js.")), {
          once: true,
        });
        return;
      }

      const script = document.createElement("script");
      script.src = CHART_JS_ASSET_PATH;
      script.async = true;
      script.addEventListener("load", () => resolve(window.Chart), { once: true });
      script.addEventListener("error", () => reject(new Error("Failed to load Chart.js.")), {
        once: true,
      });
      document.head.append(script);
    });
    return chartJsPromise;
  }

  function updateLineChart({ key, canvas, configFactory }) {
    if (!canvas) {
      return null;
    }
    const existing = pageCharts[key];
    if (existing && existing.canvas !== canvas) {
      existing.destroy();
      pageCharts[key] = null;
    }
    const chart =
      pageCharts[key] && pageCharts[key].canvas === canvas
        ? pageCharts[key]
        : new window.Chart(canvas, configFactory());
    pageCharts[key] = chart;
    return chart;
  }

  function syncLineChartData(chart, { labels, datasets }) {
    chart.data.labels = labels;
    chart.data.datasets = datasets.map((dataset, index) => ({
      ...(chart.data.datasets[index] || {}),
      ...dataset,
    }));
    chart.update("none");
  }

  async function renderFinancialChart(payload) {
    await ensureChartJs();
    const labels = payload?.financial?.monthly?.labels || [];
    const currentYear = Number(
      payload?.financial?.monthly?.currentYear ||
        payload?.financial?.yearUtc ||
        new Date().getUTCFullYear()
    );
    const comparisonYear = Number(payload?.financial?.monthly?.comparisonYear || 0) || null;
    const comparisonActual = normalizeNumericSeries(
      payload?.financial?.monthly?.comparisonActualCumulativeChf || []
    );
    const actual = normalizeNumericSeries(payload?.financial?.monthly?.actualCumulativeChf || []);
    const plan = normalizeNumericSeries(payload?.financial?.monthly?.planCumulativeChf || []);
    const forecast = normalizeNumericSeries(
      payload?.financial?.monthly?.forecastCumulativeChf || []
    );
    const datasets = [];
    if (comparisonYear && comparisonActual.some((value) => value !== null)) {
      datasets.push({
        label: `Actual Spend ${comparisonYear}`,
        data: comparisonActual,
        borderColor: METRIC_COLORS.actualPrevious,
        backgroundColor: "transparent",
        tension: 0.18,
        pointRadius: 2,
        pointHoverRadius: 4,
        borderWidth: 2,
        spanGaps: true,
      });
    }
    datasets.push(
      {
        label: `Actual Spend ${currentYear}`,
        data: actual,
        borderColor: METRIC_COLORS.actual,
        backgroundColor: "transparent",
        tension: 0.2,
        pointRadius: 2,
        pointHoverRadius: 4,
        borderWidth: 3,
        spanGaps: true,
      },
      {
        label: `Planned Budget ${currentYear}`,
        data: plan,
        borderColor: METRIC_COLORS.plan,
        backgroundColor: "transparent",
        tension: 0.15,
        pointRadius: 0,
        borderDash: [8, 6],
        borderWidth: 2,
        spanGaps: true,
      },
      {
        label: `Forecast ${currentYear}`,
        data: forecast,
        borderColor: METRIC_COLORS.forecast,
        backgroundColor: "transparent",
        tension: 0.2,
        pointRadius: 0,
        borderDash: [4, 4],
        borderWidth: 2,
        spanGaps: true,
      }
    );
    const chart =
      updateLineChart({
        key: "financial",
        canvas: financialChartCanvas(),
        configFactory: () => ({
          type: "line",
          data: {
            labels,
            datasets,
          },
          options: {
            animation: false,
            maintainAspectRatio: false,
            responsive: true,
            interaction: { mode: "index", intersect: false },
            plugins: {
              legend: { display: false },
              tooltip: {
                callbacks: {
                  label: (context) => chartTooltipLabel(context, formatChfValue),
                },
              },
            },
            scales: {
              x: timeAxisOptions(12),
              y: {
                beginAtZero: true,
                ticks: {
                  callback: (value) => formatChfAxisValue(Number(value)),
                },
              },
            },
          },
        }),
      }) || pageCharts.financial;
    if (chart) {
      syncLineChartData(chart, {
        labels,
        datasets,
      });
      renderLegend(
        financialLegendContainer(),
        chart.data.datasets,
        financialLegendTooltips(currentYear, comparisonYear)
      );
    }
  }

  async function renderFinancialServiceMixChart(payload) {
    await ensureChartJs();
    const services = Array.isArray(payload?.financial?.services) ? payload.financial.services : [];
    const activeServices = services.filter(
      (service) => Number(service?.costYtdChf || 0) > 0
    );
    const labelsSource = activeServices.length
      ? activeServices
      : [{ key: "fallback", label: "No YTD spend yet", costYtdChf: 1, subtitle: "" }];
    const labels = labelsSource.map((service) => service.label || "Service");
    const values = labelsSource.map((service) => Number(service.costYtdChf || 0));
    const colors = labelsSource.map(
      (service) => SERVICE_CATEGORY_COLORS[String(service?.key || "")] || SERVICE_CATEGORY_COLORS.fallback
    );
    const total = values.reduce((sum, value) => sum + value, 0);
    const chart =
      updateLineChart({
        key: "financialServiceMix",
        canvas: financialServiceMixChartCanvas(),
        configFactory: () => ({
          type: "doughnut",
          data: {
            labels,
            datasets: [
              {
                data: values,
                backgroundColor: colors,
                borderColor: "#ffffff",
                borderWidth: 2,
                hoverOffset: 4,
              },
            ],
          },
          options: {
            animation: false,
            maintainAspectRatio: false,
            responsive: true,
            cutout: "58%",
            plugins: {
              legend: { display: false },
              tooltip: {
                callbacks: {
                  label: (context) => {
                    const value = Number(context.raw || 0);
                    const percent = total > 0 ? (value / total) * 100 : 0;
                    return `${context.label}: ${formatChfValue(value)} (${percent.toFixed(1)}% of YTD)`;
                  },
                },
              },
            },
          },
        }),
      }) || pageCharts.financialServiceMix;
    if (chart) {
      chart.data.labels = labels;
      chart.data.datasets = [
        {
          data: values,
          backgroundColor: colors,
          borderColor: "#ffffff",
          borderWidth: 2,
          hoverOffset: 4,
        },
      ];
      chart.update("none");
      renderLegend(
        financialServiceMixLegendContainer(),
        labelsSource.map((service, index) => ({
          label: service.label || "Service",
          borderColor: colors[index],
        })),
        serviceMixLegendTooltips(activeServices.length ? activeServices : [])
      );
    }
  }

  async function renderRecentCharts(payload) {
    await ensureChartJs();

    const cpuLabels = (payload?.cpuHistory?.timestamps || []).map((timestamp) =>
      formatTimestampLabel(timestamp)
    );
    const cpuService = normalizeNumericSeries(payload?.cpuHistory?.service || []);
    const cpuNode = normalizeNumericSeries(payload?.cpuHistory?.node || []);
    const cpuChart =
      updateLineChart({
        key: "cpu",
        canvas: cpuChartCanvas(),
        configFactory: () => ({
          type: "line",
          data: {
            labels: cpuLabels,
            datasets: [
              {
                label: "Workbench service",
                data: cpuService,
                borderColor: METRIC_COLORS.cpu,
                backgroundColor: "transparent",
                tension: 0.25,
                pointRadius: 0,
                borderWidth: 2,
                spanGaps: true,
              },
              {
                label: "Current node",
                data: cpuNode,
                borderColor: METRIC_COLORS.node,
                backgroundColor: "transparent",
                tension: 0.25,
                pointRadius: 0,
                borderWidth: 2,
                spanGaps: true,
              },
            ],
          },
          options: {
            animation: false,
            maintainAspectRatio: false,
            responsive: true,
            interaction: { mode: "index", intersect: false },
            plugins: {
              legend: { display: false },
              tooltip: {
                callbacks: {
                  label: (context) => chartTooltipLabel(context, formatCpuValue),
                },
              },
            },
            scales: {
              x: timeAxisOptions(8),
              y: { beginAtZero: true },
            },
          },
        }),
      }) || pageCharts.cpu;
    if (cpuChart) {
      syncLineChartData(cpuChart, {
        labels: cpuLabels,
        datasets: [
          { label: "Workbench service", data: cpuService, borderColor: METRIC_COLORS.cpu },
          { label: "Current node", data: cpuNode, borderColor: METRIC_COLORS.node },
        ],
      });
      renderLegend(cpuLegendContainer(), cpuChart.data.datasets, LEGEND_TOOLTIPS.cpu);
    }

    const memoryLabels = (payload?.memoryHistory?.timestamps || []).map((timestamp) =>
      formatTimestampLabel(timestamp)
    );
    const memoryService = normalizeNumericSeries(payload?.memoryHistory?.service || []);
    const memoryNode = normalizeNumericSeries(payload?.memoryHistory?.node || []);
    const memoryChart =
      updateLineChart({
        key: "memory",
        canvas: memoryChartCanvas(),
        configFactory: () => ({
          type: "line",
          data: {
            labels: memoryLabels,
            datasets: [
              {
                label: "Workbench service",
                data: memoryService,
                borderColor: METRIC_COLORS.memory,
                backgroundColor: "transparent",
                tension: 0.25,
                pointRadius: 0,
                borderWidth: 2,
                spanGaps: true,
              },
              {
                label: "Current node",
                data: memoryNode,
                borderColor: METRIC_COLORS.node,
                backgroundColor: "transparent",
                tension: 0.25,
                pointRadius: 0,
                borderWidth: 2,
                spanGaps: true,
              },
            ],
          },
          options: {
            animation: false,
            maintainAspectRatio: false,
            responsive: true,
            interaction: { mode: "index", intersect: false },
            plugins: {
              legend: { display: false },
              tooltip: {
                callbacks: {
                  label: (context) => chartTooltipLabel(context, formatByteCount),
                },
              },
            },
            scales: {
              x: timeAxisOptions(8),
              y: {
                beginAtZero: true,
                ticks: {
                  callback: (value) => formatByteCount(Number(value)),
                },
              },
            },
          },
        }),
      }) || pageCharts.memory;
    if (memoryChart) {
      syncLineChartData(memoryChart, {
        labels: memoryLabels,
        datasets: [
          { label: "Workbench service", data: memoryService, borderColor: METRIC_COLORS.memory },
          { label: "Current node", data: memoryNode, borderColor: METRIC_COLORS.node },
        ],
      });
      renderLegend(memoryLegendContainer(), memoryChart.data.datasets, LEGEND_TOOLTIPS.memory);
    }
  }

  async function renderS3History(payload) {
    await ensureChartJs();
    const labels = (payload?.s3History?.timestamps || []).map((timestamp) =>
      formatTimestampLabel(timestamp)
    );
    const values = normalizeNumericSeries(payload?.s3History?.values || []);
    const chart =
      updateLineChart({
        key: "s3",
        canvas: s3ChartCanvas(),
        configFactory: () => ({
          type: "line",
          data: {
            labels,
            datasets: [
              {
                label: "Visible buckets",
                data: values,
                borderColor: METRIC_COLORS.s3,
                backgroundColor: "transparent",
                tension: 0.18,
                pointRadius: 2,
                pointHoverRadius: 3,
                borderWidth: 2,
                spanGaps: true,
              },
            ],
          },
          options: {
            animation: false,
            maintainAspectRatio: false,
            responsive: true,
            interaction: { mode: "index", intersect: false },
            plugins: {
              legend: { display: false },
              tooltip: {
                callbacks: {
                  label: (context) => chartTooltipLabel(context, formatByteCount),
                },
              },
            },
            scales: {
              x: timeAxisOptions(7),
              y: {
                beginAtZero: true,
                ticks: {
                  callback: (value) => formatByteCount(Number(value)),
                },
              },
            },
          },
        }),
      }) || pageCharts.s3;
    if (chart) {
      syncLineChartData(chart, {
        labels,
        datasets: [{ label: "Visible buckets", data: values, borderColor: METRIC_COLORS.s3 }],
      });
      renderLegend(s3LegendContainer(), chart.data.datasets, LEGEND_TOOLTIPS.s3);
    }
  }

  async function renderPersistentVolumeHistory(payload) {
    await ensureChartJs();
    const labels = (payload?.persistentVolumeHistory?.timestamps || []).map((timestamp) =>
      formatTimestampLabel(timestamp)
    );
    const values = normalizeNumericSeries(payload?.persistentVolumeHistory?.values || []);
    const chart =
      updateLineChart({
        key: "persistentVolume",
        canvas: persistentVolumeChartCanvas(),
        configFactory: () => ({
          type: "line",
          data: {
            labels,
            datasets: [
              {
                label: "Mounted volume",
                data: values,
                borderColor: METRIC_COLORS.pv,
                backgroundColor: "transparent",
                tension: 0.18,
                pointRadius: 2,
                pointHoverRadius: 3,
                borderWidth: 2,
                spanGaps: true,
              },
            ],
          },
          options: {
            animation: false,
            maintainAspectRatio: false,
            responsive: true,
            interaction: { mode: "index", intersect: false },
            plugins: {
              legend: { display: false },
              tooltip: {
                callbacks: {
                  label: (context) => chartTooltipLabel(context, formatByteCount),
                },
              },
            },
            scales: {
              x: timeAxisOptions(7),
              y: {
                beginAtZero: true,
                ticks: {
                  callback: (value) => formatByteCount(Number(value)),
                },
              },
            },
          },
        }),
      }) || pageCharts.persistentVolume;
    if (chart) {
      syncLineChartData(chart, {
        labels,
        datasets: [{ label: "Mounted volume", data: values, borderColor: METRIC_COLORS.pv }],
      });
      renderLegend(
        persistentVolumeLegendContainer(),
        chart.data.datasets,
        LEGEND_TOOLTIPS.persistentVolume
      );
    }
  }

  async function renderState(payload) {
    latestState = payload || latestState;
    stateVersion = Number(payload?.version || stateVersion || 0);
    if (!visible()) {
      return payload;
    }
    syncControls();
    ensureFinancialSummary(payload?.financial || latestState?.financialSummary || {});
    ensureTechnicalSummary(payload?.latest || {}, payload?.status || {}, payload?.topology || {});
    await renderFinancialChart(payload);
    await renderFinancialServiceMixChart(payload);
    await renderRecentCharts(payload);
    await renderS3History(payload);
    await renderPersistentVolumeHistory(payload);
    lastHistoryRefreshAt = Date.now();
    return payload;
  }

  async function loadState({ windowRange = currentWindow } = {}) {
    currentWindow = String(windowRange || currentWindow).trim() || DEFAULT_WINDOW;
    syncControls();
    const params = new URLSearchParams({
      window: currentWindow,
    });
    const payload = await fetchJsonOrThrow(`/api/service-consumption/state?${params.toString()}`);
    return renderState(payload);
  }

  function shouldRefreshHistory() {
    return Date.now() - lastHistoryRefreshAt >= (WINDOW_REFRESH_INTERVAL_MS[currentWindow] || 60000);
  }

  function bindBudgetControls() {
    const root = pageRoot();
    if (!root) {
      budgetInteractionController?.abort();
      budgetInteractionController = null;
      budgetInteractionRoot = null;
      return;
    }
    if (budgetInteractionRoot === root && budgetInteractionController) {
      return;
    }
    budgetInteractionController?.abort();
    budgetInteractionRoot = root;
    budgetInteractionController = new AbortController();
    const signal = budgetInteractionController.signal;

    const saveButton = budgetSaveButton();
    if (saveButton instanceof HTMLButtonElement) {
      saveButton.addEventListener(
        "click",
        async (event) => {
          event.preventDefault();
          event.stopPropagation();
          await saveBudget();
        },
        { signal }
      );
    }

    const form = budgetFormNode();
    if (form instanceof HTMLFormElement) {
      form.addEventListener(
        "submit",
        async (event) => {
          event.preventDefault();
          event.stopPropagation();
          await saveBudget();
        },
        { signal }
      );
    }

    const input = budgetInputNode();
    if (input instanceof HTMLInputElement) {
      input.addEventListener(
        "keydown",
        async (event) => {
          if (event.key !== "Enter") {
            return;
          }
          event.preventDefault();
          event.stopPropagation();
          await saveBudget();
        },
        { signal }
      );
      input.addEventListener(
        "input",
        () => {
          budgetInputDirty = true;
          budgetStatusMessage = "";
          ensureFinancialSummary(latestState?.financial || latestState?.financialSummary || {});
        },
        { signal }
      );
    }
  }

  async function saveBudget() {
    const input = budgetInputNode();
    if (!(input instanceof HTMLInputElement)) {
      return null;
    }
    const year = Number(
      latestState?.financial?.yearUtc ||
        latestState?.financialSummary?.yearUtc ||
        new Date().getUTCFullYear()
    );
    const rawValue = String(input.value || "").trim();
    if (!rawValue) {
      budgetStatusMessage = "Enter the shared annual CHF budget before saving.";
      ensureFinancialSummary(latestState?.financial || latestState?.financialSummary || {});
      return null;
    }
    const annualBudgetChf = Number(rawValue);
    if (!Number.isFinite(annualBudgetChf) || annualBudgetChf < 0) {
      budgetStatusMessage = "Annual budget must be a non-negative CHF amount.";
      ensureFinancialSummary(latestState?.financial || latestState?.financialSummary || {});
      return null;
    }

    budgetSaveInFlight = true;
    budgetStatusMessage = "Saving shared annual budget...";
    syncControls();
    ensureFinancialSummary(latestState?.financial || latestState?.financialSummary || {});

    try {
      const response = await fetchJsonOrThrow("/api/service-consumption/budget", {
        method: "PUT",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          year,
          annualBudgetChf,
        }),
      });
      budgetInputDirty = false;
      budgetStatusMessage = `Shared annual budget for ${year} saved.`;
      await loadState({ windowRange: currentWindow });
      return response;
    } catch (error) {
      budgetStatusMessage =
        error instanceof Error ? error.message : "The shared annual budget could not be saved.";
      ensureFinancialSummary(latestState?.financial || latestState?.financialSummary || {});
      return null;
    } finally {
      budgetSaveInFlight = false;
      syncControls();
      ensureFinancialSummary(latestState?.financial || latestState?.financialSummary || {});
    }
  }

  function applyRealtimeSnapshot(snapshot) {
    if (!snapshot || typeof snapshot !== "object") {
      return;
    }
    stateVersion = Number(snapshot.version || stateVersion || 0);
    latestState = {
      ...(latestState || {}),
      version: stateVersion,
      latest: snapshot.latest || latestState?.latest || null,
      status: snapshot.status || latestState?.status || null,
      topology: snapshot.topology || latestState?.topology || null,
      financial: snapshot.financialSummary
        ? {
            ...(latestState?.financial || {}),
            ...snapshot.financialSummary,
          }
        : latestState?.financial || null,
      financialSummary: snapshot.financialSummary || latestState?.financialSummary || null,
    };
    if (!visible()) {
      return;
    }
    ensureFinancialSummary(latestState.financial || latestState.financialSummary || {});
    ensureTechnicalSummary(
      latestState.latest || {},
      latestState.status || {},
      latestState.topology || {}
    );
    if (!shouldRefreshHistory() || historyRefreshInFlight) {
      return;
    }
    historyRefreshInFlight = loadState({ windowRange: currentWindow })
      .catch((error) => {
        console.error("Failed to refresh the service-consumption page.", error);
      })
      .finally(() => {
        historyRefreshInFlight = null;
      });
  }

  async function initializeCurrentPage() {
    syncControls();
    if (!visible()) {
      return;
    }
    bindBudgetControls();
    if (latestState?.latest || latestState?.financialSummary) {
      ensureFinancialSummary(latestState?.financialSummary || latestState?.financial || {});
      ensureTechnicalSummary(
        latestState.latest || {},
        latestState.status || {},
        latestState.topology || {}
      );
    }
    await loadState({ windowRange: currentWindow });
  }

  async function handleClick(event) {
    const budgetSave = event.target.closest("[data-service-consumption-budget-save]");
    if (budgetSave) {
      event.preventDefault();
      await saveBudget();
      return true;
    }

    const button = event.target.closest("[data-service-consumption-window]");
    if (!button) {
      return false;
    }
    event.preventDefault();
    currentWindow =
      String(button.dataset.serviceConsumptionWindow || currentWindow).trim() || DEFAULT_WINDOW;
    syncControls();
    await loadState({ windowRange: currentWindow });
    return true;
  }

  async function handleChange(event) {
    if (event.target.closest("[data-service-consumption-budget-input]")) {
      budgetInputDirty = true;
      budgetStatusMessage = "";
    }
    return false;
  }

  return {
    applyRealtimeSnapshot,
    currentWindow: () => currentWindow,
    handleChange,
    handleClick,
    initializeCurrentPage,
    loadState,
    version,
  };
}
