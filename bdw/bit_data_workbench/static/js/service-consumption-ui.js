const CHART_JS_ASSET_PATH = "/static/chartjs/chart.umd.min.js";
const DEFAULT_WINDOW = "24h";
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

function pageRoot() {
  return document.querySelector("[data-service-consumption-page]");
}

function latestMetricValue(root, metric) {
  return root?.querySelector?.(
    `[data-service-consumption-summary="${metric}"] [data-summary-value]`
  );
}

function latestMetricMeta(root, metric) {
  return root?.querySelector?.(
    `[data-service-consumption-summary="${metric}"] [data-summary-meta]`
  );
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

function sampledAtNode() {
  return document.querySelector("[data-service-consumption-sampled-at]");
}

function topologyValue(field) {
  return document.querySelector(`[data-service-consumption-topology-${field}]`);
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

function compactParts(parts) {
  return parts.filter(Boolean).join(" - ");
}

function chartTooltipLabel(context, formatter) {
  return `${context.dataset.label}: ${formatter(context.parsed.y)}`;
}

function setText(node, value) {
  if (node) {
    node.textContent = value;
  }
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
  const pageCharts = {
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
  }

  function ensurePageSummary(latest, status, topology) {
    const root = pageRoot();
    if (!root) {
      return;
    }

    setText(
      latestMetricValue(root, "cpu-service"),
      formatCurrentAndMax(
        metricPath(latest, ["cpu", "app", "coresUsed"]),
        metricPath(latest, ["cpu", "app", "limitCores"]),
        formatCpuValue
      )
    );
    setText(
      latestMetricMeta(root, "cpu-service"),
      `Limit usage ${formatPercent(metricPath(latest, ["cpu", "app", "percentOfLimit"]))}`
    );
    setText(
      latestMetricValue(root, "cpu-node"),
      formatCurrentAndMax(
        metricPath(latest, ["cpu", "node", "coresUsed"]),
        metricPath(latest, ["cpu", "node", "capacityCores"]),
        formatCpuValue
      )
    );
    setText(
      latestMetricMeta(root, "cpu-node"),
      `Node load ${formatPercent(metricPath(latest, ["cpu", "node", "percentOfCapacity"]))}`
    );
    setText(
      latestMetricValue(root, "memory-service"),
      formatCurrentAndMax(
        metricPath(latest, ["memory", "app", "bytesUsed"]),
        metricPath(latest, ["memory", "app", "limitBytes"]),
        (value) => formatBytesOrUnavailable(value, formatByteCount)
      )
    );
    setText(
      latestMetricMeta(root, "memory-service"),
      `Limit usage ${formatPercent(metricPath(latest, ["memory", "app", "percentOfLimit"]))}`
    );
    setText(
      latestMetricValue(root, "memory-node"),
      formatCurrentAndMax(
        metricPath(latest, ["memory", "node", "bytesUsed"]),
        metricPath(latest, ["memory", "node", "capacityBytes"]),
        (value) => formatBytesOrUnavailable(value, formatByteCount)
      )
    );
    setText(
      latestMetricMeta(root, "memory-node"),
      `Node load ${formatPercent(metricPath(latest, ["memory", "node", "percentOfCapacity"]))}`
    );
    setText(
      latestMetricValue(root, "s3-total"),
      formatBytesOrUnavailable(metricPath(latest, ["s3", "totalBytes"]), formatByteCount)
    );
    setText(
      latestMetricMeta(root, "s3-total"),
      `Updated hourly - ${metricPath(latest, ["s3", "bucketCount"]) || 0} bucket(s) visible`
    );
    setText(
      latestMetricValue(root, "pv-total"),
      formatCurrentAndMax(
        metricPath(latest, ["persistentVolume", "bytesUsed"]),
        metricPath(latest, ["persistentVolume", "bytesCapacity"]),
        (value) => formatBytesOrUnavailable(value, formatByteCount)
      )
    );
    setText(
      latestMetricMeta(root, "pv-total"),
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
    chart.data.datasets.forEach((dataset, index) => {
      dataset.data = datasets[index]?.data || [];
      dataset.label = datasets[index]?.label || dataset.label;
      dataset.borderColor = datasets[index]?.borderColor || dataset.borderColor;
      dataset.backgroundColor = datasets[index]?.backgroundColor || dataset.backgroundColor;
    });
    chart.update("none");
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
      renderLegend(
        memoryLegendContainer(),
        memoryChart.data.datasets,
        LEGEND_TOOLTIPS.memory
      );
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
        datasets: [
          { label: "Visible buckets", data: values, borderColor: METRIC_COLORS.s3 },
        ],
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
        datasets: [
          { label: "Mounted volume", data: values, borderColor: METRIC_COLORS.pv },
        ],
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
    ensurePageSummary(payload?.latest || {}, payload?.status || {}, payload?.topology || {});
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
    };
    if (!visible()) {
      return;
    }
    ensurePageSummary(
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
    if (latestState?.latest) {
      ensurePageSummary(
        latestState.latest || {},
        latestState.status || {},
        latestState.topology || {}
      );
    }
    await loadState({ windowRange: currentWindow });
  }

  async function handleClick(event) {
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

  async function handleChange() {
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
