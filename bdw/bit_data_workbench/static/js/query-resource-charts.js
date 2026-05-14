const CHART_JS_ASSET_PATH = "/static/chartjs/chart.umd.min.js";
const QUERY_RESOURCE_CHART_SELECTOR = "[data-query-resource-chart]";
const QUERY_RESOURCE_CHART_RENDER_VERSION = "line-contained-grid-v4";
const QUERY_RESOURCE_GRID_COLOR = "rgba(124, 140, 158, 0.14)";
const QUERY_RESOURCE_GRID_LINE_WIDTH = 0.5;

let chartJsPromise = null;

function ensureChartJs() {
  if (window.Chart) {
    return Promise.resolve(window.Chart);
  }
  if (chartJsPromise) {
    return chartJsPromise;
  }

  chartJsPromise = new Promise((resolve, reject) => {
    const resolveIfLoaded = () => {
      if (window.Chart) {
        resolve(window.Chart);
        return;
      }
      reject(new Error("Chart.js loaded without exposing window.Chart."));
    };

    const existing = document.querySelector(`script[src="${CHART_JS_ASSET_PATH}"]`);
    if (existing) {
      existing.addEventListener("load", resolveIfLoaded, { once: true });
      existing.addEventListener("error", () => reject(new Error("Failed to load Chart.js.")), {
        once: true,
      });
      return;
    }

    const script = document.createElement("script");
    script.src = CHART_JS_ASSET_PATH;
    script.async = true;
    script.addEventListener("load", resolveIfLoaded, { once: true });
    script.addEventListener("error", () => reject(new Error("Failed to load Chart.js.")), {
      once: true,
    });
    document.head.append(script);
  });
  return chartJsPromise;
}

function chartCanvases(root) {
  if (!root) {
    return [];
  }
  const canvases = [];
  if (root instanceof Element && root.matches(QUERY_RESOURCE_CHART_SELECTOR)) {
    canvases.push(root);
  }
  if (typeof root.querySelectorAll === "function") {
    canvases.push(...root.querySelectorAll(QUERY_RESOURCE_CHART_SELECTOR));
  }
  return canvases.filter((canvas) => canvas instanceof HTMLCanvasElement);
}

function parseSeries(canvas, attributeName) {
  try {
    const values = JSON.parse(canvas.getAttribute(attributeName) || "[]");
    if (!Array.isArray(values)) {
      return [];
    }
    return values.map((value) => {
      const numberValue = Number(value);
      return Number.isFinite(numberValue) ? Math.max(0, numberValue) : null;
    });
  } catch (_error) {
    return [];
  }
}

function parseLabels(canvas) {
  try {
    const values = JSON.parse(canvas.getAttribute("data-query-resource-labels") || "[]");
    if (!Array.isArray(values)) {
      return [];
    }
    return values.map((value) => String(value ?? "").trim()).filter(Boolean);
  } catch (_error) {
    return [];
  }
}

function fallbackLabels(count) {
  return Array.from({ length: count }, (_value, index) => String(index + 1));
}

function formatTickValue(value, kind) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "";
  }
  if (kind === "ram") {
    if (numeric >= 1000) {
      return `${(numeric / 1000).toFixed(numeric >= 10000 ? 0 : 1).replace(/\.0$/, "")} GB`;
    }
    return `${numeric.toFixed(numeric >= 100 ? 0 : 1).replace(/\.0$/, "")} MB`;
  }
  return `${numeric.toFixed(numeric >= 10 ? 0 : 1).replace(/\.0$/, "")}%`;
}

function chartDataKey({ currentValues, averageValues, labels, maxValue, kind, axisLabel, currentLabel, averageLabel }) {
  return JSON.stringify({
    renderVersion: QUERY_RESOURCE_CHART_RENDER_VERSION,
    currentValues,
    averageValues,
    labels,
    maxValue,
    kind,
    axisLabel,
    currentLabel,
    averageLabel,
  });
}

function createChartConfig({ currentValues, averageValues, labels, maxValue, kind, axisLabel, currentLabel, averageLabel }) {
  const labelCount = Math.max(currentValues.length, averageValues.length);
  const chartLabels = labels.length >= labelCount ? labels.slice(0, labelCount) : fallbackLabels(labelCount);
  const currentColor = "#0b4479";
  const averageColor = "#2f7d4a";
  return {
    type: "line",
    data: {
      labels: chartLabels,
      datasets: [
        {
          label: currentLabel || "Current",
          data: currentValues,
          borderColor: currentColor,
          backgroundColor: "transparent",
          borderWidth: 2,
          pointRadius: 0,
          pointHitRadius: 0,
          tension: 0.24,
          spanGaps: true,
          fill: false,
        },
        {
          label: averageLabel || "Average",
          data: averageValues,
          borderColor: averageColor,
          backgroundColor: "transparent",
          borderWidth: 2,
          pointRadius: 0,
          pointHitRadius: 0,
          tension: 0.24,
          spanGaps: true,
          fill: false,
        },
      ],
    },
    options: {
      animation: false,
      responsive: true,
      maintainAspectRatio: false,
      normalized: true,
      parsing: true,
      layout: {
        padding: {
          top: 8,
          right: 8,
          bottom: 0,
          left: 0,
        },
      },
      plugins: {
        legend: {
          display: false,
        },
        tooltip: { enabled: false },
      },
      scales: {
        x: {
          display: true,
          offset: true,
          border: { display: false },
          grid: { display: false, drawBorder: false, drawTicks: false },
          ticks: {
            display: true,
            autoSkip: true,
            maxRotation: 0,
            minRotation: 0,
            maxTicksLimit: 4,
            padding: 8,
            color: "#52667a",
            font: {
              size: 11,
              weight: "600",
            },
          },
        },
        y: {
          display: true,
          min: 0,
          max: Math.max(1, maxValue),
          border: { display: false },
          grid: {
            display: true,
            color: QUERY_RESOURCE_GRID_COLOR,
            lineWidth: QUERY_RESOURCE_GRID_LINE_WIDTH,
            drawBorder: false,
            drawTicks: false,
          },
          ticks: {
            display: true,
            maxTicksLimit: 4,
            padding: 8,
            color: "#52667a",
            font: {
              size: 11,
              weight: "600",
            },
            callback: (value) => formatTickValue(value, kind),
          },
          title: {
            display: false,
            text: axisLabel || (kind === "ram" ? "RAM (MB)" : "CPU %"),
            color: "#52667a",
            font: {
              size: 10,
              weight: "700",
            },
            padding: {
              top: 0,
              bottom: 0,
            },
          },
        },
      },
    },
  };
}

function syncChart(canvas, Chart) {
  if (!canvas.isConnected) {
    return;
  }
  const currentValues = parseSeries(canvas, "data-query-resource-current");
  const averageValues = parseSeries(canvas, "data-query-resource-average");
  if (!currentValues.length && !averageValues.length) {
    return;
  }
  const kind = String(canvas.dataset.queryResourceKind || "").trim().toLowerCase();
  const axisLabel = String(canvas.dataset.queryResourceAxisLabel || "").trim();
  const currentLabel = String(canvas.dataset.queryResourceCurrentLabel || "").trim();
  const averageLabel = String(canvas.dataset.queryResourceAverageLabel || "").trim();
  const labels = parseLabels(canvas);
  const maxValue = Math.max(
    1,
    Number(canvas.dataset.queryResourceMax || 0),
    ...currentValues.map((value) => Number(value || 0)),
    ...averageValues.map((value) => Number(value || 0))
  );
  const key = chartDataKey({ currentValues, averageValues, labels, maxValue, kind, axisLabel, currentLabel, averageLabel });
  if (canvas._bdwQueryResourceChart && canvas._bdwQueryResourceChartKey === key) {
    return;
  }

  const existingChart = canvas._bdwQueryResourceChart;
  if (existingChart && existingChart.config?.type === "line") {
    const labelCount = Math.max(currentValues.length, averageValues.length);
    existingChart.data.labels = labels.length >= labelCount ? labels.slice(0, labelCount) : fallbackLabels(labelCount);
    existingChart.data.datasets[0].label = currentLabel || "Current";
    existingChart.data.datasets[0].data = currentValues;
    existingChart.data.datasets[1].label = averageLabel || "Average";
    existingChart.data.datasets[1].data = averageValues;
    existingChart.options.parsing = true;
    existingChart.options.layout = {
      padding: {
        top: 8,
        right: 8,
        bottom: 0,
        left: 0,
      },
    };
    existingChart.options.scales.x.display = true;
    existingChart.options.scales.x.offset = true;
    existingChart.options.scales.x.grid.display = false;
    existingChart.options.scales.x.border.display = false;
    existingChart.options.scales.x.ticks.display = true;
    existingChart.options.scales.x.ticks.maxTicksLimit = 4;
    existingChart.options.scales.y.display = true;
    existingChart.options.scales.y.grid.display = true;
    existingChart.options.scales.y.grid.color = QUERY_RESOURCE_GRID_COLOR;
    existingChart.options.scales.y.grid.lineWidth = QUERY_RESOURCE_GRID_LINE_WIDTH;
    existingChart.options.scales.y.grid.drawBorder = false;
    existingChart.options.scales.y.grid.drawTicks = false;
    existingChart.options.scales.y.border.display = false;
    existingChart.options.scales.y.ticks.display = true;
    existingChart.options.scales.y.ticks.maxTicksLimit = 4;
    existingChart.options.scales.y.ticks.callback = (value) => formatTickValue(value, kind);
    existingChart.options.scales.y.max = Math.max(1, maxValue);
    existingChart.options.scales.y.title.text = axisLabel || (kind === "ram" ? "RAM (MB)" : "CPU %");
    existingChart.data.datasets.forEach((dataset) => {
      dataset.backgroundColor = "transparent";
      dataset.borderWidth = 2;
      dataset.fill = false;
      dataset.pointRadius = 0;
      dataset.pointHitRadius = 0;
      dataset.spanGaps = true;
      dataset.tension = 0.24;
    });
    existingChart.update("none");
  } else {
    if (existingChart) {
      existingChart.destroy();
    }
    canvas._bdwQueryResourceChart = new Chart(
      canvas,
      createChartConfig({ currentValues, averageValues, labels, maxValue, kind, axisLabel, currentLabel, averageLabel })
    );
  }
  canvas._bdwQueryResourceChartKey = key;
}

function destroyCharts(root) {
  chartCanvases(root).forEach((canvas) => {
    if (canvas._bdwQueryResourceChart) {
      canvas._bdwQueryResourceChart.destroy();
      delete canvas._bdwQueryResourceChart;
      delete canvas._bdwQueryResourceChartKey;
    }
  });
}

export function createQueryResourceChartsController() {
  let observer = null;
  let scheduled = false;

  async function initialize(root = document) {
    const canvases = chartCanvases(root).filter((canvas) => canvas.isConnected);
    if (!canvases.length) {
      return;
    }
    let Chart;
    try {
      Chart = await ensureChartJs();
    } catch (error) {
      console.error("Failed to initialize query resource charts.", error);
      return;
    }
    canvases.forEach((canvas) => syncChart(canvas, Chart));
  }

  function scheduleInitialize(root = document) {
    if (scheduled) {
      return;
    }
    scheduled = true;
    window.requestAnimationFrame(() => {
      scheduled = false;
      initialize(root);
    });
  }

  function start() {
    if (observer) {
      return;
    }
    scheduleInitialize(document);
    observer = new MutationObserver((mutations) => {
      let shouldInitialize = false;
      mutations.forEach((mutation) => {
        mutation.removedNodes.forEach((node) => {
          destroyCharts(node);
        });
        mutation.addedNodes.forEach((node) => {
          if (chartCanvases(node).length) {
            shouldInitialize = true;
          }
        });
      });
      if (shouldInitialize) {
        scheduleInitialize(document);
      }
    });
    observer.observe(document.body, { childList: true, subtree: true });
  }

  function disconnect() {
    if (observer) {
      observer.disconnect();
      observer = null;
    }
    destroyCharts(document);
  }

  return {
    disconnect,
    initialize,
    start,
  };
}
