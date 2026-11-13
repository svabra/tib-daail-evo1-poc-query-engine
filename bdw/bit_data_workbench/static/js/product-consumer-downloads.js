import { currentDaaifDemoUser, initializeDaaifDemoIdentity } from "./demo-identity.js";

initializeDaaifDemoIdentity();
const panel = document.querySelector("[data-product-consumer-downloads]");
if (panel) {
  const slug = panel.dataset.productSlug;
  const status = panel.querySelector("[data-product-download-status]");
  const identity = panel.querySelector("[data-product-consumer-identity]");
  identity.textContent = currentDaaifDemoUser().displayName;
  async function openFormat(format) {
    if (!["rest", "csv", "xlsx", "parquet"].includes(format)) return;
    const button = panel.querySelector(`[data-product-download-format="${format}"]`);
    if (!button || button.disabled) return;
    button.disabled = true;
    status.removeAttribute("role");
    status.textContent = "Produktfreigabe wird geprüft …";
    try {
      const path = `/api/public/data-products/${encodeURIComponent(slug)}`
        + (format === "rest" ? "" : `/download/${format}`);
      const response = await fetch(path, {
        headers: { "X-DaCa-User": currentDaaifDemoUser().id },
      });
      if (!response.ok) {
        const failure = await response.json().catch(() => ({}));
        throw new Error(failure.detail || "Der Download konnte nicht vorbereitet werden.");
      }
      if (format === "rest") {
        const preview = panel.querySelector("[data-product-rest-response]");
        preview.textContent = JSON.stringify(await response.json(), null, 2);
        preview.hidden = false;
        status.textContent = "REST-Antwort geladen. Die REST-Schnittstelle unterstützt limit und offset; Dateien enthalten den vollständigen Produktstand.";
        return;
      }
      const url = URL.createObjectURL(await response.blob());
      const link = document.createElement("a");
      link.href = url;
      link.download = `${slug}.${format}`;
      document.body.append(link);
      link.click();
      link.remove();
      setTimeout(() => URL.revokeObjectURL(url), 1000);
      status.textContent = "Download bereit. Alle Zeilen des freigegebenen Produkts sind enthalten.";
    } catch (error) {
      status.setAttribute("role", "alert");
      status.textContent = error.message;
    } finally { button.disabled = false; }
  }
  panel.addEventListener("click", event => {
    const button = event.target.closest("[data-product-download-format]");
    if (button) void openFormat(button.dataset.productDownloadFormat);
  });
  // DaCa links directly to the selected representation. Identity comes from the
  // existing demo selector; every actual data request still carries its header.
  const requestedFormat = new URL(window.location.href).searchParams.get("format");
  if (requestedFormat) void openFormat(requestedFormat);
  const sla = panel.querySelector("[data-product-sla-copy]");
  if (sla) fetch(`/api/public/data-products/${encodeURIComponent(slug)}/service-level`, {
    headers: { "X-DaCa-User": currentDaaifDemoUser().id },
  }).then(async response => {
    if (!response.ok) throw new Error("Die aktuelle SLA ist derzeit nicht abrufbar. Bitte in DaCa prüfen.");
    const summary = await response.json();
    sla.textContent = `${summary.source === "published" ? "Freigegebene Produkt-SLA" : "PoC-Standardvorlage"}: `
      + summary.current.definition.purposeAndSuitableUse;
  }).catch(error => { sla.textContent = error.message; });
}
