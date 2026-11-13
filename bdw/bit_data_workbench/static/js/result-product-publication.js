import { currentDaaifDemoUser } from "./demo-identity.js";

function basisDescription(context) {
  if (context.basisWindowType === "rolling") {
    const units={days:['Tag','Tage'],weeks:['Woche','Wochen'],months:['Monat','Monate'],years:['Jahr','Jahre']};
    const unit=units[context.rollingWindowUnit]?.[context.rollingWindowSize === 1 ? 0 : 1] || context.rollingWindowUnit;
    return `Rollierendes Zeitfenster: ${context.rollingWindowSize} ${unit}, bezogen auf den jeweiligen Datenstand. `;
  }
  return context.basisFrom ? `Fixes Zeitfenster: ${context.basisFrom} – ${context.basisUntil}. ` : `Filter: ${context.cantonCode}, ${context.taxYear}. `;
}

export async function productCreationContext() {
  const id = new URL(window.location.href).searchParams.get("creationRequest");
  if (!id) return null;
  const response = await fetch(`/api/product-creation-requests/${encodeURIComponent(id)}`, {
    headers: { "X-DaCa-User": currentDaaifDemoUser().id },
  });
  if (!response.ok) throw new Error("Der Erstellungsantrag konnte für diese Identität nicht geladen werden.");
  return response.json();
}

export async function publishStoredResult({ job, target, controller }) {
  const context = await productCreationContext();
  if (context && (context.ownerUserId !== currentDaaifDemoUser().id || context.status !== "in_progress")) {
    throw new Error("Der zugewiesene Data Owner muss den Antrag zuerst in DaCa annehmen.");
  }
  const legacy = context?.cantonCode && context?.taxYear;
  const mwst = legacy || (!context && target.bucket === "mwst-journey" && target.key === "products/mwst_firmen_sg_2026.parquet");
  const metadataDefaults = mwst ? {
    title: "MWST-Erträge nach Firmen 2026 – St. Gallen",
    slug: "mwst-ertraege-firmen-sg-2026", domain: "Mehrwertsteuer", tagsText: "mwst, synthetisch, SG, 2026",
    description: "Synthetische MWST-Erträge der 60 fiktiven Firmen mit Sitz im Kanton St. Gallen. "
      + "Reiner Ausschnitt des Firmenprodukts, keine Summierung. Berichtsjahr 2026, vorläufig per 12.11.2026. "
      + "Keine amtlichen Zahlen. " + (context?.purpose || "Demonstration der kantonalen Datennutzung."),
    updateFrequency: "annually", targetAudience: "Kanton St. Gallen · Data Analyst Jan Waser",
    customProperties: { cantonCode: context?.cantonCode || "SG", taxYear: String(context?.taxYear || 2026) },
  } : {};
  if (context) {
    metadataDefaults.title = context.proposedTitle;
    metadataDefaults.description = context.purpose;
    metadataDefaults.updateFrequency = context.updateFrequency || (legacy ? "annually" : "once");
    metadataDefaults.targetAudience = context.requesterName;
    if (!mwst) metadataDefaults.slug = `derived-${context.id}`;
    metadataDefaults.customProperties = { ...metadataDefaults.customProperties, creationRequestId: context.id,
      ...(context.basisWindowType === "rolling"
        ? { basisWindowType:"rolling",rollingWindowSize:String(context.rollingWindowSize),rollingWindowUnit:context.rollingWindowUnit }
        : context.basisFrom ? { basisWindowType:"fixed",basisFrom:context.basisFrom,basisUntil:context.basisUntil } : {}),
      updateFrequency: metadataDefaults.updateFrequency,
    };
  }
  return controller.openPublishDialog({
    source: { sourceKind: "relation", sourceId: "s3", relation: "", bucket: target.bucket,
      key: target.key, sourceDisplayName: job.notebookTitle || target.key, sourcePlatform: "s3" },
    lockSource: true, startStep: 2, metadataDefaults,
  });
}

export async function initializeCreationRequestBanner() {
  if (!new URL(window.location.href).searchParams.has("creationRequest")) return;
  const workspace = document.querySelector("#workspace-panel");
  if (!workspace) return;
  const banner = document.createElement("aside");
  banner.className = "creation-request-banner";
  banner.setAttribute("role", "status");
  workspace.prepend(banner);
  try {
    const context = await productCreationContext();
    const copy = document.createElement("p");
    copy.textContent = `${context.requestNumber} · ${context.requesterName}: ${context.proposedTitle}. `
      + basisDescription(context)
      + context.purpose + (context.legalBasis ? ` Gesetzlicher Auftrag: ${context.legalBasis}.` : "")
      + (context.updateFrequency ? ` Updates: ${{once:'Einmalig',realtime:'Real-time < 5 seconds',hourly:'Stündlich',daily:'Täglich',weekly:'Wöchentlich',annually:'Jährlich'}[context.updateFrequency] || context.updateFrequency}.` : "")
      + ` Formate: ${(context.formats || []).map(value=>({parquet:'Parquet','http-rest':'HTTP REST API',csv:'CSV',xlsx:'Excel'}[value] || value)).join(', ')}.`;
    banner.append(copy);
    if (context.daaifNotebookUrl) {
      const config = await fetch("/api/documentation/config").then(response => response.json());
      const link = document.createElement("a");
      link.href = `${config.dacaUiUrl}/product-creation-requests/${encodeURIComponent(context.id)}?demoUser=${encodeURIComponent(context.ownerUserId)}`;
      link.textContent = "Antrag in DaCa öffnen · nach Freigabe abschliessen";
      banner.append(link);
    }
  } catch (error) {
    banner.textContent = error.message;
  }
}
