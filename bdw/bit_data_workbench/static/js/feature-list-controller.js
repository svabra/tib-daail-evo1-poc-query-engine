const fallbackPresentation = {
  title: "Was kann DAAIF Factory?",
  introduction:
    "DAAIF Factory bündelt Datenzugriff, Analyse, Pipelines und die Veröffentlichung von Datenprodukten in einer gemeinsamen Arbeitsumgebung.",
  pocNote:
    "Hinweis: Diese Liste beschreibt den aktuellen PoC-Stand. Einzelne Abläufe sind simuliert und noch keine produktive Leistung.",
};

function textValue(value) {
  return String(value || "").trim();
}

function readFeatureReleaseNotes() {
  const element = document.getElementById("feature-release-notes");
  if (!element?.textContent) {
    return [];
  }

  try {
    const parsed = JSON.parse(element.textContent);
    return Array.isArray(parsed) ? parsed : [];
  } catch (_error) {
    return [];
  }
}

function releaseForVersion(releases, currentVersion) {
  return (
    releases.find((release) => textValue(release?.version) === currentVersion) ||
    releases[0] ||
    null
  );
}

function normalizedFeature(feature) {
  if (feature && typeof feature === "object") {
    return {
      title: textValue(feature.title),
      description: textValue(feature.description),
    };
  }
  return {
    title: "",
    description: textValue(feature),
  };
}

function featureListForRelease(release) {
  const presentation =
    release?.featureList && typeof release.featureList === "object"
      ? release.featureList
      : {};
  const sourceFeatures = Array.isArray(presentation.features)
    ? presentation.features
    : Array.isArray(release?.features)
      ? release.features
      : [];
  return {
    title: textValue(presentation.title) || fallbackPresentation.title,
    introduction:
      textValue(presentation.introduction) || fallbackPresentation.introduction,
    pocNote: textValue(presentation.pocNote) || fallbackPresentation.pocNote,
    features: sourceFeatures
      .map(normalizedFeature)
      .filter((feature) => feature.title || feature.description),
  };
}

function featureItemMarkup(feature, escapeHtml) {
  const title = feature.title
    ? `<h3>${escapeHtml(feature.title)}</h3>`
    : "";
  const description = feature.description
    ? `<p${feature.title ? "" : ' class="feature-list-item-copy-only"'}>${escapeHtml(feature.description)}</p>`
    : "";
  return `
    <li class="feature-list-item">
      <svg viewBox="0 0 24 24" aria-hidden="true">
        <path d="m5 12 4 4L19 6"></path>
      </svg>
      <div>${title}${description}</div>
    </li>
  `;
}

function focusableReturnTarget(trigger) {
  if (trigger?.isConnected && trigger.getClientRects().length) {
    return trigger;
  }
  const overlayTrigger = document.querySelector(".app-version-feature-trigger");
  return overlayTrigger?.isConnected && overlayTrigger.getClientRects().length
    ? overlayTrigger
    : null;
}

function syncTriggerExpanded(expanded) {
  document.querySelectorAll("[data-open-feature-list]").forEach((trigger) => {
    trigger.setAttribute("aria-expanded", String(expanded));
  });
}

export function createFeatureListController({
  applicationVersion,
  ensureDialog,
  ensureReleaseNotes,
  escapeHtml,
}) {
  async function show(trigger = null) {
    try {
      await ensureReleaseNotes();
    } catch (error) {
      console.error("Failed to load the current feature list.", error);
    }

    const dialog = ensureDialog();
    const currentVersion = textValue(applicationVersion());
    const releases = readFeatureReleaseNotes();
    const release = releaseForVersion(releases, currentVersion);
    const presentation = featureListForRelease(release);
    const displayedVersion = currentVersion || textValue(release?.version) || "unknown";
    const releaseNode = dialog.querySelector("[data-feature-list-release]");
    const titleNode = dialog.querySelector("[data-feature-list-title]");
    const introductionNode = dialog.querySelector("[data-feature-list-introduction]");
    const body = dialog.querySelector("[data-feature-list-body]");
    const noteNode = dialog.querySelector("[data-feature-list-note]");
    const closeButton = dialog.querySelector("[data-feature-list-close]");

    if (releaseNode) {
      releaseNode.textContent = `Featureliste · V${displayedVersion}`;
    }
    if (titleNode) {
      titleNode.textContent = presentation.title;
    }
    if (introductionNode) {
      introductionNode.textContent = presentation.introduction;
    }
    if (body) {
      body.innerHTML = presentation.features.length
        ? presentation.features
            .map((feature) => featureItemMarkup(feature, escapeHtml))
            .join("")
        : '<li class="feature-list-item feature-list-item-empty">Für diesen Build ist noch keine Featureliste verfügbar.</li>';
    }
    if (noteNode) {
      noteNode.textContent = presentation.pocNote;
    }

    const returnTarget = focusableReturnTarget(trigger);
    syncTriggerExpanded(true);

    return new Promise((resolve) => {
      const closeDialog = () => {
        if (typeof dialog.close === "function") {
          dialog.close("cancel");
        } else {
          dialog.removeAttribute("open");
          onClose();
        }
      };
      const onCancel = (event) => {
        event.preventDefault();
        closeDialog();
      };
      const onBackdropClick = (event) => {
        if (event.target === dialog) {
          closeDialog();
        }
      };
      const onClose = () => {
        dialog.removeEventListener("cancel", onCancel);
        dialog.removeEventListener("click", onBackdropClick);
        syncTriggerExpanded(false);
        queueMicrotask(() => focusableReturnTarget(returnTarget)?.focus());
        resolve();
      };

      dialog.addEventListener("cancel", onCancel);
      dialog.addEventListener("click", onBackdropClick);
      dialog.addEventListener("close", onClose, { once: true });
      if (typeof dialog.showModal === "function") {
        dialog.showModal();
      } else {
        dialog.setAttribute("open", "");
      }
      queueMicrotask(() => closeButton?.focus());
    });
  }

  return { show };
}
