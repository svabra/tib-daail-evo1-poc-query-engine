const fallbackPresentation = {
  title: "Was kann DAAIF Factory?",
  introduction:
    "DAAIF Factory bündelt Datenzugriff, Analyse, Pipelines und die Veröffentlichung von Datenprodukten in einer gemeinsamen Arbeitsumgebung.",
  pocNote:
    "Hinweis: Diese Liste beschreibt den aktuellen PoC-Stand. Einzelne Abläufe sind simuliert und noch keine produktive Leistung.",
};

const INTERNAL_ONLY_FEATURE_PATTERNS = [
  /^Regression coverage\b/i,
  /^The Playwright .* regression\b/i,
  /^Playwright regression coverage\b/i,
  /^The ingestion Playwright smoke\b/i,
];

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
    releases: Array.isArray(presentation.releases) ? presentation.releases : [],
  };
}

function isUserFacingFeature(feature) {
  const copy = textValue(feature?.description || feature);
  return copy && !INTERNAL_ONLY_FEATURE_PATTERNS.some((pattern) => pattern.test(copy));
}

export function featureReleaseHistory(releases) {
  return (Array.isArray(releases) ? releases : [])
    .map((release) => ({
      version: textValue(release?.version),
      releasedAt: textValue(release?.releasedAt),
      features: (Array.isArray(release?.features) ? release.features : [])
        .map(normalizedFeature)
        .filter(isUserFacingFeature),
    }))
    .filter((release) => release.version && release.features.length);
}

function swissReleaseDate(value) {
  const match = textValue(value).match(/^(\d{4})-(\d{2})-(\d{2})/);
  return match ? `${match[3]}.${match[2]}.${match[1]}` : "";
}

function releaseHeadingId(version) {
  const suffix = textValue(version).toLowerCase().replace(/[^a-z0-9]+/g, "-");
  return `feature-list-version-${suffix || "unknown"}`;
}

function featureItemMarkup(feature, escapeHtml) {
  const title = feature.title
    ? `<h4>${escapeHtml(feature.title)}</h4>`
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

function releaseGroupMarkup(release, currentVersion, escapeHtml) {
  const version = escapeHtml(release.version);
  const headingId = releaseHeadingId(release.version);
  const releasedAt = swissReleaseDate(release.releasedAt);
  const isCurrent = release.version === currentVersion;
  const currentBadge = isCurrent
    ? '<span class="feature-list-current-badge">Aktuell</span>'
    : "";
  const dateMarkup = releasedAt
    ? `<time datetime="${escapeHtml(release.releasedAt)}">${escapeHtml(releasedAt)}</time>`
    : "";
  return `
    <section
      class="feature-list-release-group${isCurrent ? " is-current" : ""}"
      aria-labelledby="${headingId}"
      data-feature-list-version="${version}"
    >
      <header class="feature-list-release-header">
        <h3 id="${headingId}">Version ${version}</h3>
        <div class="feature-list-release-meta">${currentBadge}${dateMarkup}</div>
      </header>
      <ul class="feature-list-items">
        ${release.features.map((feature) => featureItemMarkup(feature, escapeHtml)).join("")}
      </ul>
    </section>
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
  function render(dialog) {
    const currentVersion = textValue(applicationVersion());
    const releases = readFeatureReleaseNotes();
    const release = releaseForVersion(releases, currentVersion);
    const presentation = featureListForRelease(release);
    const displayedVersion = currentVersion || textValue(release?.version) || "unknown";
    const history = featureReleaseHistory(
      presentation.releases.length
        ? presentation.releases
        : [
            {
              version: displayedVersion,
              releasedAt: textValue(release?.releasedAt),
              features: presentation.features,
            },
          ]
    );
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
      introductionNode.textContent = `${presentation.introduction} Die wichtigsten Änderungen sind nach Version gegliedert.`;
    }
    if (body) {
      body.innerHTML = history.length
        ? history
            .map((entry) => releaseGroupMarkup(entry, currentVersion, escapeHtml))
            .join("")
        : '<p class="feature-list-item feature-list-item-empty">Für diesen Build ist noch keine Featureliste verfügbar.</p>';
    }
    if (noteNode) {
      noteNode.textContent = presentation.pocNote;
    }
  }

  function renderLoading(dialog) {
    const displayedVersion = textValue(applicationVersion()) || "unknown";
    const releaseNode = dialog.querySelector("[data-feature-list-release]");
    const titleNode = dialog.querySelector("[data-feature-list-title]");
    const introductionNode = dialog.querySelector("[data-feature-list-introduction]");
    const body = dialog.querySelector("[data-feature-list-body]");
    const noteNode = dialog.querySelector("[data-feature-list-note]");

    if (releaseNode) {
      releaseNode.textContent = `Featureliste · V${displayedVersion}`;
    }
    if (titleNode) {
      titleNode.textContent = fallbackPresentation.title;
    }
    if (introductionNode) {
      introductionNode.textContent = "Die aktuelle Featureliste wird geladen.";
    }
    if (body) {
      body.innerHTML =
        '<p class="feature-list-item feature-list-item-empty" data-feature-list-loading>Featureliste wird geladen…</p>';
    }
    if (noteNode) {
      noteNode.textContent = "";
    }
  }

  function renderLoadFailure(dialog) {
    const introductionNode = dialog.querySelector("[data-feature-list-introduction]");
    const body = dialog.querySelector("[data-feature-list-body]");
    const noteNode = dialog.querySelector("[data-feature-list-note]");

    if (introductionNode) {
      introductionNode.textContent = fallbackPresentation.introduction;
    }
    if (body) {
      body.innerHTML =
        '<p class="feature-list-item feature-list-item-empty" role="status">Die Featureliste konnte nicht geladen werden. Bitte versuchen Sie es erneut.</p>';
    }
    if (noteNode) {
      noteNode.textContent = fallbackPresentation.pocNote;
    }
  }

  function show(trigger = null) {
    const dialog = ensureDialog();
    const closeButton = dialog.querySelector("[data-feature-list-close]");
    if (dialog.open) {
      closeButton?.focus();
      return Promise.resolve();
    }

    renderLoading(dialog);

    const returnTarget = focusableReturnTarget(trigger);
    syncTriggerExpanded(true);

    const closed = new Promise((resolve) => {
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

    Promise.resolve()
      .then(() => ensureReleaseNotes())
      .then(() => {
        if (dialog.open) {
          render(dialog);
        }
      })
      .catch((error) => {
        console.error("Failed to load the current feature list.", error);
        if (dialog.open) {
          renderLoadFailure(dialog);
        }
      });

    return closed;
  }

  return { show };
}
