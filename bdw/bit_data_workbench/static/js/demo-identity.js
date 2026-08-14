export const DAAIF_DEMO_USER_STORAGE_KEY = "daaif-demo-user";
export const DAAIF_DEMO_USER_CHANGE_EVENT = "daaif-demo-user-change";

export const DAAIF_DEMO_USERS = Object.freeze([
  Object.freeze({
    id: "kassandra.valdata",
    displayName: "Kassandra Valdata",
    organization: "Eidgenössische Steuerverwaltung ESTV",
    email: "kassandra.valdata@estv.admin.ch",
    role: "Data Owner",
    avatarUrl: "/static/img/daca/kassandra-valdata.webp",
  }),
  Object.freeze({
    id: "noemie.rochat",
    displayName: "Noémie Rochat",
    organization: "Kanton Neuchâtel",
    email: "noemie.rochat@ne.ch",
    role: "Data Owner",
    avatarUrl: "/static/img/daca/noemie-rochat.webp",
  }),
  Object.freeze({
    id: "beat.stalder",
    displayName: "Beat Stalder",
    organization: "Kanton St. Gallen",
    email: "beat.stalder@sg.ch",
    role: "Data Consumer",
    avatarUrl: "",
  }),
  Object.freeze({
    id: "joel.ruod",
    displayName: "Joel Ruod",
    organization: "Eidgenössische Steuerverwaltung ESTV",
    email: "joel.ruod@estv.admin.ch",
    role: "Data Analyst / Data Owner",
    avatarUrl: "",
  }),
  Object.freeze({
    id: "thomas.kriegli",
    displayName: "Thomas Kriegli",
    organization: "Eidgenössische Steuerverwaltung ESTV",
    email: "thomas.kriegli@estv.admin.ch",
    role: "Leiter Datenanalyse / Publication Approver",
    avatarUrl: "",
  }),
]);

const DEFAULT_USER_ID = "joel.ruod";

function userForId(userId) {
  return DAAIF_DEMO_USERS.find((user) => user.id === userId) ?? null;
}

function storedUserId() {
  try {
    const userId = window.localStorage.getItem(DAAIF_DEMO_USER_STORAGE_KEY);
    return userForId(userId)?.id ?? DEFAULT_USER_ID;
  } catch (_error) {
    return DEFAULT_USER_ID;
  }
}

function initialsForUser(user) {
  return String(user?.displayName || "")
    .split(/\s+/)
    .filter(Boolean)
    .slice(0, 2)
    .map((part) => part[0]?.toLocaleUpperCase("de-CH") || "")
    .join("");
}

function publishIdentity(user, { announce = true } = {}) {
  document.documentElement.dataset.daaifDemoUser = user.id;
  if (document.body) {
    document.body.dataset.daaifDemoUser = user.id;
  }

  document.querySelectorAll("[data-demo-user-select]").forEach((select) => {
    if (select instanceof HTMLSelectElement) {
      select.value = user.id;
      select.setAttribute(
        "aria-label",
        `Demo-Benutzer wechseln. Aktuell ${user.displayName}, ${user.organization}.`
      );
    }
  });
  document.querySelectorAll("[data-demo-user-avatar]").forEach((avatar) => {
    if (!(avatar instanceof HTMLImageElement)) {
      return;
    }
    avatar.hidden = !user.avatarUrl;
    avatar.src = user.avatarUrl || "";
    avatar.alt = "";
  });
  document.querySelectorAll("[data-demo-user-initials]").forEach((fallback) => {
    fallback.hidden = Boolean(user.avatarUrl);
    fallback.textContent = initialsForUser(user);
  });
  document.querySelectorAll("[data-demo-user-role-copy]").forEach((node) => {
    node.textContent = user.role;
  });

  if (announce) {
    window.dispatchEvent(
      new CustomEvent(DAAIF_DEMO_USER_CHANGE_EVENT, {
        detail: { userId: user.id, user },
      })
    );
  }
}

export function currentDaaifDemoUser() {
  return userForId(document.documentElement.dataset.daaifDemoUser) ?? userForId(storedUserId());
}

export function selectDaaifDemoUser(userId, { announce = true } = {}) {
  const user = userForId(String(userId || "").trim());
  if (!user) {
    return currentDaaifDemoUser();
  }
  try {
    window.localStorage.setItem(DAAIF_DEMO_USER_STORAGE_KEY, user.id);
  } catch (_error) {
    // The demo remains usable when storage is unavailable.
  }
  publishIdentity(user, { announce });
  return user;
}

export function initializeDaaifDemoIdentity() {
  document.querySelectorAll("[data-demo-user-select]").forEach((select) => {
    if (!(select instanceof HTMLSelectElement) || select.dataset.bound === "true") {
      return;
    }
    select.dataset.bound = "true";
    select.addEventListener("change", () => selectDaaifDemoUser(select.value));
  });

  const user = userForId(storedUserId()) ?? userForId(DEFAULT_USER_ID);
  publishIdentity(user, { announce: true });
  return user;
}

export function syncDaaifFederalNavigation(section = "") {
  const normalized = String(section || "").trim().toLowerCase();
  const aliases = new Map([
    ["query-runs", "query"],
    ["service-consumption", "query"],
  ]);
  const activeSection = aliases.get(normalized) || normalized;
  document.querySelectorAll("[data-federal-nav-section]").forEach((link) => {
    const isActive = String(link.dataset.federalNavSection || "") === activeSection;
    link.classList.toggle("is-active", isActive);
    if (isActive) {
      link.setAttribute("aria-current", "page");
    } else {
      link.removeAttribute("aria-current");
    }
  });
}

if (typeof window !== "undefined") {
  Object.defineProperty(window, "daaifDemoIdentity", {
    configurable: true,
    value: Object.freeze({
      users: DAAIF_DEMO_USERS,
      currentUser: currentDaaifDemoUser,
      select: selectDaaifDemoUser,
    }),
  });
}
