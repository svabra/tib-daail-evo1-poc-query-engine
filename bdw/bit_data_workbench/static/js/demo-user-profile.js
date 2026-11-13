export function syncDemoUserProfile(user) {
  document.querySelectorAll("[data-demo-user-profile]").forEach((trigger) => {
    trigger.setAttribute("aria-label", `Profil: ${user.displayName}`);
  });
  document.querySelectorAll("[data-demo-user-name]").forEach((node) => {
    node.textContent = user.displayName;
  });
  document.querySelectorAll("[data-demo-user-organization]").forEach((node) => {
    node.textContent = user.organizationShort || user.organization;
  });
  document.querySelectorAll("[data-demo-user-email]").forEach((node) => {
    node.textContent = user.email;
    node.href = `mailto:${user.email}`;
  });
  document.querySelectorAll("[data-demo-user-select]").forEach((button) => {
    const selected = button.dataset.userId === user.id;
    button.setAttribute("aria-current", String(selected));
    const card = button.closest(".demo-user-option");
    card?.classList.toggle("is-current", selected);
    const marker = card?.querySelector("[data-demo-user-current]");
    if (marker) marker.hidden = !selected;
  });
}

export function initializeDemoUserProfile({ users, currentUser, selectUser }) {
  const dialog = document.querySelector("[data-demo-user-dialog]");
  const trigger = document.querySelector("[data-demo-user-profile]");
  if (!dialog || !trigger || dialog.dataset.bound === "true") return;

  const title = dialog.querySelector("[data-demo-user-dialog-title]");
  const description = dialog.querySelector("[data-demo-user-dialog-description]");
  const profile = dialog.querySelector("[data-demo-user-profile-view]");
  const options = dialog.querySelector("[data-demo-user-options]");
  const template = dialog.querySelector("[data-demo-user-option-template]");
  const collator = new Intl.Collator("de-CH");
  for (const user of [...users].sort((a, b) => collator.compare(a.displayName, b.displayName))) {
    const card = template.content.cloneNode(true);
    const name = card.querySelector("[data-demo-user-option-name]");
    name.textContent = user.displayName;
    name.id = `demo-user-option-${user.id}`;
    card.querySelector("article").setAttribute("aria-labelledby", name.id);
    card.querySelector("[data-demo-user-option-organization]").textContent =
      user.organizationShort || user.organization;
    const avatar = card.querySelector("[data-demo-user-option-avatar]");
    avatar.hidden = !user.avatarUrl;
    if (user.avatarUrl) avatar.src = user.avatarUrl;
    const initials = card.querySelector("[data-demo-user-option-initials]");
    initials.hidden = Boolean(user.avatarUrl);
    initials.textContent = user.displayName.split(/\s+/).slice(0, 2).map((part) => part[0]).join("");
    const button = card.querySelector("[data-demo-user-select]");
    button.dataset.userId = user.id;
    button.setAttribute("aria-label", `Als ${user.displayName} auswählen`);
    options.appendChild(card);
  }

  function showView(picker) {
    profile.hidden = picker;
    options.hidden = !picker;
    title.textContent = picker ? "Benutzer auswählen" : "Mein Profil";
    description.textContent = picker
      ? "Wählen Sie ein Benutzerprofil für diese lokale Vorschau."
      : "Ihre Organisation und Rollen in DAAIF.";
    title.focus();
  }

  trigger.addEventListener("click", () => {
    syncDemoUserProfile(currentUser());
    showView(false);
    dialog.showModal();
    trigger.setAttribute("aria-expanded", "true");
    title.focus();
  });
  dialog.querySelector("[data-demo-user-switch]").addEventListener("click", () => showView(true));
  dialog.querySelector("[data-demo-user-dialog-close]").addEventListener("click", () => dialog.close());
  options.addEventListener("click", (event) => {
    const button = event.target.closest("[data-demo-user-select]");
    if (!button) return;
    selectUser(button.dataset.userId);
    dialog.close();
  });
  dialog.addEventListener("close", () => trigger.setAttribute("aria-expanded", "false"));
  dialog.dataset.bound = "true";
}
