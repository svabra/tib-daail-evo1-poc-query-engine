import { ensureConfirmDialog, ensureMessageDialog, folderNameDialog } from "./dialogs.js";

export function closeDialog(dialog, returnValue = "") {
  if (!dialog) {
    return;
  }

  if (typeof dialog.close === "function") {
    dialog.close(returnValue);
  }
}

export function showFolderNameDialog({ title, copy, submitLabel, initialValue = "" }) {
  const dialog = folderNameDialog();
  if (!dialog) {
    const fallback = window.prompt(copy, initialValue);
    return Promise.resolve(fallback ? fallback.trim() : null);
  }

  const form = dialog.querySelector("[data-folder-name-form]");
  const titleNode = dialog.querySelector("[data-folder-name-title]");
  const copyNode = dialog.querySelector("[data-folder-name-copy]");
  const input = dialog.querySelector("[data-folder-name-input]");
  const submit = dialog.querySelector("[data-folder-name-submit]");
  const cancel = dialog.querySelector("[data-modal-cancel]");

  titleNode.textContent = title;
  copyNode.textContent = copy;
  submit.textContent = submitLabel;
  input.value = initialValue;

  return new Promise((resolve) => {
    const teardown = () => {
      form.removeEventListener("submit", onSubmit);
      cancel?.removeEventListener("click", onCancel);
      dialog.removeEventListener("close", onClose);
    };

    const onSubmit = (event) => {
      event.preventDefault();
      closeDialog(dialog, "confirm");
    };

    const onCancel = () => closeDialog(dialog, "cancel");

    const onClose = () => {
      const confirmed = dialog.returnValue === "confirm";
      const value = confirmed ? input.value.trim() : null;
      teardown();
      resolve(value || null);
    };

    form.addEventListener("submit", onSubmit);
    cancel?.addEventListener("click", onCancel);
    dialog.addEventListener("close", onClose, { once: true });
    dialog.showModal();
    input.focus();
    input.select();
  });
}

export function showConfirmDialog({ title, copy, confirmLabel, option = null, confirmTone = "danger" }) {
  const dialog = ensureConfirmDialog();

  const titleNode = dialog.querySelector("[data-confirm-title]");
  const copyNode = dialog.querySelector("[data-confirm-copy]");
  const submit = dialog.querySelector("[data-confirm-submit]");
  const cancel = dialog.querySelector("[data-modal-cancel]");
  const optionContainer = dialog.querySelector("[data-confirm-option-container]");
  const optionInput = dialog.querySelector("[data-confirm-option-input]");
  const optionLabel = dialog.querySelector("[data-confirm-option-label]");

  titleNode.textContent = title;
  copyNode.textContent = copy;
  submit.textContent = confirmLabel;
  submit.classList.toggle("modal-button-danger", confirmTone === "danger");

  if (optionContainer && optionInput && optionLabel) {
    optionInput.checked = false;

    if (option) {
      optionContainer.hidden = false;
      optionLabel.textContent = option.label;
    } else {
      optionContainer.hidden = true;
      optionLabel.textContent = "";
    }
  }

  return new Promise((resolve) => {
    const applyOptionState = () => {
      if (!optionInput || !option) {
        copyNode.textContent = copy;
        submit.textContent = confirmLabel;
        submit.disabled = false;
        return;
      }

      const optionChecked = optionInput.checked;
      copyNode.textContent = optionChecked ? option.checkedCopy ?? copy : copy;
      submit.textContent = optionChecked
        ? option.checkedConfirmLabel ?? confirmLabel
        : confirmLabel;
      submit.disabled = Boolean(option.required) && !optionChecked;
    };

    const onCancel = () => closeDialog(dialog, "cancel");
    const onClose = () => {
      cancel?.removeEventListener("click", onCancel);
      optionInput?.removeEventListener("change", applyOptionState);
      resolve({
        confirmed: dialog.returnValue === "confirm",
        optionChecked: Boolean(optionInput?.checked),
      });
    };

    applyOptionState();
    cancel?.addEventListener("click", onCancel);
    optionInput?.addEventListener("change", applyOptionState);
    dialog.addEventListener("close", onClose, { once: true });
    dialog.showModal();
  });
}

export function showMessageDialog({ title, copy, actionLabel = "OK" }) {
  const dialog = ensureMessageDialog();
  const titleNode = dialog.querySelector("[data-message-title]");
  const copyNode = dialog.querySelector("[data-message-copy]");
  const form = dialog.querySelector("[data-message-form]");
  const submit = dialog.querySelector("[data-message-submit]");

  titleNode.textContent = title;
  copyNode.textContent = copy;
  submit.textContent = actionLabel;

  return new Promise((resolve) => {
    const onSubmit = (event) => {
      event.preventDefault();
      closeDialog(dialog, "confirm");
    };

    const onClose = () => {
      form.removeEventListener("submit", onSubmit);
      resolve();
    };

    form.addEventListener("submit", onSubmit);
    dialog.addEventListener("close", onClose, { once: true });
    dialog.showModal();
  });
}