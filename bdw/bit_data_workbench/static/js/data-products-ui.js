export function createDataProductsUi() {
  function appendModalDialog(markup) {
    document.body.insertAdjacentHTML("beforeend", markup.trim());
  }

  function dataProductsPageRoot() {
    return document.querySelector("[data-data-products-page]");
  }

  function dataProductCardNodes() {
    return Array.from(document.querySelectorAll("[data-data-product-card]"));
  }

  function dataProductSearchInput() {
    return document.querySelector("[data-data-product-search]");
  }

  function dataProductSearchEmpty() {
    return document.querySelector("[data-data-product-search-empty]");
  }

  function readDataProductSourceOptions() {
    const node = document.getElementById("data-product-sources-json");
    if (!node?.textContent) {
      return [];
    }

    try {
      const parsed = JSON.parse(node.textContent);
      return Array.isArray(parsed) ? parsed.filter((item) => item?.source?.sourceKind) : [];
    } catch (_error) {
      return [];
    }
  }

  function ensurePublicationDialog() {
    let dialog = document.querySelector("[data-data-product-dialog]");
    if (dialog) {
      return dialog;
    }

    appendModalDialog(`
      <dialog class="modal-dialog modal-dialog-wide data-product-dialog" data-data-product-dialog>
        <form method="dialog" class="modal-card modal-card-wide data-product-dialog-card" data-data-product-form>
          <div class="data-product-dialog-header">
            <div>
              <h2 class="modal-title" data-data-product-dialog-title>Create data product</h2>
              <p class="modal-copy" data-data-product-dialog-copy>
                Publish a live, read-only data product from a server-visible source.
              </p>
            </div>
            <div class="data-product-stepper" data-data-product-stepper>
              <span class="data-product-step-indicator" data-data-product-step-indicator="1">1. Source</span>
              <span class="data-product-step-indicator" data-data-product-step-indicator="2">2. Rules</span>
              <span class="data-product-step-indicator" data-data-product-step-indicator="3">3. Metadata</span>
              <span class="data-product-step-indicator" data-data-product-step-indicator="4">4. Publish</span>
            </div>
          </div>

          <section class="data-product-step-panel" data-data-product-step-panel="1">
            <div class="data-product-source-picker-grid">
              <label class="result-export-field">
                <span class="result-export-field-label">Data source type</span>
                <select class="modal-input" data-data-product-source-type-select></select>
              </label>
              <label class="result-export-field">
                <span class="result-export-field-label">Source</span>
                <select class="modal-input" data-data-product-source-select></select>
              </label>
            </div>
            <p class="modal-copy" data-data-product-source-empty hidden>
              No publishable server-visible sources are currently visible in this runtime.
            </p>
            <div class="data-product-source-preview" data-data-product-source-preview></div>
          </section>

          <section class="data-product-step-panel" data-data-product-step-panel="2" hidden>
            <div class="data-product-compatibility-card" data-data-product-compatibility-card></div>
          </section>

          <section class="data-product-step-panel" data-data-product-step-panel="3" hidden>
            <div class="data-product-form-grid">
              <label class="result-export-field">
                <span class="result-export-field-label">Title</span>
                <input class="modal-input" type="text" autocomplete="off" data-data-product-title-input>
              </label>
              <label class="result-export-field">
                <span class="result-export-field-label">Slug</span>
                <input class="modal-input" type="text" autocomplete="off" data-data-product-slug-input>
              </label>
              <label class="result-export-field data-product-form-grid-wide">
                <span class="result-export-field-label">Description</span>
                <textarea class="modal-input data-product-textarea" rows="4" data-data-product-description-input></textarea>
              </label>
              <label class="result-export-field">
                <span class="result-export-field-label">Owner</span>
                <input class="modal-input" type="text" autocomplete="off" data-data-product-owner-input>
              </label>
              <label class="result-export-field">
                <span class="result-export-field-label">Domain</span>
                <input class="modal-input" type="text" autocomplete="off" data-data-product-domain-input>
              </label>
              <label class="result-export-field data-product-form-grid-wide">
                <span class="result-export-field-label">Tags</span>
                <input class="modal-input" type="text" autocomplete="off" placeholder="tax, vat, shared-workspace" data-data-product-tags-input>
              </label>
              <label class="result-export-field">
                <span class="result-export-field-label">Access level</span>
                <select class="modal-input" data-data-product-access-level-input>
                  <option value="internal">Internal</option>
                  <option value="restricted">Restricted</option>
                  <option value="confidential">Confidential</option>
                </select>
              </label>
              <label class="result-export-field">
                <span class="result-export-field-label">Request access contact</span>
                <input class="modal-input" type="text" autocomplete="off" data-data-product-request-access-contact-input>
              </label>
              <label class="result-export-field data-product-form-grid-wide">
                <span class="result-export-field-label">Access note</span>
                <textarea class="modal-input data-product-textarea" rows="3" data-data-product-access-note-input></textarea>
              </label>
            </div>
          </section>

          <section class="data-product-step-panel" data-data-product-step-panel="4" hidden>
            <div class="data-product-preview-panel" data-data-product-preview-summary></div>
            <div class="data-product-contract-panel" data-data-product-contract-panel></div>
          </section>

          <menu class="modal-actions">
            <button class="modal-button modal-button-secondary" type="button" data-modal-cancel>
              Cancel
            </button>
            <button class="modal-button modal-button-secondary" type="button" data-data-product-dialog-back hidden>
              Back
            </button>
            <button class="modal-button" type="button" data-data-product-dialog-next>
              Continue
            </button>
            <button class="modal-button" type="button" data-data-product-dialog-publish hidden>
              Publish
            </button>
          </menu>
        </form>
      </dialog>
    `);

    dialog = document.querySelector("[data-data-product-dialog]");
    return dialog;
  }

  function ensureEditDialog() {
    let dialog = document.querySelector("[data-data-product-edit-dialog]");
    if (dialog) {
      return dialog;
    }

    appendModalDialog(`
      <dialog class="modal-dialog modal-dialog-wide" data-data-product-edit-dialog>
        <form method="dialog" class="modal-card modal-card-wide data-product-dialog-card" data-data-product-edit-form>
          <div class="data-product-dialog-header">
            <div>
              <h2 class="modal-title">Edit data product metadata</h2>
              <p class="modal-copy">Update consumer-facing metadata without changing the published slug.</p>
            </div>
          </div>
          <input type="hidden" data-data-product-edit-id>
          <div class="data-product-form-grid">
            <label class="result-export-field">
              <span class="result-export-field-label">Title</span>
              <input class="modal-input" type="text" autocomplete="off" data-data-product-edit-title-input>
            </label>
            <label class="result-export-field">
              <span class="result-export-field-label">Slug</span>
              <input class="modal-input" type="text" autocomplete="off" data-data-product-edit-slug-input disabled>
            </label>
            <label class="result-export-field data-product-form-grid-wide">
              <span class="result-export-field-label">Description</span>
              <textarea class="modal-input data-product-textarea" rows="4" data-data-product-edit-description-input></textarea>
            </label>
            <label class="result-export-field">
              <span class="result-export-field-label">Owner</span>
              <input class="modal-input" type="text" autocomplete="off" data-data-product-edit-owner-input>
            </label>
            <label class="result-export-field">
              <span class="result-export-field-label">Domain</span>
              <input class="modal-input" type="text" autocomplete="off" data-data-product-edit-domain-input>
            </label>
            <label class="result-export-field data-product-form-grid-wide">
              <span class="result-export-field-label">Tags</span>
              <input class="modal-input" type="text" autocomplete="off" data-data-product-edit-tags-input>
            </label>
            <label class="result-export-field">
              <span class="result-export-field-label">Access level</span>
              <select class="modal-input" data-data-product-edit-access-level-input>
                <option value="internal">Internal</option>
                <option value="restricted">Restricted</option>
                <option value="confidential">Confidential</option>
              </select>
            </label>
            <label class="result-export-field">
              <span class="result-export-field-label">Request access contact</span>
              <input class="modal-input" type="text" autocomplete="off" data-data-product-edit-request-access-contact-input>
            </label>
            <label class="result-export-field data-product-form-grid-wide">
              <span class="result-export-field-label">Access note</span>
              <textarea class="modal-input data-product-textarea" rows="3" data-data-product-edit-access-note-input></textarea>
            </label>
          </div>
          <menu class="modal-actions">
            <button class="modal-button modal-button-secondary" type="button" data-modal-cancel>
              Cancel
            </button>
            <button class="modal-button" type="submit" value="confirm" data-data-product-edit-submit>
              Save metadata
            </button>
          </menu>
        </form>
      </dialog>
    `);

    dialog = document.querySelector("[data-data-product-edit-dialog]");
    return dialog;
  }

  return {
    dataProductCardNodes,
    dataProductSearchEmpty,
    dataProductSearchInput,
    dataProductsPageRoot,
    ensureEditDialog,
    ensurePublicationDialog,
    readDataProductSourceOptions,
  };
}
