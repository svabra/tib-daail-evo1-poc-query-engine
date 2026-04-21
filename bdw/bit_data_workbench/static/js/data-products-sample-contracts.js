export function createDataProductsSampleContracts({ escapeHtml }) {
  function prettyJson(value) {
    return escapeHtml(JSON.stringify(value, null, 2));
  }

  function previewContractMarkup(preview) {
    if (!preview) {
      return '<p class="modal-copy">Generate a preview to review the public response contract.</p>';
    }

    if (preview.responseKind === "object") {
      return `
        <div class="data-product-contract-copy">
          <p class="modal-copy">Response schema</p>
          <pre class="data-product-contract-pre">${prettyJson(preview.responseSchema)}</pre>
        </div>
        <div class="data-product-contract-copy">
          <p class="modal-copy">The endpoint streams raw object bytes.</p>
          <pre class="data-product-contract-pre">${prettyJson(preview.sampleResponse)}</pre>
        </div>
      `;
    }

    return `
      <div class="data-product-contract-copy">
        <p class="modal-copy">Response schema</p>
        <pre class="data-product-contract-pre">${prettyJson(preview.responseSchema)}</pre>
      </div>
      <div class="data-product-contract-copy">
        <p class="modal-copy">Example response shape</p>
        <pre class="data-product-contract-pre">${prettyJson(preview.sampleResponse)}</pre>
      </div>
    `;
  }

  return {
    previewContractMarkup,
  };
}
