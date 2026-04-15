export function createEditorAutosizeManager({
  currentEditorSql,
  defaultAutoRows,
  editorRegistry,
  editorSizingRegistry,
  numericCssValue,
  preferredSqlEditorRows,
}) {
  function editorSizingState(root) {
    let state = editorSizingRegistry.get(root);
    if (!state) {
      state = {
        applying: false,
        autoHeight: 0,
        interacted: false,
        manual: false,
        observer: null,
      };
      editorSizingRegistry.set(root, state);
    }
    return state;
  }

  function editorHeightMetrics(root) {
    const textarea = root.querySelector("[data-editor-source]");
    const editor = editorRegistry.get(root);
    const sizingState = editorSizingState(root);
    const baseRows = preferredSqlEditorRows(currentEditorSql(root));
    if (editor) {
      const editorStyles = window.getComputedStyle(editor.dom);
      const scroller = editor.dom.querySelector(".cm-scroller");
      const scrollerStyles = scroller ? window.getComputedStyle(scroller) : null;
      const lineHeight =
        editor.defaultLineHeight ||
        numericCssValue(editorStyles, "lineHeight") ||
        numericCssValue(scrollerStyles, "lineHeight") ||
        22;
      const borderHeight =
        numericCssValue(editorStyles, "borderTopWidth") +
        numericCssValue(editorStyles, "borderBottomWidth");
      const scrollerPadding =
        numericCssValue(scrollerStyles, "paddingTop") +
        numericCssValue(scrollerStyles, "paddingBottom");
      const minHeight = Math.ceil(lineHeight * baseRows + scrollerPadding + borderHeight);
      const contentHeight = Math.ceil((scroller?.scrollHeight ?? editor.dom.scrollHeight) + borderHeight);
      const maxAutoHeight = Math.ceil(
        lineHeight * (sizingState.interacted ? defaultAutoRows : baseRows) + scrollerPadding + borderHeight
      );
      return {
        minHeight,
        nextHeight: Math.max(minHeight, Math.min(contentHeight, maxAutoHeight)),
      };
    }

    if (!textarea) {
      return null;
    }

    const styles = window.getComputedStyle(textarea);
    const lineHeight = numericCssValue(styles, "lineHeight") || 22;
    const chromeHeight =
      numericCssValue(styles, "paddingTop") +
      numericCssValue(styles, "paddingBottom") +
      numericCssValue(styles, "borderTopWidth") +
      numericCssValue(styles, "borderBottomWidth");
    const minHeight = Math.ceil(lineHeight * baseRows + chromeHeight);

    const previousHeight = textarea.style.height;
    textarea.style.height = "auto";
    const contentHeight = Math.ceil(textarea.scrollHeight);
    textarea.style.height = previousHeight;
    const maxAutoHeight = Math.ceil(
      lineHeight * (sizingState.interacted ? defaultAutoRows : baseRows) + chromeHeight
    );

    return {
      minHeight,
      nextHeight: Math.max(minHeight, Math.min(contentHeight, maxAutoHeight)),
    };
  }

  function observeEditorResize(root) {
    if (!(root instanceof Element) || typeof window.ResizeObserver !== "function") {
      return;
    }

    const state = editorSizingState(root);
    if (state.observer) {
      return;
    }

    state.observer = new window.ResizeObserver((entries) => {
      const nextHeight = entries[0]?.contentRect?.height ?? 0;
      if (!nextHeight || state.applying || !state.autoHeight) {
        return;
      }

      state.manual = Math.abs(nextHeight - state.autoHeight) > 2;
    });
    state.observer.observe(root);
  }

  function markEditorInteracted(root) {
    editorSizingState(root).interacted = true;
  }

  function autosizeEditor(root) {
    if (!(root instanceof Element)) {
      return;
    }

    observeEditorResize(root);
    const state = editorSizingState(root);
    if (state.manual) {
      return;
    }

    const metrics = editorHeightMetrics(root);
    if (!metrics) {
      return;
    }

    state.autoHeight = metrics.nextHeight;
    state.applying = true;
    root.style.minHeight = `${metrics.minHeight}px`;
    root.style.height = `${metrics.nextHeight}px`;
    window.setTimeout(() => {
      state.applying = false;
    }, 0);
  }

  return {
    autosizeEditor,
    markEditorInteracted,
  };
}