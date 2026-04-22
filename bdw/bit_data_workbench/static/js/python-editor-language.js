let pythonLanguageFactory = null;

try {
  ({ python: pythonLanguageFactory } = await import(
    "https://esm.sh/@codemirror/lang-python@6.2.1?target=es2022"
  ));
} catch (_error) {
  pythonLanguageFactory = null;
}

export function pythonLanguageSupport() {
  return typeof pythonLanguageFactory === "function" ? pythonLanguageFactory() : [];
}
