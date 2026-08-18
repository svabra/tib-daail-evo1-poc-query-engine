export function dataProductSourceForPublication(source, { publishToDaca = false } = {}) {
  if (!source || typeof source !== "object") {
    return source ?? null;
  }
  if (
    publishToDaca &&
    source.sourceKind === "object" &&
    source.sourceId === "s3"
  ) {
    return {
      ...source,
      sourceKind: "relation",
      relation: "",
    };
  }
  return source;
}
