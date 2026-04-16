const TRUNCATION_MARKER = "[..]";
const MAX_SOURCE_NAVIGATION_STEM_CHARS = 25;

export function truncateSourceNavigationLabel(value, maxStemChars = MAX_SOURCE_NAVIGATION_STEM_CHARS) {
  const text = String(value || "").trim();
  if (!text) {
    return "";
  }

  const lastDot = text.lastIndexOf(".");
  const hasFileSuffix = lastDot > 0 && lastDot < text.length - 1;
  if (hasFileSuffix) {
    const stem = text.slice(0, lastDot);
    const suffix = text.slice(lastDot);
    if (stem.length <= maxStemChars) {
      return text;
    }
    return `${stem.slice(0, maxStemChars)}${TRUNCATION_MARKER}${suffix}`;
  }

  if (text.length <= maxStemChars) {
    return text;
  }
  return `${text.slice(0, maxStemChars)}${TRUNCATION_MARKER}`;
}
