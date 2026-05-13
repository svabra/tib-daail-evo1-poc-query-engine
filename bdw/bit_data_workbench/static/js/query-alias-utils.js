export function normalizeQueryAliasSegment(value, fallback = "item") {
  let normalized = String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/_+/g, "_")
    .replace(/^_+|_+$/g, "");
  if (!normalized) {
    normalized = fallback;
  }
  if (/^[0-9]/.test(normalized)) {
    normalized = `n_${normalized}`;
  }
  return normalized;
}

function aliasHash(value) {
  let hash = 2166136261;
  const text = String(value || "");
  for (let index = 0; index < text.length; index += 1) {
    hash ^= text.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return (hash >>> 0).toString(16).padStart(8, "0").slice(0, 8);
}

function fileAliasSegments(fileName, fallback = "file") {
  const name = String(fileName || "").trim().split(/[\\/]/).filter(Boolean).pop() || "";
  if (!name) {
    return [normalizeQueryAliasSegment(fallback, fallback)];
  }
  const dotIndex = name.lastIndexOf(".");
  const hasExtension = dotIndex > 0 && dotIndex < name.length - 1;
  const stem = hasExtension ? name.slice(0, dotIndex) : name;
  const segments = [normalizeQueryAliasSegment(stem, fallback)];
  if (hasExtension) {
    segments.push(normalizeQueryAliasSegment(name.slice(dotIndex + 1), "file"));
  }
  return segments;
}

export function localWorkspaceQueryAlias(entry = {}) {
  const segments = ["local"];
  String(entry.folderPath || "")
    .replace(/\\/g, "/")
    .split("/")
    .filter((part) => part.trim())
    .forEach((part) => {
      segments.push(normalizeQueryAliasSegment(part, "folder"));
    });
  segments.push(...fileAliasSegments(entry.fileName, "file"));
  return segments.join(".");
}

function addStableAliasSuffix(alias, seed) {
  const parts = String(alias || "").split(".").filter(Boolean);
  if (!parts.length) {
    return alias;
  }
  const targetIndex = parts.length >= 3 ? parts.length - 2 : parts.length - 1;
  parts[targetIndex] = `${parts[targetIndex]}_${aliasHash(seed || alias)}`;
  return parts.join(".");
}

export function localWorkspaceQueryAliases(entries = []) {
  const normalizedEntries = Array.isArray(entries) ? entries : [];
  const counts = new Map();
  const baseAliases = new Map();
  normalizedEntries.forEach((entry) => {
    const alias = localWorkspaceQueryAlias(entry);
    baseAliases.set(String(entry?.id || ""), alias);
    const key = alias.toLowerCase();
    counts.set(key, (counts.get(key) || 0) + 1);
  });

  const aliases = new Map();
  normalizedEntries.forEach((entry) => {
    const entryId = String(entry?.id || "").trim();
    if (!entryId) {
      return;
    }
    const alias = baseAliases.get(entryId);
    aliases.set(
      entryId,
      counts.get(String(alias || "").toLowerCase()) > 1
        ? addStableAliasSuffix(alias, entryId)
        : alias
    );
  });
  return aliases;
}

function isIdentifierStart(char) {
  return /[A-Za-z_$]/.test(char || "");
}

function isIdentifierContinue(char) {
  return /[A-Za-z0-9_$-]/.test(char || "");
}

function readIdentifier(text, index) {
  let current = index + 1;
  while (current < text.length && isIdentifierContinue(text[current])) {
    current += 1;
  }
  return { value: text.slice(index, current), end: current };
}

function skipQuoted(text, index, quote, doubledEscape = true) {
  let current = index + 1;
  while (current < text.length) {
    if (text[current] === quote) {
      if (doubledEscape && current + 1 < text.length && text[current + 1] === quote) {
        current += 2;
        continue;
      }
      return current + 1;
    }
    current += 1;
  }
  return current;
}

function minimumAliasParts(root, configuredRoots) {
  if (root === "local") {
    return 3;
  }
  if (root === "s3" || root === "workspace") {
    return 4;
  }
  return configuredRoots.has(root) ? 2 : 3;
}

export function findQueryAliasReferences(sql, roots = new Set(["local", "s3"])) {
  const configuredRoots = new Set(Array.from(roots).map((root) => String(root || "").toLowerCase()));
  const text = String(sql || "");
  const references = [];
  let index = 0;

  while (index < text.length) {
    const char = text[index];
    const nextChar = index + 1 < text.length ? text[index + 1] : "";
    if (char === "-" && nextChar === "-") {
      const newline = text.indexOf("\n", index + 2);
      index = newline < 0 ? text.length : newline + 1;
      continue;
    }
    if (char === "/" && nextChar === "*") {
      const end = text.indexOf("*/", index + 2);
      index = end < 0 ? text.length : end + 2;
      continue;
    }
    if (char === "'") {
      index = skipQuoted(text, index, "'", true);
      continue;
    }
    if (char === '"') {
      index = skipQuoted(text, index, '"', true);
      continue;
    }
    if (char === "`") {
      index = skipQuoted(text, index, "`", false);
      continue;
    }
    if (char === "[") {
      const end = text.indexOf("]", index + 1);
      index = end < 0 ? text.length : end + 1;
      continue;
    }
    if (!isIdentifierStart(char)) {
      index += 1;
      continue;
    }
    if (index > 0 && (text[index - 1] === "." || isIdentifierContinue(text[index - 1]))) {
      index += 1;
      continue;
    }

    const root = readIdentifier(text, index);
    const normalizedRoot = root.value.toLowerCase();
    if (!configuredRoots.has(normalizedRoot)) {
      index = root.end;
      continue;
    }

    const parts = [root.value];
    const partEnds = [root.end];
    let current = root.end;
    while (current < text.length && text[current] === ".") {
      const nextIndex = current + 1;
      if (nextIndex >= text.length || !isIdentifierStart(text[nextIndex])) {
        break;
      }
      const part = readIdentifier(text, nextIndex);
      parts.push(part.value);
      partEnds.push(part.end);
      current = part.end;
    }

    if (parts.length >= minimumAliasParts(normalizedRoot, configuredRoots)) {
      references.push({
        alias: parts.join("."),
        parts,
        partEnds,
        start: index,
        end: partEnds[partEnds.length - 1],
      });
      index = partEnds[partEnds.length - 1];
      continue;
    }
    index += 1;
  }
  return references;
}

export function replaceQueryAliases(sql, aliasMap = new Map()) {
  const normalizedMap = new Map();
  const roots = new Set();
  aliasMap.forEach((replacement, alias) => {
    const key = String(alias || "").trim().toLowerCase();
    if (!key || !String(replacement || "").trim()) {
      return;
    }
    normalizedMap.set(key, String(replacement).trim());
    roots.add(key.split(".", 1)[0]);
  });
  if (!normalizedMap.size) {
    return String(sql || "");
  }

  const text = String(sql || "");
  const pieces = [];
  let lastIndex = 0;
  findQueryAliasReferences(text, roots).forEach((reference) => {
    let replacement = null;
    let replacementEnd = reference.end;
    for (let count = reference.parts.length; count > 1; count -= 1) {
      const candidate = reference.parts.slice(0, count).join(".").toLowerCase();
      if (normalizedMap.has(candidate)) {
        replacement = normalizedMap.get(candidate);
        replacementEnd = reference.partEnds[count - 1];
        break;
      }
    }
    if (!replacement) {
      return;
    }
    pieces.push(text.slice(lastIndex, reference.start));
    pieces.push(replacement);
    lastIndex = replacementEnd;
  });
  pieces.push(text.slice(lastIndex));
  return pieces.join("");
}
