const sqlFormatKeywordPhrases = [
  ["LEFT", "OUTER", "JOIN"],
  ["RIGHT", "OUTER", "JOIN"],
  ["FULL", "OUTER", "JOIN"],
  ["INSERT", "INTO"],
  ["DELETE", "FROM"],
  ["GROUP", "BY"],
  ["ORDER", "BY"],
  ["UNION", "ALL"],
  ["INNER", "JOIN"],
  ["LEFT", "JOIN"],
  ["RIGHT", "JOIN"],
  ["FULL", "JOIN"],
  ["CROSS", "JOIN"],
];

const sqlFormatKeywords = new Set([
  "ALL",
  "AND",
  "AS",
  "ASC",
  "BETWEEN",
  "BY",
  "CASE",
  "DELETE",
  "DESC",
  "DISTINCT",
  "ELSE",
  "END",
  "EXCEPT",
  "EXISTS",
  "FETCH",
  "FROM",
  "FULL",
  "GROUP",
  "HAVING",
  "ILIKE",
  "IN",
  "INNER",
  "INSERT",
  "INTERSECT",
  "INTO",
  "IS",
  "JOIN",
  "LEFT",
  "LIKE",
  "LIMIT",
  "NOT",
  "NULL",
  "OFFSET",
  "ON",
  "OR",
  "ORDER",
  "OUTER",
  "QUALIFY",
  "RETURNING",
  "RIGHT",
  "SELECT",
  "SET",
  "THEN",
  "UNION",
  "UPDATE",
  "USING",
  "VALUES",
  "WHEN",
  "WHERE",
  "WINDOW",
  "WITH",
]);

const sqlFormatClauseKeywords = new Set([
  "DELETE FROM",
  "EXCEPT",
  "FETCH",
  "FROM",
  "GROUP BY",
  "HAVING",
  "INSERT INTO",
  "INTERSECT",
  "LIMIT",
  "OFFSET",
  "ORDER BY",
  "QUALIFY",
  "RETURNING",
  "SELECT",
  "SET",
  "UNION",
  "UNION ALL",
  "UPDATE",
  "VALUES",
  "WHERE",
  "WINDOW",
  "WITH",
]);

const sqlFormatJoinKeywords = new Set([
  "CROSS JOIN",
  "FULL JOIN",
  "FULL OUTER JOIN",
  "INNER JOIN",
  "JOIN",
  "LEFT JOIN",
  "LEFT OUTER JOIN",
  "RIGHT JOIN",
  "RIGHT OUTER JOIN",
]);

const sqlFormatBreakAfterKeywords = new Set([
  "GROUP BY",
  "HAVING",
  "ORDER BY",
  "QUALIFY",
  "RETURNING",
  "SELECT",
  "SET",
  "VALUES",
  "WHERE",
]);

const sqlFormatListKeywords = new Set([
  "GROUP BY",
  "ORDER BY",
  "RETURNING",
  "SELECT",
  "SET",
  "VALUES",
]);

const sqlFormatLogicalClauses = new Set(["HAVING", "ON", "USING", "WHERE"]);

function readSqlDollarQuotedLiteral(sqlText, startIndex) {
  const delimiterMatch = sqlText.slice(startIndex).match(/^\$[A-Za-z_][A-Za-z0-9_]*\$|^\$\$/);
  if (!delimiterMatch) {
    return null;
  }

  const delimiter = delimiterMatch[0];
  const endIndex = sqlText.indexOf(delimiter, startIndex + delimiter.length);
  if (endIndex === -1) {
    return {
      value: sqlText.slice(startIndex),
      nextIndex: sqlText.length,
    };
  }

  return {
    value: sqlText.slice(startIndex, endIndex + delimiter.length),
    nextIndex: endIndex + delimiter.length,
  };
}

function readSqlQuotedLiteral(sqlText, startIndex, delimiter) {
  let index = startIndex + 1;
  while (index < sqlText.length) {
    const current = sqlText[index];
    if (current === delimiter) {
      if (delimiter !== "`" && sqlText[index + 1] === delimiter) {
        index += 2;
        continue;
      }

      index += 1;
      break;
    }

    if (current === "\\" && index + 1 < sqlText.length) {
      index += 2;
      continue;
    }

    index += 1;
  }

  return {
    value: sqlText.slice(startIndex, index),
    nextIndex: index,
  };
}

function tokenizeSql(sqlText) {
  const tokens = [];
  let index = 0;

  while (index < sqlText.length) {
    const current = sqlText[index];

    if (/\s/.test(current)) {
      index += 1;
      continue;
    }

    if (current === "-" && sqlText[index + 1] === "-") {
      const startIndex = index;
      index += 2;
      while (index < sqlText.length && sqlText[index] !== "\n") {
        index += 1;
      }
      tokens.push({ type: "comment", value: sqlText.slice(startIndex, index) });
      continue;
    }

    if (current === "/" && sqlText[index + 1] === "*") {
      const startIndex = index;
      index += 2;
      while (index < sqlText.length && !(sqlText[index] === "*" && sqlText[index + 1] === "/")) {
        index += 1;
      }
      index = Math.min(index + 2, sqlText.length);
      tokens.push({ type: "comment", value: sqlText.slice(startIndex, index) });
      continue;
    }

    if (current === "$") {
      const dollarQuoted = readSqlDollarQuotedLiteral(sqlText, index);
      if (dollarQuoted) {
        tokens.push({ type: "string", value: dollarQuoted.value });
        index = dollarQuoted.nextIndex;
        continue;
      }
    }

    if (current === "'" || current === '"' || current === "`") {
      const quoted = readSqlQuotedLiteral(sqlText, index, current);
      tokens.push({ type: current === "'" ? "string" : "identifier", value: quoted.value });
      index = quoted.nextIndex;
      continue;
    }

    if (current === "[") {
      const endIndex = sqlText.indexOf("]", index + 1);
      tokens.push({
        type: "identifier",
        value: endIndex === -1 ? sqlText.slice(index) : sqlText.slice(index, endIndex + 1),
      });
      index = endIndex === -1 ? sqlText.length : endIndex + 1;
      continue;
    }

    if (/[A-Za-z_]/.test(current)) {
      const startIndex = index;
      index += 1;
      while (index < sqlText.length && /[A-Za-z0-9_$]/.test(sqlText[index])) {
        index += 1;
      }
      tokens.push({ type: "word", value: sqlText.slice(startIndex, index) });
      continue;
    }

    if (/[0-9]/.test(current)) {
      const startIndex = index;
      index += 1;
      while (index < sqlText.length && /[0-9.]/.test(sqlText[index])) {
        index += 1;
      }
      tokens.push({ type: "number", value: sqlText.slice(startIndex, index) });
      continue;
    }

    const doubleCharacterSymbol = sqlText.slice(index, index + 2);
    if (["!=", "<=", "<>", "::", "=>", ">=", "||"].includes(doubleCharacterSymbol)) {
      tokens.push({ type: "symbol", value: doubleCharacterSymbol });
      index += 2;
      continue;
    }

    tokens.push({ type: "symbol", value: current });
    index += 1;
  }

  return tokens;
}

function sqlKeywordPhraseMatches(tokens, startIndex, phrase) {
  if (startIndex + phrase.length > tokens.length) {
    return false;
  }

  return phrase.every((part, offset) => {
    const token = tokens[startIndex + offset];
    return token?.type === "word" && token.value.toUpperCase() === part;
  });
}

function combineSqlKeywordTokens(tokens) {
  const combined = [];

  for (let index = 0; index < tokens.length; index += 1) {
    const token = tokens[index];
    if (token.type !== "word") {
      combined.push(token);
      continue;
    }

    const matchedPhrase = sqlFormatKeywordPhrases.find((phrase) =>
      sqlKeywordPhraseMatches(tokens, index, phrase)
    );
    if (matchedPhrase) {
      combined.push({ type: "keyword", value: matchedPhrase.join(" ") });
      index += matchedPhrase.length - 1;
      continue;
    }

    const uppercaseValue = token.value.toUpperCase();
    if (sqlFormatKeywords.has(uppercaseValue)) {
      combined.push({ type: "keyword", value: uppercaseValue });
      continue;
    }

    combined.push(token);
  }

  return combined;
}

export function formatSqlText(sqlText) {
  const normalizedSql = String(sqlText ?? "").replace(/\r\n?/g, "\n").trim();
  if (!normalizedSql) {
    return "";
  }

  const tokens = combineSqlKeywordTokens(tokenizeSql(normalizedSql));
  const parts = [];
  let lineStart = true;
  let pendingSpace = false;
  let lineIndent = 0;
  let parenDepth = 0;
  let currentClause = null;
  let currentClauseDepth = 0;
  let currentClauseValueIndent = 0;
  let previousToken = null;

  const trimTrailingSpace = () => {
    while (parts[parts.length - 1] === " ") {
      parts.pop();
    }
  };

  const newline = (indent = lineIndent) => {
    trimTrailingSpace();
    if (parts.length && parts[parts.length - 1] !== "\n") {
      parts.push("\n");
    }
    lineStart = true;
    pendingSpace = false;
    lineIndent = Math.max(indent, 0);
  };

  const write = (value, { spaceBefore = true } = {}) => {
    if (!value) {
      return;
    }

    if (lineStart) {
      parts.push("  ".repeat(Math.max(lineIndent, 0)));
      lineStart = false;
    } else if (pendingSpace && spaceBefore) {
      parts.push(" ");
    }

    parts.push(value);
    pendingSpace = false;
  };

  const setClauseState = (keyword, clauseDepth, valueIndent) => {
    currentClause = keyword;
    currentClauseDepth = clauseDepth;
    currentClauseValueIndent = valueIndent;
  };

  tokens.forEach((token, index) => {
    if (token.type === "comment") {
      if (!lineStart) {
        newline(lineIndent);
      }
      write(token.value, { spaceBefore: false });
      if (index < tokens.length - 1) {
        newline(lineIndent);
      }
      previousToken = token;
      return;
    }

    if (token.type === "keyword") {
      if (sqlFormatJoinKeywords.has(token.value)) {
        newline(parenDepth);
        write(token.value, { spaceBefore: false });
        setClauseState(token.value, parenDepth, parenDepth + 1);
        pendingSpace = true;
        previousToken = token;
        return;
      }

      if (token.value === "ON" || token.value === "USING") {
        newline(parenDepth + 1);
        write(token.value, { spaceBefore: false });
        setClauseState(token.value, parenDepth, parenDepth + 1);
        pendingSpace = true;
        previousToken = token;
        return;
      }

      if ((token.value === "AND" || token.value === "OR") && sqlFormatLogicalClauses.has(currentClause)) {
        newline(currentClauseValueIndent);
        write(token.value, { spaceBefore: false });
        pendingSpace = true;
        previousToken = token;
        return;
      }

      if (sqlFormatClauseKeywords.has(token.value)) {
        if (!lineStart) {
          newline(parenDepth);
        }
        write(token.value, { spaceBefore: false });
        const clauseIndent = parenDepth;
        const valueIndent = sqlFormatBreakAfterKeywords.has(token.value) ? clauseIndent + 1 : clauseIndent;
        setClauseState(token.value, clauseIndent, valueIndent);
        if (sqlFormatBreakAfterKeywords.has(token.value)) {
          newline(valueIndent);
        } else {
          pendingSpace = true;
        }
        previousToken = token;
        return;
      }

      if (token.value === "WHEN" || token.value === "ELSE") {
        newline(parenDepth + 1);
        write(token.value, { spaceBefore: false });
        pendingSpace = true;
        previousToken = token;
        return;
      }

      write(token.value);
      pendingSpace = true;
      previousToken = token;
      return;
    }

    if (token.type === "symbol") {
      if (token.value === ",") {
        write(",", { spaceBefore: false });
        if (sqlFormatListKeywords.has(currentClause) && parenDepth === currentClauseDepth) {
          newline(currentClauseValueIndent);
        } else {
          pendingSpace = true;
        }
        previousToken = token;
        return;
      }

      if (token.value === ".") {
        write(".", { spaceBefore: false });
        previousToken = token;
        return;
      }

      if (token.value === "(") {
        write("(", {
          spaceBefore: previousToken?.type === "keyword",
        });
        parenDepth += 1;
        previousToken = token;
        return;
      }

      if (token.value === ")") {
        parenDepth = Math.max(parenDepth - 1, 0);
        if (lineStart) {
          lineIndent = parenDepth;
        }
        write(")", { spaceBefore: false });
        if (currentClause && parenDepth < currentClauseDepth) {
          currentClause = null;
          currentClauseDepth = 0;
          currentClauseValueIndent = 0;
        }
        previousToken = token;
        return;
      }

      if (token.value === ";") {
        write(";", { spaceBefore: false });
        if (index < tokens.length - 1) {
          newline(0);
          newline(0);
        }
        currentClause = null;
        currentClauseDepth = 0;
        currentClauseValueIndent = 0;
        previousToken = token;
        return;
      }

      if (token.value === "::") {
        write("::", { spaceBefore: false });
        previousToken = token;
        return;
      }

      write(token.value);
      pendingSpace = true;
      previousToken = token;
      return;
    }

    write(token.value);
    pendingSpace = true;
    previousToken = token;
  });

  return parts
    .join("")
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}