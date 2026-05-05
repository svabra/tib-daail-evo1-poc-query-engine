const ZIP_EOCD_SIGNATURE = 0x06054b50;
const ZIP64_EOCD_SIGNATURE = 0x06064b50;
const ZIP64_LOCATOR_SIGNATURE = 0x07064b50;
const ZIP_CENTRAL_FILE_SIGNATURE = 0x02014b50;
const ZIP_LOCAL_FILE_SIGNATURE = 0x04034b50;
const ZIP64_EXTRA_ID = 0x0001;
const UINT16_MAX = 0xffff;
const UINT32_MAX = 0xffffffff;
const MAX_ARCHIVE_BYTES = 5 * 1024 * 1024 * 1024;
const MAX_CSV_BYTES = 20 * 1024 * 1024 * 1024;
const MAX_EXTRACTED_BYTES = 20 * 1024 * 1024 * 1024;
const MAX_ENTRIES = 100;
const MAX_EXPANSION_RATIO = 100;

export function isZipFile(file) {
  return String(file?.name || "").trim().toLowerCase().endsWith(".zip");
}

function toNumber(value, label) {
  const numberValue = Number(value);
  if (!Number.isSafeInteger(numberValue)) {
    throw new Error(`${label} is too large for browser ZIP processing.`);
  }
  return numberValue;
}

function readUint64(view, offset, label) {
  const value = view.getBigUint64(offset, true);
  return toNumber(value, label);
}

function decodeBytes(bytes) {
  return new TextDecoder("utf-8").decode(bytes);
}

async function readSlice(file, start, end) {
  return file.slice(start, end).arrayBuffer();
}

async function findEndOfCentralDirectory(file) {
  if (file.size > MAX_ARCHIVE_BYTES) {
    throw new Error("The ZIP archive exceeds the browser upload size limit.");
  }
  const searchLength = Math.min(file.size, 65_557);
  const searchStart = file.size - searchLength;
  const buffer = await readSlice(file, searchStart, file.size);
  const view = new DataView(buffer);
  for (let offset = view.byteLength - 22; offset >= 0; offset -= 1) {
    if (view.getUint32(offset, true) === ZIP_EOCD_SIGNATURE) {
      return { view, offset, absoluteOffset: searchStart + offset };
    }
  }
  throw new Error("The uploaded .zip file is not a valid ZIP archive.");
}

async function readCentralDirectoryInfo(file) {
  const eocd = await findEndOfCentralDirectory(file);
  const { view, offset, absoluteOffset } = eocd;
  let entryCount = view.getUint16(offset + 10, true);
  let centralDirectorySize = view.getUint32(offset + 12, true);
  let centralDirectoryOffset = view.getUint32(offset + 16, true);

  const needsZip64 =
    entryCount === UINT16_MAX ||
    centralDirectorySize === UINT32_MAX ||
    centralDirectoryOffset === UINT32_MAX;
  if (!needsZip64) {
    return { entryCount, centralDirectorySize, centralDirectoryOffset };
  }

  if (absoluteOffset < 20) {
    throw new Error("The ZIP64 archive is missing its central directory locator.");
  }
  const locatorBuffer = await readSlice(file, absoluteOffset - 20, absoluteOffset);
  const locator = new DataView(locatorBuffer);
  if (locator.getUint32(0, true) !== ZIP64_LOCATOR_SIGNATURE) {
    throw new Error("The ZIP64 archive is missing its central directory locator.");
  }
  const zip64EocdOffset = readUint64(locator, 8, "ZIP64 central directory offset");
  const zip64Buffer = await readSlice(file, zip64EocdOffset, zip64EocdOffset + 56);
  const zip64 = new DataView(zip64Buffer);
  if (zip64.getUint32(0, true) !== ZIP64_EOCD_SIGNATURE) {
    throw new Error("The ZIP64 archive is missing its central directory record.");
  }
  entryCount = readUint64(zip64, 32, "ZIP64 entry count");
  centralDirectorySize = readUint64(zip64, 40, "ZIP64 central directory size");
  centralDirectoryOffset = readUint64(zip64, 48, "ZIP64 central directory offset");
  return { entryCount, centralDirectorySize, centralDirectoryOffset };
}

function parseZip64Extra(extraBytes, entry) {
  let offset = 0;
  while (offset + 4 <= extraBytes.length) {
    const headerId = extraBytes[offset] | (extraBytes[offset + 1] << 8);
    const dataSize = extraBytes[offset + 2] | (extraBytes[offset + 3] << 8);
    const dataStart = offset + 4;
    const dataEnd = dataStart + dataSize;
    if (dataEnd > extraBytes.length) {
      break;
    }
    if (headerId === ZIP64_EXTRA_ID) {
      const view = new DataView(extraBytes.buffer, extraBytes.byteOffset + dataStart, dataSize);
      let cursor = 0;
      if (entry.uncompressedSize === UINT32_MAX && cursor + 8 <= dataSize) {
        entry.uncompressedSize = readUint64(view, cursor, "ZIP64 uncompressed size");
        cursor += 8;
      }
      if (entry.compressedSize === UINT32_MAX && cursor + 8 <= dataSize) {
        entry.compressedSize = readUint64(view, cursor, "ZIP64 compressed size");
        cursor += 8;
      }
      if (entry.localHeaderOffset === UINT32_MAX && cursor + 8 <= dataSize) {
        entry.localHeaderOffset = readUint64(view, cursor, "ZIP64 local header offset");
      }
      return entry;
    }
    offset = dataEnd;
  }
  return entry;
}

function safeCsvEntryName(rawName) {
  const normalizedName = String(rawName || "").replace(/\\/g, "/").trim();
  if (!normalizedName) {
    throw new Error("ZIP archive contains an entry without a file name.");
  }
  if (normalizedName.startsWith("/") || /^[a-zA-Z]:/.test(normalizedName)) {
    throw new Error(`ZIP archive entry '${rawName}' uses an unsafe path.`);
  }
  const parts = normalizedName.split("/");
  if (parts.some((part) => !part || part === "." || part === "..")) {
    throw new Error(`ZIP archive entry '${rawName}' uses an unsafe path.`);
  }
  const fileName = parts[parts.length - 1];
  if (!fileName.toLowerCase().endsWith(".csv")) {
    throw new Error(
      `ZIP archive entry '${rawName}' is not a CSV file. Archives may contain directories and .csv files only.`
    );
  }
  return fileName;
}

function uniqueCsvName(fileName, seen) {
  const key = fileName.toLowerCase();
  const nextIndex = seen.get(key) || 0;
  seen.set(key, nextIndex + 1);
  if (nextIndex === 0) {
    return fileName;
  }
  const dotIndex = fileName.lastIndexOf(".");
  const stem = dotIndex > 0 ? fileName.slice(0, dotIndex) : fileName;
  const suffix = dotIndex > 0 ? fileName.slice(dotIndex) : ".csv";
  return `${stem}_${nextIndex + 1}${suffix}`;
}

async function readZipEntries(file) {
  const centralDirectory = await readCentralDirectoryInfo(file);
  const buffer = await readSlice(
    file,
    centralDirectory.centralDirectoryOffset,
    centralDirectory.centralDirectoryOffset + centralDirectory.centralDirectorySize
  );
  const view = new DataView(buffer);
  const entries = [];
  const seen = new Map();
  let offset = 0;
  let totalUncompressed = 0;

  while (offset < view.byteLength) {
    if (view.getUint32(offset, true) !== ZIP_CENTRAL_FILE_SIGNATURE) {
      throw new Error("The ZIP archive central directory is invalid.");
    }
    const generalPurposeFlag = view.getUint16(offset + 8, true);
    const compressionMethod = view.getUint16(offset + 10, true);
    const fileNameLength = view.getUint16(offset + 28, true);
    const extraLength = view.getUint16(offset + 30, true);
    const commentLength = view.getUint16(offset + 32, true);
    const externalAttributes = view.getUint32(offset + 38, true);
    const fileNameStart = offset + 46;
    const extraStart = fileNameStart + fileNameLength;
    const commentStart = extraStart + extraLength;
    const nextOffset = commentStart + commentLength;
    if (nextOffset > view.byteLength) {
      throw new Error("The ZIP archive central directory is truncated.");
    }

    const rawName = decodeBytes(new Uint8Array(buffer, fileNameStart, fileNameLength));
    const isDirectory = rawName.replace(/\\/g, "/").endsWith("/");
    if (!isDirectory) {
      if ((generalPurposeFlag & 0x1) !== 0) {
        throw new Error(`ZIP archive entry '${rawName}' is encrypted.`);
      }
      if (compressionMethod !== 0 && compressionMethod !== 8) {
        throw new Error(`ZIP archive entry '${rawName}' uses an unsupported compression method.`);
      }
      const mode = (externalAttributes >>> 16) & 0o777777;
      if ((mode & 0o170000) === 0o120000) {
        throw new Error(`ZIP archive entry '${rawName}' is a symbolic link.`);
      }
      const safeName = uniqueCsvName(safeCsvEntryName(rawName), seen);
      const entry = parseZip64Extra(
        new Uint8Array(buffer, extraStart, extraLength),
        {
          rawName,
          fileName: safeName,
          compressionMethod,
          compressedSize: view.getUint32(offset + 20, true),
          uncompressedSize: view.getUint32(offset + 24, true),
          localHeaderOffset: view.getUint32(offset + 42, true),
        }
      );
      if (entry.uncompressedSize > MAX_CSV_BYTES) {
        throw new Error(`ZIP archive entry '${rawName}' exceeds the CSV size limit.`);
      }
      if (
        entry.compressedSize > 0 &&
        entry.uncompressedSize / entry.compressedSize > MAX_EXPANSION_RATIO
      ) {
        throw new Error(`ZIP archive entry '${rawName}' expands too much to be accepted.`);
      }
      totalUncompressed += entry.uncompressedSize;
      if (totalUncompressed > MAX_EXTRACTED_BYTES) {
        throw new Error("The ZIP archive expands beyond the extracted-size limit.");
      }
      entries.push(entry);
      if (entries.length > MAX_ENTRIES) {
        throw new Error("The ZIP archive contains too many CSV files.");
      }
    }
    offset = nextOffset;
  }

  if (!entries.length) {
    throw new Error("The ZIP archive does not contain any CSV files.");
  }
  return entries;
}

async function entryDataSlice(file, entry) {
  const localHeaderBuffer = await readSlice(
    file,
    entry.localHeaderOffset,
    entry.localHeaderOffset + 30
  );
  const localHeader = new DataView(localHeaderBuffer);
  if (localHeader.getUint32(0, true) !== ZIP_LOCAL_FILE_SIGNATURE) {
    throw new Error(`ZIP archive entry '${entry.rawName}' has an invalid local header.`);
  }
  const fileNameLength = localHeader.getUint16(26, true);
  const extraLength = localHeader.getUint16(28, true);
  const dataStart = entry.localHeaderOffset + 30 + fileNameLength + extraLength;
  return file.slice(dataStart, dataStart + entry.compressedSize);
}

async function inflateRawBlob(blob, entry) {
  if (typeof DecompressionStream !== "function") {
    throw new Error(
      "This browser cannot expand compressed ZIP files for Local Workspace imports."
    );
  }
  let stream;
  try {
    stream = blob.stream().pipeThrough(new DecompressionStream("deflate-raw"));
  } catch (error) {
    throw new Error(
      "This browser cannot expand compressed ZIP files for Local Workspace imports."
    );
  }
  const inflated = await new Response(stream).blob();
  if (inflated.size !== entry.uncompressedSize) {
    throw new Error(`ZIP archive entry '${entry.rawName}' did not expand to the expected size.`);
  }
  return inflated;
}

async function extractZipEntry(file, entry) {
  const data = await entryDataSlice(file, entry);
  if (entry.compressionMethod === 0) {
    if (data.size !== entry.uncompressedSize) {
      throw new Error(`ZIP archive entry '${entry.rawName}' has an invalid stored size.`);
    }
    return new Blob([data], { type: "text/csv" });
  }
  return inflateRawBlob(data, entry);
}

export async function buildCsvZipPreviewState(file) {
  const entries = await readZipEntries(file);
  return {
    status: "ready",
    fileName: file.name,
    delimiter: "",
    hasHeader: true,
    columns: [],
    rows: [],
    error: "",
    archiveEntryCount: entries.length,
  };
}

export async function readCsvFilesFromZip(file, { onProgress } = {}) {
  const entries = await readZipEntries(file);
  const results = [];
  let transferredBytes = 0;
  const totalBytes = entries.reduce((sum, entry) => sum + entry.uncompressedSize, 0);
  for (const entry of entries) {
    const blob = await extractZipEntry(file, entry);
    transferredBytes += blob.size;
    if (typeof onProgress === "function") {
      onProgress({ transferredBytes, totalBytes, fileName: entry.fileName });
    }
    results.push({
      fileName: entry.fileName,
      blob,
      sizeBytes: blob.size,
    });
  }
  return results;
}
