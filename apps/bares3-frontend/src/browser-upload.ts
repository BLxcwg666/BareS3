export type UploadCandidate = {
  file: File;
  key: string;
};

type WebkitDataTransferItem = DataTransferItem & {
  webkitGetAsEntry?: () => WebkitEntry | null;
};

type WebkitEntry = {
  fullPath: string;
  isFile: boolean;
  isDirectory: boolean;
  name: string;
};

type WebkitFileEntry = WebkitEntry & {
  isFile: true;
  isDirectory: false;
  file: (success: (file: File) => void, error?: (error: DOMException) => void) => void;
};

type WebkitDirectoryReader = {
  readEntries: (success: (entries: WebkitEntry[]) => void, error?: (error: DOMException) => void) => void;
};

type WebkitDirectoryEntry = WebkitEntry & {
  isFile: false;
  isDirectory: true;
  createReader: () => WebkitDirectoryReader;
};

export function collectInputUploadCandidates(files: FileList | null): UploadCandidate[] {
  return Array.from(files ?? [])
    .map((file) => ({
      file,
      key: normalizeUploadKey(file.webkitRelativePath || file.name),
    }))
    .filter((item) => item.key !== '');
}

export async function collectDropUploadCandidates(dataTransfer: DataTransfer): Promise<UploadCandidate[]> {
  const items = Array.from(dataTransfer.items ?? []).filter((item) => item.kind === 'file');
  if (items.length === 0) {
    return collectInputUploadCandidates(dataTransfer.files);
  }

  const collected = await Promise.all(
    items.map(async (item) => {
      const entry = (item as WebkitDataTransferItem).webkitGetAsEntry?.();
      if (entry) {
        return collectEntryUploadCandidates(entry);
      }

      const file = item.getAsFile();
      return file
        ? [
            {
              file,
              key: normalizeUploadKey(file.name),
            },
          ]
        : [];
    }),
  );

  const flattened = collected.flat();
  return flattened.length > 0 ? flattened : collectInputUploadCandidates(dataTransfer.files);
}

async function collectEntryUploadCandidates(entry: WebkitEntry): Promise<UploadCandidate[]> {
  if (entry.isFile) {
    const file = await readEntryFile(entry as WebkitFileEntry);
    return [
      {
        file,
        key: normalizeUploadKey(entry.fullPath || file.name),
      },
    ];
  }

  if (entry.isDirectory) {
    const entries = await readAllDirectoryEntries((entry as WebkitDirectoryEntry).createReader());
    const nested = await Promise.all(entries.map((child) => collectEntryUploadCandidates(child)));
    return nested.flat();
  }

  return [];
}

function readEntryFile(entry: WebkitFileEntry): Promise<File> {
  return new Promise((resolve, reject) => {
    entry.file(resolve, reject);
  });
}

function readAllDirectoryEntries(reader: WebkitDirectoryReader): Promise<WebkitEntry[]> {
  return new Promise((resolve, reject) => {
    const entries: WebkitEntry[] = [];

    const readBatch = () => {
      reader.readEntries(
        (batch) => {
          if (batch.length === 0) {
            resolve(entries);
            return;
          }

          entries.push(...batch);
          readBatch();
        },
        (error) => reject(error),
      );
    };

    readBatch();
  });
}

function normalizeUploadKey(value: string): string {
  return value
    .replace(/\\/g, '/')
    .replace(/^\/+/, '')
    .split('/')
    .map((segment) => segment.trim())
    .filter(Boolean)
    .join('/');
}
