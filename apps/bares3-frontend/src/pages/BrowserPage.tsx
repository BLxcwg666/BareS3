import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { ChangeEvent, DragEvent } from 'react';
import { CheckCircleOutlined, CheckOutlined, CloseCircleOutlined, CloseOutlined, CloudSyncOutlined, CopyOutlined, EditOutlined, FileOutlined, FolderOpenOutlined, SearchOutlined, UploadOutlined } from '@ant-design/icons';
import {
  App as AntApp,
  Button,
  Dropdown,
  Empty,
  Input,
  InputNumber,
  Popconfirm,
  Progress,
  Select,
  Tooltip,
  Space,
  Spin,
  Table,
  Tag,
} from 'antd';
import type { MenuProps, TableColumnsType } from 'antd';
import {
  createShareLink,
  deleteBrowserPrefix,
  deleteObject,
  listShareLinks,
  moveBrowserEntry,
  presignObject,
  removeShareLink,
  revokeShareLink,
  updateObjectMetadata,
  uploadObject,
  type ObjectInfo,
  type ShareLinkInfo,
} from '../api';
import { collectDropUploadCandidates, collectInputUploadCandidates, type UploadCandidate } from '../browser-upload';
import { ConsoleShell } from '../components/ConsoleShell';
import { Section } from '../components/Section';
import { useBucketObjects } from '../hooks/useBucketObjects';
import { useBucketsData } from '../hooks/useBucketsData';
import { useObjectDetail } from '../hooks/useObjectDetail';
import { useRuntimeData } from '../hooks/useRuntimeData';
import { copyText, formatBytes, formatDateTime, formatRelativeTime, normalizeApiError, syncStatusLabel } from '../utils';
import { useSearchParams } from 'react-router-dom';

function encodeObjectKeyPath(key: string) {
  return key
    .split('/')
    .map((part) => encodeURIComponent(part))
    .join('/');
}

type BrowserEntry =
  | {
      kind: 'parent';
      key: string;
      prefix: string;
    }
  | {
      kind: 'folder';
      bucket: string;
      key: string;
      name: string;
      prefix: string;
      lastModified: string | null;
    }
  | {
      kind: 'object';
      key: string;
      name: string;
      object: ObjectInfo;
    };

type UploadProgressState = {
  total: number;
  completed: number;
  succeeded: number;
  failed: number;
  current: string;
  phase: 'uploading' | 'done';
  failures: UploadFailure[];
};

type UploadFailure = {
  key: string;
  reason: string;
};

type RenameState =
  | {
      kind: 'object';
      sourceBucket: string;
      sourceKey: string;
      parentPrefix: string;
      value: string;
    }
  | {
      kind: 'folder';
      sourceBucket: string;
      sourcePrefix: string;
      parentPrefix: string;
      value: string;
    };

type MetadataField = 'content_type' | 'content_disposition' | 'cache_control' | 'user_metadata';

type MetadataEditState = {
  field: MetadataField;
  value: string;
};

function normalizePrefix(value: string | null) {
  const normalized = (value ?? '')
    .replace(/\\/g, '/')
    .replace(/^\/+/, '')
    .split('/')
    .map((segment) => segment.trim())
    .filter(Boolean)
    .join('/');

  return normalized ? `${normalized}/` : '';
}

function parentPrefix(prefix: string) {
  const trimmed = prefix.replace(/\/+$/, '');
  if (!trimmed) {
    return '';
  }

  const parts = trimmed.split('/');
  parts.pop();
  return parts.length > 0 ? `${parts.join('/')}/` : '';
}

function objectParentPrefix(key: string) {
  const normalized = key.replace(/\\/g, '/');
  const index = normalized.lastIndexOf('/');
  if (index < 0) {
    return '';
  }
  return normalized.slice(0, index + 1);
}

function formatUploadFailure(failure: UploadFailure) {
  return failure.reason ? `${failure.key} (${failure.reason})` : failure.key;
}

function summarizeUploadFailures(failures: UploadFailure[], limit = 3) {
  if (failures.length === 0) {
    return '';
  }

  const visible = failures.slice(0, limit).map(formatUploadFailure);
  if (failures.length <= limit) {
    return visible.join(', ');
  }

  return `${visible.join(', ')}, +${failures.length - limit} more`;
}

function buildBrowserEntries(objects: ObjectInfo[], bucket: string | null, prefix: string): BrowserEntry[] {
  const folders = new Map<string, { key: string; name: string; prefix: string; lastModified: string | null }>();
  const files: Array<Extract<BrowserEntry, { kind: 'object' }>> = [];

  for (const object of objects) {
    const relative = prefix ? object.key.slice(prefix.length) : object.key;
    if (!relative) {
      continue;
    }

    const [head, ...tail] = relative.split('/');
    if (!head) {
      continue;
    }

    if (tail.length > 0) {
      const folderPrefix = `${prefix}${head}/`;
      const current = folders.get(folderPrefix);
      if (!current || (object.last_modified && (!current.lastModified || object.last_modified > current.lastModified))) {
        folders.set(folderPrefix, {
          key: folderPrefix,
          name: head,
          prefix: folderPrefix,
          lastModified: object.last_modified,
        });
      }
      continue;
    }

    files.push({
      kind: 'object',
      key: object.key,
      name: head,
      object,
    });
  }

  const folderEntries: BrowserEntry[] = Array.from(folders.values())
    .sort((left, right) => left.name.localeCompare(right.name))
    .map((folder) => ({
      kind: 'folder',
      bucket: bucket ?? '',
      key: folder.key,
      name: folder.name,
      prefix: folder.prefix,
      lastModified: folder.lastModified,
    }));

  const fileEntries = files.sort((left, right) => left.name.localeCompare(right.name));
  const parentEntry: BrowserEntry[] = prefix
    ? [
        {
          kind: 'parent',
          key: `parent:${prefix}`,
          prefix: parentPrefix(prefix),
        },
      ]
    : [];

  return [...parentEntry, ...folderEntries, ...fileEntries];
}

export function BrowserPage() {
  const { message } = AntApp.useApp();
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const folderInputRef = useRef<HTMLInputElement | null>(null);
  const dragDepthRef = useRef(0);
  const uploadProgressHideTimeoutRef = useRef<number | null>(null);
  const uploadProgressRemoveTimeoutRef = useRef<number | null>(null);
  const draggedEntryRef = useRef<Extract<BrowserEntry, { kind: 'object' | 'folder' }> | null>(null);
  const [searchParams, setSearchParams] = useSearchParams();
  const requestedBucket = searchParams.get('bucket')?.trim() ?? '';
  const requestedKey = searchParams.get('key')?.trim() ?? '';
  const requestedQuery = searchParams.get('q')?.trim() ?? '';
  const requestedPath = normalizePrefix(searchParams.get('path'));
  const { items: buckets, loading: bucketsLoading } = useBucketsData();
  const { runtime } = useRuntimeData();
  const syncEnabled = Boolean(runtime?.sync.enabled);
  const [selectedBucket, setSelectedBucket] = useState<string | null>(null);
  const [selectedKey, setSelectedKey] = useState<string | null>(null);
  const [selectedFolderPrefix, setSelectedFolderPrefix] = useState<string | null>(null);
  const [searchValue, setSearchValue] = useState('');
  const [uploading, setUploading] = useState(false);
  const [dragActive, setDragActive] = useState(false);
  const [moveDragging, setMoveDragging] = useState(false);
  const [bucketMenuOpen, setBucketMenuOpen] = useState(false);
  const [dropTargetKey, setDropTargetKey] = useState<string | null>(null);
  const [uploadProgress, setUploadProgress] = useState<UploadProgressState | null>(null);
  const [uploadProgressClosing, setUploadProgressClosing] = useState(false);
  const [renameState, setRenameState] = useState<RenameState | null>(null);
  const [metadataEditState, setMetadataEditState] = useState<MetadataEditState | null>(null);
  const [savingMetadata, setSavingMetadata] = useState(false);
  const [renaming, setRenaming] = useState(false);
  const [deletingKey, setDeletingKey] = useState<string | null>(null);
  const [presigningKey, setPresigningKey] = useState<string | null>(null);
  const [shareLinkTTL, setShareLinkTTL] = useState(86400);
  const [creatingShareLink, setCreatingShareLink] = useState(false);
  const [objectShareLinks, setObjectShareLinks] = useState<ShareLinkInfo[]>([]);
  const [shareLinksLoading, setShareLinksLoading] = useState(false);
  const [revokingShareLinkId, setRevokingShareLinkId] = useState<string | null>(null);
  const [removingShareLinkId, setRemovingShareLinkId] = useState<string | null>(null);
  const shareLinksRequestIdRef = useRef(0);

  useEffect(
    () => () => {
      if (uploadProgressHideTimeoutRef.current !== null) {
        window.clearTimeout(uploadProgressHideTimeoutRef.current);
      }
      if (uploadProgressRemoveTimeoutRef.current !== null) {
        window.clearTimeout(uploadProgressRemoveTimeoutRef.current);
      }
    },
    [],
  );

  useEffect(() => {
    if (requestedBucket && buckets.some((item) => item.name === requestedBucket) && selectedBucket !== requestedBucket) {
      setSelectedBucket(requestedBucket);
      return;
    }
    if (!selectedBucket && buckets.length > 0) {
      setSelectedBucket(buckets[0].name);
    }
    if (selectedBucket && !buckets.some((item) => item.name === selectedBucket)) {
      setSelectedBucket(buckets[0]?.name ?? null);
    }
  }, [buckets, requestedBucket, selectedBucket]);

  useEffect(() => {
    setSearchValue(requestedQuery);
  }, [requestedQuery]);

  useEffect(() => {
    if (!moveDragging) {
      setBucketMenuOpen(false);
      setDropTargetKey(null);
    }
  }, [moveDragging]);

  const currentPrefix = selectedBucket === requestedBucket ? requestedPath : '';
  const selectedBucketInfo = useMemo(() => buckets.find((item) => item.name === selectedBucket) ?? null, [buckets, selectedBucket]);
  const selectedBucketIsPublic = selectedBucketInfo?.access_mode === 'public';
  const pathSignature = `${selectedBucket ?? ''}:${currentPrefix}`;
  const { items: objects, loading: objectsLoading, loadingMore, hasMore, refresh, loadMore } = useBucketObjects(
    selectedBucket,
    currentPrefix,
    searchValue,
  );

  useEffect(() => {
    setSelectedFolderPrefix(null);
    setRenameState(null);
    setMetadataEditState(null);
  }, [pathSignature]);

  const browserEntries = useMemo(() => buildBrowserEntries(objects, selectedBucket, currentPrefix), [currentPrefix, objects, selectedBucket]);

  useEffect(() => {
    const visibleKeys = new Set(browserEntries.filter((entry) => entry.kind === 'object').map((entry) => entry.object.key));
    if (requestedKey && visibleKeys.has(requestedKey) && selectedKey !== requestedKey) {
      setSelectedFolderPrefix(null);
      setSelectedKey(requestedKey);
      return;
    }
    if (selectedKey && !visibleKeys.has(selectedKey)) {
      setSelectedKey(null);
    }
  }, [browserEntries, requestedKey, selectedKey]);

  const selectedObject = useMemo(() => {
    const matched = browserEntries.find((entry) => entry.kind === 'object' && entry.object.key === selectedKey);
    return matched?.kind === 'object' ? matched.object : null;
  }, [browserEntries, selectedKey]);
  const selectedObjectKey = selectedObject?.key ?? null;

  const selectedBucketRef = useRef<string | null>(selectedBucket);
  const selectedObjectKeyRef = useRef<string | null>(selectedObjectKey);

  selectedBucketRef.current = selectedBucket;
  selectedObjectKeyRef.current = selectedObjectKey;

  const selectedFolder = useMemo(() => {
    const matched = browserEntries.find((entry) => entry.kind === 'folder' && entry.prefix === selectedFolderPrefix);
    return matched?.kind === 'folder' ? matched : null;
  }, [browserEntries, selectedFolderPrefix]);

  useEffect(() => {
    if (selectedFolderPrefix && !selectedFolder) {
      setSelectedFolderPrefix(null);
    }
  }, [selectedFolder, selectedFolderPrefix]);

  const { item: objectDetail, loading: objectDetailLoading, refresh: refreshObjectDetail } = useObjectDetail(
    selectedBucket,
    selectedObjectKey,
  );
  const inspectorObject = objectDetail ?? selectedObject;

  const refreshShareLinks = useCallback(
    async (showError = true) => {
      const nextBucket = selectedBucketRef.current;
      const nextKey = selectedObjectKeyRef.current;
      const requestId = shareLinksRequestIdRef.current + 1;
      shareLinksRequestIdRef.current = requestId;

      if (!nextBucket || !nextKey) {
        setObjectShareLinks([]);
        setShareLinksLoading(false);
        return;
      }

      setObjectShareLinks([]);
      setShareLinksLoading(true);
      try {
        const links = await listShareLinks();
        if (shareLinksRequestIdRef.current !== requestId) {
          return;
        }
        if (selectedBucketRef.current !== nextBucket || selectedObjectKeyRef.current !== nextKey) {
          return;
        }

        setObjectShareLinks(links.filter((item) => item.bucket === nextBucket && item.key === nextKey));
      } catch (error) {
        if (shareLinksRequestIdRef.current !== requestId) {
          return;
        }
        if (showError) {
          message.error(normalizeApiError(error, 'Failed to load object share links'));
        }
      } finally {
        if (
          shareLinksRequestIdRef.current === requestId
          && selectedBucketRef.current === nextBucket
          && selectedObjectKeyRef.current === nextKey
        ) {
          setShareLinksLoading(false);
        }
      }
    },
    [message],
  );

  useEffect(() => {
    void refreshShareLinks(false);
  }, [refreshShareLinks, selectedBucket, selectedObjectKey]);

  const syncSearchParams = useCallback(
    (nextBucket: string | null, nextPrefix = '', nextKey?: string | null, nextQuery?: string) => {
      const params = new URLSearchParams();
      if (nextBucket) {
        params.set('bucket', nextBucket);
      }
      if (nextPrefix) {
        params.set('path', normalizePrefix(nextPrefix));
      }
      if (nextKey) {
        params.set('key', nextKey);
      }
      if (nextQuery && nextQuery.trim()) {
        params.set('q', nextQuery.trim());
      }
      setSearchParams(params, { replace: true });
    },
    [setSearchParams],
  );

  const handleSelectBucket = (value: string) => {
    setSelectedBucket(value);
    setSelectedKey(null);
    setSelectedFolderPrefix(null);
    syncSearchParams(value, '', null, searchValue);
  };

  const closeUploadProgressSoon = useCallback(() => {
    if (uploadProgressHideTimeoutRef.current !== null) {
      window.clearTimeout(uploadProgressHideTimeoutRef.current);
    }
    if (uploadProgressRemoveTimeoutRef.current !== null) {
      window.clearTimeout(uploadProgressRemoveTimeoutRef.current);
    }

    uploadProgressHideTimeoutRef.current = window.setTimeout(() => {
      setUploadProgressClosing(true);
      uploadProgressHideTimeoutRef.current = null;

      uploadProgressRemoveTimeoutRef.current = window.setTimeout(() => {
        setUploadProgress(null);
        setUploadProgressClosing(false);
        uploadProgressRemoveTimeoutRef.current = null;
      }, 220);
    }, 2380);
  }, []);

  const closeUploadProgress = useCallback(() => {
    if (uploadProgressHideTimeoutRef.current !== null) {
      window.clearTimeout(uploadProgressHideTimeoutRef.current);
      uploadProgressHideTimeoutRef.current = null;
    }
    if (uploadProgressRemoveTimeoutRef.current !== null) {
      window.clearTimeout(uploadProgressRemoveTimeoutRef.current);
    }

    setUploadProgressClosing(true);
    uploadProgressRemoveTimeoutRef.current = window.setTimeout(() => {
      setUploadProgress(null);
      setUploadProgressClosing(false);
      uploadProgressRemoveTimeoutRef.current = null;
    }, 220);
  }, []);

  const uploadCandidates = useCallback(
    async (candidates: UploadCandidate[]) => {
      if (!selectedBucket) {
        message.error('Select a bucket before uploading');
        return;
      }
      if (candidates.length === 0) {
        return;
      }

      if (uploadProgressHideTimeoutRef.current !== null) {
        window.clearTimeout(uploadProgressHideTimeoutRef.current);
        uploadProgressHideTimeoutRef.current = null;
      }
      if (uploadProgressRemoveTimeoutRef.current !== null) {
        window.clearTimeout(uploadProgressRemoveTimeoutRef.current);
        uploadProgressRemoveTimeoutRef.current = null;
      }

      const resolvedCandidates = candidates
        .map((candidate) => ({
          file: candidate.file,
          key: currentPrefix ? `${currentPrefix}${candidate.key}` : candidate.key,
        }))
        .filter((candidate) => candidate.key.trim() !== '');

      if (resolvedCandidates.length === 0) {
        return;
      }

      const uploadedItems: ObjectInfo[] = [];
      const failedUploads: UploadFailure[] = [];

      setUploading(true);
      setUploadProgressClosing(false);
      setUploadProgress({
        total: resolvedCandidates.length,
        completed: 0,
        succeeded: 0,
        failed: 0,
        current: resolvedCandidates[0].key,
        phase: 'uploading',
        failures: [],
      });

      try {
        for (const candidate of resolvedCandidates) {
          setUploadProgress((current) =>
            current
              ? {
                  ...current,
                  current: candidate.key,
                }
              : current,
          );

          try {
            const uploaded = await uploadObject(selectedBucket, candidate.file, candidate.key);
            uploadedItems.push(uploaded);
            setUploadProgress((current) =>
              current
                ? {
                    ...current,
                    completed: current.completed + 1,
                    succeeded: current.succeeded + 1,
                  }
                : current,
            );
          } catch (error) {
            failedUploads.push({
              key: candidate.key,
              reason: normalizeApiError(error, 'Upload failed'),
            });
            setUploadProgress((current) =>
              current
                ? {
                    ...current,
                    completed: current.completed + 1,
                    failed: current.failed + 1,
                  }
                : current,
            );
          }
        }

        await refresh();

        const lastUploaded = uploadedItems[uploadedItems.length - 1] ?? null;
        if (lastUploaded && objectParentPrefix(lastUploaded.key) === currentPrefix) {
          setSelectedKey(lastUploaded.key);
          syncSearchParams(selectedBucket, currentPrefix, lastUploaded.key, searchValue);
        } else {
          setSelectedKey(null);
          syncSearchParams(selectedBucket, currentPrefix, null, searchValue);
        }

        setUploadProgress((current) =>
          current
            ? {
                ...current,
                phase: 'done',
                current:
                  failedUploads.length > 0
                    ? 'Review failed uploads below'
                    : uploadedItems.length === 1
                      ? uploadedItems[0].key
                      : `Finished ${uploadedItems.length} uploads`,
                failures: failedUploads,
              }
            : current,
        );
        if (failedUploads.length === 0) {
          closeUploadProgressSoon();
        }

        if (uploadedItems.length > 0 && failedUploads.length === 0) {
          message.success(uploadedItems.length === 1 ? `Uploaded ${uploadedItems[0].key}` : `Uploaded ${uploadedItems.length} items`);
          return;
        }
        if (uploadedItems.length === 0 && failedUploads.length > 0) {
          message.error(
            failedUploads.length === 1
              ? formatUploadFailure(failedUploads[0])
              : `Failed to upload ${failedUploads.length} items: ${summarizeUploadFailures(failedUploads)}`,
          );
          return;
        }
        if (uploadedItems.length > 0 && failedUploads.length > 0) {
          message.warning(`Uploaded ${uploadedItems.length} items. Failed: ${summarizeUploadFailures(failedUploads)}`);
        }
      } finally {
        setUploading(false);
      }
    },
    [closeUploadProgressSoon, currentPrefix, message, refresh, searchValue, selectedBucket, syncSearchParams],
  );

  const handleInputUpload = async (event: ChangeEvent<HTMLInputElement>) => {
    const candidates = collectInputUploadCandidates(event.target.files);
    event.target.value = '';
    await uploadCandidates(candidates);
  };

  const handleDragEnter = (event: DragEvent<HTMLDivElement>) => {
    if (!Array.from(event.dataTransfer.types).includes('Files')) {
      return;
    }

    event.preventDefault();
    dragDepthRef.current += 1;
    setDragActive(true);
  };

  const handleDragOver = (event: DragEvent<HTMLDivElement>) => {
    if (!Array.from(event.dataTransfer.types).includes('Files')) {
      return;
    }

    event.preventDefault();
    event.dataTransfer.dropEffect = 'copy';
  };

  const handleDragLeave = (event: DragEvent<HTMLDivElement>) => {
    if (!Array.from(event.dataTransfer.types).includes('Files')) {
      return;
    }

    event.preventDefault();
    dragDepthRef.current = Math.max(0, dragDepthRef.current - 1);
    if (dragDepthRef.current === 0) {
      setDragActive(false);
    }
  };

  const handleDrop = async (event: DragEvent<HTMLDivElement>) => {
    if (!Array.from(event.dataTransfer.types).includes('Files')) {
      return;
    }

    event.preventDefault();
    dragDepthRef.current = 0;
    setDragActive(false);

    const candidates = await collectDropUploadCandidates(event.dataTransfer);
    await uploadCandidates(candidates);
  };

  const openFilePicker = () => {
    fileInputRef.current?.click();
  };

  const openFolderPicker = () => {
    folderInputRef.current?.click();
  };

  const setFolderInputNode = useCallback((node: HTMLInputElement | null) => {
    folderInputRef.current = node;
    if (!node) {
      return;
    }

    node.setAttribute('webkitdirectory', '');
    node.setAttribute('directory', '');
    node.multiple = true;
  }, []);

  const handleOpenFolder = (prefix: string) => {
    setSelectedKey(null);
    setSelectedFolderPrefix(null);
    syncSearchParams(selectedBucket, prefix, null, searchValue);
  };

  const handleOpenParent = () => {
    handleOpenFolder(parentPrefix(currentPrefix));
  };

  const moveEntryToDestination = useCallback(
    async (entry: Extract<BrowserEntry, { kind: 'object' | 'folder' }>, destinationBucket: string, destinationPrefix: string) => {
      const normalizedDestinationPrefix = normalizePrefix(destinationPrefix);
      if (entry.kind === 'object') {
        const destinationKey = `${normalizedDestinationPrefix}${entry.name}`;
        if (entry.object.bucket === destinationBucket && entry.object.key === destinationKey) {
          return;
        }

        await moveBrowserEntry({
          kind: 'object',
          source_bucket: entry.object.bucket,
          source_key: entry.object.key,
          destination_bucket: destinationBucket,
          destination_key: destinationKey,
        });

        await refresh();
        if (selectedBucket === destinationBucket && currentPrefix === normalizedDestinationPrefix) {
          setSelectedFolderPrefix(null);
          setSelectedKey(destinationKey);
          syncSearchParams(destinationBucket, normalizedDestinationPrefix, destinationKey, searchValue);
        } else if (entry.object.key === selectedKey) {
          setSelectedKey(null);
        }
        message.success(`Moved ${entry.object.key}`);
        return;
      }

      const destinationFolderPrefix = `${normalizedDestinationPrefix}${entry.name}/`;
      if (entry.prefix === destinationFolderPrefix && selectedBucket === destinationBucket) {
        return;
      }

      await moveBrowserEntry({
        kind: 'prefix',
        source_bucket: entry.bucket,
        source_prefix: entry.prefix,
        destination_bucket: destinationBucket,
        destination_prefix: destinationFolderPrefix,
      });

      await refresh();
      if (selectedBucket === destinationBucket && currentPrefix === normalizedDestinationPrefix) {
        setSelectedKey(null);
        setSelectedFolderPrefix(destinationFolderPrefix);
      } else if (selectedFolderPrefix === entry.prefix) {
        setSelectedFolderPrefix(null);
      }
      message.success(`Moved ${entry.prefix}`);
    },
    [currentPrefix, message, refresh, searchValue, selectedBucket, selectedFolderPrefix, selectedKey, syncSearchParams],
  );

  const handleStartRename = () => {
    setMetadataEditState(null);
    if (inspectorObject && selectedBucket) {
      setRenameState({
        kind: 'object',
        sourceBucket: selectedBucket,
        sourceKey: inspectorObject.key,
        parentPrefix: objectParentPrefix(inspectorObject.key),
        value: inspectorObject.key.split('/').pop() ?? inspectorObject.key,
      });
      return;
    }

    if (selectedFolder && selectedBucket) {
      setRenameState({
        kind: 'folder',
        sourceBucket: selectedFolder.bucket,
        sourcePrefix: selectedFolder.prefix,
        parentPrefix: parentPrefix(selectedFolder.prefix),
        value: selectedFolder.name,
      });
    }
  };

  const handleCommitRename = async () => {
    if (!renameState) {
      return;
    }

    const nextSegment = renameState.value.trim();
    if (!nextSegment || nextSegment.includes('/')) {
      message.error('Name must be a single path segment');
      return;
    }

    setRenaming(true);
    try {
      if (renameState.kind === 'object') {
        const destinationKey = `${renameState.parentPrefix}${nextSegment}`;
        await moveBrowserEntry({
          kind: 'object',
          source_bucket: renameState.sourceBucket,
          source_key: renameState.sourceKey,
          destination_bucket: renameState.sourceBucket,
          destination_key: destinationKey,
        });
        await refresh();
        setSelectedFolderPrefix(null);
        setSelectedKey(destinationKey);
        syncSearchParams(renameState.sourceBucket, currentPrefix, destinationKey, searchValue);
        message.success(`Renamed to ${destinationKey}`);
      } else {
        const destinationPrefix = `${renameState.parentPrefix}${nextSegment}/`;
        await moveBrowserEntry({
          kind: 'prefix',
          source_bucket: renameState.sourceBucket,
          source_prefix: renameState.sourcePrefix,
          destination_bucket: renameState.sourceBucket,
          destination_prefix: destinationPrefix,
        });
        await refresh();
        setSelectedKey(null);
        setSelectedFolderPrefix(destinationPrefix);
        message.success(`Renamed to ${destinationPrefix}`);
      }
      setRenameState(null);
    } catch (error) {
      message.error(normalizeApiError(error, 'Failed to rename entry'));
    } finally {
      setRenaming(false);
    }
  };

  const formatUserMetadataValue = (value: Record<string, string> | undefined) => {
    if (!value || Object.keys(value).length === 0) {
      return 'Not set';
    }
    return JSON.stringify(value);
  };

  const handleStartMetadataEdit = (field: MetadataField) => {
    if (!inspectorObject) {
      return;
    }

    setRenameState(null);

    switch (field) {
      case 'content_type':
        setMetadataEditState({ field, value: inspectorObject.content_type || '' });
        return;
      case 'content_disposition':
        setMetadataEditState({ field, value: inspectorObject.content_disposition || '' });
        return;
      case 'cache_control':
        setMetadataEditState({ field, value: inspectorObject.cache_control || '' });
        return;
      case 'user_metadata':
        setMetadataEditState({
          field,
          value:
            inspectorObject.user_metadata && Object.keys(inspectorObject.user_metadata).length > 0
              ? JSON.stringify(inspectorObject.user_metadata, null, 2)
              : '{}',
        });
        return;
      default:
        return;
    }
  };

  const parseUserMetadataInput = (value: string) => {
    const trimmed = value.trim();
    if (!trimmed) {
      return {} as Record<string, string>;
    }

    const parsed = JSON.parse(trimmed) as unknown;
    if (!parsed || Array.isArray(parsed) || typeof parsed !== 'object') {
      throw new Error('User metadata must be a JSON object');
    }

    const result: Record<string, string> = {};
    for (const [key, entry] of Object.entries(parsed)) {
      if (Array.isArray(entry) || (entry !== null && typeof entry === 'object')) {
        throw new Error('User metadata values must be strings, numbers, booleans, or null');
      }
      result[key] = entry == null ? '' : String(entry);
    }
    return result;
  };

  const handleCommitMetadataEdit = async () => {
    if (!inspectorObject || !selectedBucket || !metadataEditState) {
      return;
    }

    setSavingMetadata(true);
    try {
      const payload = {
        content_type: inspectorObject.content_type || '',
        content_disposition: inspectorObject.content_disposition || '',
        cache_control: inspectorObject.cache_control || '',
        user_metadata: inspectorObject.user_metadata ?? {},
      };

      switch (metadataEditState.field) {
        case 'content_type':
          payload.content_type = metadataEditState.value;
          break;
        case 'content_disposition':
          payload.content_disposition = metadataEditState.value;
          break;
        case 'cache_control':
          payload.cache_control = metadataEditState.value;
          break;
        case 'user_metadata':
          payload.user_metadata = parseUserMetadataInput(metadataEditState.value);
          break;
      }

      await updateObjectMetadata(selectedBucket, inspectorObject.key, payload);
      await Promise.all([refresh(), refreshObjectDetail()]);
      setMetadataEditState(null);
      message.success('Updated object metadata');
    } catch (error) {
      message.error(normalizeApiError(error, 'Failed to update object metadata'));
    } finally {
      setSavingMetadata(false);
    }
  };

  const handleDeleteFolder = async () => {
    if (!selectedFolder) {
      return;
    }

    try {
      await deleteBrowserPrefix(selectedFolder.bucket, selectedFolder.prefix);
      setSelectedFolderPrefix(null);
      setRenameState(null);
      setMetadataEditState(null);
      await refresh();
      syncSearchParams(selectedFolder.bucket, currentPrefix, null, searchValue);
      message.success(`Deleted ${selectedFolder.prefix}`);
    } catch (error) {
      message.error(normalizeApiError(error, 'Failed to delete folder'));
    }
  };

  const handleRowDragStart = (entry: BrowserEntry, event: DragEvent<HTMLTableRowElement>) => {
    if (entry.kind !== 'object' && entry.kind !== 'folder') {
      return;
    }

    draggedEntryRef.current = entry;
    setMoveDragging(true);
    event.dataTransfer.effectAllowed = 'move';
    event.dataTransfer.setData('application/x-bares3-move', entry.key);
  };

  const handleRowDragEnd = () => {
    draggedEntryRef.current = null;
    setMoveDragging(false);
  };

  const handleBucketOptionDragEnter = (value: string) => {
    if (!moveDragging || value === selectedBucket) {
      return;
    }

    setBucketMenuOpen(false);
    handleSelectBucket(value);
  };

  const handleDropMove = async (destinationBucket: string, destinationPrefix: string) => {
    const entry = draggedEntryRef.current;
    if (!entry) {
      return;
    }

    try {
      await moveEntryToDestination(entry, destinationBucket, destinationPrefix);
    } catch (error) {
      message.error(normalizeApiError(error, 'Failed to move entry'));
    } finally {
      draggedEntryRef.current = null;
      setMoveDragging(false);
      setDropTargetKey(null);
    }
  };

  const handleRootMoveDrop = async (event: DragEvent<HTMLDivElement>) => {
    if (!moveDragging || !selectedBucket) {
      return;
    }

    event.preventDefault();
    event.stopPropagation();
    await handleDropMove(selectedBucket, currentPrefix);
  };

  const handleDeleteObject = async () => {
    if (!selectedBucket || !selectedObject) {
      return;
    }

    setDeletingKey(selectedObject.key);
    try {
      await deleteObject(selectedBucket, selectedObject.key);
      message.success(`Deleted ${selectedObject.key}`);
      setSelectedKey(null);
      setObjectShareLinks([]);
      setMetadataEditState(null);
      await refresh();
      syncSearchParams(selectedBucket, currentPrefix, null, searchValue);
    } catch (error) {
      message.error(normalizeApiError(error, 'Failed to delete object'));
    } finally {
      setDeletingKey(null);
    }
  };

  const handleCopyDownloadUrl = async () => {
    if (!selectedBucket || !selectedObject) {
      return;
    }

    setPresigningKey(selectedObject.key);
    try {
      const result = await presignObject(selectedBucket, selectedObject.key);
      await copyText(result.url);
      message.success(`Copied download link for ${selectedObject.key}`);
    } catch (error) {
      message.error(normalizeApiError(error, 'Failed to generate download link'));
    } finally {
      setPresigningKey(null);
    }
  };

  const handleCreateShareLink = async () => {
    if (!selectedBucket || !selectedObject) {
      return;
    }

    setCreatingShareLink(true);
    try {
      await createShareLink(selectedBucket, selectedObject.key, shareLinkTTL);
      await refreshShareLinks(false);
      message.success(`Created share link for ${selectedObject.key}`);
    } catch (error) {
      message.error(normalizeApiError(error, 'Failed to create share link'));
    } finally {
      setCreatingShareLink(false);
    }
  };

  const handleCopyShareLink = async (value: string, label: string) => {
    if (!value) {
      return;
    }

    try {
      await copyText(value);
      message.success(`Copied ${label}`);
    } catch (error) {
      message.error(normalizeApiError(error, `Failed to copy ${label}`));
    }
  };

  const handleRevokeShareLink = async (link: ShareLinkInfo) => {
    setRevokingShareLinkId(link.id);
    try {
      const revoked = await revokeShareLink(link.id);
      setObjectShareLinks((current) => current.map((item) => (item.id === revoked.id ? revoked : item)));
      message.success('Revoked share link');
    } catch (error) {
      message.error(normalizeApiError(error, 'Failed to revoke share link'));
    } finally {
      setRevokingShareLinkId(null);
    }
  };

  const handleRemoveShareLink = async (link: ShareLinkInfo) => {
    setRemovingShareLinkId(link.id);
    try {
      await removeShareLink(link.id);
      setObjectShareLinks((current) => current.filter((item) => item.id !== link.id));
      message.success('Removed share link');
    } catch (error) {
      message.error(normalizeApiError(error, 'Failed to remove share link'));
    } finally {
      setRemovingShareLinkId(null);
    }
  };

  const removeShareLinkTitle = (link: ShareLinkInfo) =>
    `Permanently remove this ${link.status === 'expired' ? 'expired' : 'revoked'} link?`;

  const renderShareLinkStatus = (status: ShareLinkInfo['status']) => {
    switch (status) {
      case 'active':
        return <Tag color="success">Active</Tag>;
      case 'expired':
        return <Tag>Expired</Tag>;
      case 'revoked':
        return <Tag color="warning">Revoked</Tag>;
      default:
        return <Tag>{status}</Tag>;
    }
  };

  const uploadMenu: MenuProps = {
    items: [
      { key: 'files', label: 'Add files' },
      { key: 'folder', label: 'Add folder' },
    ],
    onClick: ({ key }) => {
      if (key === 'folder') {
        openFolderPicker();
        return;
      }
      openFilePicker();
    },
  };

  const pathSegments = currentPrefix
    .split('/')
    .filter(Boolean)
    .map((segment, index, parts) => ({
      label: segment,
      prefix: `${parts.slice(0, index + 1).join('/')}/`,
    }));

  const currentPath = selectedBucket ? `${selectedBucket}/${currentPrefix}` : '';
  const selectedFolderObjectCount = selectedFolder ? objects.filter((item) => item.key.startsWith(selectedFolder.prefix)).length : 0;
  const publicObjectURL = useMemo(() => {
    const baseURL = runtime?.storage.public_base_url?.trim();
    if (!selectedBucketIsPublic || !baseURL || !selectedBucket || !selectedObject) {
      return null;
    }

    return `${baseURL.replace(/\/+$/, '')}/pub/${encodeURIComponent(selectedBucket)}/${encodeObjectKeyPath(selectedObject.key)}`;
  }, [runtime?.storage.public_base_url, selectedBucket, selectedBucketIsPublic, selectedObject]);

  const handleCopyCurrentPath = async () => {
    if (!currentPath) {
      return;
    }

    try {
      await copyText(currentPath);
      message.success('Copied path');
    } catch (error) {
      message.error(normalizeApiError(error, 'Failed to copy path'));
    }
  };

  const renderInspectorRow = (
    label: string,
    value: string,
    options?: {
      editable?: boolean;
      editing?: boolean;
      multiline?: boolean;
      onChange?: (value: string) => void;
      onEdit?: () => void;
      onCancel?: () => void;
      onSave?: () => void;
      saving?: boolean;
    },
  ) => (
    <div className="inspector-row" key={label}>
      <div className="inspector-row-label">{label}:</div>
      <div className="inspector-row-content">
        {options?.editing ? (
          <div className="inspector-edit-controls inspector-edit-controls-inline">
            {options.multiline ? (
              <Input.TextArea autoSize={{ minRows: 3, maxRows: 8 }} onChange={(event) => options.onChange?.(event.target.value)} value={value} />
            ) : (
              <Input autoFocus onChange={(event) => options.onChange?.(event.target.value)} onPressEnter={() => void options.onSave?.()} size="small" value={value} />
            )}
            <Button icon={<CheckOutlined />} loading={options.saving} onClick={() => void options.onSave?.()} size="small" type="primary" />
            <Button icon={<CloseOutlined />} onClick={options.onCancel} size="small" />
          </div>
        ) : (
          <div className="inspector-edit-display inspector-row-display">
            <span className="inspector-edit-value">{value}</span>
            {options?.editable ? (
              <Tooltip title={`Edit ${label.toLowerCase()}`}>
                <button className="inspector-icon-button" onClick={options.onEdit} type="button">
                  <EditOutlined />
                </button>
              </Tooltip>
            ) : null}
          </div>
        )}
      </div>
    </div>
  );

  const renderObjectSyncStatus = (object: ObjectInfo) => {
    if (!syncEnabled) {
      return null;
    }

    const state = object.sync_status?.status;
    if (!state || state === 'ready') {
      return (
        <Tooltip title="Replica is up to date">
          <span className="browser-sync-icon browser-sync-icon-ready" aria-label="Ready">
            <CheckCircleOutlined />
          </span>
        </Tooltip>
      );
    }

    if (state === 'error') {
      return (
        <Tooltip title={object.sync_status?.last_error?.trim() || 'Latest sync attempt failed'}>
          <span className="browser-sync-icon browser-sync-icon-error" aria-label="Sync error">
            <CloseCircleOutlined />
          </span>
        </Tooltip>
      );
    }

    if (state === 'conflict') {
      return (
        <Tooltip title={object.sync_status?.last_error?.trim() || 'Replication conflict detected'}>
          <span className="browser-sync-icon browser-sync-icon-error" aria-label="Conflict">
            <CloseCircleOutlined />
          </span>
        </Tooltip>
      );
    }

    if (state === 'verifying') {
      return (
        <Tooltip title="Verifying local replica against cluster baseline">
          <span className="browser-sync-icon browser-sync-icon-active" aria-label="Verifying">
            <CloudSyncOutlined />
          </span>
        </Tooltip>
      );
    }

    return (
      <Tooltip title={state === 'pending' ? 'Waiting to sync latest version' : 'Syncing latest version'}>
        <span className="browser-sync-icon browser-sync-icon-active" aria-label="Syncing">
          <CloudSyncOutlined />
        </span>
      </Tooltip>
    );
  };

  const renderInspectorSyncTag = (object: ObjectInfo) => {
    if (!syncEnabled) {
      return null;
    }

    const state = object.sync_status?.status;
    if (!state) {
      return (
        <Tag color="success" icon={<CheckCircleOutlined />}>
          Ready
        </Tag>
      );
    }
    switch (state) {
      case 'pending':
        return (
          <Tag color="processing" icon={<CloudSyncOutlined />}>
            Queued
          </Tag>
        );
      case 'verifying':
        return (
          <Tag color="processing" icon={<CloudSyncOutlined />}>
            Verifying
          </Tag>
        );
      case 'downloading':
        return (
          <Tag color="processing" icon={<CloudSyncOutlined />}>
            Syncing
          </Tag>
        );
      case 'error':
        return (
          <Tag color="error" icon={<CloseCircleOutlined />}>
            Error
          </Tag>
        );
      case 'conflict':
        return (
          <Tag color="error" icon={<CloseCircleOutlined />}>
            Conflict
          </Tag>
        );
      default:
        return (
          <Tag color="success" icon={<CheckCircleOutlined />}>
            Ready
          </Tag>
        );
    }
  };

  const browserColumns = useMemo<TableColumnsType<BrowserEntry>>(
    () => [
      {
        dataIndex: 'name',
        key: 'name',
        title: 'Name',
        render: (_value, row) =>
          row.kind === 'parent' ? (
            <div className="browser-entry browser-entry-parent">
              <FolderOpenOutlined />
              <div className="row-title">..</div>
            </div>
          ) : row.kind === 'folder' ? (
            <div className="browser-entry browser-entry-folder">
              <FolderOpenOutlined />
              <div className="row-title">{`${row.name}/`}</div>
            </div>
          ) : (
            <div className="browser-entry">
              <FileOutlined />
              <div className="row-title">{row.name}</div>
              {renderObjectSyncStatus(row.object)}
            </div>
          ),
      },
      {
        key: 'content_type',
        title: 'Type',
        width: 180,
        render: (_value, row) => (row.kind === 'object' ? row.object.content_type || 'application/octet-stream' : 'Folder'),
      },
      {
        key: 'size',
        title: 'Size',
        width: 110,
        render: (_value, row) => (row.kind === 'object' ? formatBytes(row.object.size) : '--'),
      },
      {
        key: 'cache',
        title: 'Cache',
        width: 150,
        render: (_value, row) => (row.kind === 'object' ? row.object.cache_control || 'private' : '--'),
      },
      {
        key: 'updated',
        title: 'Updated',
        width: 170,
        render: (_value, row) =>
          row.kind === 'object'
            ? formatDateTime(row.object.last_modified)
            : row.kind === 'folder'
            ? row.lastModified
              ? formatDateTime(row.lastModified)
              : '--'
            : '--',
      },
    ],
    [],
  );

  return (
    <ConsoleShell
      showHeaderSearch={false}
      actions={
        <>
          <input hidden multiple onChange={(event) => void handleInputUpload(event)} ref={fileInputRef} type="file" />
          <input hidden onChange={(event) => void handleInputUpload(event)} ref={setFolderInputNode} type="file" />
          <Dropdown.Button disabled={!selectedBucket} loading={uploading} menu={uploadMenu} onClick={openFilePicker} type="primary">
            <Space size={8}>
              <UploadOutlined />
              Add object
            </Space>
          </Dropdown.Button>
        </>
      }
    >
      <div
        className={dragActive ? 'workspace-stack browser-dropzone browser-dropzone-active' : 'workspace-stack browser-dropzone'}
        onDragEnter={handleDragEnter}
        onDragLeave={handleDragLeave}
        onDragOver={handleDragOver}
        onDrop={(event) => void handleDrop(event)}
      >
        {dragActive ? (
          <div className="browser-drop-overlay">
            <div className="browser-drop-copy">Drop files or folders to upload into {selectedBucket ?? 'the selected bucket'}</div>
          </div>
        ) : null}

        <div className="browser-pjax-frame route-fade" key={pathSignature}>
            <div className="path-strip">
              <div className="path-context">
                <div className="path-label">Current path</div>
                {selectedBucket ? (
                  <div className="path-trail">
                    <button className="path-link" onClick={() => handleOpenFolder('')} type="button">
                      {selectedBucket}
                    </button>
                    {pathSegments.map((segment) => (
                      <button className="path-link" key={segment.prefix} onClick={() => handleOpenFolder(segment.prefix)} type="button">
                        / {segment.label}
                      </button>
                    ))}
                    <button aria-label="Copy path" className="path-copy-button" onClick={() => void handleCopyCurrentPath()} title="Copy path" type="button">
                      <CopyOutlined />
                    </button>
                  </div>
                ) : (
                  <div className="path-value">No bucket selected</div>
                )}
              </div>

              <div
                className="path-actions"
                onDragEnter={() => {
                  if (moveDragging) {
                    setBucketMenuOpen(true);
                  }
                }}
                onDragOver={(event) => {
                  if (!moveDragging) {
                    return;
                  }
                  event.preventDefault();
                  setBucketMenuOpen(true);
                }}
              >
                <Select
                  className="bucket-select"
                  loading={bucketsLoading}
                  onOpenChange={setBucketMenuOpen}
                  onChange={handleSelectBucket}
                  open={moveDragging ? bucketMenuOpen : undefined}
                  optionRender={(option) => (
                    <div
                      className="bucket-option-row"
                      onDragEnter={() => handleBucketOptionDragEnter(String((option as { data: { value: string } }).data.value))}
                      onDragOver={(event) => {
                        if (!moveDragging) {
                          return;
                        }
                        event.preventDefault();
                        handleBucketOptionDragEnter(String((option as { data: { value: string } }).data.value));
                      }}
                    >
                      {(option as { data: { label: string } }).data.label}
                    </div>
                  )}
                  options={buckets.map((bucket) => ({ label: bucket.name, value: bucket.name }))}
                  placeholder="Select bucket"
                  value={selectedBucket ?? undefined}
                />
              </div>
            </div>

            {uploadProgress ? (
              <div className={uploadProgressClosing ? 'upload-progress-card upload-progress-card-leave' : 'upload-progress-card upload-progress-card-enter'}>
                <div className="upload-progress-head">
                  <div>
                    <div className="row-title">{uploadProgress.phase === 'uploading' ? 'Uploading objects' : 'Upload complete'}</div>
                    <div className="row-note">{uploadProgress.current}</div>
                  </div>
                  <div className="upload-progress-actions">
                    <div className="upload-progress-summary">
                      {uploadProgress.completed}/{uploadProgress.total}
                    </div>
                    {uploadProgress.phase === 'done' ? (
                      <button
                        aria-label="Close upload progress"
                        className="inspector-icon-button"
                        onClick={() => closeUploadProgress()}
                        type="button"
                      >
                        <CloseOutlined />
                      </button>
                    ) : null}
                  </div>
                </div>
                <Progress percent={Math.round((uploadProgress.completed / uploadProgress.total) * 100)} showInfo={false} size="small" />
                <div className="row-note">
                  {uploadProgress.succeeded} uploaded
                  {uploadProgress.failed > 0 ? ` · ${uploadProgress.failed} failed` : ''}
                </div>
                {uploadProgress.failures.length > 0 ? (
                  <div className="upload-progress-failures">
                    {uploadProgress.failures.map((failure) => (
                      <div className="upload-progress-failure" key={`${failure.key}:${failure.reason}`}>
                        {formatUploadFailure(failure)}
                      </div>
                    ))}
                  </div>
                ) : null}
              </div>
            ) : null}

            <div className="workspace-grid workspace-grid-main">
              <Section
                flush
                title="Objects"
                note="Drop files or folders anywhere in this view, or use Add object to upload from disk."
                extra={
                  <Input
                    allowClear
                    className="section-search"
                    onChange={(event) => {
                      const nextValue = event.target.value;
                      setSearchValue(nextValue);
                      syncSearchParams(selectedBucket, currentPrefix, null, nextValue);
                    }}
                    placeholder="Search current path"
                    prefix={<SearchOutlined />}
                    value={searchValue}
                  />
                }
              >
                <div
                  className="browser-table-dropzone"
                  onDragLeave={() => {
                    if (dropTargetKey === `root:${selectedBucket}:${currentPrefix}`) {
                      setDropTargetKey(null);
                    }
                  }}
                  onDragOver={(event) => {
                    if (!moveDragging || !selectedBucket) {
                      return;
                    }
                    event.preventDefault();
                    setDropTargetKey(`root:${selectedBucket}:${currentPrefix}`);
                  }}
                  onDrop={(event) => void handleRootMoveDrop(event)}
                >
                  <Table
                    columns={browserColumns}
                    dataSource={browserEntries}
                    loading={objectsLoading}
                    locale={{
                      emptyText: selectedBucket ? (
                        <Empty description="No files or folders in this path yet" image={Empty.PRESENTED_IMAGE_SIMPLE} />
                      ) : (
                        <Empty description="Create a bucket first" image={Empty.PRESENTED_IMAGE_SIMPLE} />
                      ),
                    }}
                    onRow={(record) => ({
                      draggable: record.kind === 'object' || record.kind === 'folder',
                      onClick: () => {
                        if (record.kind === 'parent') {
                          return;
                        }
                        setRenameState(null);
                        setMetadataEditState(null);
                        if (record.kind === 'folder') {
                          setSelectedKey(null);
                          setSelectedFolderPrefix(record.prefix);
                          syncSearchParams(selectedBucket, currentPrefix, null, searchValue);
                          return;
                        }
                        setSelectedFolderPrefix(null);
                        setSelectedKey(record.object.key);
                        syncSearchParams(selectedBucket, currentPrefix, record.object.key, searchValue);
                      },
                      onDoubleClick: () => {
                        if (record.kind === 'parent') {
                          handleOpenParent();
                          return;
                        }
                        if (record.kind === 'folder') {
                          handleOpenFolder(record.prefix);
                        }
                      },
                      onDragStart: (event) => handleRowDragStart(record, event),
                      onDragEnd: handleRowDragEnd,
                      onDragOver: (event) => {
                        if (!moveDragging || !selectedBucket || (record.kind !== 'folder' && record.kind !== 'parent')) {
                          return;
                        }
                        event.preventDefault();
                        setDropTargetKey(record.key);
                      },
                      onDrop: (event) => {
                        if (!moveDragging || !selectedBucket || (record.kind !== 'folder' && record.kind !== 'parent')) {
                          return;
                        }
                        event.preventDefault();
                        event.stopPropagation();
                        void handleDropMove(selectedBucket, record.kind === 'folder' ? record.prefix : record.prefix);
                      },
                    })}
                    pagination={false}
                    rowClassName={(record) => {
                      const classes = [];
                      if (record.kind === 'object' && record.object.key === selectedObject?.key) {
                        classes.push('table-row-selected');
                      }
                      if (record.kind === 'folder' && record.prefix === selectedFolderPrefix) {
                        classes.push('table-row-selected');
                      }
                      if (record.key === dropTargetKey) {
                        classes.push('browser-row-drop-target');
                      }
                      return classes.join(' ');
                    }}
                    rowKey="key"
                    scroll={{ x: 880 }}
                    size="small"
                  />

                  {selectedBucket ? (
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 12, marginTop: 12, flexWrap: 'wrap' }}>
                      {hasMore ? (
                        <Button loading={loadingMore} onClick={() => void loadMore()} size="small">
                          Load more
                        </Button>
                      ) : null}
                    </div>
                  ) : null}
                </div>
              </Section>

              <Section title="Inspector">
                {inspectorObject || selectedFolder ? (
                  <Spin spinning={objectDetailLoading}>
                    <div className="inspector-stack">
                      <div className="inspector-fields">
                        {renderInspectorRow('Key', renameState?.value ?? (inspectorObject ? inspectorObject.key : selectedFolder?.prefix ?? 'Not set'), {
                          editable: true,
                          editing: Boolean(renameState),
                          onChange: (value) => renameState && setRenameState({ ...renameState, value }),
                          onEdit: handleStartRename,
                          onCancel: () => setRenameState(null),
                          onSave: () => void handleCommitRename(),
                          saving: renaming,
                        })}

                        {inspectorObject
                          ? [
                              renderInspectorRow('Content-Type', metadataEditState?.field === 'content_type' ? metadataEditState.value : inspectorObject.content_type || 'application/octet-stream', {
                                editable: true,
                                editing: metadataEditState?.field === 'content_type',
                                onChange: (value) => setMetadataEditState({ field: 'content_type', value }),
                                onEdit: () => handleStartMetadataEdit('content_type'),
                                onCancel: () => setMetadataEditState(null),
                                onSave: () => void handleCommitMetadataEdit(),
                                saving: savingMetadata,
                              }),
                              renderInspectorRow(
                                'Content-Disposition',
                                metadataEditState?.field === 'content_disposition' ? metadataEditState.value : inspectorObject.content_disposition || 'Not set',
                                {
                                  editable: true,
                                  editing: metadataEditState?.field === 'content_disposition',
                                  onChange: (value) => setMetadataEditState({ field: 'content_disposition', value }),
                                  onEdit: () => handleStartMetadataEdit('content_disposition'),
                                  onCancel: () => setMetadataEditState(null),
                                  onSave: () => void handleCommitMetadataEdit(),
                                  saving: savingMetadata,
                                },
                              ),
                              renderInspectorRow('Size', formatBytes(inspectorObject.size)),
                              renderInspectorRow(
                                'Cache-Control',
                                metadataEditState?.field === 'cache_control' ? metadataEditState.value : inspectorObject.cache_control || 'private',
                                {
                                  editable: true,
                                  editing: metadataEditState?.field === 'cache_control',
                                  onChange: (value) => setMetadataEditState({ field: 'cache_control', value }),
                                  onEdit: () => handleStartMetadataEdit('cache_control'),
                                  onCancel: () => setMetadataEditState(null),
                                  onSave: () => void handleCommitMetadataEdit(),
                                  saving: savingMetadata,
                                },
                              ),
                              renderInspectorRow('ETag', inspectorObject.etag || 'Not set'),
                              ...(syncEnabled
                                ? [
                                    renderInspectorRow('Sync', syncStatusLabel(inspectorObject.sync_status?.status)),
                                    renderInspectorRow('Checksum', inspectorObject.sync_status?.expected_checksum_sha256 || 'Not tracked'),
                                  ]
                                : []),
                              renderInspectorRow(
                                'User metadata',
                                metadataEditState?.field === 'user_metadata'
                                  ? metadataEditState.value
                                  : formatUserMetadataValue(inspectorObject.user_metadata),
                                {
                                  editable: true,
                                  editing: metadataEditState?.field === 'user_metadata',
                                  multiline: true,
                                  onChange: (value) => setMetadataEditState({ field: 'user_metadata', value }),
                                  onEdit: () => handleStartMetadataEdit('user_metadata'),
                                  onCancel: () => setMetadataEditState(null),
                                  onSave: () => void handleCommitMetadataEdit(),
                                  saving: savingMetadata,
                                },
                              ),
                              renderInspectorRow('Updated', formatDateTime(inspectorObject.last_modified)),
                            ]
                          : selectedFolder
                            ? [
                                renderInspectorRow('Bucket', selectedFolder.bucket),
                                renderInspectorRow('Items', String(selectedFolderObjectCount)),
                                renderInspectorRow('Updated', selectedFolder.lastModified ? formatDateTime(selectedFolder.lastModified) : 'N/A'),
                              ]
                            : null}
                      </div>

                      <div className="inspector-panel">
                        <div className="inspector-panel-head">
                          <div className="row-title">Actions</div>
                          <div className="row-note">
                            {inspectorObject
                              ? 'Keep object tools inside the inspector, including share-link creation.'
                              : 'Single-click selects a folder, double-click enters it, and drag moves it.'}
                          </div>
                        </div>

                        {inspectorObject ? (
                          <>
                            <div className="inspector-actions-row">
                              {syncEnabled ? (
                                <div className="inspector-share-controls inspector-sync-controls">
                                  <span className="inspector-field-label">Replication</span>
                                  {renderInspectorSyncTag(inspectorObject)}
                                </div>
                              ) : null}
                              <div className="inspector-share-controls">
                                <span className="inspector-field-label">Share TTL seconds</span>
                                <InputNumber
                                  min={60}
                                  onChange={(value) => setShareLinkTTL(typeof value === 'number' ? value : 86400)}
                                  size="small"
                                  step={3600}
                                  style={{ width: 150 }}
                                  value={shareLinkTTL}
                                />
                                <Button loading={creatingShareLink} onClick={() => void handleCreateShareLink()} size="small" type="primary">
                                  Create share link
                                </Button>
                              </div>

                              <Space size={8} wrap>
                                {selectedBucketIsPublic && publicObjectURL ? (
                                  <Button href={publicObjectURL} rel="noreferrer" size="small" target="_blank">
                                    Open
                                  </Button>
                                ) : (
                                  <Button loading={presigningKey === selectedObject?.key} onClick={() => void handleCopyDownloadUrl()} size="small">
                                    Copy download URL
                                  </Button>
                                )}
                                <Button loading={objectDetailLoading} onClick={() => void refreshObjectDetail()} size="small">
                                  Refresh
                                </Button>
                                <Popconfirm
                                  cancelText="Cancel"
                                  okButtonProps={{ danger: true, loading: deletingKey === selectedObject?.key }}
                                  okText="Delete"
                                  onConfirm={() => void handleDeleteObject()}
                                  title={`Delete ${selectedObject?.key}?`}
                                >
                                  <Button danger loading={deletingKey === selectedObject?.key} size="small">
                                    Delete
                                  </Button>
                                </Popconfirm>
                              </Space>
                            </div>

                            <div className="inspector-share-result">
                              <div className="inspector-panel-head">
                                <div className="row-title">Share links for this object</div>
                                <div className="row-note">Recent links stay here so you can reopen, copy, or revoke them without leaving the inspector.</div>
                              </div>

                              {shareLinksLoading ? (
                                <Spin size="small" />
                              ) : objectShareLinks.length > 0 ? (
                                <div className="inspector-share-list">
                                  {objectShareLinks.map((link) => (
                                    <div className="inspector-share-item" key={link.id}>
                                      <div className="inspector-share-item-meta">
                                        <div className="inspector-share-item-top">
                                          <div className="row-title row-title-small">{link.filename || link.key}</div>
                                          {renderShareLinkStatus(link.status)}
                                        </div>
                                        <div className="row-note">{link.id}</div>
                                        <div className="row-note">Expires {formatDateTime(link.expires_at)} · {formatRelativeTime(link.expires_at)}</div>
                                      </div>

                                      <Space size={8} wrap>
                                        <Button onClick={() => void handleCopyShareLink(link.url, 'share URL')} size="small">
                                          Copy
                                        </Button>
                                        <Button href={link.url} rel="noreferrer" size="small" target="_blank">
                                          Open
                                        </Button>
                                        <Button href={link.download_url} rel="noreferrer" size="small" target="_blank">
                                          Download
                                        </Button>
                                        {link.status === 'active' ? (
                                          <Popconfirm okText="Revoke" onConfirm={() => void handleRevokeShareLink(link)} title="Revoke this share link?">
                                            <Button danger loading={revokingShareLinkId === link.id} size="small">
                                              Revoke
                                            </Button>
                                          </Popconfirm>
                                        ) : link.status === 'revoked' || link.status === 'expired' ? (
                                          <Popconfirm okText="Remove" onConfirm={() => void handleRemoveShareLink(link)} title={removeShareLinkTitle(link)}>
                                            <Button danger loading={removingShareLinkId === link.id} size="small">
                                              Remove
                                            </Button>
                                          </Popconfirm>
                                        ) : null}
                                      </Space>
                                    </div>
                                  ))}
                                </div>
                              ) : (
                                <Empty description="No share links for this object yet" image={Empty.PRESENTED_IMAGE_SIMPLE} />
                              )}
                            </div>
                          </>
                        ) : (
                          <div className="inspector-actions-row">
                            <div className="inspector-folder-note">Drag this folder onto another folder, `..`, or another bucket to move it.</div>
                            <Popconfirm okText="Delete" onConfirm={() => void handleDeleteFolder()} title={`Delete ${selectedFolder?.prefix}?`}>
                              <Button danger size="small">
                                Delete folder
                              </Button>
                            </Popconfirm>
                          </div>
                        )}
                      </div>
                    </div>
                  </Spin>
                ) : objectsLoading ? (
                  <Spin />
                ) : (
                  <Empty description="Select a file or folder to inspect" image={Empty.PRESENTED_IMAGE_SIMPLE} />
                )}
              </Section>
            </div>
        </div>
      </div>
    </ConsoleShell>
  );
}
