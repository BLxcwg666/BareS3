import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { ChangeEvent, DragEvent } from 'react';
import { CopyOutlined, FileOutlined, FolderOpenOutlined, SearchOutlined, UploadOutlined } from '@ant-design/icons';
import {
  App as AntApp,
  Button,
  Descriptions,
  Dropdown,
  Empty,
  Input,
  InputNumber,
  Popconfirm,
  Progress,
  Select,
  Space,
  Spin,
  Table,
  Tag,
} from 'antd';
import type { MenuProps, TableColumnsType } from 'antd';
import {
  createShareLink,
  deleteObject,
  listShareLinks,
  presignObject,
  removeShareLink,
  revokeShareLink,
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
import { copyText, formatBytes, formatDateTime, formatRelativeTime, nodeSummaryToItems, normalizeApiError } from '../utils';
import { useSearchParams } from 'react-router-dom';

type BrowserEntry =
  | {
      kind: 'parent';
      key: string;
      prefix: string;
    }
  | {
      kind: 'folder';
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

function buildBrowserEntries(objects: ObjectInfo[], prefix: string): BrowserEntry[] {
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
  const uploadProgressTimeoutRef = useRef<number | null>(null);
  const [searchParams, setSearchParams] = useSearchParams();
  const requestedBucket = searchParams.get('bucket')?.trim() ?? '';
  const requestedKey = searchParams.get('key')?.trim() ?? '';
  const requestedQuery = searchParams.get('q')?.trim() ?? '';
  const requestedPath = normalizePrefix(searchParams.get('path'));
  const { items: buckets, loading: bucketsLoading } = useBucketsData();
  const [selectedBucket, setSelectedBucket] = useState<string | null>(null);
  const [selectedKey, setSelectedKey] = useState<string | null>(null);
  const [searchValue, setSearchValue] = useState('');
  const [uploading, setUploading] = useState(false);
  const [dragActive, setDragActive] = useState(false);
  const [uploadProgress, setUploadProgress] = useState<UploadProgressState | null>(null);
  const [deletingKey, setDeletingKey] = useState<string | null>(null);
  const [presigningKey, setPresigningKey] = useState<string | null>(null);
  const [shareLinkTTL, setShareLinkTTL] = useState(86400);
  const [creatingShareLink, setCreatingShareLink] = useState(false);
  const [objectShareLinks, setObjectShareLinks] = useState<ShareLinkInfo[]>([]);
  const [shareLinksLoading, setShareLinksLoading] = useState(false);
  const [revokingShareLinkId, setRevokingShareLinkId] = useState<string | null>(null);
  const [removingShareLinkId, setRemovingShareLinkId] = useState<string | null>(null);

  useEffect(
    () => () => {
      if (uploadProgressTimeoutRef.current !== null) {
        window.clearTimeout(uploadProgressTimeoutRef.current);
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

  const currentPrefix = selectedBucket === requestedBucket ? requestedPath : '';
  const pathSignature = `${selectedBucket ?? ''}:${currentPrefix}`;
  const { items: objects, loading: objectsLoading, refresh } = useBucketObjects(selectedBucket, currentPrefix);

  const filteredObjects = useMemo(() => {
    const keyword = searchValue.trim().toLowerCase();
    if (!keyword) {
      return objects;
    }

    return objects.filter((item) =>
      [item.key, item.content_type, item.cache_control ?? '', item.etag ?? ''].some((field) => field.toLowerCase().includes(keyword)),
    );
  }, [objects, searchValue]);

  const browserEntries = useMemo(() => buildBrowserEntries(filteredObjects, currentPrefix), [currentPrefix, filteredObjects]);

  useEffect(() => {
    const visibleKeys = new Set(browserEntries.filter((entry) => entry.kind === 'object').map((entry) => entry.object.key));
    if (requestedKey && visibleKeys.has(requestedKey) && selectedKey !== requestedKey) {
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

  const { item: objectDetail, loading: objectDetailLoading, refresh: refreshObjectDetail } = useObjectDetail(
    selectedBucket,
    selectedObject?.key ?? null,
  );
  const inspectorObject = objectDetail ?? selectedObject;

  const refreshShareLinks = useCallback(
    async (showError = true) => {
      if (!selectedBucket || !selectedObject) {
        setObjectShareLinks([]);
        setShareLinksLoading(false);
        return;
      }

      setObjectShareLinks([]);
      setShareLinksLoading(true);
      try {
        const links = await listShareLinks();
        setObjectShareLinks(links.filter((item) => item.bucket === selectedBucket && item.key === selectedObject.key));
      } catch (error) {
        if (showError) {
          message.error(normalizeApiError(error, 'Failed to load object share links'));
        }
      } finally {
        setShareLinksLoading(false);
      }
    },
    [message, selectedBucket, selectedObject],
  );

  useEffect(() => {
    void refreshShareLinks(false);
  }, [refreshShareLinks]);

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
    syncSearchParams(value, '', null, searchValue);
  };

  const closeUploadProgressSoon = useCallback(() => {
    if (uploadProgressTimeoutRef.current !== null) {
      window.clearTimeout(uploadProgressTimeoutRef.current);
    }
    uploadProgressTimeoutRef.current = window.setTimeout(() => {
      setUploadProgress(null);
      uploadProgressTimeoutRef.current = null;
    }, 2600);
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

      if (uploadProgressTimeoutRef.current !== null) {
        window.clearTimeout(uploadProgressTimeoutRef.current);
        uploadProgressTimeoutRef.current = null;
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
      const failedKeys: string[] = [];

      setUploading(true);
      setUploadProgress({
        total: resolvedCandidates.length,
        completed: 0,
        succeeded: 0,
        failed: 0,
        current: resolvedCandidates[0].key,
        phase: 'uploading',
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
          } catch (_error) {
            failedKeys.push(candidate.key);
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
                  failedKeys.length > 0
                    ? `Finished with ${failedKeys.length} failed`
                    : uploadedItems.length === 1
                      ? uploadedItems[0].key
                      : `Finished ${uploadedItems.length} uploads`,
              }
            : current,
        );
        closeUploadProgressSoon();

        if (uploadedItems.length > 0 && failedKeys.length === 0) {
          message.success(uploadedItems.length === 1 ? `Uploaded ${uploadedItems[0].key}` : `Uploaded ${uploadedItems.length} items`);
          return;
        }
        if (uploadedItems.length === 0 && failedKeys.length > 0) {
          message.error(failedKeys.length === 1 ? `Failed to upload ${failedKeys[0]}` : `Failed to upload ${failedKeys.length} items`);
          return;
        }
        if (uploadedItems.length > 0 && failedKeys.length > 0) {
          message.warning(`Uploaded ${uploadedItems.length} items, ${failedKeys.length} failed`);
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
    syncSearchParams(selectedBucket, prefix, null, searchValue);
  };

  const handleOpenParent = () => {
    handleOpenFolder(parentPrefix(currentPrefix));
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

              <div className="path-actions">
                <Select
                  className="bucket-select"
                  loading={bucketsLoading}
                  onChange={handleSelectBucket}
                  options={buckets.map((bucket) => ({ label: bucket.name, value: bucket.name }))}
                  placeholder="Select bucket"
                  value={selectedBucket ?? undefined}
                />
              </div>
            </div>

            {uploadProgress ? (
              <div className="upload-progress-card">
                <div className="upload-progress-head">
                  <div>
                    <div className="row-title">{uploadProgress.phase === 'uploading' ? 'Uploading objects' : 'Upload complete'}</div>
                    <div className="row-note">{uploadProgress.current}</div>
                  </div>
                  <div className="upload-progress-summary">
                    {uploadProgress.completed}/{uploadProgress.total}
                  </div>
                </div>
                <Progress percent={Math.round((uploadProgress.completed / uploadProgress.total) * 100)} showInfo={false} size="small" />
                <div className="row-note">
                  {uploadProgress.succeeded} uploaded
                  {uploadProgress.failed > 0 ? ` · ${uploadProgress.failed} failed` : ''}
                </div>
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
                    onClick: () => {
                      if (record.kind === 'parent') {
                        handleOpenParent();
                        return;
                      }
                      if (record.kind === 'folder') {
                        handleOpenFolder(record.prefix);
                        return;
                      }
                      setSelectedKey(record.object.key);
                      syncSearchParams(selectedBucket, currentPrefix, record.object.key, searchValue);
                    },
                  })}
                  pagination={false}
                  rowClassName={(record) => (record.kind === 'object' && record.object.key === selectedObject?.key ? 'table-row-selected' : '')}
                  rowKey="key"
                  scroll={{ x: 880 }}
                  size="small"
                />
              </Section>

              <Section title="Inspector">
                {inspectorObject ? (
                  <Spin spinning={objectDetailLoading}>
                    <div className="inspector-stack">
                      <Descriptions
                        column={1}
                        items={nodeSummaryToItems([
                          { label: 'Key', value: inspectorObject.key },
                          { label: 'Content-Type', value: inspectorObject.content_type || 'application/octet-stream' },
                          { label: 'Content-Disposition', value: inspectorObject.content_disposition || 'Not set' },
                          { label: 'Size', value: formatBytes(inspectorObject.size) },
                          { label: 'Cache-Control', value: inspectorObject.cache_control || 'private' },
                          { label: 'ETag', value: inspectorObject.etag || 'Not set' },
                          { label: 'User metadata', value: String(Object.keys(inspectorObject.user_metadata ?? {}).length) },
                          { label: 'Updated', value: formatDateTime(inspectorObject.last_modified) },
                        ])}
                        size="small"
                      />

                      <div className="inspector-panel">
                        <div className="inspector-panel-head">
                          <div className="row-title">Actions</div>
                          <div className="row-note">Keep object tools inside the inspector, including share-link creation.</div>
                        </div>

                        <div className="inspector-actions-row">
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
                            <Button loading={presigningKey === selectedObject?.key} onClick={() => void handleCopyDownloadUrl()} size="small">
                              Copy download URL
                            </Button>
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
                      </div>
                    </div>
                  </Spin>
                ) : objectsLoading ? (
                  <Spin />
                ) : (
                  <Empty description="Select a file to inspect" image={Empty.PRESENTED_IMAGE_SIMPLE} />
                )}
              </Section>
            </div>
        </div>
      </div>
    </ConsoleShell>
  );
}
