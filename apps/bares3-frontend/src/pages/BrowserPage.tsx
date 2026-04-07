import { useEffect, useMemo, useRef, useState } from 'react';
import type { ChangeEvent } from 'react';
import { SearchOutlined, UploadOutlined } from '@ant-design/icons';
import { Breadcrumb, Button, Descriptions, Empty, Input, Popconfirm, Select, Space, Spin, Table, message } from 'antd';
import { deleteObject, presignObject, uploadObject } from '../api';
import { ConsoleShell } from '../components/ConsoleShell';
import { Section } from '../components/Section';
import { useBucketObjects } from '../hooks/useBucketObjects';
import { useBucketsData } from '../hooks/useBucketsData';
import { useObjectDetail } from '../hooks/useObjectDetail';
import { objectColumns } from '../tables';
import { copyText, formatBytes, formatDateTime, nodeSummaryToItems, normalizeApiError } from '../utils';
import { useSearchParams } from 'react-router-dom';

export function BrowserPage() {
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const [searchParams, setSearchParams] = useSearchParams();
  const requestedBucket = searchParams.get('bucket')?.trim() ?? '';
  const requestedKey = searchParams.get('key')?.trim() ?? '';
  const requestedQuery = searchParams.get('q')?.trim() ?? '';
  const { items: buckets, loading: bucketsLoading } = useBucketsData();
  const [selectedBucket, setSelectedBucket] = useState<string | null>(null);
  const [selectedKey, setSelectedKey] = useState<string | null>(null);
  const [searchValue, setSearchValue] = useState('');
  const [uploading, setUploading] = useState(false);
  const [deletingKey, setDeletingKey] = useState<string | null>(null);
  const [presigningKey, setPresigningKey] = useState<string | null>(null);
  const { items: objects, loading: objectsLoading, refresh } = useBucketObjects(selectedBucket);

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

  const filteredObjects = useMemo(() => {
    const keyword = searchValue.trim().toLowerCase();
    if (!keyword) {
      return objects;
    }

    return objects.filter((item) =>
      [item.key, item.content_type, item.cache_control ?? '', item.etag ?? ''].some((field) =>
        field.toLowerCase().includes(keyword),
      ),
    );
  }, [objects, searchValue]);

  useEffect(() => {
    if (requestedKey && filteredObjects.some((item) => item.key === requestedKey) && selectedKey !== requestedKey) {
      setSelectedKey(requestedKey);
      return;
    }
    if (!selectedKey || !filteredObjects.some((item) => item.key === selectedKey)) {
      setSelectedKey(filteredObjects[0]?.key ?? null);
    }
  }, [filteredObjects, requestedKey, selectedKey]);

  const selectedObject = useMemo(
    () => filteredObjects.find((item) => item.key === selectedKey) ?? filteredObjects[0] ?? null,
    [filteredObjects, selectedKey],
  );
  const { item: objectDetail, loading: objectDetailLoading, refresh: refreshObjectDetail } = useObjectDetail(
    selectedBucket,
    selectedObject?.key ?? null,
  );
  const inspectorObject = objectDetail ?? selectedObject;

  const syncSearchParams = (nextBucket: string | null, nextKey?: string | null, nextQuery?: string) => {
    const params = new URLSearchParams();
    if (nextBucket) {
      params.set('bucket', nextBucket);
    }
    if (nextKey) {
      params.set('key', nextKey);
    }
    if (nextQuery && nextQuery.trim()) {
      params.set('q', nextQuery.trim());
    }
    setSearchParams(params, { replace: true });
  };

  const handleSelectBucket = (value: string) => {
    setSelectedBucket(value);
    setSelectedKey(null);
    syncSearchParams(value, null, searchValue);
  };

  const handleFileUpload = async (event: ChangeEvent<HTMLInputElement>) => {
    if (!selectedBucket) {
      return;
    }
    const file = event.target.files?.[0];
    if (!file) {
      return;
    }

    setUploading(true);
    try {
      const uploaded = await uploadObject(selectedBucket, file);
      message.success(`Uploaded ${uploaded.key}`);
      await refresh();
      setSelectedKey(uploaded.key);
      syncSearchParams(selectedBucket, uploaded.key, searchValue);
    } catch (error) {
      message.error(normalizeApiError(error, 'Failed to upload object'));
    } finally {
      setUploading(false);
      event.target.value = '';
    }
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
      await refresh();
      syncSearchParams(selectedBucket, null, searchValue);
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

  const breadcrumbItems = selectedObject
    ? selectedObject.key.split('/').map((item) => ({ title: item }))
    : selectedBucket
      ? [{ title: selectedBucket }]
      : [{ title: 'No bucket selected' }];

  return (
    <ConsoleShell
      showHeaderSearch={false}
      actions={
        <>
          <input hidden onChange={handleFileUpload} ref={fileInputRef} type="file" />
          <Button
            disabled={!selectedBucket}
            icon={<UploadOutlined />}
            loading={uploading}
            onClick={() => fileInputRef.current?.click()}
            type="primary"
          >
            Add object
          </Button>
        </>
      }
    >
      <div className="workspace-stack">
        <div className="path-strip">
          <Breadcrumb items={breadcrumbItems} />
          <Select
            className="bucket-select"
            loading={bucketsLoading}
            onChange={handleSelectBucket}
            options={buckets.map((bucket) => ({ label: bucket.name, value: bucket.name }))}
            placeholder="Select bucket"
            value={selectedBucket ?? undefined}
          />
        </div>

        <div className="workspace-grid workspace-grid-main">
          <Section
            flush
            title="Objects"
            extra={
              <Input
                allowClear
                className="section-search"
                onChange={(event) => {
                  const nextValue = event.target.value;
                  setSearchValue(nextValue);
                  syncSearchParams(selectedBucket, null, nextValue);
                }}
                placeholder="Search current path"
                prefix={<SearchOutlined />}
                value={searchValue}
              />
            }
          >
            <Table
              columns={objectColumns}
              dataSource={filteredObjects}
              loading={objectsLoading}
              locale={{
                emptyText: selectedBucket ? (
                  <Empty description="No objects in this bucket yet" image={Empty.PRESENTED_IMAGE_SIMPLE} />
                ) : (
                  <Empty description="Create a bucket first" image={Empty.PRESENTED_IMAGE_SIMPLE} />
                ),
              }}
              onRow={(record) => ({
                onClick: () => {
                  setSelectedKey(record.key);
                  syncSearchParams(selectedBucket, record.key, searchValue);
                },
              })}
              pagination={false}
              rowClassName={(record) => (record.key === selectedObject?.key ? 'table-row-selected' : '')}
              rowKey="key"
              scroll={{ x: 880 }}
              size="small"
            />
          </Section>

          <Section
            title="Inspector"
            extra={
              selectedObject ? (
                <Space size={8}>
                  <Button loading={presigningKey === selectedObject.key} onClick={() => void handleCopyDownloadUrl()} size="small">
                    Copy download URL
                  </Button>
                  <Button loading={objectDetailLoading} onClick={() => void refreshObjectDetail()} size="small">
                    Refresh
                  </Button>
                  <Popconfirm
                    cancelText="Cancel"
                    okButtonProps={{ danger: true, loading: deletingKey === selectedObject.key }}
                    okText="Delete"
                    onConfirm={() => void handleDeleteObject()}
                    title={`Delete ${selectedObject.key}?`}
                  >
                    <Button danger loading={deletingKey === selectedObject.key} size="small">
                      Delete
                    </Button>
                  </Popconfirm>
                </Space>
              ) : null
            }
          >
            {inspectorObject ? (
              <Spin spinning={objectDetailLoading}>
                <Descriptions
                  column={1}
                  items={nodeSummaryToItems([
                    { label: 'Key', value: inspectorObject.key },
                    { label: 'Path', value: inspectorObject.path },
                    { label: 'Metadata path', value: inspectorObject.metadata_path || 'None' },
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
              </Spin>
            ) : objectsLoading ? (
              <Spin />
            ) : (
              <Empty description="Select an object to inspect" image={Empty.PRESENTED_IMAGE_SIMPLE} />
            )}
          </Section>
        </div>
      </div>
    </ConsoleShell>
  );
}
