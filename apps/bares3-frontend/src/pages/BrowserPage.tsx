import { useEffect, useMemo, useRef, useState } from 'react';
import type { ChangeEvent } from 'react';
import { SearchOutlined, UploadOutlined } from '@ant-design/icons';
import { Breadcrumb, Button, Descriptions, Empty, Input, Select, Spin, Table, message } from 'antd';
import { uploadObject } from '../api';
import { ConsoleShell } from '../components/ConsoleShell';
import { Section } from '../components/Section';
import { useBucketObjects } from '../hooks/useBucketObjects';
import { useBucketsData } from '../hooks/useBucketsData';
import { objectColumns } from '../tables';
import { formatBytes, formatDateTime, nodeSummaryToItems, normalizeApiError } from '../utils';

export function BrowserPage() {
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const { items: buckets, loading: bucketsLoading } = useBucketsData();
  const [selectedBucket, setSelectedBucket] = useState<string | null>(null);
  const [selectedKey, setSelectedKey] = useState<string | null>(null);
  const [searchValue, setSearchValue] = useState('');
  const [uploading, setUploading] = useState(false);
  const { items: objects, loading: objectsLoading, refresh } = useBucketObjects(selectedBucket);

  useEffect(() => {
    if (!selectedBucket && buckets.length > 0) {
      setSelectedBucket(buckets[0].name);
    }
    if (selectedBucket && !buckets.some((item) => item.name === selectedBucket)) {
      setSelectedBucket(buckets[0]?.name ?? null);
    }
  }, [buckets, selectedBucket]);

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
    if (!selectedKey || !filteredObjects.some((item) => item.key === selectedKey)) {
      setSelectedKey(filteredObjects[0]?.key ?? null);
    }
  }, [filteredObjects, selectedKey]);

  const selectedObject = useMemo(
    () => filteredObjects.find((item) => item.key === selectedKey) ?? filteredObjects[0] ?? null,
    [filteredObjects, selectedKey],
  );

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
    } catch (error) {
      message.error(normalizeApiError(error, 'Failed to upload object'));
    } finally {
      setUploading(false);
      event.target.value = '';
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
            onChange={(value) => {
              setSelectedBucket(value);
              setSelectedKey(null);
            }}
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
                onChange={(event) => setSearchValue(event.target.value)}
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
                onClick: () => setSelectedKey(record.key),
              })}
              pagination={false}
              rowClassName={(record) => (record.key === selectedObject?.key ? 'table-row-selected' : '')}
              rowKey="key"
              scroll={{ x: 880 }}
              size="small"
            />
          </Section>

          <Section title="Inspector">
            {selectedObject ? (
              <Descriptions
                column={1}
                items={nodeSummaryToItems([
                  { label: 'Key', value: selectedObject.key },
                  { label: 'Content-Type', value: selectedObject.content_type || 'application/octet-stream' },
                  { label: 'Size', value: formatBytes(selectedObject.size) },
                  { label: 'Cache-Control', value: selectedObject.cache_control || 'private' },
                  { label: 'ETag', value: selectedObject.etag || 'Not set' },
                  { label: 'Updated', value: formatDateTime(selectedObject.last_modified) },
                ])}
                size="small"
              />
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
