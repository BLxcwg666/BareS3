import { useEffect, useMemo, useState } from 'react';
import { Alert, Button, Descriptions, Empty, InputNumber, Select, Space, Typography, message } from 'antd';
import { presignObject } from '../api';
import { ConsoleShell } from '../components/ConsoleShell';
import { Section } from '../components/Section';
import { useBucketObjects } from '../hooks/useBucketObjects';
import { useBucketsData } from '../hooks/useBucketsData';
import { useRuntimeData } from '../hooks/useRuntimeData';
import { copyText, formatDateTime, nodeSummaryToItems, normalizeApiError } from '../utils';

const { Text } = Typography;

function joinPublicObjectUrl(baseUrl: string, bucket: string, key: string) {
  const base = baseUrl.trim().replace(/\/+$/, '');
  const encodedKey = key
    .split('/')
    .map((part) => encodeURIComponent(part))
    .join('/');
  return `${base}/pub/${encodeURIComponent(bucket)}/${encodedKey}`;
}

export function LinksPage() {
  const { runtime } = useRuntimeData();
  const { items: buckets, loading: bucketsLoading } = useBucketsData();
  const [selectedBucket, setSelectedBucket] = useState<string | null>(null);
  const { items: objects, loading: objectsLoading } = useBucketObjects(selectedBucket);
  const [selectedKey, setSelectedKey] = useState<string | null>(null);
  const [expiresSeconds, setExpiresSeconds] = useState(900);
  const [signedUrl, setSignedUrl] = useState<string>('');
  const [signedExpiresAt, setSignedExpiresAt] = useState<string>('');
  const [generating, setGenerating] = useState(false);

  useEffect(() => {
    if (!selectedBucket && buckets.length > 0) {
      setSelectedBucket(buckets[0].name);
      return;
    }
    if (selectedBucket && !buckets.some((item) => item.name === selectedBucket)) {
      setSelectedBucket(buckets[0]?.name ?? null);
    }
  }, [buckets, selectedBucket]);

  useEffect(() => {
    if (!selectedKey || !objects.some((item) => item.key === selectedKey)) {
      setSelectedKey(objects[0]?.key ?? null);
    }
  }, [objects, selectedKey]);

  const selectedObject = useMemo(
    () => objects.find((item) => item.key === selectedKey) ?? null,
    [objects, selectedKey],
  );

  const publicUrl =
    runtime?.storage.public_base_url && selectedBucket && selectedObject
      ? joinPublicObjectUrl(runtime.storage.public_base_url, selectedBucket, selectedObject.key)
      : '';

  const handleCopyPublicUrl = async () => {
    if (!publicUrl) {
      return;
    }
    try {
      await copyText(publicUrl);
      message.success('Copied public URL');
    } catch (error) {
      message.error(normalizeApiError(error, 'Failed to copy public URL'));
    }
  };

  const handleGenerateSignedUrl = async () => {
    if (!selectedBucket || !selectedObject) {
      return;
    }

    setGenerating(true);
    try {
      const result = await presignObject(selectedBucket, selectedObject.key, expiresSeconds);
      setSignedUrl(result.url);
      setSignedExpiresAt(result.expires_at);
      message.success('Generated signed download URL');
    } catch (error) {
      message.error(normalizeApiError(error, 'Failed to generate signed URL'));
    } finally {
      setGenerating(false);
    }
  };

  const handleCopySignedUrl = async () => {
    if (!signedUrl) {
      return;
    }
    try {
      await copyText(signedUrl);
      message.success('Copied signed download URL');
    } catch (error) {
      message.error(normalizeApiError(error, 'Failed to copy signed URL'));
    }
  };

  return (
    <ConsoleShell showHeaderSearch={false}>
      <div className="workspace-stack">
        <Section title="Link target" note="Pick an object, then copy its public route or generate a signed download URL.">
          <Space direction="vertical" size={12} style={{ width: '100%' }}>
            <Space size={12} wrap>
              <div>
                <Text type="secondary">Bucket</Text>
                <Select
                  loading={bucketsLoading}
                  onChange={(value) => {
                    setSelectedBucket(value);
                    setSignedUrl('');
                    setSignedExpiresAt('');
                  }}
                  options={buckets.map((bucket) => ({ label: bucket.name, value: bucket.name }))}
                  placeholder="Select bucket"
                  style={{ minWidth: 220 }}
                  value={selectedBucket ?? undefined}
                />
              </div>

              <div>
                <Text type="secondary">Object</Text>
                <Select
                  loading={objectsLoading}
                  onChange={(value) => {
                    setSelectedKey(value);
                    setSignedUrl('');
                    setSignedExpiresAt('');
                  }}
                  options={objects.map((object) => ({ label: object.key, value: object.key }))}
                  placeholder="Select object"
                  showSearch
                  style={{ minWidth: 360 }}
                  value={selectedKey ?? undefined}
                />
              </div>

              <div>
                <Text type="secondary">Signed URL TTL</Text>
                <InputNumber
                  min={60}
                  onChange={(value) => setExpiresSeconds(typeof value === 'number' ? value : 900)}
                  step={60}
                  style={{ width: 140 }}
                  value={expiresSeconds}
                />
              </div>
            </Space>

            {selectedObject ? (
              <Descriptions
                column={1}
                items={nodeSummaryToItems([
                  { label: 'Key', value: selectedObject.key },
                  { label: 'Content-Type', value: selectedObject.content_type || 'application/octet-stream' },
                  { label: 'Size', value: `${selectedObject.size} B` },
                ])}
                size="small"
              />
            ) : (
              <Empty description="Pick an object to create links" image={Empty.PRESENTED_IMAGE_SIMPLE} />
            )}
          </Space>
        </Section>

        <Section
          title="Public route"
          extra={
            <Space size={8}>
              <Button disabled={!publicUrl} onClick={() => void handleCopyPublicUrl()} size="small">
                Copy
              </Button>
              <Button disabled={!publicUrl} href={publicUrl || undefined} rel="noreferrer" size="small" target="_blank">
                Open
              </Button>
            </Space>
          }
        >
          {publicUrl ? (
            <Descriptions
              column={1}
              items={nodeSummaryToItems([
                { label: 'URL', value: publicUrl },
                { label: 'Route type', value: 'Direct file service public route' },
              ])}
              size="small"
            />
          ) : (
            <Empty description="Select an object to preview its public route" image={Empty.PRESENTED_IMAGE_SIMPLE} />
          )}
        </Section>

        <Section
          title="Signed download"
          extra={
            <Space size={8}>
              <Button disabled={!selectedObject} loading={generating} onClick={() => void handleGenerateSignedUrl()} size="small" type="primary">
                Generate
              </Button>
              <Button disabled={!signedUrl} onClick={() => void handleCopySignedUrl()} size="small">
                Copy
              </Button>
              <Button disabled={!signedUrl} href={signedUrl || undefined} rel="noreferrer" size="small" target="_blank">
                Open
              </Button>
            </Space>
          }
        >
          {signedUrl ? (
            <Descriptions
              column={1}
              items={nodeSummaryToItems([
                { label: 'URL', value: signedUrl },
                { label: 'Expires at', value: formatDateTime(signedExpiresAt) },
                { label: 'Method', value: 'GET' },
              ])}
              size="small"
            />
          ) : (
            <Alert
              message="No signed URL generated yet"
              showIcon
              type="info"
              description="Select an object and click Generate to create a temporary download link."
            />
          )}
        </Section>
      </div>
    </ConsoleShell>
  );
}
