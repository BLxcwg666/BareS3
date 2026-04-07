import { useState } from 'react';
import { UploadOutlined } from '@ant-design/icons';
import { Button, Descriptions, Empty, List, Skeleton, Table, Typography } from 'antd';
import { useNavigate } from 'react-router-dom';
import { BucketCreateModal } from '../components/BucketCreateModal';
import { ConsoleShell } from '../components/ConsoleShell';
import { MetricStrip } from '../components/MetricStrip';
import { Section } from '../components/Section';
import { useAuditActivity } from '../hooks/useAuditActivity';
import { useBucketsData } from '../hooks/useBucketsData';
import { useRuntimeData } from '../hooks/useRuntimeData';
import { bucketColumns, bucketDisplayRows } from '../tables';
import type { ActivityDisplayItem, MetricItem } from '../types';
import { formatBytes, formatCount, formatRelativeTime, nodeSummaryToItems, usagePercentLabel } from '../utils';

const { Text } = Typography;

export function OverviewPage() {
  const navigate = useNavigate();
  const { runtime, loading: runtimeLoading, refresh: refreshRuntime } = useRuntimeData();
  const { items: buckets, loading: bucketsLoading, refresh: refreshBuckets } = useBucketsData();
  const { items: auditEntries, loading: activityLoading, refresh: refreshActivity } = useAuditActivity();
  const [isBucketModalOpen, setIsBucketModalOpen] = useState(false);
  const bucketCount = runtime?.storage.bucket_count ?? buckets.length;
  const totalObjects = buckets.reduce((sum, bucket) => sum + bucket.object_count, 0);

  const metrics: MetricItem[] = [
    {
      label: 'Buckets',
      value: runtimeLoading && bucketsLoading ? '--' : formatCount(bucketCount),
      detail: bucketCount > 0 ? `${formatCount(bucketCount)} bucket${bucketCount === 1 ? '' : 's'} currently configured` : 'No buckets created yet',
    },
    {
      label: 'Used',
      value: runtimeLoading ? '--' : runtime ? (runtime.storage.max_bytes > 0 ? usagePercentLabel(runtime.storage.used_bytes, runtime.storage.max_bytes) : formatBytes(runtime.storage.used_bytes)) : 'Unavailable',
      detail: runtime
        ? runtime.storage.max_bytes > 0
          ? `${formatBytes(runtime.storage.used_bytes)} of ${formatBytes(runtime.storage.max_bytes)} allocated`
          : `${formatBytes(runtime.storage.used_bytes)} used with no instance limit configured`
        : 'Runtime data is unavailable right now',
    },
    {
      label: 'Active links',
      value: runtimeLoading ? '--' : formatCount(runtime?.storage.active_link_count ?? 0),
      detail:
        runtime && runtime.storage.active_link_count > 0
          ? `${formatCount(runtime.storage.active_link_count)} share link${runtime.storage.active_link_count === 1 ? '' : 's'} currently live`
          : totalObjects > 0
            ? 'No active share links yet'
            : 'Objects appear here after uploads',
    },
  ];

  const overviewBuckets = bucketDisplayRows(buckets);
  const activityItems: ActivityDisplayItem[] = auditEntries.map((entry) => ({
    key: `${entry.time}-${entry.action}-${entry.target ?? entry.title}`,
    title: entry.title,
    meta: [entry.detail, entry.actor ? `by ${entry.actor}` : '', entry.remote].filter(Boolean).join(' · '),
    time: formatRelativeTime(entry.time),
  }));

  const nodeItems = runtime
    ? [
        { label: 'Console', value: runtime.app.name || 'BareS3' },
        { label: 'Environment', value: runtime.app.env || 'Unknown' },
        { label: 'Endpoint', value: runtime.storage.s3_base_url || 'Not configured' },
        { label: 'Region', value: runtime.storage.region || 'Not configured' },
      ]
    : [];

  return (
    <ConsoleShell
      actions={
        <>
          <Button onClick={() => setIsBucketModalOpen(true)}>New bucket</Button>
          <Button icon={<UploadOutlined />} onClick={() => navigate('/browser')} type="primary">
            Upload
          </Button>
        </>
      }
    >
      <div className="workspace-stack">
        <Section flush title="At a glance">
          <MetricStrip items={metrics} />
        </Section>

        <BucketCreateModal
          onCancel={() => setIsBucketModalOpen(false)}
          onCreated={() => Promise.all([refreshBuckets(), refreshRuntime(), refreshActivity()]).then(() => undefined)}
          open={isBucketModalOpen}
        />

        <div className="workspace-grid workspace-grid-main">
          <Section
            flush
            title="Buckets"
            extra={
              <Button onClick={() => navigate('/buckets')} size="small" type="link">
                Open
              </Button>
            }
          >
            <Table
              columns={bucketColumns(true)}
              dataSource={overviewBuckets}
              loading={bucketsLoading}
              locale={{ emptyText: <Empty description="No buckets yet" image={Empty.PRESENTED_IMAGE_SIMPLE} /> }}
              pagination={false}
              rowKey="name"
              scroll={{ x: 720 }}
              size="small"
            />
          </Section>

          <Section
            title="Activity"
            extra={
              <Button onClick={() => navigate('/audit')} size="small" type="link">
                Open
              </Button>
            }
          >
            {activityLoading ? (
              <Skeleton active paragraph={{ rows: 4 }} title={false} />
            ) : activityItems.length === 0 ? (
              <Empty description="No recent activity" image={Empty.PRESENTED_IMAGE_SIMPLE} />
            ) : (
              <List
                dataSource={activityItems}
                renderItem={(item: ActivityDisplayItem) => (
                  <List.Item key={item.key}>
                    <List.Item.Meta description={item.meta || undefined} title={item.title} />
                    <Text type="secondary">{item.time}</Text>
                  </List.Item>
                )}
              />
            )}
          </Section>
        </div>

        <Section title="Node">
          {runtimeLoading ? (
            <Skeleton active paragraph={{ rows: 4 }} title={false} />
          ) : runtime ? (
            <Descriptions column={1} items={nodeSummaryToItems(nodeItems)} size="small" />
          ) : (
            <Empty description="Runtime details are unavailable" image={Empty.PRESENTED_IMAGE_SIMPLE} />
          )}
        </Section>
      </div>
    </ConsoleShell>
  );
}
