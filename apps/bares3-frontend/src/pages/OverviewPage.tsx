import { useState } from 'react';
import { UploadOutlined } from '@ant-design/icons';
import { Button, Descriptions, Empty, List, Skeleton, Table, Typography } from 'antd';
import { useNavigate } from 'react-router-dom';
import { nodeSummary as placeholderNodeSummary, overviewMetrics as placeholderOverviewMetrics } from '../console-data';
import { BucketCreateModal } from '../components/BucketCreateModal';
import { ConsoleShell } from '../components/ConsoleShell';
import { MetricStrip } from '../components/MetricStrip';
import { Section } from '../components/Section';
import { useAuditActivity } from '../hooks/useAuditActivity';
import { useBucketsData } from '../hooks/useBucketsData';
import { useRuntimeData } from '../hooks/useRuntimeData';
import { bucketColumns, bucketDisplayRows } from '../tables';
import type { ActivityDisplayItem, MetricItem } from '../types';
import { formatBytes, formatRelativeTime, nodeSummaryToItems, usagePercentLabel } from '../utils';

const { Text } = Typography;

export function OverviewPage() {
  const navigate = useNavigate();
  const { runtime, loading: runtimeLoading, refresh: refreshRuntime } = useRuntimeData();
  const { items: buckets, loading: bucketsLoading, refresh: refreshBuckets } = useBucketsData();
  const { items: auditEntries, loading: activityLoading, refresh: refreshActivity } = useAuditActivity();
  const [isBucketModalOpen, setIsBucketModalOpen] = useState(false);

  const metrics: MetricItem[] = placeholderOverviewMetrics.map((item) => ({ ...item }));
  metrics[0] = {
    ...metrics[0],
    value: String(runtime?.storage.bucket_count ?? buckets.length),
    detail: buckets.length > 0 ? `${buckets.length} bucket${buckets.length === 1 ? '' : 's'} currently configured` : 'No buckets created yet',
  };
  metrics[1] = {
    ...metrics[1],
    value:
      runtimeLoading || !runtime?.storage.max_bytes
        ? 'N/A'
        : usagePercentLabel(runtime.storage.used_bytes, runtime.storage.max_bytes),
    detail:
      runtime?.storage.max_bytes && runtime.storage.max_bytes > 0
        ? `${formatBytes(runtime.storage.used_bytes)} of ${formatBytes(runtime.storage.max_bytes)} allocated`
        : 'Set an instance limit in Settings',
  };
  metrics[2] = {
    ...metrics[2],
    value: runtimeLoading ? 'N/A' : String(runtime?.storage.active_link_count ?? 0),
    detail: 'Link analytics will appear when share management is connected',
  };

  const overviewBuckets = bucketDisplayRows(buckets);
  const activityItems: ActivityDisplayItem[] = auditEntries.map((entry) => ({
    key: `${entry.time}-${entry.action}-${entry.target ?? entry.title}`,
    title: entry.title,
    meta: [entry.detail, entry.actor ? `by ${entry.actor}` : '', entry.remote].filter(Boolean).join(' · '),
    time: formatRelativeTime(entry.time),
  }));

  const nodeItems = placeholderNodeSummary.map((item) => ({ ...item }));
  nodeItems[0] = { label: 'Console', value: runtime?.app.name ?? 'BareS3' };
  nodeItems[1] = { label: 'Endpoint', value: runtime?.storage.s3_base_url ?? 'N/A' };
  nodeItems[2] = { label: 'Region', value: runtime?.storage.region ?? 'N/A' };
  nodeItems[3] = { label: 'Write mode', value: 'temp file then atomic rename' };

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
          ) : (
            <Descriptions column={1} items={nodeSummaryToItems(nodeItems)} size="small" />
          )}
        </Section>
      </div>
    </ConsoleShell>
  );
}
