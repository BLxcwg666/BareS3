import { useState } from 'react';
import { Button, Descriptions, Empty, Skeleton } from 'antd';
import { ConsoleShell } from '../components/ConsoleShell';
import { Section } from '../components/Section';
import { StorageLimitModal } from '../components/StorageLimitModal';
import { useRuntimeData } from '../hooks/useRuntimeData';
import { formatBytes, nodeSummaryToItems, quotaLabel } from '../utils';

export function SettingsPage() {
  const { runtime, loading, refresh } = useRuntimeData();
  const [isStorageModalOpen, setIsStorageModalOpen] = useState(false);

  const groups = runtime
    ? [
        {
          title: 'Endpoint identity',
          items: [
            { label: 'Console name', value: runtime.app.name },
            { label: 'Environment', value: runtime.app.env },
            { label: 'Config source', value: runtime.config.used ? runtime.config.path : 'Defaults only (no config file loaded)' },
          ],
        },
        {
          title: 'Storage paths',
          items: [
            { label: 'Data directory', value: runtime.paths.data_dir },
            { label: 'Temp directory', value: runtime.paths.tmp_dir },
            { label: 'Log directory', value: runtime.paths.log_dir },
          ],
        },
        {
          title: 'Delivery defaults',
          items: [
            { label: 'S3 endpoint', value: runtime.storage.s3_base_url },
            { label: 'Public base URL', value: runtime.storage.public_base_url },
            { label: 'Metadata mode', value: runtime.storage.metadata_layout },
          ],
        },
      ]
    : [];

  const maxBytes = runtime?.storage.max_bytes ?? 0;
  const usedBytes = runtime?.storage.used_bytes ?? 0;
  const remainingValue =
    maxBytes > 0
      ? usedBytes > maxBytes
        ? `Over by ${formatBytes(usedBytes - maxBytes)}`
        : formatBytes(maxBytes - usedBytes)
      : 'Unlimited';

  const capacityItems = [
    { label: 'Instance limit', value: quotaLabel(maxBytes) },
    { label: 'Used now', value: formatBytes(usedBytes) },
    { label: 'Remaining', value: remainingValue },
  ];

  return (
    <ConsoleShell>
      {loading ? (
        <div className="workspace-stack">
          <Section title="Runtime">
            <Skeleton active paragraph={{ rows: 8 }} title={false} />
          </Section>
        </div>
      ) : !runtime ? (
        <div className="workspace-stack">
          <Section title="Runtime">
            <Empty description="Runtime settings are unavailable" image={Empty.PRESENTED_IMAGE_SIMPLE} />
          </Section>
        </div>
      ) : (
        <div className="workspace-stack">
          <StorageLimitModal
            currentMaxBytes={maxBytes}
            onCancel={() => setIsStorageModalOpen(false)}
            onSaved={() => refresh()}
            open={isStorageModalOpen}
          />

          <Section
            title="Capacity"
            note="Set the total space this BareS3 node is allowed to consume."
            extra={<Button onClick={() => setIsStorageModalOpen(true)}>Edit limit</Button>}
          >
            <Descriptions column={1} items={nodeSummaryToItems(capacityItems)} size="small" />
          </Section>

          <div className="workspace-grid workspace-grid-thirds">
            {groups.map((group) => (
              <Section key={group.title} title={group.title}>
                <Descriptions column={1} items={nodeSummaryToItems(group.items)} size="small" />
              </Section>
            ))}
          </div>
        </div>
      )}
    </ConsoleShell>
  );
}
